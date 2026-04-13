"""
Lead assignment logic for GetNextLeadView.
Extracted for clarity and testability; view remains a thin HTTP layer.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from django.db.models import Q, F
from django.db import transaction
from django.utils import timezone

try:
    from dateutil import parser as date_parser
except ImportError:
    date_parser = None

from .models import Record, EventLog, CallAttemptMatrix
from user_settings.models import UserSettings
from user_settings.routing import apply_routing_rule_to_queryset
from support_ticket.services import MixpanelService, RMAssignedMixpanelService

logger = logging.getLogger(__name__)

ASSIGNED_STATUS = "assigned"

# SQL fragments for queueable leads (assignment + status + next_call_at)
QUEUEABLE_WHERE = """
    (
        (data->>'assigned_to' IS NULL OR data->>'assigned_to' = '' OR data->>'assigned_to' = 'null' OR data->>'assigned_to' = 'None')
        OR UPPER(COALESCE(data->>'lead_stage','')) = 'IN_QUEUE'
        OR (
            UPPER(COALESCE(data->>'lead_stage','')) = 'SNOOZED'
            AND data->>'next_call_at' IS NOT NULL AND data->>'next_call_at' != '' AND data->>'next_call_at' != 'null'
            AND (data->>'next_call_at')::timestamptz <= NOW()
        )
    )
    AND (
        UPPER(COALESCE(data->>'lead_stage','')) IN ('IN_QUEUE', 'ASSIGNED', 'CALL_LATER', 'SCHEDULED')
        OR data->>'lead_stage' IS NULL OR data->>'lead_stage' = ''
        OR (
            UPPER(COALESCE(data->>'lead_stage','')) = 'SNOOZED'
            AND data->>'next_call_at' IS NOT NULL AND data->>'next_call_at' != '' AND data->>'next_call_at' != 'null'
            AND (data->>'next_call_at')::timestamptz <= NOW()
        )
    )
    AND (
        COALESCE((data->>'call_attempts')::int, 0) = 0
        OR data->>'next_call_at' IS NULL OR data->>'next_call_at' = '' OR data->>'next_call_at' = 'null'
        OR (data->>'next_call_at')::timestamptz <= NOW()
    )
"""
AFFILIATED_EXTRA = """
    AND data->>'affiliated_party' IS NOT NULL AND data->>'affiliated_party' != '' AND data->>'affiliated_party' != 'null'
"""


@dataclass
class Eligibility:
    user_uuid: Optional[uuid.UUID]
    tenant_membership: Any
    eligible_lead_types: list
    eligible_lead_sources: list
    eligible_lead_statuses: list
    daily_limit: Optional[int]


def affiliated_party_aliases(lead_type: str) -> list[str]:
    """Normalize lead type for filtering (typos and case variants)."""
    aliases = {"in_trail": ["in_trial", "in_trail"], "in_trial": ["in_trial", "in_trail"]}
    base = aliases.get(lead_type, [lead_type])
    if isinstance(base, str):
        base = [base]
    seen = set()
    out = []
    for a in base:
        if a and a not in seen:
            seen.add(a)
            out.append(a)
        for v in (a.lower(), a.upper(), a.title()) if a else []:
            if v not in seen:
                seen.add(v)
                out.append(v)
    return out if out else [lead_type]


def get_call_attempt_matrix(tenant, lead_type: str) -> Optional[CallAttemptMatrix]:
    try:
        return CallAttemptMatrix.objects.get(tenant=tenant, lead_type=lead_type)
    except CallAttemptMatrix.DoesNotExist:
        return None


def should_exclude_lead_by_matrix(record, lead_data: dict, matrix: CallAttemptMatrix, now) -> tuple[bool, Optional[str]]:
    if not matrix:
        return False, None
    call_attempts = lead_data.get("call_attempts", 0)
    try:
        call_attempts_int = int(call_attempts) if call_attempts is not None else 0
    except (TypeError, ValueError):
        call_attempts_int = 0
    if call_attempts_int >= matrix.max_call_attempts:
        return True, f"Max call attempts ({matrix.max_call_attempts}) reached"
    if record and record.created_at:
        if (now - record.created_at).days > matrix.sla_days:
            return True, f"SLA ({matrix.sla_days} days) exceeded"
    next_call_at_str = lead_data.get("next_call_at")
    if next_call_at_str and call_attempts_int > 0:
        try:
            next_call_at = date_parser.parse(next_call_at_str) if date_parser else datetime.fromisoformat(next_call_at_str.replace("Z", "+00:00"))
            if now.tzinfo is None and next_call_at.tzinfo:
                next_call_at = next_call_at.replace(tzinfo=None)
            elif now.tzinfo and next_call_at.tzinfo is None:
                next_call_at = timezone.make_aware(next_call_at)
            if (now - next_call_at).total_seconds() / 3600 < matrix.min_time_between_calls_hours:
                return True, f"Minimum time between calls ({matrix.min_time_between_calls_hours} hours) not met"
        except Exception as e:
            logger.debug("[GetNextLead] Error parsing next_call_at: %s", e)
    return False, None


def order_queryset_by_lead_score(qs, now_iso=None):
    """Order by: expired snoozed first, then call_attempts asc, updated_at desc, lead_score desc."""
    select = {
        "lead_score": "COALESCE((data->>'lead_score')::float, -1)",
        "call_attempts_int": "COALESCE((data->>'call_attempts')::int, 0)",
    }
    order = ["call_attempts_int", "-updated_at", F("lead_score").desc(nulls_last=True), "created_at", "id"]
    if now_iso:
        select["is_expired_snoozed"] = """
            CASE WHEN data->>'lead_stage' = 'SNOOZED' AND data->>'next_call_at' IS NOT NULL
            AND data->>'next_call_at' != '' AND data->>'next_call_at' != 'null'
            AND (data->>'next_call_at')::timestamptz <= NOW() THEN 0 ELSE 1 END
        """
        order = ["is_expired_snoozed"] + order
    return qs.extra(select=select).order_by(*order)


def get_eligibility(tenant, user_identifier: str) -> Eligibility:
    """Resolve user UUID, tenant membership, and eligible lead types/sources/statuses and daily limit."""
    user_uuid = None
    tenant_membership = None
    eligible_lead_types = eligible_lead_sources = eligible_lead_statuses = []
    daily_limit = None
    try:
        try:
            user_uuid = uuid.UUID(str(user_identifier))
        except (ValueError, AttributeError):
            from accounts.models import LegacyUser
            legacy = LegacyUser.objects.filter(tenant=tenant, email=user_identifier).first()
            user_uuid = legacy.uid if legacy and legacy.uid else None
        if user_uuid:
            from authz.models import TenantMembership
            tenant_membership = TenantMembership.objects.filter(tenant=tenant, user_id=user_uuid).first()
            if tenant_membership:
                any_setting = UserSettings.objects.filter(tenant=tenant, tenant_membership=tenant_membership).first()
                daily_limit = getattr(any_setting, "daily_limit", None) if any_setting else None
                try:
                    setting = UserSettings.objects.get(tenant=tenant, tenant_membership=tenant_membership, key="LEAD_TYPE_ASSIGNMENT")
                    eligible_lead_types = setting.value if isinstance(setting.value, list) else []
                    eligible_lead_sources = setting.lead_sources if isinstance(getattr(setting, "lead_sources", None), list) else []
                    try:
                        eligible_lead_statuses = setting.lead_statuses if isinstance(getattr(setting, "lead_statuses", None), list) else []
                    except (AttributeError, Exception):
                        eligible_lead_statuses = []
                except UserSettings.DoesNotExist:
                    eligible_lead_types = eligible_lead_sources = eligible_lead_statuses = []
    except Exception as e:
        logger.error("[GetNextLead] Error fetching user settings: %s", e)
        eligible_lead_types = eligible_lead_sources = eligible_lead_statuses = []
        daily_limit = None
    return Eligibility(
        user_uuid=user_uuid,
        tenant_membership=tenant_membership,
        eligible_lead_types=eligible_lead_types or [],
        eligible_lead_sources=eligible_lead_sources or [],
        eligible_lead_statuses=eligible_lead_statuses or [],
        daily_limit=daily_limit,
    )


def apply_query_param_overrides(eligibility: Eligibility, query_params) -> Eligibility:
    """Override eligibility from request query params (party, lead_sources, lead_statuses)."""
    types_override = query_params.get("party") or query_params.get("lead_types")
    if types_override:
        lst = [s.strip() for s in str(types_override).split(",") if s.strip()]
        if lst:
            eligibility = Eligibility(eligibility.user_uuid, eligibility.tenant_membership, lst, eligibility.eligible_lead_sources, eligibility.eligible_lead_statuses, eligibility.daily_limit)
    sources = query_params.get("lead_sources")
    if sources is not None:
        lst = [s.strip() for s in str(sources).split(",") if s.strip()]
        eligibility = Eligibility(eligibility.user_uuid, eligibility.tenant_membership, eligibility.eligible_lead_types, lst, eligibility.eligible_lead_statuses, eligibility.daily_limit)
    statuses = query_params.get("lead_statuses")
    if statuses is not None:
        lst = [s.strip() for s in str(statuses).split(",") if s.strip()]
        eligibility = Eligibility(eligibility.user_uuid, eligibility.tenant_membership, eligibility.eligible_lead_types, eligibility.eligible_lead_sources, lst, eligibility.daily_limit)
    return eligibility


def assigned_today_count(tenant, user_identifier: str, start_of_day) -> int:
    return Record.objects.filter(tenant=tenant, entity_type="lead").extra(
        where=[
            """
            ( (data->>'first_assigned_to' = %s AND data->>'first_assigned_at' IS NOT NULL AND data->>'first_assigned_at' != ''
              AND (data->>'first_assigned_at')::timestamptz >= %s)
            OR ( (data->>'first_assigned_at' IS NULL OR data->>'first_assigned_at' = '')
              AND data->>'assigned_to' = %s AND updated_at >= %s
              AND COALESCE((data->>'call_attempts')::int, 0) = 0 )
            """
        ],
        params=[user_identifier, start_of_day, user_identifier, start_of_day],
    ).count()


def get_retry_lead_candidate(tenant, user_identifier: str) -> Optional[Record]:
    """Return one 'not connected' retry lead already assigned to user (for daily-limit fallback)."""
    where_sql = """
        COALESCE((data->>'call_attempts')::int, 0) >= 1 AND COALESCE((data->>'call_attempts')::int, 0) <= 3
        AND ( UPPER(COALESCE(data->>'lead_stage','')) = 'NOT_CONNECTED'
              OR LOWER(COALESCE(data->>'last_call_outcome','')) IN ('not connected', 'not_connected', 'notconnected') )
        AND ( data->>'lead_stage' IN ('assigned', 'call_later', 'scheduled', 'SNOOZED', 'in_queue', 'NOT_CONNECTED') OR data->>'lead_stage' IS NULL )
    """
    return (
        Record.objects.filter(tenant=tenant, entity_type="lead", data__assigned_to=user_identifier)
        .extra(
            select={"call_attempts_int": "COALESCE((data->>'call_attempts')::int, 0)"},
            where=[where_sql],
        )
        .order_by("call_attempts_int", "updated_at", "id")
        .first()
    )


def build_queueable_querysets(tenant, user_uuid, eligibility: Eligibility):
    """Return (base_qs, relaxed_base_qs) for queueable leads; apply routing if user_uuid."""
    base_qs = Record.objects.filter(tenant=tenant, entity_type="lead").extra(where=[QUEUEABLE_WHERE + AFFILIATED_EXTRA])
    relaxed_base_qs = Record.objects.filter(tenant=tenant, entity_type="lead").extra(where=[QUEUEABLE_WHERE])
    if user_uuid:
        base_qs = apply_routing_rule_to_queryset(base_qs, tenant=tenant, user_id=user_uuid, queue_type="lead")
        relaxed_base_qs = apply_routing_rule_to_queryset(relaxed_base_qs, tenant=tenant, user_id=user_uuid, queue_type="lead")
    return base_qs, relaxed_base_qs


def filter_by_eligibility(base_qs, relaxed_base_qs, eligibility: Eligibility, call_attempt_matrices: dict, now):
    """Filter queueable queryset by lead types, sources, statuses, and call attempt matrix."""
    if not eligibility.eligible_lead_types:
        unassigned = relaxed_base_qs
    else:
        aff_filter = Q()
        for lead_type in eligibility.eligible_lead_types:
            for alias in affiliated_party_aliases(lead_type):
                aff_filter |= Q(data__affiliated_party=alias)
        unassigned = base_qs.filter(aff_filter)
    if eligibility.eligible_lead_sources:
        unassigned = unassigned.filter(data__lead_source__in=eligibility.eligible_lead_sources)
    if eligibility.eligible_lead_statuses:
        unassigned = unassigned.filter(data__lead_status__in=eligibility.eligible_lead_statuses)
    if call_attempt_matrices:
        exclusion_filters = Q()
        for lead_type, matrix in call_attempt_matrices.items():
            lead_type_filter = Q()
            for alias in affiliated_party_aliases(lead_type):
                lead_type_filter |= Q(data__affiliated_party=alias)
            exclusion_filters |= lead_type_filter & Q(data__call_attempts__gte=matrix.max_call_attempts)
            cutoff = now - timedelta(days=matrix.sla_days)
            exclusion_filters |= lead_type_filter & Q(created_at__lt=cutoff)
        if exclusion_filters:
            unassigned = unassigned.exclude(exclusion_filters)
        valid_ids = []
        for lead in unassigned[:1000]:
            lead_data = lead.data or {}
            matrix = None
            for lt, m in call_attempt_matrices.items():
                if lead_data.get("affiliated_party") == lt or lead_data.get("affiliated_party") in affiliated_party_aliases(lt):
                    matrix = m
                    break
            if matrix:
                exclude, _ = should_exclude_lead_by_matrix(lead, lead_data, matrix, now)
                if exclude:
                    continue
            valid_ids.append(lead.id)
        if valid_ids:
            unassigned = unassigned.filter(id__in=valid_ids)
        else:
            unassigned = unassigned.none()
    return unassigned


def assign_lead_to_user(record, user_identifier: str, user_uuid, tenant, user, tenant_membership, now):
    """Atomically set assigned_to, first_assigned_*, log event, and send Mixpanel. Modifies record in place and saves."""
    data = (record.data or {}).copy()
    previous = data.get("assigned_to")
    is_fresh = previous in (None, "", "null", "None")
    data["assigned_to"] = user_identifier
    data["lead_stage"] = ASSIGNED_STATUS
    if data.get("call_attempts") in (None, "", "null"):
        data["call_attempts"] = 0
    call_attempts_int = int(data.get("call_attempts", 0) or 0) if data.get("call_attempts") is not None else 0
    last_outcome = (data.get("last_call_outcome") or "").lower()
    lead_stage_upper = (data.get("lead_stage") or "").upper()
    is_retry = (
        call_attempts_int > 0
        or last_outcome in ("not connected", "not_connected", "notconnected")
        or last_outcome == "call_back_later"
        or lead_stage_upper in ("NOT_CONNECTED", "CALL_BACK_LATER", "IN_QUEUE")
    )
    if is_fresh and "first_assigned_at" not in data and not is_retry:
        data["first_assigned_at"] = now.isoformat()
        data["first_assigned_to"] = user_identifier
    record.data = data
    record.updated_at = timezone.now()
    record.save(update_fields=["data", "updated_at"])
    try:
        EventLog.objects.create(record=record, tenant=tenant, event="lead.get_next_lead", payload={"user_id": str(user_uuid) if user_uuid else user_identifier, "lead_id": record.id, "record_id": record.id}, timestamp=timezone.now())
    except Exception as e:
        logger.warning("[GetNextLead] Failed to log event: %s", e)
    _send_mixpanel_events(record, user_identifier, user, tenant_membership)


def _send_mixpanel_events(record, user_identifier: str, user, tenant_membership):
    lead_data_dict = record.data or {}
    lead_name = lead_data_dict.get("name", "") if isinstance(lead_data_dict, dict) else ""
    praja_id = lead_data_dict.get("praja_id")
    rm_email = getattr(user, "email", None) or (getattr(tenant_membership, "email", None) if tenant_membership else None)
    try:
        if praja_id:
            mixpanel_user_id = str(praja_id)
            if isinstance(praja_id, str):
                cleaned = praja_id.upper().replace("PRAJA", "").replace("-", "").replace("_", "").strip()
                mixpanel_user_id = cleaned if cleaned.isdigit() else praja_id
            props = {"lead_id": record.id, "lead_name": lead_name, "lead_status": lead_data_dict.get("lead_stage", "assigned"), "lead_score": lead_data_dict.get("lead_score"), "lead_type": lead_data_dict.get("affiliated_party"), "assigned_to": user_identifier, "praja_id": praja_id, "rm_email": rm_email}
            props.update(lead_data_dict)
            MixpanelService().send_to_mixpanel_sync(mixpanel_user_id, "pyro_crm_rm_assigned_backend", props)
        if praja_id and rm_email:
            try:
                RMAssignedMixpanelService().send_to_mixpanel_sync(int(praja_id), rm_email)
            except (ValueError, TypeError):
                pass
    except Exception as e:
        logger.error("[GetNextLead] Mixpanel error: %s", e)


def flatten_lead_response(record) -> dict:
    """Build flattened lead payload for API (same shape as GetNextLeadView / GetMyCurrentLeadView)."""
    from .serializers import RecordSerializer
    serialized = RecordSerializer(record).data
    lead_data = record.data or {}
    return {
        "id": record.id,
        "name": (record.data or {}).get("name", "") if isinstance(record.data, dict) else "",
        "phone_no": lead_data.get("phone_number", ""),
        "praja_id": lead_data.get("praja_id"),
        "lead_status": lead_data.get("lead_stage") or "",
        "lead_score": lead_data.get("lead_score"),
        "lead_type": lead_data.get("affiliated_party") or lead_data.get("poster"),
        "assigned_to": lead_data.get("assigned_to"),
        "attempt_count": lead_data.get("call_attempts", 0),
        "last_call_outcome": lead_data.get("last_call_outcome"),
        "next_call_at": lead_data.get("next_call_at"),
        "do_not_call": lead_data.get("do_not_call", False),
        "resolved_at": lead_data.get("closure_time"),
        "premium_poster_count": lead_data.get("premium_poster_count"),
        "package_to_pitch": lead_data.get("package_to_pitch"),
        "last_active_date_time": lead_data.get("last_active_date_time"),
        "latest_remarks": lead_data.get("latest_remarks"),
        "lead_description": lead_data.get("lead_description"),
        "affiliated_party": lead_data.get("affiliated_party"),
        "rm_dashboard": lead_data.get("rm_dashboard"),
        "user_profile_link": lead_data.get("user_profile_link"),
        "whatsapp_link": lead_data.get("whatsapp_link"),
        "lead_source": lead_data.get("lead_source"),
        "created_at": serialized.get("created_at"),
        "updated_at": serialized.get("updated_at"),
        "data": lead_data,
        "record": serialized,
    }


def get_next_lead(tenant, user, query_params) -> tuple[dict, int]:
    """
    Orchestrate next-lead assignment. Returns (response_body, status_code).
    response_body: {} when no lead; debug dict when debug=True; else flattened lead dict.
    """
    from django.utils import timezone as tz
    now = tz.now()
    now_iso = now.isoformat()
    user_identifier = getattr(user, "supabase_uid", None) or getattr(user, "email", None)
    if not user_identifier:
        return {}, 200
    debug_mode = query_params.get("debug") in ("1", "true", "yes")
    eligibility = get_eligibility(tenant, user_identifier)
    eligibility = apply_query_param_overrides(eligibility, query_params)

    # Daily limit
    if eligibility.daily_limit is not None:
        try:
            daily_limit_int = int(eligibility.daily_limit)
        except (TypeError, ValueError):
            daily_limit_int = None
        if daily_limit_int is not None and daily_limit_int >= 0:
            if tz.is_aware(now):
                start_of_day = tz.localtime(now).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
            assigned_today = assigned_today_count(tenant, user_identifier, start_of_day)
            if assigned_today >= daily_limit_int and not debug_mode:
                retry = get_retry_lead_candidate(tenant, user_identifier)
                if retry:
                    return flatten_lead_response(retry), 200
                return {}, 200

    base_qs, relaxed_base_qs = build_queueable_querysets(tenant, eligibility.user_uuid, eligibility)
    call_attempt_matrices = {}
    if eligibility.eligible_lead_types:
        for m in CallAttemptMatrix.objects.filter(tenant=tenant, lead_type__in=eligibility.eligible_lead_types):
            call_attempt_matrices[m.lead_type] = m
    unassigned = filter_by_eligibility(base_qs, relaxed_base_qs, eligibility, call_attempt_matrices, now)
    unassigned_cnt = unassigned.count()
    total_unassigned_cnt = base_qs.count()

    # Relaxed fallback when no queueable leads have affiliated_party: requery without that requirement
    if unassigned_cnt == 0 and total_unassigned_cnt == 0:
        unassigned = filter_by_eligibility(relaxed_base_qs, relaxed_base_qs, eligibility, call_attempt_matrices, now)
        unassigned_cnt = unassigned.count()

    if debug_mode:
        from user_settings.routing import _get_active_rule
        rule = _get_active_rule(tenant=tenant, user_id=eligibility.user_uuid, queue_type="lead") if eligibility.user_uuid else None
        unassigned_minimal = Record.objects.filter(tenant=tenant, entity_type="lead").extra(where=["(data->>'assigned_to' IS NULL OR data->>'assigned_to' = '' OR data->>'assigned_to' = 'null' OR data->>'assigned_to' = 'None')"])
        pre_aff = Record.objects.filter(tenant=tenant, entity_type="lead").extra(where=[QUEUEABLE_WHERE + AFFILIATED_EXTRA]).count()
        pre_no_aff = Record.objects.filter(tenant=tenant, entity_type="lead").extra(where=[QUEUEABLE_WHERE]).count()
        sample = list(Record.objects.filter(tenant=tenant, entity_type="lead").values("id", "data")[:5])
        return {
            "debug": True,
            "user_identifier": user_identifier,
            "user_uuid": str(eligibility.user_uuid) if eligibility.user_uuid else None,
            "tenant_id": str(tenant.id) if tenant else None,
            "eligible_lead_types": eligibility.eligible_lead_types,
            "daily_limit": eligibility.daily_limit,
            "counts": {
                "total_leads_in_tenant": Record.objects.filter(tenant=tenant, entity_type="lead").count(),
                "unassigned_minimal": unassigned_minimal.count(),
                "queueable_with_affiliated_party": pre_aff,
                "queueable_without_affiliated_party": pre_no_aff,
                "after_routing_and_filter": unassigned_cnt,
                "base_qs_count": total_unassigned_cnt,
            },
            "routing_rule": {"has_rule": rule is not None, "conditions": rule.conditions if rule else None},
            "sample_leads_data": [{"id": s["id"], "lead_stage": (s.get("data") or {}).get("lead_stage"), "affiliated_party": (s.get("data") or {}).get("affiliated_party"), "assigned_to": (s.get("data") or {}).get("assigned_to"), "call_attempts": (s.get("data") or {}).get("call_attempts"), "next_call_at": (s.get("data") or {}).get("next_call_at")} for s in sample],
            "distinct_lead_stages": list(Record.objects.filter(tenant=tenant, entity_type="lead").values_list("data__lead_stage", flat=True).distinct()[:20]),
            "distinct_affiliated_parties": list(Record.objects.filter(tenant=tenant, entity_type="lead").values_list("data__affiliated_party", flat=True).distinct()[:20]),
        }, 200

    candidate = order_queryset_by_lead_score(unassigned, now_iso).first()
    if not candidate:
        return {}, 200

    with transaction.atomic():
        candidate_locked = Record.objects.select_for_update(skip_locked=True).filter(pk=candidate.pk).first()
        if not candidate_locked:
            return {}, 200
        assign_lead_to_user(candidate_locked, user_identifier, eligibility.user_uuid, tenant, user, eligibility.tenant_membership, now)

    candidate_locked.refresh_from_db()
    candidate_locked = Record.objects.get(pk=candidate_locked.pk)
    return flatten_lead_response(candidate_locked), 200
