"""
Get Next Lead service: orchestrates fetching and assigning the next available lead.
Refactored from GetNextLeadView for clarity and testability.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone as std_utc
from typing import Optional

from django.db import transaction
from django.db.models import Q, F
from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response

try:
    from dateutil import parser as date_parser
except ImportError:
    date_parser = None

from .models import Record, EventLog, CallAttemptMatrix
from .serializers import RecordSerializer
from user_settings.models import UserSettings
from user_settings.routing import apply_routing_rule_to_queryset
from background_jobs.queue_service import get_queue_service
from background_jobs.models import JobType

logger = logging.getLogger(__name__)

# --- Constants ---

NEXT_CALL_READY_WHERE = """
    (
        COALESCE((data->>'call_attempts')::int, 0) = 0
        OR (
            data->>'next_call_at' IS NOT NULL
            AND data->>'next_call_at' != ''
            AND data->>'next_call_at' != 'null'
            AND (data->>'next_call_at')::timestamptz <= NOW()
        )
    )
"""

QUEUEABLE_WHERE = """
    (
        (data->>'assigned_to' IS NULL OR
         data->>'assigned_to' = '' OR
         data->>'assigned_to' = 'null' OR
         data->>'assigned_to' = 'None')
        OR UPPER(COALESCE(data->>'lead_stage','')) = 'IN_QUEUE'
        OR (
            UPPER(COALESCE(data->>'lead_stage','')) = 'SNOOZED'
            AND data->>'next_call_at' IS NOT NULL
            AND data->>'next_call_at' != ''
            AND data->>'next_call_at' != 'null'
            AND (data->>'next_call_at')::timestamptz <= NOW()
        )
    )
    AND (
        UPPER(COALESCE(data->>'lead_stage','')) IN ('IN_QUEUE', 'ASSIGNED', 'CALL_LATER', 'SCHEDULED')
        OR data->>'lead_stage' IS NULL
        OR data->>'lead_stage' = ''
        OR (
            UPPER(COALESCE(data->>'lead_stage','')) = 'SNOOZED'
            AND data->>'next_call_at' IS NOT NULL
            AND data->>'next_call_at' != ''
            AND data->>'next_call_at' != 'null'
            AND (data->>'next_call_at')::timestamptz <= NOW()
        )
    )
    AND (
        COALESCE((data->>'call_attempts')::int, 0) = 0
        OR (
            data->>'next_call_at' IS NOT NULL
            AND data->>'next_call_at' != ''
            AND data->>'next_call_at' != 'null'
            AND (data->>'next_call_at')::timestamptz <= NOW()
        )
    )
"""

AFFILIATED_EXTRA = """
    AND data->>'affiliated_party' IS NOT NULL
    AND data->>'affiliated_party' != ''
    AND data->>'affiliated_party' != 'null'
"""

ASSIGNED_STATUS = "assigned"


@dataclass
class GetNextLeadContext:
    """Request-scoped context for get-next-lead pipeline."""
    tenant: object
    user: object
    user_identifier: str
    user_uuid: Optional[uuid.UUID]
    tenant_membership: Optional[object]
    now: datetime
    now_iso: str
    debug_mode: bool
    eligible_lead_types: list = field(default_factory=list)
    eligible_lead_sources: list = field(default_factory=list)
    eligible_lead_statuses: list = field(default_factory=list)
    daily_limit: Optional[int] = None


# --- Helpers: affiliated party & call attempt matrix ---


def affiliated_party_aliases(lead_type: str) -> list[str]:
    """Normalize affiliated party type for filtering (typos/synonyms/case)."""
    aliases = {
        "in_trail": ["in_trial", "in_trail"],
        "in_trial": ["in_trial", "in_trail"],
    }
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


def should_exclude_lead_by_matrix(record, lead_data: dict, matrix: CallAttemptMatrix, now: datetime) -> tuple[bool, Optional[str]]:
    """Check if lead should be excluded by call attempt matrix. Returns (should_exclude, reason)."""
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
        days_since_creation = (now - record.created_at).days
        if days_since_creation > matrix.sla_days:
            return True, f"SLA ({matrix.sla_days} days) exceeded"
    next_call_at_str = lead_data.get("next_call_at")
    if next_call_at_str and call_attempts_int > 0:
        try:
            if date_parser:
                next_call_at = date_parser.parse(next_call_at_str)
            else:
                next_call_at = datetime.fromisoformat(next_call_at_str.replace("Z", "+00:00"))
            if now.tzinfo is None and next_call_at.tzinfo:
                next_call_at = next_call_at.replace(tzinfo=None)
            elif now.tzinfo and next_call_at.tzinfo is None:
                next_call_at = next_call_at.replace(tzinfo=std_utc.utc)
            hours_since = (now - next_call_at).total_seconds() / 3600
            if hours_since < matrix.min_time_between_calls_hours:
                return True, f"Minimum time between calls ({matrix.min_time_between_calls_hours} hours) not met"
        except Exception as e:
            logger.debug("[GetNextLead] Error parsing next_call_at for min time check: %s", e)
    return False, None


def lead_is_due_for_call(lead_data: dict, now: datetime) -> bool:
    """True if lead is eligible to be called now (cooldown respected)."""
    if not isinstance(lead_data, dict):
        return True
    try:
        call_attempts_int = int(lead_data.get("call_attempts") or 0)
    except (TypeError, ValueError):
        call_attempts_int = 0
    if call_attempts_int == 0:
        return True
    raw = lead_data.get("next_call_at")
    if raw is None or raw == "" or raw == "null":
        return False
    try:
        if isinstance(raw, datetime):
            next_call_at = raw
        elif date_parser:
            next_call_at = date_parser.parse(str(raw))
        else:
            next_call_at = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if now.tzinfo is None and next_call_at.tzinfo:
            next_call_at = next_call_at.replace(tzinfo=None)
        elif now.tzinfo and next_call_at.tzinfo is None:
            next_call_at = next_call_at.replace(tzinfo=std_utc.utc)
        return next_call_at <= now
    except Exception:
        return False


def order_by_score(qs, now_iso: Optional[str] = None):
    """Order queryset: expired snoozed first, then call_attempts asc, LIFO, score desc."""
    qs = qs.extra(where=[NEXT_CALL_READY_WHERE])
    select = {
        "lead_score": "COALESCE((data->>'lead_score')::float, -1)",
        "call_attempts_int": "COALESCE((data->>'call_attempts')::int, 0)",
    }
    if now_iso:
        select["is_expired_snoozed"] = """
            CASE
                WHEN data->>'lead_stage' = 'SNOOZED'
                AND data->>'next_call_at' IS NOT NULL
                AND data->>'next_call_at' != ''
                AND data->>'next_call_at' != 'null'
                AND (data->>'next_call_at')::timestamptz <= NOW()
                THEN 0
                ELSE 1
            END
        """
        qs = qs.extra(select=select).order_by(
            "is_expired_snoozed",
            "call_attempts_int",
            "-updated_at",
            F("lead_score").desc(nulls_last=True),
            "created_at",
            "id",
        )
    else:
        qs = qs.extra(select=select).order_by(
            "call_attempts_int",
            "-updated_at",
            F("lead_score").desc(nulls_last=True),
            "created_at",
            "id",
        )
    return qs


# --- Pipeline steps ---


def resolve_context(request) -> Optional[GetNextLeadContext]:
    """Step 0–1: Validate tenant/user and build context. Returns None if request should end with empty response."""
    tenant = getattr(request, "tenant", None)
    user = getattr(request, "user", None)
    if not tenant:
        logger.warning("[GetNextLead] Step 0: Abort - no tenant context available")
        return None
    user_identifier = getattr(user, "supabase_uid", None) or getattr(user, "email", None)
    if not user_identifier:
        logger.warning("[GetNextLead] Step 1: Abort - no user identifier available")
        return None
    logger.info("[GetNextLead] Step 1: user_identifier=%s", user_identifier)
    now = timezone.now()
    now_iso = now.isoformat()
    logger.info("[GetNextLead] Step 1 done: now=%s", now_iso)

    try:
        try:
            user_uuid = uuid.UUID(str(user_identifier))
        except (ValueError, AttributeError):
            from accounts.models import LegacyUser
            legacy = LegacyUser.objects.filter(tenant=tenant, email=user_identifier).first()
            user_uuid = legacy.uid if legacy and legacy.uid else None
        tenant_membership = None
        if user_uuid:
            from authz.models import TenantMembership
            tenant_membership = TenantMembership.objects.filter(
                tenant=tenant, user_id=uuid.UUID(str(user_uuid))
            ).first()

        eligible_lead_types = []
        eligible_lead_sources = []
        eligible_lead_statuses = []
        daily_limit = None
        if tenant_membership:
            any_setting = UserSettings.objects.filter(
                tenant=tenant, tenant_membership=tenant_membership
            ).first()
            daily_limit = getattr(any_setting, "daily_limit", None) if any_setting else None
            try:
                setting = UserSettings.objects.get(
                    tenant=tenant,
                    tenant_membership=tenant_membership,
                    key="LEAD_TYPE_ASSIGNMENT",
                )
                eligible_lead_types = setting.value if isinstance(setting.value, list) else []
                eligible_lead_sources = (
                    setting.lead_sources if isinstance(getattr(setting, "lead_sources", None), list) else []
                )
                try:
                    eligible_lead_statuses = (
                        setting.lead_statuses if isinstance(getattr(setting, "lead_statuses", None), list) else []
                    )
                except (AttributeError, Exception):
                    eligible_lead_statuses = []
            except UserSettings.DoesNotExist:
                pass
        else:
            if user_uuid:
                logger.warning("[GetNextLead] TenantMembership not found for user UUID %s", user_uuid)
            else:
                logger.warning("[GetNextLead] Could not resolve user UUID for %s", user_identifier)

        try:
            daily_limit_int = int(daily_limit) if daily_limit is not None else None
            if daily_limit_int is not None and daily_limit_int < 0:
                daily_limit_int = None
        except (TypeError, ValueError):
            daily_limit_int = None

        ctx = GetNextLeadContext(
            tenant=tenant,
            user=user,
            user_identifier=user_identifier,
            user_uuid=user_uuid,
            tenant_membership=tenant_membership,
            now=now,
            now_iso=now_iso,
            debug_mode=request.query_params.get("debug") in ("1", "true", "yes"),
            eligible_lead_types=eligible_lead_types,
            eligible_lead_sources=eligible_lead_sources,
            eligible_lead_statuses=eligible_lead_statuses,
            daily_limit=daily_limit_int,
        )
        logger.info(
            "[GetNextLead] Step 2 done: eligible_lead_types=%s eligible_lead_sources=%s eligible_lead_statuses=%s daily_limit=%s",
            ctx.eligible_lead_types,
            ctx.eligible_lead_sources or "(none)",
            ctx.eligible_lead_statuses or "(none)",
            ctx.daily_limit,
        )
        return ctx
    except Exception as e:
        logger.error("[GetNextLead] Step 2: Error fetching user settings: %s", str(e))
        return None


def apply_request_overrides(ctx: GetNextLeadContext, request) -> None:
    """Apply optional query params (party, lead_sources, lead_statuses) to context."""
    party_param = request.query_params.get("party") or request.query_params.get("lead_types")
    if party_param is not None:
        party_list = [s.strip() for s in str(party_param).split(",") if s.strip()]
        if party_list:
            ctx.eligible_lead_types = party_list
    lead_sources_param = request.query_params.get("lead_sources")
    if lead_sources_param is not None:
        ctx.eligible_lead_sources = [s.strip() for s in str(lead_sources_param).split(",") if s.strip()]
    lead_statuses_param = request.query_params.get("lead_statuses")
    if lead_statuses_param is not None:
        ctx.eligible_lead_statuses = [s.strip() for s in str(lead_statuses_param).split(",") if s.strip()]


def daily_limit_retry_response(ctx: GetNextLeadContext) -> Optional[Response]:
    """
    Step 2.5: If daily limit is reached, try to return a not-connected retry lead for this user.
    Returns a Response if one was returned, else None (caller continues with main queue).
    """
    if ctx.daily_limit is None or ctx.daily_limit < 0:
        return None
    if ctx.debug_mode:
        return None
    if timezone.is_aware(ctx.now):
        start_of_day = timezone.localtime(ctx.now).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_of_day = ctx.now.replace(hour=0, minute=0, second=0, microsecond=0)
    assigned_today = Record.objects.filter(tenant=ctx.tenant, entity_type="lead").extra(
        where=[
            """
            (
                (data->>'first_assigned_to' = %s
                AND data->>'first_assigned_at' IS NOT NULL
                AND data->>'first_assigned_at' != ''
                AND (data->>'first_assigned_at')::timestamptz >= %s)
            OR (
                (data->>'first_assigned_at' IS NULL OR data->>'first_assigned_at' = '')
                AND data->>'assigned_to' = %s
                AND updated_at >= %s
                AND COALESCE((data->>'call_attempts')::int, 0) = 0
            )
            """
        ],
        params=[ctx.user_identifier, start_of_day, ctx.user_identifier, start_of_day],
    ).count()
    logger.info(
        "[GetNextLead] Step 2.5: assigned_today=%d daily_limit_int=%s limit_reached=%s",
        assigned_today, ctx.daily_limit, assigned_today >= ctx.daily_limit,
    )
    if assigned_today < ctx.daily_limit:
        return None

    logger.info(
        "[GetNextLead] Step 2.5: Daily limit reached for user=%s (assigned_today=%d, daily_limit=%d).",
        ctx.user_identifier, assigned_today, ctx.daily_limit,
    )
    retry_candidate = Record.objects.filter(
        tenant=ctx.tenant,
        entity_type="lead",
        data__assigned_to=ctx.user_identifier,
    ).extra(
        select={
            "call_attempts_int": "COALESCE((data->>'call_attempts')::int, 0)",
            "lead_stage_norm": "UPPER(COALESCE(data->>'lead_stage',''))",
            "last_call_outcome_norm": "LOWER(COALESCE(data->>'last_call_outcome',''))",
        },
        where=[
            """
            COALESCE((data->>'call_attempts')::int, 0) >= 1
            AND COALESCE((data->>'call_attempts')::int, 0) <= 6
            AND (
                UPPER(COALESCE(data->>'lead_stage','')) = 'NOT_CONNECTED'
                OR LOWER(COALESCE(data->>'last_call_outcome','')) IN ('not connected', 'not_connected', 'notconnected')
            )
            AND (
                data->>'lead_stage' IN ('assigned', 'call_later', 'scheduled', 'SNOOZED', 'in_queue', 'NOT_CONNECTED')
                OR data->>'lead_stage' IS NULL
            )
            AND data->>'next_call_at' IS NOT NULL
            AND data->>'next_call_at' != ''
            AND data->>'next_call_at' != 'null'
            AND (data->>'next_call_at')::timestamptz <= NOW()
            """
        ],
    ).order_by("call_attempts_int", "updated_at", "id").first()

    if not retry_candidate:
        logger.info("[GetNextLead] END EMPTY: daily limit reached, no due retry leads.")
        return Response({}, status=status.HTTP_200_OK)

    serialized_data = RecordSerializer(retry_candidate).data
    lead_data = retry_candidate.data or {}
    flattened = _build_flattened_response(retry_candidate, serialized_data, lead_data)
    return Response(flattened, status=status.HTTP_200_OK)


def build_main_queue(ctx: GetNextLeadContext) -> tuple[object, int, int]:
    """
    Step 3: Build main queue (queueable + routing + eligible types/sources/statuses + exclusions).
    Returns (unassigned_queryset, unassigned_cnt, total_unassigned_cnt).
    """
    from django.db.models import Q
    logger.info("[GetNextLead] Step 3: Building main queue (queueable WHERE + affiliated_party + routing)...")
    base_where = QUEUEABLE_WHERE + AFFILIATED_EXTRA
    relaxed_where = QUEUEABLE_WHERE
    base_qs = Record.objects.filter(tenant=ctx.tenant, entity_type="lead").extra(where=[base_where])
    relaxed_base_qs = Record.objects.filter(tenant=ctx.tenant, entity_type="lead").extra(where=[relaxed_where])

    if ctx.user_uuid:
        base_qs = apply_routing_rule_to_queryset(
            base_qs, tenant=ctx.tenant, user_id=ctx.user_uuid, queue_type="lead"
        )
        relaxed_base_qs = apply_routing_rule_to_queryset(
            relaxed_base_qs, tenant=ctx.tenant, user_id=ctx.user_uuid, queue_type="lead"
        )

    affiliated_party_filter = Q()
    if not ctx.eligible_lead_types:
        unassigned = relaxed_base_qs
        base_qs = relaxed_base_qs
    else:
        for lead_type in ctx.eligible_lead_types:
            for alias in affiliated_party_aliases(lead_type):
                affiliated_party_filter |= Q(data__affiliated_party=alias)
        unassigned = base_qs.filter(affiliated_party_filter)
    if ctx.eligible_lead_sources:
        unassigned = unassigned.filter(data__lead_source__in=ctx.eligible_lead_sources)
    if ctx.eligible_lead_statuses:
        unassigned = unassigned.filter(data__lead_status__in=ctx.eligible_lead_statuses)

    # Leads in in_queue/SNOOZED (or NOT_CONNECTED / call_back_later) that already have an assignee
    # must only be pullable by that assignee. Exclude for other users. Use case-insensitive lead_stage.
    unassigned = unassigned.extra(
        where=["""
            NOT (
                UPPER(COALESCE(data->>'lead_stage','')) IN ('IN_QUEUE', 'SNOOZED')
                AND data->>'assigned_to' IS NOT NULL
                AND data->>'assigned_to' != ''
                AND data->>'assigned_to' != 'null'
                AND data->>'assigned_to' != 'None'
                AND data->>'assigned_to' != %s
            )
        """],
        params=[ctx.user_identifier],
    )

    call_attempt_matrices = {}
    if ctx.eligible_lead_types:
        matrices = CallAttemptMatrix.objects.filter(
            tenant=ctx.tenant, lead_type__in=ctx.eligible_lead_types
        )
        for matrix in matrices:
            call_attempt_matrices[matrix.lead_type] = matrix

    if call_attempt_matrices:
        exclusion_filters = Q()
        for lead_type, matrix in call_attempt_matrices.items():
            lead_type_filter = Q()
            for alias in affiliated_party_aliases(lead_type):
                lead_type_filter |= Q(data__affiliated_party=alias)
            exclusion_filters |= lead_type_filter & Q(data__call_attempts__gte=matrix.max_call_attempts)
            cutoff = ctx.now - timedelta(days=matrix.sla_days)
            exclusion_filters |= lead_type_filter & Q(created_at__lt=cutoff)
        if exclusion_filters:
            unassigned = unassigned.exclude(exclusion_filters)
        final_valid_ids = []
        for lead in unassigned[:1000]:
            lead_data = lead.data or {}
            lt = lead_data.get("affiliated_party")
            matrix = None
            for m_lt, m in call_attempt_matrices.items():
                if lt == m_lt or lt in affiliated_party_aliases(m_lt):
                    matrix = m
                    break
            if matrix:
                exclude, _ = should_exclude_lead_by_matrix(lead, lead_data, matrix, ctx.now)
                if exclude:
                    continue
            final_valid_ids.append(lead.id)
        if final_valid_ids:
            unassigned = unassigned.filter(id__in=final_valid_ids)
        else:
            unassigned = unassigned.none()

    unassigned_cnt = unassigned.count()
    total_unassigned_cnt = base_qs.count()
    logger.info(
        "[GetNextLead] Step 3 done: total_queueable=%d unassigned_matching_filters=%d user=%s",
        total_unassigned_cnt, unassigned_cnt, ctx.user_identifier,
    )

    if total_unassigned_cnt == 0:
        relaxed_where = """
            (
                (data->>'assigned_to' IS NULL OR data->>'assigned_to' = '' OR data->>'assigned_to' = 'null' OR data->>'assigned_to' = 'None')
                OR UPPER(COALESCE(data->>'lead_stage','')) = 'IN_QUEUE'
                OR (
                    UPPER(COALESCE(data->>'lead_stage','')) = 'SNOOZED'
                    AND data->>'next_call_at' IS NOT NULL AND data->>'next_call_at' != '' AND data->>'next_call_at' != 'null'
                    AND (data->>'next_call_at')::timestamptz <= %s
                )
            )
            AND (
                UPPER(COALESCE(data->>'lead_stage','')) IN ('IN_QUEUE', 'ASSIGNED', 'CALL_LATER', 'SCHEDULED')
                OR data->>'lead_stage' IS NULL OR data->>'lead_stage' = ''
                OR (
                    UPPER(COALESCE(data->>'lead_stage','')) = 'SNOOZED'
                    AND data->>'next_call_at' IS NOT NULL AND data->>'next_call_at' != '' AND data->>'next_call_at' != 'null'
                    AND (data->>'next_call_at')::timestamptz <= %s
                )
            )
            AND (
                COALESCE((data->>'call_attempts')::int, 0) = 0
                OR (
                    data->>'next_call_at' IS NOT NULL AND data->>'next_call_at' != '' AND data->>'next_call_at' != 'null'
                    AND (data->>'next_call_at')::timestamptz <= NOW()
                )
            )
        """
        relaxed_qs = Record.objects.filter(tenant=ctx.tenant, entity_type="lead").extra(
            where=[relaxed_where], params=[ctx.now_iso, ctx.now_iso]
        )
        if ctx.user_uuid:
            relaxed_qs = apply_routing_rule_to_queryset(
                relaxed_qs, tenant=ctx.tenant, user_id=ctx.user_uuid, queue_type="lead"
            )
        if ctx.eligible_lead_types:
            relaxed_unassigned = relaxed_qs.filter(affiliated_party_filter)
        else:
            relaxed_unassigned = relaxed_qs
        if ctx.eligible_lead_sources:
            relaxed_unassigned = relaxed_unassigned.filter(data__lead_source__in=ctx.eligible_lead_sources)
        if ctx.eligible_lead_statuses:
            relaxed_unassigned = relaxed_unassigned.filter(data__lead_status__in=ctx.eligible_lead_statuses)
        relaxed_cnt = relaxed_unassigned.count()
        if relaxed_cnt > 0:
            logger.info(
                "[GetNextLead] Step 3: Relaxed fallback found %d unassigned leads (intersection of party/source/status applied). Using as unassigned pool.",
                relaxed_cnt,
            )
            unassigned = relaxed_unassigned
            unassigned_cnt = relaxed_cnt
    return unassigned, unassigned_cnt, total_unassigned_cnt


def select_candidate(unassigned, ctx: GetNextLeadContext):
    """Step 4: Order by score and pick first due lead from top 50."""
    logger.info("[GetNextLead] Step 4: Ordering by score (call_attempts asc, score desc, LIFO)...")
    ordered = order_by_score(unassigned, ctx.now_iso)
    for i, c in enumerate(ordered[:50], 1):
        if lead_is_due_for_call(c.data, ctx.now):
            logger.info("[GetNextLead] Step 4: Selected candidate lead_id=%s (checked %d, call_attempts=%s)", c.id, i, (c.data or {}).get("call_attempts"))
            return c
        logger.info("[GetNextLead] Step 4: Skipping lead_id=%s (not due yet)", c.id)
    return None


def lock_assign_and_respond(unassigned, ctx: GetNextLeadContext) -> Response:
    """
    Step 5: Atomically select, lock, assign, log event, enqueue Mixpanel, build response.
    Selection and locking happen inside one transaction so no other request can take the same lead.
    """
    candidate_locked = None
    with transaction.atomic():
        ordered = order_by_score(unassigned, ctx.now_iso)
        to_check = list(ordered.select_for_update(skip_locked=True)[:50])
        for i, c in enumerate(to_check, 1):
            if lead_is_due_for_call(c.data, timezone.now()):
                candidate_locked = c
                logger.info(
                    "[GetNextLead] Step 5: Locked candidate lead_id=%s (checked %d, call_attempts=%s)",
                    c.id, i, (c.data or {}).get("call_attempts"),
                )
                break
            logger.debug("[GetNextLead] Step 5: Skipping lead_id=%s (not due yet)", c.id)

        if not candidate_locked:
            logger.info("[GetNextLead] END EMPTY: no lead locked (none due or all taken by other requests).")
            return Response({}, status=status.HTTP_200_OK)

        data = candidate_locked.data.copy() if candidate_locked.data else {}
        previous = data.get("assigned_to")
        is_fresh = previous in (None, "", "null", "None")
        data["assigned_to"] = ctx.user_identifier
        data["lead_stage"] = ASSIGNED_STATUS
        if data.get("call_attempts") in (None, "", "null"):
            data["call_attempts"] = 0
        call_attempts_int = int(data.get("call_attempts", 0) or 0)
        last_outcome = (data.get("last_call_outcome") or "").lower()
        lead_stage = (data.get("lead_stage") or "").upper()
        is_not_connected_retry = (
            call_attempts_int > 0
            or last_outcome in ("not connected", "not_connected", "notconnected", "call_back_later")
            or lead_stage in ("NOT_CONNECTED", "CALL_BACK_LATER", "IN_QUEUE")
        )
        if is_fresh and "first_assigned_at" not in data and not is_not_connected_retry:
            data["first_assigned_at"] = ctx.now.isoformat()
            data["first_assigned_to"] = ctx.user_identifier
        candidate_locked.data = data
        candidate_locked.updated_at = timezone.now()
        candidate_locked.save(update_fields=["data", "updated_at"])

        try:
            EventLog.objects.create(
                record=candidate_locked,
                tenant=ctx.tenant,
                event="lead.get_next_lead",
                payload={
                    "user_id": str(ctx.user_uuid) if ctx.user_uuid else ctx.user_identifier,
                    "lead_id": candidate_locked.id,
                    "record_id": candidate_locked.id,
                },
                timestamp=timezone.now(),
            )
        except Exception as e:
            logger.warning("[GetNextLead] Failed to log get_next_lead event: record_id=%s error=%s", candidate_locked.id, e)

        enqueue_mixpanel_jobs(ctx, candidate_locked)

    candidate_locked.refresh_from_db()
    candidate_locked = Record.objects.select_related().get(pk=candidate_locked.pk)
    serialized_data = RecordSerializer(candidate_locked).data
    lead_data = candidate_locked.data or {}
    flattened = _build_flattened_response(candidate_locked, serialized_data, lead_data)
    logger.info("[GetNextLead] END SUCCESS: assigned lead_id=%s to user=%s", candidate_locked.id, ctx.user_identifier)
    return Response(flattened, status=status.HTTP_200_OK)


def enqueue_mixpanel_jobs(ctx: GetNextLeadContext, record: Record) -> None:
    """Enqueue Mixpanel RM-assigned jobs (non-blocking)."""
    try:
        lead_data_dict = record.data or {}
        rm_email = getattr(ctx.user, "email", None)
        if not rm_email and ctx.tenant_membership:
            rm_email = getattr(ctx.tenant_membership, "email", None)
        praja_id = lead_data_dict.get("praja_id")
        queue_service = get_queue_service()
        tenant_id = str(ctx.tenant.id) if ctx.tenant else None
        if praja_id:
            try:
                if isinstance(praja_id, int):
                    mixpanel_user_id = str(praja_id)
                elif isinstance(praja_id, str):
                    cleaned = praja_id.upper().replace("PRAJA", "").replace("-", "").replace("_", "").strip()
                    mixpanel_user_id = cleaned if cleaned.isdigit() else praja_id
                else:
                    mixpanel_user_id = str(praja_id)
            except (ValueError, TypeError, AttributeError):
                mixpanel_user_id = str(praja_id) if praja_id else None
            if mixpanel_user_id:
                mixpanel_properties = {
                    "lead_id": record.id,
                    "lead_name": lead_data_dict.get("name", ""),
                    "lead_status": lead_data_dict.get("lead_stage", "assigned"),
                    "lead_score": lead_data_dict.get("lead_score"),
                    "lead_type": lead_data_dict.get("affiliated_party"),
                    "assigned_to": ctx.user_identifier,
                    "praja_id": praja_id,
                    "rm_email": rm_email,
                }
                mixpanel_properties.update(lead_data_dict)
                queue_service.enqueue_job(
                    job_type=JobType.SEND_MIXPANEL_EVENT,
                    payload={
                        "user_id": mixpanel_user_id,
                        "event_name": "pyro_crm_rm_assigned_backend",
                        "properties": mixpanel_properties,
                    },
                    tenant_id=tenant_id,
                )
        if praja_id and rm_email:
            try:
                queue_service.enqueue_job(
                    job_type=JobType.SEND_RM_ASSIGNED_EVENT,
                    payload={"praja_id": int(praja_id), "rm_email": rm_email},
                    tenant_id=tenant_id,
                )
            except (ValueError, TypeError):
                pass
    except Exception as e:
        logger.exception("[GetNextLead] Exception while enqueueing Mixpanel jobs: %s", e)


def _build_flattened_response(record, serialized_data: dict, lead_data: dict) -> dict:
    """Build the flattened response dict expected by the frontend."""
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
        "created_at": serialized_data.get("created_at"),
        "updated_at": serialized_data.get("updated_at"),
        "data": lead_data,
        "record": serialized_data,
    }


def build_debug_response(ctx: GetNextLeadContext, unassigned_cnt: int, total_unassigned_cnt: int, base_qs, unassigned) -> Response:
    """Build debug-mode response with pipeline counts and sample data."""
    from user_settings.routing import _get_active_rule
    rule = _get_active_rule(tenant=ctx.tenant, user_id=ctx.user_uuid, queue_type="lead") if ctx.user_uuid else None
    pre_routing_with_aff = Record.objects.filter(tenant=ctx.tenant, entity_type="lead").extra(where=[QUEUEABLE_WHERE + AFFILIATED_EXTRA]).count()
    pre_routing_no_aff = Record.objects.filter(tenant=ctx.tenant, entity_type="lead").extra(where=[QUEUEABLE_WHERE]).count()
    sample_leads = list(Record.objects.filter(tenant=ctx.tenant, entity_type="lead").values("id", "data")[:5])
    unassigned_minimal = Record.objects.filter(tenant=ctx.tenant, entity_type="lead").extra(
        where=["(data->>'assigned_to' IS NULL OR data->>'assigned_to' = '' OR data->>'assigned_to' = 'null' OR data->>'assigned_to' = 'None')"]
    )
    return Response({
        "debug": True,
        "user_identifier": ctx.user_identifier,
        "user_uuid": str(ctx.user_uuid) if ctx.user_uuid else None,
        "tenant_id": str(ctx.tenant.id) if ctx.tenant else None,
        "eligible_lead_types": ctx.eligible_lead_types,
        "daily_limit": ctx.daily_limit,
        "counts": {
            "total_leads_in_tenant": Record.objects.filter(tenant=ctx.tenant, entity_type="lead").count(),
            "unassigned_minimal": unassigned_minimal.count(),
            "queueable_with_affiliated_party": pre_routing_with_aff,
            "queueable_without_affiliated_party": pre_routing_no_aff,
            "after_routing_and_filter": unassigned_cnt,
            "base_qs_count": total_unassigned_cnt,
        },
        "routing_rule": {"has_rule": rule is not None, "conditions": rule.conditions if rule else None},
        "sample_leads_data": [
            {
                "id": s["id"],
                "lead_stage": (s.get("data") or {}).get("lead_stage"),
                "affiliated_party": (s.get("data") or {}).get("affiliated_party"),
                "assigned_to": (s.get("data") or {}).get("assigned_to"),
                "call_attempts": (s.get("data") or {}).get("call_attempts"),
                "next_call_at": (s.get("data") or {}).get("next_call_at"),
            }
            for s in sample_leads
        ],
        "distinct_lead_stages": list(Record.objects.filter(tenant=ctx.tenant, entity_type="lead").values_list("data__lead_stage", flat=True).distinct()[:20]),
        "distinct_affiliated_parties": list(Record.objects.filter(tenant=ctx.tenant, entity_type="lead").values_list("data__affiliated_party", flat=True).distinct()[:20]),
    }, status=status.HTTP_200_OK)


def get_next_lead(request) -> Response:
    """
    Main entry: run the get-next-lead pipeline and return the appropriate Response.
    """
    logger.info(
        "[GetNextLead] START request user=%s tenant=%s debug=%s",
        getattr(request.user, "email", getattr(request.user, "supabase_uid", None)),
        request.tenant.id if request.tenant else None,
        request.query_params.get("debug") in ("1", "true", "yes"),
    )
    ctx = resolve_context(request)
    if ctx is None:
        logger.info("[GetNextLead] END EMPTY: no tenant or no user identifier.")
        return Response({}, status=status.HTTP_200_OK)

    apply_request_overrides(ctx, request)
    ret = daily_limit_retry_response(ctx)
    if ret is not None:
        return ret

    unassigned, unassigned_cnt, total_unassigned_cnt = build_main_queue(ctx)
    if ctx.debug_mode:
        return build_debug_response(ctx, unassigned_cnt, total_unassigned_cnt, None, unassigned)

    if unassigned_cnt == 0:
        logger.info("[GetNextLead] END EMPTY: no unassigned leads in queue.")
        return Response({}, status=status.HTTP_200_OK)

    return lock_assign_and_respond(unassigned, ctx)
