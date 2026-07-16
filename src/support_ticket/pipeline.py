"""
Support-ticket get-next pipeline (sales-style buckets).

Order:
1. cse_fresh — unassigned Open (ST and other equal weight; dual daily limits)
2. cse_nc_today — assigned NC, first_assigned today (IST), call-ready
3. cse_wip_today — assigned WIP, first_assigned today (IST), call-ready (90m)
4. cse_nc_yesterday — assigned NC, first_assigned yesterday (IST), call-ready
5. cse_wip_yesterday — assigned WIP, first_assigned yesterday (IST), call-ready (90m)
"""

from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from crm_records.lead_pipeline.bucket_resolver import BucketResolver
from crm_records.lead_pipeline.candidate_selector import CandidateSelector
from crm_records.lead_pipeline.daily_limit import DailyLimitChecker
from crm_records.lead_pipeline.pull_strategy import PullStrategyApplier
from crm_records.lead_pipeline.queryset_builder import BucketQuerysetBuilder
from crm_records.lead_pipeline.user_resolver import UserResolver
from crm_records.models import Record
from support_ticket.buckets import (
    DAILY_LIMIT_KIND_DUAL,
    DAILY_LIMIT_KIND_OTHER,
    DAILY_LIMIT_KIND_SELF_TRIAL,
)
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from support_ticket.ticket_types import q_record_self_trial
from user_settings.models import Group, TenantMemberSetting
from user_settings.services import (
    USER_KV_GROUP_ID_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
    coerce_kv_int,
)

logger = logging.getLogger(__name__)

_EXPIRED_SUPPORT_TICKET_TYPES = frozenset({
    "Trial Expired",
    "Premium Expired",
    "trial_expired",
    "premium_expired",
})


def _exclude_expired_support_ticket_types(qs):
    expired = list(_EXPIRED_SUPPORT_TICKET_TYPES)
    return qs.exclude(
        Q(data__support_ticket_type__in=expired) | Q(data__poster__in=expired)
    )


def _apply_support_record_group_filters(qs, *, tenant, request_user):
    from authz.models import TenantMembership

    membership = TenantMembership.objects.filter(
        tenant=tenant,
        user_id=request_user.supabase_uid,
    ).first()
    if not membership:
        return qs

    group_row = TenantMemberSetting.objects.filter(
        tenant=tenant,
        tenant_membership=membership,
        key=USER_KV_GROUP_ID_KEY,
    ).first()
    group_id = group_row.value if group_row else None
    if not isinstance(group_id, int):
        return qs

    group = Group.objects.filter(tenant=tenant, id=group_id).first()
    group_data = group.group_data if group and isinstance(group.group_data, dict) else {}
    states = group_data.get("states") if isinstance(group_data.get("states"), list) else []
    ticket_types = group_data.get("support_ticket_types")
    if not isinstance(ticket_types, list):
        ticket_types = group_data.get("posters") if isinstance(group_data.get("posters"), list) else []

    if states:
        qs = qs.filter(data__state__in=states)
    if ticket_types:
        qs = qs.filter(
            Q(data__support_ticket_type__in=ticket_types)
            | Q(data__poster__in=ticket_types)
        )
    return qs


def _support_daily_limits(tenant, membership) -> tuple[Optional[int], Optional[int]]:
    if not membership:
        return None, None
    rows = TenantMemberSetting.objects.filter(
        tenant=tenant,
        tenant_membership=membership,
        key__in=[
            USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
            USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY,
        ],
    )
    by_key = {r.key: coerce_kv_int(r.value) for r in rows}
    return (
        by_key.get(USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY),
        by_key.get(USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY),
    )


class SupportTicketPipeline:
    """Bucket-based get-next for support tickets."""

    def __init__(self):
        self.user_resolver = UserResolver()
        self.bucket_resolver = BucketResolver()
        self.queryset_builder = BucketQuerysetBuilder()
        self.strategy_applier = PullStrategyApplier()
        self.daily_limit_checker = DailyLimitChecker()
        self.candidate_selector = CandidateSelector()

    def get_next(self, *, tenant, request_user, user_email: str, debug: bool = False) -> Optional[Record]:
        now = timezone.now()
        now_iso = now.isoformat()

        resolved_user = self.user_resolver.resolve(tenant, request_user)
        user_identifier = resolved_user.identifier
        if not user_identifier:
            logger.warning("[SupportTicketPipeline] abort: no user_identifier")
            return None

        try:
            user_uuid = UUID(str(user_identifier))
        except (ValueError, TypeError):
            logger.error("[SupportTicketPipeline] invalid user_identifier=%s", user_identifier)
            return None

        st_limit, other_limit = _support_daily_limits(tenant, resolved_user.membership)
        st_status = self.daily_limit_checker.check(
            tenant=tenant,
            user_identifier=user_identifier,
            daily_limit=st_limit,
            now=now,
            debug=debug,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            type_q=q_record_self_trial(),
        )
        other_status = self.daily_limit_checker.check(
            tenant=tenant,
            user_identifier=user_identifier,
            daily_limit=other_limit,
            now=now,
            debug=debug,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            type_q=~q_record_self_trial(),
        )

        assignments = self.bucket_resolver.resolve(
            tenant, resolved_user, entity_type=SUPPORT_TICKET_ENTITY_TYPE
        )
        if not assignments:
            logger.warning(
                "[SupportTicketPipeline] no support buckets for tenant=%s — seed cse buckets",
                getattr(tenant, "id", None),
            )
            return None

        for assignment in assignments:
            fc = dict(assignment.filter_conditions or {})
            if fc.get("daily_limit_applies"):
                kind = fc.get("daily_limit_kind") or DAILY_LIMIT_KIND_OTHER
                if kind == DAILY_LIMIT_KIND_DUAL:
                    if st_status.is_reached and other_status.is_reached and not debug:
                        logger.info(
                            "[SupportTicketPipeline] skip bucket=%s dual daily limits "
                            "reached st=%s/%s other=%s/%s",
                            assignment.bucket_slug,
                            st_status.assigned_today,
                            st_status.daily_limit,
                            other_status.assigned_today,
                            other_status.daily_limit,
                        )
                        continue
                else:
                    status = (
                        st_status
                        if kind == DAILY_LIMIT_KIND_SELF_TRIAL
                        else other_status
                    )
                    if status.is_reached and not debug:
                        logger.info(
                            "[SupportTicketPipeline] skip bucket=%s daily_limit_kind=%s "
                            "assigned_today=%s limit=%s",
                            assignment.bucket_slug,
                            kind,
                            status.assigned_today,
                            status.daily_limit,
                        )
                        continue

            scopes = [fc.get("assigned_scope", "unassigned")]
            if fallback := fc.get("fallback_assigned_scope"):
                scopes.append(fallback)

            for scope in scopes:
                fc_copy = {**fc, "assigned_scope": scope}
                qs = self.queryset_builder.build(
                    tenant=tenant,
                    bucket_filter_conditions=fc_copy,
                    user_identifier=user_identifier,
                    user_uuid=user_uuid,
                    eligible_lead_types=[],
                    eligible_lead_sources=[],
                    eligible_lead_statuses=[],
                    eligible_states=[],
                    debug=debug,
                    entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                )
                qs = _exclude_expired_support_ticket_types(qs)
                qs = _apply_support_record_group_filters(
                    qs, tenant=tenant, request_user=request_user
                )

                # Dual fresh pool: drop ticket types whose daily limit is already hit.
                if (
                    fc.get("daily_limit_applies")
                    and (fc.get("daily_limit_kind") or "") == DAILY_LIMIT_KIND_DUAL
                    and not debug
                ):
                    if st_status.is_reached:
                        st_ids = set(
                            qs.filter(q_record_self_trial()).values_list("id", flat=True)
                        )
                        if st_ids:
                            qs = qs.exclude(id__in=st_ids)
                    if other_status.is_reached:
                        qs = qs.filter(q_record_self_trial())

                qs = self.strategy_applier.apply(
                    qs=qs, strategy=assignment.pull_strategy, now_iso=now_iso
                )

                for candidate in qs[:50]:
                    if not self.candidate_selector.is_due_for_call(candidate.data, now):
                        continue
                    record = self._assign(
                        candidate_pk=candidate.pk,
                        user_uuid=user_uuid,
                        user_email=user_email,
                        user_identifier=user_identifier,
                        now=now,
                    )
                    if record:
                        logger.info(
                            "[SupportTicketPipeline] assigned record_id=%s bucket=%s user=%s",
                            record.pk,
                            assignment.bucket_slug,
                            user_identifier,
                        )
                        return record

        return None

    def _assign(
        self,
        *,
        candidate_pk: int,
        user_uuid: UUID,
        user_email: str,
        user_identifier: str,
        now,
    ) -> Optional[Record]:
        with transaction.atomic():
            locked = (
                Record.objects.select_for_update(skip_locked=True)
                .filter(pk=candidate_pk, entity_type=SUPPORT_TICKET_ENTITY_TYPE)
                .first()
            )
            if not locked:
                return None

            if not self.candidate_selector.is_due_for_call(locked.data, timezone.now()):
                return None

            data = dict(locked.data or {})
            previous = data.get("assigned_to")
            is_fresh = previous in (None, "", "null", "None")

            if not is_fresh and str(previous) != str(user_identifier):
                return None

            data["assigned_to"] = str(user_uuid)
            data["cse_name"] = user_email
            if "call_attempts" not in data or data.get("call_attempts") in (None, "", "null"):
                data["call_attempts"] = 0

            if is_fresh and not data.get("first_assigned_at"):
                data["first_assigned_at"] = now.isoformat()
                data["first_assigned_to"] = str(user_uuid)

            locked.data = data
            locked.updated_at = timezone.now()
            locked.save(update_fields=["data", "updated_at"])
            return locked
