from __future__ import annotations

import logging
from typing import Optional

from django.db import transaction
from django.utils import timezone

from crm_records.lead_assignment_tracking import merge_first_assignment_today_anchor
from crm_records.lead_pipeline.bucket_resolver import BucketResolver
from crm_records.lead_pipeline.candidate_selector import CandidateSelector
from crm_records.lead_pipeline.daily_limit import DailyLimitChecker
from crm_records.lead_pipeline.lead_assigner import LeadAssigner
from crm_records.lead_pipeline.matrix_filter import CallAttemptMatrixFilter
from crm_records.lead_pipeline.pull_strategy import PullStrategyApplier
from crm_records.lead_pipeline.queryset_builder import BucketQuerysetBuilder
from crm_records.lead_pipeline.user_resolver import UserResolver
from crm_records.models import Record
from user_settings.routing import apply_routing_rule_to_queryset

logger = logging.getLogger(__name__)


class LeadPipeline:
    """
    Sales-lead lead retrieval + assignment flow, in bucket priority order.
    """

    def __init__(self):
        self.user_resolver = UserResolver()
        self.bucket_resolver = BucketResolver()
        self.queryset_builder = BucketQuerysetBuilder()
        self.strategy_applier = PullStrategyApplier()
        self.daily_limit_checker = DailyLimitChecker()
        self.matrix_filter = CallAttemptMatrixFilter()
        self.candidate_selector = CandidateSelector()
        self.assigner = LeadAssigner(candidate_selector=self.candidate_selector)

    def get_next(self, *, tenant, request_user, debug: bool = False) -> Optional[Record]:
        now = timezone.now()
        now_iso = now.isoformat()

        resolved_user = self.user_resolver.resolve(tenant, request_user)
        user_identifier = resolved_user.identifier
        user_uuid = resolved_user.uuid

        if not user_identifier:
            return None

        limit_status = None
        if resolved_user.daily_limit is not None:
            limit_status = self.daily_limit_checker.check(
                tenant=tenant,
                user_identifier=user_identifier,
                daily_limit=resolved_user.daily_limit,
                now=now,
                debug=debug,
            )

        assignments = self.bucket_resolver.resolve(tenant, resolved_user)

        for assignment in assignments:
            fc = dict(assignment.filter_conditions or {})

            if fc.get("daily_limit_applies") and limit_status and limit_status.is_reached and not debug:
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
                    eligible_lead_types=resolved_user.eligible_lead_types,
                    eligible_lead_sources=resolved_user.eligible_lead_sources,
                    eligible_lead_statuses=resolved_user.eligible_lead_statuses,
                )

                # qs = self.matrix_filter.apply(
                #     qs=qs,
                #     tenant=tenant,
                #     eligible_lead_types=resolved_user.eligible_lead_types,
                #     now=now,
                # )

                qs = self.strategy_applier.apply(qs=qs, strategy=assignment.pull_strategy, now_iso=now_iso)

                for c in qs[:50]:
                    if self.candidate_selector.is_due_for_call(c.data, now):
                        result = self.assigner.assign_main_queue(
                            candidate_pk=c.pk,
                            tenant=tenant,
                            user=request_user,
                            tenant_membership=resolved_user.membership,
                            user_identifier=user_identifier,
                            user_uuid=user_uuid,
                            now=now,
                        )
                        if result:
                            return result.record

        if limit_status and limit_status.is_reached and not debug:
            retry_candidate = self._daily_limit_retry_fallback(
                tenant=tenant,
                resolved_user=resolved_user,
                user_uuid=user_uuid,
                user_identifier=user_identifier,
                now=now,
            )
            if retry_candidate:
                return retry_candidate

        return None

    def _daily_limit_retry_fallback(
        self,
        *,
        tenant,
        resolved_user,
        user_uuid,
        user_identifier: str,
        now,
    ) -> Optional[Record]:
        """
        Ported from GetNextLeadView Step 2.5 fallback (assigned-to-me first, then unassigned).
        """
        eligible_lead_types = resolved_user.eligible_lead_types
        eligible_lead_sources = resolved_user.eligible_lead_sources
        eligible_lead_statuses = resolved_user.eligible_lead_statuses

        # Retry ordering for sales leads:
        # min call_attempts -> max lead_score -> LIFO (updated_at desc).
        retry_strategy = {
            "order_by": "score_desc",
            "include_snoozed_due": False,
            "ignore_score_for_sources": [],
            "tiebreaker": "lifo",
        }

        # 1) Assigned-to-me retry candidate (legacy code does NOT apply lead filters here).
        assigned_retry_qs = Record.objects.filter(tenant=tenant, entity_type="lead", data__assigned_to=user_identifier).extra(
            select={
                "call_attempts_int": "COALESCE((data->>'call_attempts')::int, 0)",
                "lead_stage_norm": "UPPER(COALESCE(data->>'lead_stage',''))",
                "last_call_outcome_norm": "LOWER(COALESCE(data->>'last_call_outcome',''))",
            },
            where=[
                """
                COALESCE((data->>'call_attempts')::int, 0) >= 1
                AND COALESCE((data->>'call_attempts')::int, 0) <= 6
                AND UPPER(COALESCE(data->>'lead_stage','')) IN ('NOT_CONNECTED', 'IN_QUEUE')
                AND (data->>'next_call_at') IS NOT NULL
                AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
                AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                AND (data->>'next_call_at')::timestamptz <= NOW()
                """,
            ],
        )

        assigned_retry_qs = self.strategy_applier.apply(qs=assigned_retry_qs, strategy=retry_strategy, now_iso=now.isoformat())
        retry_candidate = assigned_retry_qs.first()

        if retry_candidate:
            return retry_candidate

        # 2) Unassigned retry candidate (legacy code DOES apply eligible filters + routing here).
        _unassigned_not_connected_where = """
            (
                (data->>'assigned_to') IS NULL
                OR TRIM(COALESCE(data->>'assigned_to', '')) = ''
                OR LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) IN ('null', 'none')
            )
            AND COALESCE((data->>'call_attempts')::int, 0) >= 1
            AND COALESCE((data->>'call_attempts')::int, 0) <= 6
            AND UPPER(COALESCE(data->>'lead_stage','')) IN ('NOT_CONNECTED', 'IN_QUEUE')
            AND (data->>'next_call_at') IS NOT NULL
            AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
            AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
            AND (data->>'next_call_at')::timestamptz <= NOW()
        """

        unassigned_retry_qs = Record.objects.filter(tenant=tenant, entity_type="lead").extra(
            select={"call_attempts_int": "COALESCE((data->>'call_attempts')::int, 0)"},
            where=[_unassigned_not_connected_where],
        )

        if eligible_lead_types:
            unassigned_retry_qs = unassigned_retry_qs.filter(data__affiliated_party__in=eligible_lead_types)
        if eligible_lead_sources:
            unassigned_retry_qs = unassigned_retry_qs.filter(data__lead_source__in=eligible_lead_sources)
        if eligible_lead_statuses:
            unassigned_retry_qs = unassigned_retry_qs.filter(data__lead_status__in=eligible_lead_statuses)

        if user_uuid:
            unassigned_retry_qs = apply_routing_rule_to_queryset(
                unassigned_retry_qs,
                tenant=tenant,
                user_id=user_uuid,
                queue_type="lead",
            )

        unassigned_retry_qs = self.strategy_applier.apply(qs=unassigned_retry_qs, strategy=retry_strategy, now_iso=now.isoformat())
        unassigned_retry_candidate = unassigned_retry_qs.first()

        if not unassigned_retry_candidate:
            return None

        # Assign the unassigned retry lead to the user (legacy code assigns it).
        with transaction.atomic():
            candidate_locked = (
                Record.objects.select_for_update(skip_locked=True).filter(pk=unassigned_retry_candidate.pk).first()
            )
            if not candidate_locked:
                return None

            data = candidate_locked.data or {}
            data = data.copy() if isinstance(data, dict) else {}
            data["assigned_to"] = user_identifier
            data["lead_stage"] = LeadAssigner.ASSIGNED_STATUS
            if "call_attempts" not in data or data.get("call_attempts") in (None, "", "null"):
                data["call_attempts"] = 0

            merge_first_assignment_today_anchor(data, timezone.now())

            candidate_locked.data = data
            candidate_locked.updated_at = timezone.now()
            candidate_locked.save(update_fields=["data", "updated_at"])
            return candidate_locked

