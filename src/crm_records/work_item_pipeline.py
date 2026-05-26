"""Unified pull pipeline for self-trial leads + support tickets (work items).

Ordering: **day-first, bucket-second**.  For each day (Day0, Day-1, …) all
buckets are tried in priority order before moving to the next older day.
Within each day+bucket combination records are pulled LIFO (``-created_at``).
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from django.utils import timezone

from crm_records.lead_pipeline.pipeline import LeadPipeline
from crm_records.lead_pipeline.bucket_resolver import BucketAssignmentView
from crm_records.models import Record

logger = logging.getLogger(__name__)

WORK_ITEM_BUCKET_SLUG_PREFIXES: Tuple[str, ...] = ("self_trial_", "support_")
SELF_TRIAL_BUCKET_SLUG_PREFIX = "self_trial_"
_DEFAULT_DAY_TZ = "Asia/Kolkata"


def is_work_item_bucket_slug(slug: str) -> bool:
    return any(slug.startswith(prefix) for prefix in WORK_ITEM_BUCKET_SLUG_PREFIXES)


def is_self_trial_bucket_slug(slug: str) -> bool:
    return slug.startswith(SELF_TRIAL_BUCKET_SLUG_PREFIX)


def ui_profile_for_record(record: Record) -> str:
    if record.entity_type == "support_ticket":
        return "support"
    data = record.data or {}
    lead_source = (data.get("lead_source") or "").upper().replace("_", " ")
    lead_status = (data.get("lead_status") or "").upper().replace("_", " ")
    if lead_source == "SELF TRIAL" or lead_status == "SELF TRIAL":
        return "self_trial"
    return "sales"


def _tenant_label(tenant) -> str:
    return str(getattr(tenant, "slug", None) or getattr(tenant, "id", "") or tenant)


class WorkItemPipeline(LeadPipeline):
    """Day-first work-item pipeline.

    For each day (newest first) all work-item buckets are tried in priority
    order.  Only after every bucket is exhausted for a given day does the
    pipeline move to the previous day.  This guarantees that a Day-0 support
    ticket is always served before a Day-1 self-trial lead, regardless of
    bucket priority.
    """

    _MAX_DAY_LOOKBACK = 30
    _CONSECUTIVE_EMPTY_DAYS_STOP = 7

    def _filter_work_item_assignments(
        self, assignments: List[BucketAssignmentView]
    ) -> List[BucketAssignmentView]:
        return [a for a in assignments if is_work_item_bucket_slug(a.bucket_slug)]

    @staticmethod
    def _resolve_day_tz(assignments: List[BucketAssignmentView]) -> str:
        for a in assignments:
            strategy = a.pull_strategy if isinstance(a.pull_strategy, dict) else {}
            raw = strategy.get("day_timezone")
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
        return _DEFAULT_DAY_TZ

    def get_next(self, *, tenant, request_user, debug: bool = False) -> Optional[Record]:
        now = timezone.now()
        now_iso = now.isoformat()

        resolved_user = self.user_resolver.resolve(tenant, request_user)
        if not resolved_user.identifier:
            logger.warning(
                "[WorkItemPipeline] abort: no user_identifier tenant=%s",
                _tenant_label(tenant),
            )
            return None

        user_identifier = resolved_user.identifier
        user_uuid = resolved_user.uuid

        assignments = self._filter_work_item_assignments(
            self.bucket_resolver.resolve(tenant, resolved_user)
        )
        if not assignments:
            logger.info(
                "[WorkItemPipeline] no work-item bucket assignments tenant=%s user=%s",
                _tenant_label(tenant), user_identifier,
            )
            return None

        bucket_order = [(a.bucket_slug, a.priority) for a in assignments]
        logger.info(
            "[WorkItemPipeline] start tenant=%s user=%s buckets=%s",
            _tenant_label(tenant), user_identifier, bucket_order,
        )

        tz_name = self._resolve_day_tz(assignments)
        safe_tz = tz_name.replace("'", "''")
        today = now.astimezone(ZoneInfo(tz_name)).date()
        consecutive_empty = 0

        for day_offset in range(self._MAX_DAY_LOOKBACK):
            target_date = today - timedelta(days=day_offset)
            day_where = (
                f"(timezone('{safe_tz}', created_at))::date = '{target_date.isoformat()}'"
            )

            any_candidates_in_day = False

            for assignment in assignments:
                fc = dict(assignment.filter_conditions or {})
                scopes = [fc.get("assigned_scope", "unassigned")]
                if fallback := fc.get("fallback_assigned_scope"):
                    scopes.append(fallback)

                if debug:
                    logger.info(
                        "[WorkItemPipeline] trying bucket=%s priority=%s day=%s scopes=%s",
                        assignment.bucket_slug, assignment.priority,
                        target_date, scopes,
                    )

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
                        eligible_states=resolved_user.eligible_states,
                        debug=debug,
                    )
                    qs = qs.extra(where=[day_where])
                    qs = self.strategy_applier.apply(
                        qs=qs, strategy=assignment.pull_strategy, now_iso=now_iso,
                    )

                    due_seen = 0
                    for c in qs[:50]:
                        any_candidates_in_day = True
                        if not self.candidate_selector.is_due_for_call(c.data, now):
                            continue
                        due_seen += 1
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
                            logger.info(
                                "[WorkItemPipeline] assigned record_id=%s bucket=%s "
                                "scope=%s day=%s day_offset=%d user=%s",
                                result.record.pk, assignment.bucket_slug,
                                scope, target_date, day_offset, user_identifier,
                            )
                            return result.record

                    if debug and due_seen == 0:
                        logger.info(
                            "[WorkItemPipeline] no due candidates bucket=%s scope=%s day=%s user=%s",
                            assignment.bucket_slug, scope, target_date, user_identifier,
                        )

            if any_candidates_in_day:
                consecutive_empty = 0
            else:
                consecutive_empty += 1
                if consecutive_empty >= self._CONSECUTIVE_EMPTY_DAYS_STOP:
                    logger.info(
                        "[WorkItemPipeline] stopping after %d consecutive empty days "
                        "at day=%s user=%s",
                        consecutive_empty, target_date, user_identifier,
                    )
                    break

        logger.info(
            "[WorkItemPipeline] end_empty tenant=%s user=%s buckets_tried=%d",
            _tenant_label(tenant), user_identifier, len(assignments),
        )
        return None


class SelfTrialPipeline(WorkItemPipeline):
    """Work-item pipeline restricted to self-trial buckets only (legacy get-next-lead cutover)."""

    def _filter_work_item_assignments(self, assignments: List[BucketAssignmentView]) -> List[BucketAssignmentView]:
        return [a for a in assignments if is_self_trial_bucket_slug(a.bucket_slug)]
