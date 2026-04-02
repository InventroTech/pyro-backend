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


def _tenant_label(tenant) -> str:
    return str(getattr(tenant, "slug", None) or getattr(tenant, "id", "") or tenant)


class LeadPipeline:
    """
    Lead retrieval + assignment in ``UserBucketAssignment`` priority order (all RMs, including SELF TRIAL).

    Applies ``CallAttemptMatrix`` exclusions on the built queryset, then ``PullStrategyApplier``
    (default seeds use ``order_by: call_attempts_asc`` — no ``lead_score`` in sort), due checks, and ``LeadAssigner``.
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
            logger.warning(
                "[LeadPipeline] abort: no user_identifier tenant=%s",
                _tenant_label(tenant),
            )
            return None

        mem_id = getattr(resolved_user.membership, "id", None) if resolved_user.membership else None
        logger.info(
            "[LeadPipeline] start tenant=%s user=%s membership_id=%s user_uuid=%s "
            "filters: affiliated_party=%s lead_source=%s lead_status=%s daily_limit=%s debug=%s",
            _tenant_label(tenant),
            user_identifier,
            mem_id,
            user_uuid,
            resolved_user.eligible_lead_types,
            resolved_user.eligible_lead_sources or "(none)",
            resolved_user.eligible_lead_statuses or "(none)",
            resolved_user.daily_limit,
            debug,
        )

        limit_status = None
        if resolved_user.daily_limit is not None:
            limit_status = self.daily_limit_checker.check(
                tenant=tenant,
                user_identifier=user_identifier,
                daily_limit=resolved_user.daily_limit,
                now=now,
                debug=debug,
            )
            remaining = None
            try:
                if limit_status.daily_limit is not None:
                    lim = int(limit_status.daily_limit)
                    remaining = max(0, lim - int(limit_status.assigned_today))
            except (TypeError, ValueError):
                remaining = None
            logger.info(
                "[LeadPipeline] daily_limit_check raw=%s effective_limit=%s assigned_today=%s "
                "is_reached=%s remaining_slots=%s debug=%s",
                resolved_user.daily_limit,
                limit_status.daily_limit,
                limit_status.assigned_today,
                limit_status.is_reached,
                remaining,
                debug,
            )
        else:
            logger.info("[LeadPipeline] daily_limit_check daily_limit not set — no fresh-bucket cap from UserSettings")

        assignments = self.bucket_resolver.resolve(tenant, resolved_user)
        bucket_order = [(a.bucket_slug, a.priority) for a in assignments]
        logger.info(
            "[LeadPipeline] buckets resolved count=%s order=%s user=%s",
            len(assignments),
            bucket_order,
            user_identifier,
        )
        if debug:
            logger.info(
                "[LeadPipeline] resolved %d bucket assignments for user_identifier=%s tenant=%s",
                len(assignments),
                user_identifier,
                getattr(tenant, "id", None) or getattr(tenant, "slug", None),
            )

        for assignment in assignments:
            fc = dict(assignment.filter_conditions or {})

            if fc.get("daily_limit_applies") and limit_status and limit_status.is_reached and not debug:
                logger.info(
                    "[LeadPipeline] skip bucket (daily_limit reached, fresh bucket gated) "
                    "bucket_slug=%s priority=%s daily_limit_applies=%s assigned_today=%s limit=%s",
                    assignment.bucket_slug,
                    assignment.priority,
                    fc.get("daily_limit_applies"),
                    getattr(limit_status, "assigned_today", None),
                    getattr(limit_status, "daily_limit", None),
                )
                continue

            scopes = [fc.get("assigned_scope", "unassigned")]
            if fallback := fc.get("fallback_assigned_scope"):
                scopes.append(fallback)

            if debug:
                logger.info(
                    "[LeadPipeline] trying assignment bucket_slug=%s priority=%s pull_strategy=%s filter_conditions=%s scopes=%s",
                    assignment.bucket_slug,
                    assignment.priority,
                    assignment.pull_strategy,
                    fc,
                    scopes,
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
                    debug=debug,
                )

                qs = self.matrix_filter.apply(
                    qs=qs,
                    tenant=tenant,
                    eligible_lead_types=resolved_user.eligible_lead_types,
                    now=now,
                )

                qs = self.strategy_applier.apply(qs=qs, strategy=assignment.pull_strategy, now_iso=now_iso)

                qs_count = None
                try:
                    qs_count = qs.count()
                except Exception:
                    logger.exception(
                        "[LeadPipeline] qs.count() failed bucket=%s scope=%s",
                        assignment.bucket_slug,
                        scope,
                    )

                logger.info(
                    "[LeadPipeline] bucket_try bucket=%s priority=%s scope=%s qs_after_strategy=%s pull_strategy=%s",
                    assignment.bucket_slug,
                    assignment.priority,
                    scope,
                    qs_count,
                    assignment.pull_strategy,
                )

                if debug:
                    try:
                        logger.info(
                            "[LeadPipeline] bucket_slug=%s scope=%s qs_count=%s",
                            assignment.bucket_slug,
                            scope,
                            qs_count,
                        )
                    except Exception:
                        logger.exception("[LeadPipeline] failed qs.count() for bucket debug logging")

                checked = 0
                due_seen = 0
                for c in qs[:50]:
                    due = self.candidate_selector.is_due_for_call(c.data, now)
                    if due:
                        due_seen += 1
                    if debug and checked < 5:
                        data = c.data or {}
                        logger.info(
                            "[LeadPipeline] candidate check bucket_slug=%s scope=%s lead_stage=%s call_attempts=%s next_call_at=%s assigned_to=%s due=%s",
                            assignment.bucket_slug,
                            scope,
                            (data.get("lead_stage") or "").upper(),
                            data.get("call_attempts"),
                            data.get("next_call_at"),
                            data.get("assigned_to"),
                            due,
                        )
                        checked += 1

                    if due:
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
                                "[LeadPipeline] assigned record_id=%s bucket=%s scope=%s "
                                "is_fresh_assignment=%s user=%s",
                                result.record.pk,
                                assignment.bucket_slug,
                                scope,
                                result.is_fresh_assignment,
                                user_identifier,
                            )
                            if debug:
                                logger.info(
                                    "[LeadPipeline] assigned lead record_id=%s from bucket_slug=%s scope=%s",
                                    result.record.pk,
                                    assignment.bucket_slug,
                                    scope,
                                )
                            return result.record

                if qs_count == 0:
                    logger.info(
                        "[LeadPipeline] no candidates in queryset bucket=%s scope=%s user=%s",
                        assignment.bucket_slug,
                        scope,
                        user_identifier,
                    )
                elif qs_count is not None and due_seen == 0:
                    logger.info(
                        "[LeadPipeline] bucket had rows but none due for call (first 50) bucket=%s scope=%s "
                        "qs_count=%s user=%s",
                        assignment.bucket_slug,
                        scope,
                        qs_count,
                        user_identifier,
                    )
                elif due_seen > 0:
                    logger.info(
                        "[LeadPipeline] due candidates failed assign (lock/race) bucket=%s scope=%s "
                        "due_in_first_50=%s qs_count=%s user=%s",
                        assignment.bucket_slug,
                        scope,
                        due_seen,
                        qs_count,
                        user_identifier,
                    )

        if limit_status and limit_status.is_reached and not debug:
            logger.info(
                "[LeadPipeline] entering daily_limit_retry_fallback assigned_today=%s limit=%s user=%s",
                limit_status.assigned_today,
                limit_status.daily_limit,
                user_identifier,
            )
            retry_candidate = self._daily_limit_retry_fallback(
                tenant=tenant,
                resolved_user=resolved_user,
                user_uuid=user_uuid,
                user_identifier=user_identifier,
                now=now,
            )
            if retry_candidate:
                logger.info(
                    "[LeadPipeline] daily_limit_retry_fallback returned record_id=%s user=%s",
                    retry_candidate.pk,
                    user_identifier,
                )
                return retry_candidate
            logger.info(
                "[LeadPipeline] daily_limit_retry_fallback empty user=%s (no assigned-to-me or unassigned due retry)",
                user_identifier,
            )

        logger.info(
            "[LeadPipeline] end_empty tenant=%s user=%s assignments_tried=%s "
            "limit_reached=%s assigned_today=%s daily_limit=%s debug=%s",
            _tenant_label(tenant),
            user_identifier,
            len(assignments),
            getattr(limit_status, "is_reached", None) if limit_status else None,
            getattr(limit_status, "assigned_today", None) if limit_status else None,
            getattr(limit_status, "daily_limit", None) if limit_status else None,
            debug,
        )
        if debug:
            logger.info(
                "[LeadPipeline] no lead assigned. assignments_tried=%d limit_status=%s limit_reached=%s",
                len(assignments),
                bool(limit_status),
                getattr(limit_status, "is_reached", None),
            )
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

        retry_strategy = {
            "order_by": "call_attempts_asc",
            "include_snoozed_due": False,
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
        try:
            assigned_count = assigned_retry_qs.count()
        except Exception:
            assigned_count = None
            logger.exception("[LeadPipeline] fallback assigned_retry_qs.count() failed user=%s", user_identifier)
        retry_candidate = assigned_retry_qs.first()
        logger.info(
            "[LeadPipeline] fallback step=assigned_to_me qs_count=%s first_record_id=%s user=%s",
            assigned_count,
            retry_candidate.pk if retry_candidate else None,
            user_identifier,
        )

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
        try:
            unassigned_count = unassigned_retry_qs.count()
        except Exception:
            unassigned_count = None
            logger.exception("[LeadPipeline] fallback unassigned_retry_qs.count() failed user=%s", user_identifier)
        unassigned_retry_candidate = unassigned_retry_qs.first()
        logger.info(
            "[LeadPipeline] fallback step=unassigned filters=affiliated_party=%s lead_source=%s lead_status=%s "
            "routing_applied=%s qs_count=%s first_record_id=%s user=%s",
            eligible_lead_types,
            eligible_lead_sources or "(none)",
            eligible_lead_statuses or "(none)",
            bool(user_uuid),
            unassigned_count,
            unassigned_retry_candidate.pk if unassigned_retry_candidate else None,
            user_identifier,
        )

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
            logger.info(
                "[LeadPipeline] fallback assigned unassigned retry record_id=%s user=%s",
                candidate_locked.pk,
                user_identifier,
            )
            return candidate_locked

