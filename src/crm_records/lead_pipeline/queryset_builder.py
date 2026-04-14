from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from django.db.models import QuerySet

from crm_records.models import Record

logger = logging.getLogger(__name__)


class BucketQuerysetBuilder:
    """
    Builds lead querysets from ``Bucket.filter_conditions`` (generic bucket engine).

    User-specific eligible filters (affiliated_party / lead_source / lead_status)
    are applied here because buckets are system-wide.
    """

    _UNASSIGNED_WHERE = """
        (
            (data->>'assigned_to') IS NULL
            OR TRIM(COALESCE(data->>'assigned_to', '')) = ''
            OR LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) IN ('null', 'none')
        )
    """

    _NEXT_CALL_DUE_FRAGMENT = """
        (data->>'next_call_at') IS NOT NULL
        AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
        AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
        AND (data->>'next_call_at')::timestamptz <= NOW()
    """

    _EXCLUDE_OTHER_ASSIGNEES_WHERE = """
        NOT (
            (data->>'assigned_to') IS NOT NULL
            AND TRIM(COALESCE(data->>'assigned_to', '')) != ''
            AND LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) NOT IN ('null', 'none')
            AND data->>'assigned_to' != %s
        )
    """

    def build(
        self,
        *,
        tenant,
        bucket_filter_conditions: Dict[str, Any],
        user_identifier: str,
        user_uuid,
        eligible_lead_types: List[str],
        eligible_lead_sources: List[str],
        eligible_lead_statuses: List[str],
        eligible_states: List[str],
        debug: bool = False,
    ) -> QuerySet:
        qs = Record.objects.filter(tenant=tenant, entity_type="lead")

        scope = bucket_filter_conditions.get("assigned_scope", "unassigned")
        qs = self._apply_assigned_scope(
            qs,
            scope=scope,
            user_identifier=user_identifier,
            exclude_other_assignees=self._should_exclude_other_assignees(bucket_filter_conditions, scope),
        )

        if stages := bucket_filter_conditions.get("lead_stage"):
            stage_list = ", ".join(f"'{s.upper()}'" for s in stages)
            qs = qs.extra(where=[f"UPPER(COALESCE(data->>'lead_stage','')) IN ({stage_list})"])
        if debug:
            logger.info(
                "[BucketQuerysetBuilder] after scope+stages bucket_conditions=%s scope=%s count=%s",
                {k: bucket_filter_conditions.get(k) for k in ("assigned_scope", "lead_stage", "call_attempts", "next_call_due", "apply_routing_rule", "daily_limit_applies", "fallback_assigned_scope") if k in bucket_filter_conditions},
                scope,
                qs.count(),
            )

        ca = bucket_filter_conditions.get("call_attempts")
        if ca:
            qs = self._apply_call_attempts_range(qs, ca)
        if debug:
            logger.info(
                "[BucketQuerysetBuilder] after call_attempts_range call_attempts=%s count=%s",
                ca,
                qs.count(),
            )

        if bucket_filter_conditions.get("next_call_due"):
            qs = qs.extra(where=[f"({self._NEXT_CALL_DUE_FRAGMENT.strip()})"])
        if debug:
            logger.info(
                "[BucketQuerysetBuilder] after next_call_due=%s count=%s",
                bucket_filter_conditions.get("next_call_due"),
                qs.count(),
            )

        if debug:
            logger.info(
                "[BucketQuerysetBuilder] routing rule skipped (group/KV-only lead flow) user_uuid=%s count=%s",
                bool(user_uuid),
                qs.count(),
            )

        if eligible_lead_types:
            qs = qs.filter(data__affiliated_party__in=eligible_lead_types)
            if debug:
                logger.info(
                    "[BucketQuerysetBuilder] after eligible_lead_types=%s count=%s",
                    eligible_lead_types,
                    qs.count(),
                )
        if eligible_lead_sources:
            qs = qs.filter(data__lead_source__in=eligible_lead_sources)
            if debug:
                logger.info(
                    "[BucketQuerysetBuilder] after eligible_lead_sources=%s count=%s",
                    eligible_lead_sources,
                    qs.count(),
                )
        if eligible_lead_statuses:
            qs = qs.filter(data__lead_status__in=eligible_lead_statuses)
            if debug:
                logger.info(
                    "[BucketQuerysetBuilder] after eligible_lead_statuses=%s count=%s",
                    eligible_lead_statuses,
                    qs.count(),
                )
        if eligible_states:
            qs = qs.filter(data__state__in=eligible_states)
            if debug:
                logger.info(
                    "[BucketQuerysetBuilder] after eligible_states=%s count=%s",
                    eligible_states,
                    qs.count(),
                )

        return qs

    def _should_exclude_other_assignees(self, fc: Dict[str, Any], scope: str) -> bool:
        if scope != "unassigned":
            return False
        if "exclude_other_assignees" in fc:
            return bool(fc["exclude_other_assignees"])
        return True

    def _apply_assigned_scope(
        self,
        qs: QuerySet,
        *,
        scope: str,
        user_identifier: str,
        exclude_other_assignees: bool,
    ) -> QuerySet:
        if scope == "me":
            where = (
                "data->>'assigned_to' IS NOT NULL AND TRIM(COALESCE(data->>'assigned_to', '')) != '' AND "
                "data->>'assigned_to' = %s"
            )
            return qs.extra(where=[where], params=[user_identifier])
        if scope == "any":
            return qs
        # unassigned (default)
        qs = qs.extra(where=[self._UNASSIGNED_WHERE])
        if exclude_other_assignees:
            qs = qs.extra(where=[self._EXCLUDE_OTHER_ASSIGNEES_WHERE], params=[user_identifier])
        return qs

    def _apply_call_attempts_range(self, qs: QuerySet, ca: Dict[str, Any]) -> QuerySet:
        col = "COALESCE((data->>'call_attempts')::int, 0)"
        parts = []
        params: List[int] = []
        if "lte" in ca:
            parts.append(f"{col} <= %s")
            params.append(int(ca["lte"]))
        if "gte" in ca:
            parts.append(f"{col} >= %s")
            params.append(int(ca["gte"]))
        if "lt" in ca:
            parts.append(f"{col} < %s")
            params.append(int(ca["lt"]))
        if "gt" in ca:
            parts.append(f"{col} > %s")
            params.append(int(ca["gt"]))
        if not parts:
            return qs
        return qs.extra(where=[" AND ".join(parts)], params=params)
