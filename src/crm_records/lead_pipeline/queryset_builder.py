from __future__ import annotations

import logging
from functools import reduce
from operator import or_
from typing import Any, Dict, List, Optional

from django.db.models import Q, QuerySet

from crm_records.models import Record

logger = logging.getLogger(__name__)


class BucketQuerysetBuilder:
    """
    Builds record querysets from ``Bucket.filter_conditions`` (generic bucket engine).

    User-specific eligible filters (affiliated_party / lead_source / lead_status)
    are applied for leads because buckets are system-wide.

    Support-ticket buckets may set:
    - ``entity_type`` (usually via pipeline arg, not only filter_conditions)
    - ``resolution_status`` (list[str]): match ``data.resolution_status`` (case-sensitive values)
    - ``self_trial`` (bool): if True only Self Trial types; if False exclude them
    - ``call_status`` (str | list[str]): match ``data.call_status`` (case-insensitive)
    - ``first_assigned_day`` (``"today"`` | ``"yesterday"``): calendar day of
      ``data.first_assigned_at`` in ``day_timezone`` (default Asia/Kolkata)
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
        entity_type: str = "lead",
    ) -> QuerySet:
        qs = Record.objects.filter(tenant=tenant, entity_type=entity_type)

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

        if resolution_statuses := bucket_filter_conditions.get("resolution_status"):
            qs = self._apply_resolution_status(qs, resolution_statuses)

        if "self_trial" in bucket_filter_conditions:
            qs = self._apply_self_trial_filter(qs, bool(bucket_filter_conditions["self_trial"]))

        if call_statuses := bucket_filter_conditions.get("call_status"):
            qs = self._apply_call_status(qs, call_statuses)

        if day_key := bucket_filter_conditions.get("first_assigned_day"):
            qs = self._apply_first_assigned_day(
                qs,
                day_key=str(day_key),
                tz=str(
                    bucket_filter_conditions.get("day_timezone")
                    or "Asia/Kolkata"
                ),
            )

        if debug:
            logger.info(
                "[BucketQuerysetBuilder] after scope+stages bucket_conditions=%s scope=%s entity_type=%s count=%s",
                {
                    k: bucket_filter_conditions.get(k)
                    for k in (
                        "assigned_scope",
                        "lead_stage",
                        "resolution_status",
                        "self_trial",
                        "call_status",
                        "first_assigned_day",
                        "call_attempts",
                        "next_call_due",
                        "apply_routing_rule",
                        "daily_limit_applies",
                        "fallback_assigned_scope",
                    )
                    if k in bucket_filter_conditions
                },
                scope,
                entity_type,
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

        # Lead-only eligibility filters (no-op for support when lists are empty).
        if eligible_lead_types:
            qs = qs.filter(self._build_contains_in_q("affiliated_party", eligible_lead_types))
            if debug:
                logger.info(
                    "[BucketQuerysetBuilder] after eligible_lead_types=%s count=%s",
                    eligible_lead_types,
                    qs.count(),
                )
        if eligible_lead_sources:
            qs = qs.filter(self._build_contains_in_q("lead_source", eligible_lead_sources))
            if debug:
                logger.info(
                    "[BucketQuerysetBuilder] after eligible_lead_sources=%s count=%s",
                    eligible_lead_sources,
                    qs.count(),
                )
        if eligible_lead_statuses:
            qs = qs.filter(self._build_contains_in_q("lead_status", eligible_lead_statuses))
            if debug:
                logger.info(
                    "[BucketQuerysetBuilder] after eligible_lead_statuses=%s count=%s",
                    eligible_lead_statuses,
                    qs.count(),
                )
        if eligible_states:
            qs = qs.filter(self._build_contains_in_q("state", eligible_states))
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

    @staticmethod
    def _build_contains_in_q(field: str, values: List[str]) -> Q:
        """Build OR of data__contains lookups to leverage the GIN index."""
        return reduce(or_, [Q(data__contains={field: v}) for v in values])

    @staticmethod
    def _apply_resolution_status(qs: QuerySet, statuses: Any) -> QuerySet:
        if not isinstance(statuses, list) or not statuses:
            return qs
        normalized = [str(s) for s in statuses if s is not None and str(s).strip()]
        if not normalized:
            return qs
        # Include unset/empty when Open is allowed (pending tickets).
        q = Q(data__resolution_status__in=normalized)
        if any(s.lower() == "open" for s in normalized):
            from support_ticket.records import q_data_unset

            q = q | q_data_unset("resolution_status") | Q(data__resolution_status="")
        return qs.filter(q)

    @staticmethod
    def _apply_self_trial_filter(qs: QuerySet, self_trial: bool) -> QuerySet:
        from support_ticket.ticket_types import q_record_self_trial

        if self_trial:
            return qs.filter(q_record_self_trial())
        # Prefer positive exclude-by-id: ``exclude(q_record_self_trial())`` drops rows with
        # NULL poster/type due to SQL three-valued logic on large OR conditions.
        st_ids = set(qs.filter(q_record_self_trial()).values_list("id", flat=True))
        if st_ids:
            return qs.exclude(id__in=st_ids)
        return qs

    @staticmethod
    def _apply_call_status(qs: QuerySet, statuses: Any) -> QuerySet:
        if isinstance(statuses, str):
            statuses = [statuses]
        if not isinstance(statuses, list) or not statuses:
            return qs
        normalized = [
            str(s).strip().upper()
            for s in statuses
            if s is not None and str(s).strip()
        ]
        if not normalized:
            return qs
        quoted = ", ".join(f"'{s.replace(chr(39), chr(39)+chr(39))}'" for s in normalized)
        return qs.extra(
            where=[
                f"UPPER(TRIM(COALESCE(data->>'call_status', ''))) IN ({quoted})"
            ]
        )

    @staticmethod
    def _apply_first_assigned_day(qs: QuerySet, *, day_key: str, tz: str) -> QuerySet:
        """
        Filter by calendar day of ``data.first_assigned_at`` in ``tz``.

        ``day_key``: ``today`` (offset 0) or ``yesterday`` (offset 1).
        """
        key = day_key.strip().lower()
        if key == "today":
            day_offset = 0
        elif key == "yesterday":
            day_offset = 1
        else:
            raise ValueError(
                f"first_assigned_day must be 'today' or 'yesterday', got: {day_key}"
            )
        safe_tz = tz.replace("'", "''")
        ts_expr = """
            CASE
                WHEN (data->>'first_assigned_at') IS NOT NULL
                    AND TRIM(COALESCE(data->>'first_assigned_at', '')) != ''
                    AND LOWER(TRIM(COALESCE(data->>'first_assigned_at', '')))
                        NOT IN ('null', 'none')
                THEN (data->>'first_assigned_at')::timestamptz
                ELSE NULL
            END
        """
        where = (
            f"(timezone('{safe_tz}', {ts_expr.strip()}))::date "
            f"= ((timezone('{safe_tz}', NOW()))::date - {day_offset})"
        )
        return qs.extra(where=[where])

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
