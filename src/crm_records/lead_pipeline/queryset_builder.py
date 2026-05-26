from __future__ import annotations

import logging
import re
from functools import reduce
from operator import or_
from typing import Any, Dict, List, Optional, Tuple

from django.db.models import Q, QuerySet

from crm_records.models import Record
from crm_records.record_data_sql import CALL_ATTEMPTS_INT_EXPR

logger = logging.getLogger(__name__)

# Pipeline control keys — not Record.data attribute filters.
_RESERVED_PIPELINE_KEYS = frozenset(
    {
        "entity_type",
        "assigned_scope",
        "fallback_assigned_scope",
        "exclude_other_assignees",
        "call_attempts",
        "next_call_due",
        "daily_limit_applies",
        "apply_routing_rule",
    }
)

_DATA_FIELD_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class BucketQuerysetBuilder:
    """
    Builds record querysets from ``Bucket.filter_conditions`` (generic bucket engine).

    User-specific eligible filters (affiliated_party / lead_source / lead_status)
    are applied here because buckets are system-wide.

    Data attribute filters (any ``data`` JSON key):

    - ``{field}_in`` / ``{field}_not_in`` — list membership on ``data->>'field'``
    - ``resolution_status_in`` — same as ``_in`` but treats null/empty as a match
    - ``lead_stage`` — list, matched case-insensitively (legacy key name)
    - ``lead_source`` / ``lead_status`` — list, JSON ``contains`` match (GIN-friendly)
    - ``atleast_paid_once`` — bool, true/false parsing for string booleans in JSON
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
        fc = bucket_filter_conditions
        entity_type = fc.get("entity_type", "lead")
        qs = Record.objects.filter(tenant=tenant, entity_type=entity_type)

        scope = fc.get("assigned_scope", "unassigned")
        qs = self._apply_assigned_scope(
            qs,
            scope=scope,
            user_identifier=user_identifier,
            exclude_other_assignees=self._should_exclude_other_assignees(fc, scope),
        )

        qs = self._apply_data_field_filters(qs, fc)

        if debug:
            logger.info(
                "[BucketQuerysetBuilder] after scope+entity+data_filters bucket_conditions=%s scope=%s count=%s",
                {k: v for k, v in fc.items() if k not in _RESERVED_PIPELINE_KEYS or k in ("entity_type", "assigned_scope")},
                scope,
                qs.count(),
            )

        ca = fc.get("call_attempts")
        if ca:
            qs = self._apply_call_attempts_range(qs, ca)
        if debug:
            logger.info(
                "[BucketQuerysetBuilder] after call_attempts_range call_attempts=%s count=%s",
                ca,
                qs.count(),
            )

        if fc.get("next_call_due"):
            qs = qs.extra(where=[f"({self._NEXT_CALL_DUE_FRAGMENT.strip()})"])
        if debug:
            logger.info(
                "[BucketQuerysetBuilder] after next_call_due=%s count=%s",
                fc.get("next_call_due"),
                qs.count(),
            )

        if debug:
            logger.info(
                "[BucketQuerysetBuilder] routing rule skipped (group/KV-only lead flow) user_uuid=%s count=%s",
                bool(user_uuid),
                qs.count(),
            )

        is_support = entity_type == "support_ticket"

        if eligible_lead_types and not is_support:
            qs = qs.filter(self._build_contains_in_q("affiliated_party", eligible_lead_types))
            if debug:
                logger.info(
                    "[BucketQuerysetBuilder] after eligible_lead_types=%s count=%s",
                    eligible_lead_types,
                    qs.count(),
                )
        if eligible_lead_sources and not is_support:
            qs = qs.filter(self._build_contains_in_q("lead_source", eligible_lead_sources))
            if debug:
                logger.info(
                    "[BucketQuerysetBuilder] after eligible_lead_sources=%s count=%s",
                    eligible_lead_sources,
                    qs.count(),
                )
        if eligible_lead_statuses and not is_support:
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

    def _apply_data_field_filters(self, qs: QuerySet, fc: Dict[str, Any]) -> QuerySet:
        for key, value in fc.items():
            spec = self._parse_field_filter(key, value)
            if spec is None:
                continue
            field, mode, filter_value = spec
            qs = self._apply_field_filter(qs, field=field, mode=mode, value=filter_value)
        return qs

    def _parse_field_filter(self, key: str, value: Any) -> Optional[Tuple[str, str, Any]]:
        if key in _RESERVED_PIPELINE_KEYS:
            return None
        if key == "lead_stage" and isinstance(value, list):
            return ("lead_stage", "upper_in", value)
        if key in ("lead_source", "lead_status") and isinstance(value, list):
            return (key, "contains_in", value)
        if key == "atleast_paid_once" and isinstance(value, bool):
            return ("atleast_paid_once", "bool_false_true", value)
        if key.endswith("_not_in") and isinstance(value, list):
            return (self._safe_data_field(key[: -len("_not_in")]), "not_in", value)
        if key.endswith("_in") and isinstance(value, list):
            field = self._safe_data_field(key[: -len("_in")])
            mode = "nullable_in" if field == "resolution_status" else "in"
            return (field, mode, value)
        return None

    def _apply_field_filter(self, qs: QuerySet, *, field: str, mode: str, value: Any) -> QuerySet:
        if mode == "upper_in":
            stage_list = ", ".join(f"'{str(s).upper()}'" for s in value)
            return qs.extra(where=[f"UPPER(COALESCE(data->>'{field}','')) IN ({stage_list})"])
        if mode == "contains_in":
            return qs.filter(self._build_contains_in_q(field, value))
        if mode == "bool_false_true":
            return self._apply_atleast_paid_once(qs, value)
        if mode == "nullable_in":
            return self._apply_resolution_status_in(qs, value)
        if mode == "in":
            return self._apply_string_list_in(qs, field, value)
        if mode == "not_in":
            return self._apply_string_list_not_in(qs, field, value)
        return qs

    @staticmethod
    def _safe_data_field(field: str) -> str:
        if not _DATA_FIELD_NAME_RE.match(field):
            raise ValueError(f"Invalid data field name for bucket filter: {field!r}")
        return field

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
        qs = qs.extra(where=[self._UNASSIGNED_WHERE])
        if exclude_other_assignees:
            qs = qs.extra(where=[self._EXCLUDE_OTHER_ASSIGNEES_WHERE], params=[user_identifier])
        return qs

    @staticmethod
    def _build_contains_in_q(field: str, values: List[str]) -> Q:
        """Build OR of data__contains lookups to leverage the GIN index."""
        return reduce(or_, [Q(data__contains={field: v}) for v in values])

    def _apply_call_attempts_range(self, qs: QuerySet, ca: Dict[str, Any]) -> QuerySet:
        col = CALL_ATTEMPTS_INT_EXPR
        parts = []
        params: List[int] = []
        if "eq" in ca:
            parts.append(f"{col} = %s")
            params.append(int(ca["eq"]))
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

    def _apply_atleast_paid_once(self, qs: QuerySet, value: bool) -> QuerySet:
        if value:
            return qs.extra(
                where=[
                    "LOWER(COALESCE(data->>'atleast_paid_once', '')) IN ('true', 't', '1', 'yes')"
                ]
            )
        return qs.extra(
            where=[
                """
                (
                    data->>'atleast_paid_once' IS NULL
                    OR TRIM(COALESCE(data->>'atleast_paid_once', '')) = ''
                    OR LOWER(TRIM(COALESCE(data->>'atleast_paid_once', ''))) IN ('false', 'f', '0', 'no', 'null', 'none')
                )
                """
            ]
        )

    def _apply_resolution_status_in(self, qs: QuerySet, values: List[Any]) -> QuerySet:
        """Match resolution_status; null/empty in values matches open tickets."""
        normalized = list(values or [])
        allow_null = any(v is None or v == "" for v in normalized)
        concrete = [str(v) for v in normalized if v is not None and v != ""]
        parts = []
        params: List[str] = []
        if allow_null:
            parts.append(
                """
                (
                    data->>'resolution_status' IS NULL
                    OR TRIM(COALESCE(data->>'resolution_status', '')) = ''
                    OR LOWER(TRIM(COALESCE(data->>'resolution_status', ''))) IN ('null', 'none')
                )
                """
            )
        if concrete:
            placeholders = ", ".join(["%s"] * len(concrete))
            parts.append(f"COALESCE(data->>'resolution_status', '') IN ({placeholders})")
            params.extend(concrete)
        if not parts:
            return qs
        return qs.extra(where=[f"({' OR '.join(parts)})"], params=params)

    def _apply_string_list_in(self, qs: QuerySet, field: str, values: List[str]) -> QuerySet:
        if not values:
            return qs
        field = self._safe_data_field(field)
        placeholders = ", ".join(["%s"] * len(values))
        return qs.extra(
            where=[f"COALESCE(data->>'{field}', '') IN ({placeholders})"],
            params=list(values),
        )

    def _apply_string_list_not_in(self, qs: QuerySet, field: str, values: List[str]) -> QuerySet:
        if not values:
            return qs
        field = self._safe_data_field(field)
        placeholders = ", ".join(["%s"] * len(values))
        return qs.extra(
            where=[f"COALESCE(data->>'{field}', '') NOT IN ({placeholders})"],
            params=list(values),
        )
