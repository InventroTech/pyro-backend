from __future__ import annotations

import logging
from typing import Optional, Iterable, Any, Dict, List, Tuple

from django.db.models import QuerySet, Q

from .models import RoutingRule


logger = logging.getLogger(__name__)


def _get_active_rule(tenant, user_id, queue_type: str) -> Optional[RoutingRule]:
    """
    Return the active routing rule for this user and queue type.
    Rules are keyed by TenantMembership; we resolve user_id (UUID) to the membership, then look up the rule.
    """
    if not tenant or not user_id or not queue_type:
        logger.debug(
            "[RoutingRule] _get_active_rule: skip (missing tenant=%s user_id=%s queue_type=%s)",
            tenant is not None, user_id is not None, queue_type,
        )
        return None

    from authz.models import TenantMembership

    membership = TenantMembership.objects.filter(
        tenant=tenant, user_id=user_id
    ).first()
    if not membership:
        logger.info(
            "[RoutingRule] _get_active_rule: no TenantMembership for tenant=%s user_id=%s queue_type=%s → no rule",
            getattr(tenant, "slug", getattr(tenant, "id", tenant)),
            user_id,
            queue_type,
        )
        return None

    rule = (
        RoutingRule.objects.filter(
            tenant=tenant,
            tenant_membership=membership,
            queue_type=queue_type,
            is_active=True,
        )
        .order_by("id")
        .first()
    )
    if rule:
        logger.info(
            "[RoutingRule] _get_active_rule: found rule id=%s queue_type=%s conditions=%s",
            rule.id, rule.queue_type, rule.conditions,
        )
    else:
        logger.info(
            "[RoutingRule] _get_active_rule: no active rule for tenant=%s user_id=%s queue_type=%s → queryset unchanged",
            getattr(tenant, "slug", getattr(tenant, "id", tenant)), user_id, queue_type,
        )
    return rule


def _normalize_string_for_match(s: str) -> str:
    """Lowercase and remove spaces so 'Tamil Nadu' and 'tamilnadu' match."""
    if not isinstance(s, str):
        return str(s) if s is not None else ""
    return s.lower().replace(" ", "").strip()


def _build_filters_from_conditions(
    queue_type: str,
    conditions: Dict[str, Any],
) -> Tuple[Q, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Translate a simple conditions JSON into a Django Q object.

    Expected JSON shape (v1):
        {
          "filters": [
            {"field": "state", "op": "equals", "value": "Tamil Nadu"},
            {"field": "poster", "op": "in", "value": ["Facebook", "Google"]}
          ]
        }

    We use a very small whitelist of fields to keep queries index-friendly.
    For "equals" on string values we use normalized (case- and space-insensitive) matching
    so e.g. rule value "tamilnadu" matches lead data "Tamil Nadu".
    Returns (q, applied_filters, normalized_equals) where normalized_equals is list of
    {"data_key": "state", "value": "tamilnadu"} for lead queue JSON fields.
    """
    filters = conditions.get("filters") if isinstance(conditions, dict) else None
    if not filters or not isinstance(filters, Iterable):
        return Q(), [], []

    q = Q()
    applied: List[Dict[str, Any]] = []
    normalized_equals: List[Dict[str, Any]] = []  # for lead queue: {"data_key": "state", "value": "..."}

    # Map logical fields to model fields / JSON paths
    if queue_type == RoutingRule.QUEUE_TYPE_TICKET:
        field_map = {
            "poster": "poster",
            "state": "state",
        }
    elif queue_type == RoutingRule.QUEUE_TYPE_LEAD:
        # Leads live in Record.data JSON
        field_map = {
            "state": "data__state",
            "lead_source": "data__lead_source",
            "affiliated_party": "data__affiliated_party",
        }
    else:
        field_map = {}

    logger.info(
        "[RoutingRule] _build_filters_from_conditions: queue_type=%s allowed_fields=%s raw_filters_count=%d",
        queue_type, list(field_map.keys()), len(filters) if filters else 0,
    )
    for i, item in enumerate(filters):
        if not isinstance(item, dict):
            logger.warning("[RoutingRule] _build_filters_from_conditions: filter[%d] not a dict, skip", i)
            continue
        field = item.get("field")
        op = (item.get("op") or "equals").lower()
        value = item.get("value")

        if not field or value is None:
            logger.info(
                "[RoutingRule] _build_filters_from_conditions: filter[%d] skip (field=%r value is None/empty)",
                i, field,
            )
            continue

        model_field = field_map.get(field)
        if not model_field:
            logger.warning(
                "[RoutingRule] _build_filters_from_conditions: filter[%d] field=%r not in allowed fields for queue_type=%s (allowed=%s) → skip",
                i, field, queue_type, list(field_map.keys()),
            )
            continue

        if op == "equals":
            if isinstance(value, str) and queue_type == RoutingRule.QUEUE_TYPE_LEAD and model_field.startswith("data__"):
                # Use normalized (case- and space-insensitive) match so "tamilnadu" matches "Tamil Nadu"
                data_key = model_field.replace("data__", "", 1)
                normalized_equals.append({"data_key": data_key, "value": value})
                applied.append({"field": field, "op": "equals (normalized)", "value": value, "model_field": model_field})
            else:
                q &= Q(**{model_field: value})
                applied.append({"field": field, "op": op, "value": value, "model_field": model_field})
        elif op == "in" and isinstance(value, (list, tuple)):
            q &= Q(**{f"{model_field}__in": list(value)})
            applied.append({"field": field, "op": op, "value": value, "model_field": f"{model_field}__in"})
        else:
            logger.warning(
                "[RoutingRule] _build_filters_from_conditions: filter[%d] op=%r not supported (use equals or in) → skip",
                i, op,
            )
            continue

    if applied:
        logger.info(
            "[RoutingRule] _build_filters_from_conditions: applied %d filter(s): %s",
            len(applied), applied,
        )
    else:
        logger.warning(
            "[RoutingRule] _build_filters_from_conditions: no filters applied (all skipped or unknown field/op). conditions=%s",
            conditions,
        )
    return q, applied, normalized_equals


def _apply_normalized_equals(qs: QuerySet, normalized_equals: List[Dict[str, Any]], queue_type: str) -> QuerySet:
    """
    Apply normalized (case- and space-insensitive) string filters for lead queue.
    Uses raw SQL so e.g. rule value 'tamilnadu' matches DB value 'Tamil Nadu'.
    """
    if not normalized_equals:
        return qs
    from django.db import connection
    meta = qs.model._meta
    table = connection.ops.quote_name(meta.db_table)
    try:
        data_field = meta.get_field("data")
        data_col = connection.ops.quote_name(data_field.column)
    except Exception:
        data_col = connection.ops.quote_name("data")
    where_parts = []
    params = []
    for item in normalized_equals:
        key = item.get("data_key")
        value = item.get("value")
        if not key or value is None:
            continue
        # PostgreSQL: LOWER(REPLACE(COALESCE("table"."data"->>'key', ''), ' ', '')) = LOWER(REPLACE(%s, ' ', ''))
        where_parts.append(
            f"LOWER(REPLACE(COALESCE({table}.{data_col}->>%s, ''), ' ', '')) = LOWER(REPLACE(%s, ' ', ''))"
        )
        params.extend([key, value])
    if not where_parts:
        return qs
    logger.info(
        "[RoutingRule] _apply_normalized_equals: applying %d normalized string filter(s) (case- and space-insensitive)",
        len(where_parts),
    )
    return qs.extra(where=where_parts, params=params)


def apply_routing_rule_to_queryset(
    qs: QuerySet,
    *,
    tenant,
    user_id,
    queue_type: str,
) -> QuerySet:
    """
    Apply the current user's active routing rule (if any) to the given queryset.

    Enforcement logic:
    - If no rule exists, return the queryset unchanged (whole possible queryset).
    - If a rule exists, enforce it strictly by applying its filters, even if it results in 0 matches.
    - If a rule exists but has no valid filters (empty condition_q), return empty queryset to enforce the rule.
    - String "equals" on lead queue uses normalized matching (case- and space-insensitive).
    """
    rule = _get_active_rule(tenant=tenant, user_id=user_id, queue_type=queue_type)
    if not rule:
        # No rule exists - return whole queryset
        return qs

    # Rule exists but has no conditions or no valid filters - treat as "no restrictions"
    # (return qs unchanged; previously returned empty which blocked all leads)
    if not rule.conditions:
        logger.info(
            "[RoutingRule] apply_routing_rule_to_queryset: rule id=%s has no conditions → queryset unchanged",
            rule.id,
        )
        return qs

    condition_q, applied_filters, normalized_equals = _build_filters_from_conditions(queue_type, rule.conditions)
    has_q = isinstance(condition_q, Q) and len(condition_q.children) > 0
    if not has_q and not normalized_equals:
        logger.info(
            "[RoutingRule] apply_routing_rule_to_queryset: rule id=%s produced no conditions (all filters skipped) → queryset unchanged",
            rule.id,
        )
        return qs

    # Apply the filters strictly (even if it results in 0 matches)
    before_count = qs.count()
    if has_q:
        qs = qs.filter(condition_q)
    if normalized_equals:
        qs = _apply_normalized_equals(qs, normalized_equals, queue_type)
    filtered_qs = qs
    after_count = filtered_qs.count()
    logger.info(
        "[RoutingRule] apply_routing_rule_to_queryset: rule id=%s queue_type=%s applied_filters=%s "
        "count before=%d after=%d (leads must match: %s)",
        rule.id,
        queue_type,
        applied_filters,
        before_count,
        after_count,
        " AND ".join(
            f"{f['field']}={f['value']!r}" if f.get('op') == 'equals' or 'normalized' in str(f.get('op', '')) else f"{f['field']} in {f['value']!r}"
            for f in applied_filters
        ) or "(none)",
    )
    if before_count > 0 and after_count == 0:
        # Log sample of actual values in DB for the fields the rule filters on (to compare with rule value)
        try:
            sample_fields = [f.get("model_field") or f.get("field") for f in applied_filters]
            actual_samples = {}
            if queue_type == RoutingRule.QUEUE_TYPE_LEAD:
                for f in applied_filters:
                    field = f.get("field")
                    if field == "state":
                        actual_samples["data__state"] = list(qs.values_list("data__state", flat=True).distinct()[:20])
                    elif field == "lead_source":
                        actual_samples["data__lead_source"] = list(qs.values_list("data__lead_source", flat=True).distinct()[:20])
                    elif field == "affiliated_party":
                        actual_samples["data__affiliated_party"] = list(qs.values_list("data__affiliated_party", flat=True).distinct()[:20])
            logger.warning(
                "[RoutingRule] apply_routing_rule_to_queryset: rule filtered out ALL %d leads. "
                "Rule expects: %s. Actual distinct values in queryset (sample): %s. "
                "Rule conditions: %s",
                before_count,
                applied_filters,
                actual_samples,
                rule.conditions,
            )
        except Exception as e:
            logger.warning(
                "[RoutingRule] apply_routing_rule_to_queryset: rule filtered out ALL %d leads. "
                "Check that lead data (state, lead_source, affiliated_party) matches rule values exactly (case/whitespace). "
                "Rule conditions: %s applied_filters: %s (sample query failed: %s)",
                before_count, rule.conditions, applied_filters, e,
            )
    return filtered_qs
