from __future__ import annotations

from typing import Optional, Iterable, Any, Dict

from django.db.models import QuerySet, Q

from .models import RoutingRule


def _get_active_rule(tenant, user_id, queue_type: str) -> Optional[RoutingRule]:
    """
    Return the single active routing rule for this (tenant, user_id, queue_type), if any.
    """
    if not tenant or not user_id or not queue_type:
        return None

    return (
        RoutingRule.objects.filter(
            tenant=tenant,
            user_id=user_id,
            queue_type=queue_type,
            is_active=True,
        )
        .order_by("id")
        .first()
    )


def _build_filters_from_conditions(
    queue_type: str,
    conditions: Dict[str, Any],
) -> Q:
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
    """
    filters = conditions.get("filters") if isinstance(conditions, dict) else None
    if not filters or not isinstance(filters, Iterable):
        return Q()

    q = Q()

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

    for item in filters:
        if not isinstance(item, dict):
            continue
        field = item.get("field")
        op = (item.get("op") or "equals").lower()
        value = item.get("value")

        if not field or value is None:
            continue

        model_field = field_map.get(field)
        if not model_field:
            # Unknown/unsupported field for this queue_type – ignore silently in v1
            continue

        if op == "equals":
            q &= Q(**{model_field: value})
        elif op == "in" and isinstance(value, (list, tuple)):
            q &= Q(**{f"{model_field}__in": list(value)})
        else:
            # Unsupported operator in v1 – ignore
            continue

    return q


def apply_routing_rule_to_queryset(
    qs: QuerySet,
    *,
    tenant,
    user_id,
    queue_type: str,
) -> QuerySet:
    """
    Apply the current user's active routing rule (if any) to the given queryset.

    This is intentionally light-weight and read-only:
    - If no rule exists, or no valid filters are defined, we return the queryset unchanged.
    - If there is a rule, we AND its filters onto the existing queryset.
    """
    rule = _get_active_rule(tenant=tenant, user_id=user_id, queue_type=queue_type)
    if not rule or not rule.conditions:
        return qs

    condition_q = _build_filters_from_conditions(queue_type, rule.conditions)
    if not condition_q:
        return qs

    return qs.filter(condition_q)


