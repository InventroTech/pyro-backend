"""Support ticket type normalization and record queryset filters."""

from __future__ import annotations

from typing import Any, Dict, Set

from django.db.models import Q

SELF_TRIAL_TICKET_TYPE_KEY = "self_trail"

_TICKET_TYPE_ROUTING_KEYS = frozenset(
    {
        "self_trail",
        "in_trial",
        "paid",
        "trial_extension",
        "premium_extension",
        "rest",
    }
)

_TICKET_TYPE_ROUTING_ALIASES: Dict[str, str] = {
    "self trail": "self_trail",
    "self trial": "self_trail",
    "self_trial": "self_trail",
    "selftrail": "self_trail",
    "in trial": "in_trial",
    "intrial": "in_trial",
    "trial extension": "trial_extension",
    "in trial extension": "trial_extension",
    "in_trial_extension": "trial_extension",
    "premium extension": "premium_extension",
    "in premium extension": "premium_extension",
    "in_premium_extension": "premium_extension",
}


def normalize_support_ticket_type_key(value: Any) -> str:
    if value is None:
        return ""
    normalized = str(value).strip().lower().replace("-", " ").replace("_", " ")
    return " ".join(normalized.split())


def canonical_support_ticket_type_key(ticket_type: Any) -> str:
    normalized = normalize_support_ticket_type_key(ticket_type)
    if not normalized:
        return "rest"
    slug = normalized.replace(" ", "_")
    if slug in _TICKET_TYPE_ROUTING_KEYS:
        return slug
    return _TICKET_TYPE_ROUTING_ALIASES.get(normalized, "rest")


def _case_variants(value: str) -> Set[str]:
    variants = {value, value.lower(), value.title(), value.upper()}
    if " " in value:
        underscored = value.replace(" ", "_")
        variants.add(underscored)
        variants.add(underscored.upper())
    return variants


def raw_field_values_for_type_key(type_key: str) -> frozenset[str]:
    """Raw ``data`` field values that canonicalize to ``type_key`` (for exact JSON Q filters)."""
    candidates = {type_key, type_key.replace("_", " ")}
    for alias, target in _TICKET_TYPE_ROUTING_ALIASES.items():
        if target == type_key:
            candidates.add(alias)

    validated: Set[str] = set()
    for candidate in candidates:
        if canonical_support_ticket_type_key(candidate) != type_key:
            continue
        validated.update(_case_variants(candidate))
    return frozenset(validated)


def q_record_support_ticket_type_key(type_key: str) -> Q:
    """Q filter for records whose ``support_ticket_type``/``poster`` match ``type_key``."""
    clauses = [
        Q(data__support_ticket_type=value) | Q(data__poster=value)
        for value in raw_field_values_for_type_key(type_key)
    ]
    if not clauses:
        return Q(pk__in=[])
    combined = clauses[0]
    for clause in clauses[1:]:
        combined |= clause
    return combined


def q_record_self_trial() -> Q:
    return q_record_support_ticket_type_key(SELF_TRIAL_TICKET_TYPE_KEY)
