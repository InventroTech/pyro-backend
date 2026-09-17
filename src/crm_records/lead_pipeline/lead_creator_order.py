"""
Alternate next-lead ORDER BY for groups with Lead Creator Prioritization.

Default pull_strategy ranks calendar day first. This module ranks:

  0-attempt → Lead Creator → Day 0 / Day -1 → score → district
"""

from __future__ import annotations

from typing import Any, Sequence

from crm_records.lead_pipeline.pull_strategy import (
    PullStrategyApplier,
    _parse_order_token,
)

_CREATOR_ORDER_FIELDS = frozenset({"creator_priority", "lead_creator"})
_ATTEMPT_ORDER_FIELDS = frozenset({"call_attempts", "call_attempts_int"})


def tokens_for_creator_first(tokens: Sequence[Any]) -> list[str]:
    """0-attempt, then Lead Creator, then the rest (day/score). Keeps snoozed-due first."""
    filtered: list[str] = []
    has_attempts = False
    for raw in tokens:
        if not isinstance(raw, str):
            continue
        parsed = _parse_order_token(raw)
        if parsed and parsed[2] in _CREATOR_ORDER_FIELDS:
            continue
        if parsed and parsed[2] in _ATTEMPT_ORDER_FIELDS:
            has_attempts = True
        filtered.append(raw)

    result: list[str] = []
    inserted = False
    for raw in filtered:
        result.append(raw)
        parsed = _parse_order_token(raw)
        if parsed and parsed[2] in _ATTEMPT_ORDER_FIELDS:
            result.append("creator_priority")
            inserted = True
    if not inserted:
        prefix = ["call_attempts"] if not has_attempts else []
        prefix.append("creator_priority")
        if filtered:
            first = _parse_order_token(filtered[0])
            if first and first[2] == "is_expired_snoozed":
                result = [filtered[0], *prefix, *filtered[1:]]
            else:
                result = [*prefix, *filtered]
        else:
            result = prefix

    if not any(_token_field(t) == "district_priority" for t in result):
        result.append("district_priority")
    return result


def _token_field(token: str) -> str | None:
    parsed = _parse_order_token(token) if isinstance(token, str) else None
    return parsed[2] if parsed else None


class LeadCreatorOrderApplier(PullStrategyApplier):
    """Pull order for lead groups with ``prioritize_lead_creator`` enabled."""

    def _rewrite_order_tokens(self, tokens: list[str], strategy: dict) -> list[str]:
        return tokens_for_creator_first(tokens)
