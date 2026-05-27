from __future__ import annotations

from typing import Any, List, Sequence

from django.conf import settings
from django.db.models import F, QuerySet

_DEFAULT_DAY_TIMEZONE = "Asia/Kolkata"
_DEFAULT_ORDER = ["-lead_score", "-created_at"]
_MODEL_ORDER_FIELDS = frozenset({"created_at", "updated_at"})

_JSON_TS = """
    CASE
        WHEN (data->>'{key}') IS NOT NULL
            AND TRIM(COALESCE(data->>'{key}', '')) != ''
            AND LOWER(TRIM(COALESCE(data->>'{key}', ''))) NOT IN ('null', 'none')
        THEN (data->>'{key}')::timestamptz
        ELSE NULL
    END
"""
_NEXT_CALL_AT_TS = _JSON_TS.format(key="next_call_at")
_DAY_FIELDS = frozenset({"created_at"})


def _parse_order_token(token: str) -> tuple[bool, str, str] | None:
    """Return (descending, kind, field) for ``-day(foo)`` or ``-bar``."""
    token = token.strip()
    if not token:
        return None
    descending = token.startswith("-")
    body = token[1:] if descending else token
    if body.startswith("day(") and body.endswith(")"):
        field = body[4:-1].strip().lower()
        return (descending, "day", field) if field else None
    field = body.lower()
    return (descending, "field", field) if field else None


def _resolve_order_tokens(strategy: dict) -> list[str]:
    """Build final ``order`` list; prepend ``is_expired_snoozed`` when ``include_snoozed_due``."""
    raw = strategy.get("order")
    tokens = [t for t in raw if isinstance(t, str)] if isinstance(raw, list) and raw else list(_DEFAULT_ORDER)

    if not strategy.get("include_snoozed_due"):
        return tokens

    for token in tokens:
        parsed = _parse_order_token(token.strip())
        if parsed and parsed[2] == "is_expired_snoozed":
            return tokens
    return ["is_expired_snoozed", *tokens]


class PullStrategyApplier:
    """
    Applies next-call filter and ORDER BY from ``pull_strategy``.

    ``order``: sort keys (``-`` prefix = descending). Day bucketing: ``day(created_at)`` only.
    ``include_snoozed_due``: when true, due SNOOZED rows sort first (prepends ``is_expired_snoozed`` unless already in ``order``).
    """

    _NEXT_CALL_READY_WHERE = """
        (
            COALESCE((data->>'call_attempts')::int, 0) = 0
            OR (
                (data->>'next_call_at') IS NOT NULL
                AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
                AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                AND (data->>'next_call_at')::timestamptz <= NOW()
            )
        )
    """

    def apply(
        self,
        *,
        qs: QuerySet,
        strategy: dict,
        now_iso: str,
        enforce_next_call_ready: bool = True,
    ) -> QuerySet:
        if enforce_next_call_ready:
            qs = qs.extra(where=[self._NEXT_CALL_READY_WHERE])
        tokens = _resolve_order_tokens(strategy)
        return self._apply_order_list(
            qs,
            strategy=strategy,
            tokens=tokens,
            call_attempts_expr="COALESCE((data->>'call_attempts')::int, 0)",
            score_expr=self._build_score_expr(strategy.get("ignore_score_for_sources") or []),
        )

    def _apply_order_list(
        self,
        qs: QuerySet,
        *,
        strategy: dict,
        tokens: Sequence[Any],
        call_attempts_expr: str,
        score_expr: str,
    ) -> QuerySet:
        tz = _day_timezone(strategy)
        select: dict[str, str] = {}
        order_parts: list[Any] = []

        for raw in tokens:
            parsed = _parse_order_token(raw) if isinstance(raw, str) else None
            if not parsed:
                continue
            descending, kind, field = parsed

            if kind == "field" and field in _MODEL_ORDER_FIELDS:
                order_parts.append(f"-{field}" if descending else field)
                continue

            alias, sql = self._select_expr(
                kind=kind,
                field=field,
                tz=tz,
                call_attempts_expr=call_attempts_expr,
                score_expr=score_expr,
            )
            if alias not in select:
                select[alias] = sql
            expr = F(alias)
            order_parts.append(
                expr.desc(nulls_last=True) if descending else expr.asc(nulls_last=True)
            )

        order_parts.append("id")
        return qs.extra(select=select).order_by(*order_parts)

    def _select_expr(
        self,
        *,
        kind: str,
        field: str,
        tz: str,
        call_attempts_expr: str,
        score_expr: str,
    ) -> tuple[str, str]:
        if kind == "day":
            return f"sort_day_{field}", _day_sql(field, tz)
        if field in ("lead_score", "lead_score_for_sort"):
            return "lead_score_for_sort", score_expr
        if field in ("call_attempts", "call_attempts_int"):
            return "call_attempts_int", call_attempts_expr
        if field == "is_expired_snoozed":
            return "is_expired_snoozed", self._is_expired_snoozed_expr()
        if field == "next_call_at":
            return "sort_next_call_at", _NEXT_CALL_AT_TS
        raise ValueError(f"Unsupported pull_strategy order field: {field}")

    @staticmethod
    def _is_expired_snoozed_expr() -> str:
        return """
            CASE
                WHEN data->>'lead_stage' = 'SNOOZED'
                AND (data->>'next_call_at') IS NOT NULL
                AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
                AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                AND (data->>'next_call_at')::timestamptz <= NOW()
                THEN 0 ELSE 1
            END
        """

    def _build_score_expr(self, ignore_score_sources: List[str]) -> str:
        if not ignore_score_sources:
            return "COALESCE((data->>'lead_score')::float, -1)"
        source_list = ", ".join(f"'{s}'" for s in ignore_score_sources)
        return f"""
            CASE
                WHEN data->>'lead_source' IN ({source_list}) THEN 0
                ELSE COALESCE((data->>'lead_score')::float, -1)
            END
        """


def _day_timezone(strategy: dict) -> str:
    raw = strategy.get("day_timezone")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return getattr(settings, "LEAD_PIPELINE_DAY_TIMEZONE", None) or _DEFAULT_DAY_TIMEZONE


def _day_sql(field: str, tz: str) -> str:
    if field not in _DAY_FIELDS:
        raise ValueError(f"day() only supports created_at, got: {field}")
    safe_tz = tz.replace("'", "''")
    return f"(timezone('{safe_tz}', created_at))::date"
