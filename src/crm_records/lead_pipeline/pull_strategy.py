from __future__ import annotations

from typing import Any, List, Optional, Sequence

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
# Model columns or JSON timestamptz keys usable with day(...)
_DAY_MODEL_FIELDS = frozenset({"created_at"})
_DAY_JSON_FIELDS = frozenset({"first_assigned_at"})
_DAY_FIELDS = _DAY_MODEL_FIELDS | _DAY_JSON_FIELDS


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


# Django .extra() treats % as a param placeholder — double it for LIKE.
_REFERRAL_SOURCE_SQL = (
    "UPPER(TRIM(COALESCE(data->>'lead_source', ''))) LIKE '%%REFERRAL%%'"
)


def _sql_literal(value: str) -> str:
    return value.strip().replace("'", "''").lower()


def _json_id_rank_sql(field: str, rm_id: str) -> str:
    """
    Soft-rank a JSON ID field (``data->>'…'`` is always text, so string/number both work):
      0 = matches RM id
      1 = lead has a (different) non-blank id
      2 = id blank / missing
    """
    safe = _sql_literal(rm_id)
    raw = f"TRIM(COALESCE(data->>'{field}', ''))"
    present = f"{raw} != '' AND LOWER({raw}) NOT IN ('null', 'none')"
    return f"""
        CASE
            WHEN {present} AND LOWER({raw}) = '{safe}' THEN 0
            WHEN {present} THEN 1
            ELSE 2
        END
    """


def _district_rank_sql(rm_district: str) -> str:
    """Soft-rank RM district against lead ``data.district_id``."""
    return _json_id_rank_sql("district_id", rm_district)


def _party_rank_sql(rm_party: str) -> str:
    """Soft-rank RM party against lead ``data.affiliated_party_id``."""
    return _json_id_rank_sql("affiliated_party_id", rm_party)


def _lead_creator_rank_sql(rm_email: str) -> str:
    """
    Soft-rank for referral Lead Creator vs RM email:
      0 = creator matches RM email
      1 = creator present but different RM
      2 = creator blank / missing
    """
    safe = _sql_literal(rm_email)
    creator_raw = "TRIM(COALESCE(data->>'lead_creator', ''))"
    creator_blank = f"""(
        {creator_raw} = ''
        OR LOWER({creator_raw}) IN ('null', 'none')
    )"""
    creator_matches = f"""(
        NOT ({creator_blank})
        AND LOWER({creator_raw}) = '{safe}'
    )"""
    return f"""
        CASE
            WHEN {creator_matches} THEN 0
            WHEN {creator_blank} THEN 2
            ELSE 1
        END
    """


def _routing_priority_sql(
    rm_district: Optional[str],
    rm_email: Optional[str],
) -> str:
    """
    First routing tiebreaker after ``pull_strategy.order``.

    Referral sources (``lead_source`` contains ``REFERRAL``): Lead Creator only —
    district is ignored. Non-referral: district rank (or ``0`` if RM has no district).
    """
    if rm_email and rm_district:
        return f"""
            CASE
                WHEN {_REFERRAL_SOURCE_SQL} THEN ({_lead_creator_rank_sql(rm_email)})
                ELSE ({_district_rank_sql(rm_district)})
            END
        """
    if rm_email:
        return f"""
            CASE
                WHEN {_REFERRAL_SOURCE_SQL} THEN ({_lead_creator_rank_sql(rm_email)})
                ELSE 0
            END
        """
    return f"""
        CASE
            WHEN {_REFERRAL_SOURCE_SQL} THEN 0
            ELSE ({_district_rank_sql(rm_district or "")})
        END
    """


def _party_priority_sql(rm_party: str) -> str:
    """Party tiebreaker for non-referral leads only; referral rows get ``0``."""
    return f"""
        CASE
            WHEN {_REFERRAL_SOURCE_SQL} THEN 0
            ELSE ({_party_rank_sql(rm_party)})
        END
    """


class PullStrategyApplier:
    """
    Applies next-call filter and ORDER BY from ``pull_strategy``.

    ``order``: sort keys (``-`` prefix = descending). Day bucketing: ``day(created_at)``
    or ``day(first_assigned_at)`` (JSON timestamptz).
    ``include_snoozed_due``: when true, due SNOOZED rows sort first (prepends ``is_expired_snoozed`` unless already in ``order``).
    ``rm_district`` / ``rm_email``: after normal order, first routing tiebreaker is
    ``data.district_id`` for non-referral leads, or ``data.lead_creator`` vs RM email
    for referral sources (``lead_source`` contains ``REFERRAL``). District/party do not
    apply to referrals.
    ``rm_party``: when set, soft-ranks matching ``data.affiliated_party_id`` after that
    (non-referral only). Soft-rank is always after ``pull_strategy.order``.
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
        require_next_call_ready: bool = True,
        rm_district: Optional[str] = None,
        rm_party: Optional[str] = None,
        rm_email: Optional[str] = None,
    ) -> QuerySet:
        if require_next_call_ready:
            qs = qs.extra(where=[self._NEXT_CALL_READY_WHERE])
        tokens = _resolve_order_tokens(strategy)
        return self._apply_order_list(
            qs,
            strategy=strategy,
            tokens=tokens,
            call_attempts_expr="COALESCE((data->>'call_attempts')::int, 0)",
            score_expr=self._build_score_expr(strategy.get("ignore_score_for_sources") or []),
            rm_district=(rm_district or "").strip() or None,
            rm_party=(rm_party or "").strip() or None,
            rm_email=(rm_email or "").strip() or None,
        )

    def _apply_order_list(
        self,
        qs: QuerySet,
        *,
        strategy: dict,
        tokens: Sequence[Any],
        call_attempts_expr: str,
        score_expr: str,
        rm_district: Optional[str] = None,
        rm_party: Optional[str] = None,
        rm_email: Optional[str] = None,
    ) -> QuerySet:
        tz = _day_timezone(strategy)
        select: dict[str, str] = {}
        order_parts: list[Any] = []

        # Normal pull_strategy order first; district/party/creator only as tiebreakers.
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

        # Referral → creator email; non-referral → district. Party is non-referral only.
        if rm_district or rm_email:
            select["routing_priority"] = _routing_priority_sql(rm_district, rm_email)
            # String alias (not F()) — Django resolves F() before extra(select=...).
            order_parts.append("routing_priority")

        if rm_party:
            select["party_priority"] = _party_priority_sql(rm_party)
            order_parts.append("party_priority")

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
        raise ValueError(
            f"day() only supports created_at or first_assigned_at, got: {field}"
        )
    safe_tz = tz.replace("'", "''")
    if field in _DAY_MODEL_FIELDS:
        return f"(timezone('{safe_tz}', created_at))::date"
    # JSON timestamptz (e.g. first_assigned_at)
    return (
        f"(timezone('{safe_tz}', "
        f"CASE "
        f"WHEN (data->>'{field}') IS NOT NULL "
        f"AND TRIM(COALESCE(data->>'{field}', '')) != '' "
        f"AND LOWER(TRIM(COALESCE(data->>'{field}', ''))) NOT IN ('null', 'none') "
        f"THEN (data->>'{field}')::timestamptz "
        f"ELSE NULL END"
        f"))::date"
    )
