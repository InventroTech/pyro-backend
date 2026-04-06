from __future__ import annotations

from typing import List

from django.db.models import F, QuerySet

_ALLOWED_TIEBREAKER_FIELDS = frozenset({"created_at", "updated_at"})


class PullStrategyApplier:
    """
    Applies ordering and cooldown pre-filtering based on pull_strategy.
    """

    @staticmethod
    def _normalize_tiebreaker_field(strategy: dict) -> str:
        """From ``pull_strategy.tiebreaker_field``; invalid or missing → ``created_at``."""
        raw = strategy.get("tiebreaker_field")
        if isinstance(raw, str):
            name = raw.strip().lower()
            if name in _ALLOWED_TIEBREAKER_FIELDS:
                return name
        return "created_at"

    @staticmethod
    def _tiebreaker_is_ascending(strategy: dict) -> bool:
        """
        ``pull_strategy.tiebreaker``: ``asc`` / ``desc`` (legacy ``fifo`` / ``lifo``).
        Missing or unknown → ``desc`` (newest first on ``tiebreaker_field``).
        """
        raw = strategy.get("tiebreaker")
        if raw is None:
            return False
        if not isinstance(raw, str):
            return False
        t = raw.strip().lower()
        if t == "asc":
            return True
        if t == "desc":
            return False
        if t == "fifo":
            return True
        if t == "lifo":
            return False
        return False

    @staticmethod
    def _tiebreaker_primary_order_key(strategy: dict) -> str:
        """Primary ORDER BY fragment for ``tiebreaker_field`` (asc vs desc)."""
        field = PullStrategyApplier._normalize_tiebreaker_field(strategy)
        if PullStrategyApplier._tiebreaker_is_ascending(strategy):
            return field
        return f"-{field}"

    @staticmethod
    def _tiebreaker_secondary_order_key(strategy: dict) -> str:
        """The other model timestamp, descending, when the primary tiebreak column ties."""
        field = PullStrategyApplier._normalize_tiebreaker_field(strategy)
        other = "updated_at" if field == "created_at" else "created_at"
        return f"-{other}"

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

    def apply(self, *, qs: QuerySet, strategy: dict, now_iso: str) -> QuerySet:
        qs = qs.extra(where=[self._NEXT_CALL_READY_WHERE])

        ignore_score_sources = strategy.get("ignore_score_for_sources", []) or []
        order_by = strategy.get("order_by", "score_desc")
        include_snoozed_due = bool(strategy.get("include_snoozed_due", False))

        call_attempts_expr = "COALESCE((data->>'call_attempts')::int, 0)"

        lead_score_for_sort = self._build_score_expr(ignore_score_sources)
        tiebreaker_key = self._tiebreaker_primary_order_key(strategy)
        secondary_ts_key = self._tiebreaker_secondary_order_key(strategy)

        if include_snoozed_due:
            is_expired_snoozed_expr = """
                CASE
                    WHEN data->>'lead_stage' = 'SNOOZED'
                    AND (data->>'next_call_at') IS NOT NULL
                    AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
                    AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                    AND (data->>'next_call_at')::timestamptz <= NOW()
                    THEN 0 ELSE 1
                END
            """

            qs = qs.extra(
                select={
                    "call_attempts_int": call_attempts_expr,
                    "lead_score_for_sort": lead_score_for_sort,
                    "is_expired_snoozed": is_expired_snoozed_expr,
                }
            ).order_by(
                "is_expired_snoozed",
                "call_attempts_int",
                F("lead_score_for_sort").desc(nulls_last=True),
                tiebreaker_key,
                secondary_ts_key,
                "id",
            )
        else:
            qs = qs.extra(
                select={
                    "call_attempts_int": call_attempts_expr,
                    "lead_score_for_sort": lead_score_for_sort,
                }
            )

            if order_by == "call_attempts_asc":
                qs = qs.order_by(
                    "call_attempts_int",
                    tiebreaker_key,
                    secondary_ts_key,
                    "id",
                )
            else:
                qs = qs.order_by(
                    "call_attempts_int",
                    F("lead_score_for_sort").desc(nulls_last=True),
                    tiebreaker_key,
                    secondary_ts_key,
                    "id",
                )

        return qs

    def _build_score_expr(self, ignore_score_sources: List[str]) -> str:
        # Sources that use a neutral sort key come from pull_strategy.ignore_score_for_sources only.
        sources = set(ignore_score_sources or [])

        if not sources:
            return "COALESCE((data->>'lead_score')::float, -1)"
        source_list = ", ".join(f"'{s}'" for s in sources)
        return f"""
            CASE
                WHEN data->>'lead_source' IN ({source_list}) THEN 0
                ELSE COALESCE((data->>'lead_score')::float, -1)
            END
        """

