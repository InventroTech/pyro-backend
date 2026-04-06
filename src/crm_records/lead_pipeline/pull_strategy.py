from __future__ import annotations

from typing import List

from django.db.models import F, QuerySet


class PullStrategyApplier:
    """
    Applies ordering and cooldown pre-filtering based on pull_strategy.
    """

    @staticmethod
    def _created_at_tiebreaker_key(strategy: dict) -> str:
        """Tiebreaker after call_attempts / score: lifo = newest created first, fifo = oldest first."""
        raw = strategy.get("tiebreaker")
        if raw is None:
            return "-created_at"
        tb = str(raw).strip().lower()
        if tb == "fifo":
            return "created_at"
        return "-created_at"

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
        created_at_key = self._created_at_tiebreaker_key(strategy)

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
                created_at_key,
                "-updated_at",
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
                    created_at_key,
                    "-updated_at",
                    "id",
                )
            else:
                qs = qs.order_by(
                    "call_attempts_int",
                    F("lead_score_for_sort").desc(nulls_last=True),
                    created_at_key,
                    "-updated_at",
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

