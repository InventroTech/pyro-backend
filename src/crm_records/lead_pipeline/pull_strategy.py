from __future__ import annotations

from typing import List

from django.db.models import F, QuerySet


class PullStrategyApplier:
    """
    Applies ordering and cooldown pre-filtering based on pull_strategy.

    JSON keys (optional):
    - ``order_by``: ``"call_attempts_asc"`` — order by attempts, then ``-updated_at`` (no ``lead_score``).
      ``"score_desc"`` (default) — include ``lead_score`` in ordering.
    - ``ignore_score_for_sources``: when using ``score_desc``, neutral sort key for matching ``lead_source`` values.
    - ``include_snoozed_due``: expired snoozed rows sort first when True.
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

    def apply(self, *, qs: QuerySet, strategy: dict, now_iso: str) -> QuerySet:
        qs = qs.extra(where=[self._NEXT_CALL_READY_WHERE])

        ignore_score_sources = strategy.get("ignore_score_for_sources", []) or []
        order_by = strategy.get("order_by", "score_desc")
        include_snoozed_due = bool(strategy.get("include_snoozed_due", False))
        use_score = order_by != "call_attempts_asc"

        call_attempts_expr = "COALESCE((data->>'call_attempts')::int, 0)"

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

            if use_score:
                lead_score_for_sort = self._build_score_expr(ignore_score_sources)
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
                    "-updated_at",
                    "created_at",
                    "id",
                )
            else:
                qs = qs.extra(
                    select={
                        "call_attempts_int": call_attempts_expr,
                        "is_expired_snoozed": is_expired_snoozed_expr,
                    }
                ).order_by(
                    "is_expired_snoozed",
                    "call_attempts_int",
                    "-updated_at",
                    "created_at",
                    "id",
                )
        else:
            if use_score:
                lead_score_for_sort = self._build_score_expr(ignore_score_sources)
                qs = qs.extra(
                    select={
                        "call_attempts_int": call_attempts_expr,
                        "lead_score_for_sort": lead_score_for_sort,
                    }
                )
                qs = qs.order_by(
                    "call_attempts_int",
                    F("lead_score_for_sort").desc(nulls_last=True),
                    "-updated_at",
                    "created_at",
                    "id",
                )
            else:
                qs = qs.extra(select={"call_attempts_int": call_attempts_expr})
                qs = qs.order_by(
                    "call_attempts_int",
                    "-updated_at",
                    "created_at",
                    "id",
                )

        return qs

    def _build_score_expr(self, ignore_score_sources: List[str]) -> str:
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
