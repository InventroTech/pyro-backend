from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from django.db.models import Q, QuerySet
from django.utils import timezone

from crm_records.models import Record


@dataclass(frozen=True)
class DailyLimitStatus:
    daily_limit: Optional[int]
    assigned_today: int
    is_reached: bool


class DailyLimitChecker:
    """
    Extracted from GetNextLeadView Step 2.5.

    ``entity_type`` defaults to ``lead``. Optional ``type_q`` further scopes the
    count (e.g. Self Trial vs other support tickets).
    """

    def count_assigned_today(
        self,
        *,
        tenant,
        user_identifier: str,
        now,
        entity_type: str = "lead",
        type_q: Optional[Q] = None,
        resolution_statuses: Optional[list[str]] = None,
    ) -> int:
        """Count records first-assigned to ``user_identifier`` since local start of day."""
        if timezone.is_aware(now):
            start_of_day = timezone.localtime(now).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        else:
            start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

        qs = Record.objects.filter(tenant=tenant, entity_type=entity_type)
        if type_q is not None:
            qs = qs.filter(type_q)
        if resolution_statuses:
            qs = qs.filter(data__resolution_status__in=list(resolution_statuses))
        return qs.extra(
            where=[
                """
                (
                    data->>'first_assigned_to' = %s
                    AND data->>'first_assigned_at' IS NOT NULL
                    AND data->>'first_assigned_at' != ''
                    AND (data->>'first_assigned_at')::timestamptz >= %s
                )
                OR (
                    (data->>'first_assigned_at' IS NULL OR TRIM(COALESCE(data->>'first_assigned_at', '')) = '')
                    AND (data->>'assigned_to') IS NOT NULL
                    AND TRIM(COALESCE(data->>'assigned_to', '')) != ''
                    AND LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) NOT IN ('null', 'none')
                    AND data->>'assigned_to' = %s
                    AND updated_at >= %s
                    AND COALESCE((data->>'call_attempts')::int, 0) = 0
                )
                """
            ],
            params=[user_identifier, start_of_day, user_identifier, start_of_day],
        ).count()

    def check(
        self,
        *,
        tenant,
        user_identifier: str,
        daily_limit: Optional[int],
        now,
        debug: bool,
        entity_type: str = "lead",
        type_q: Optional[Q] = None,
    ) -> DailyLimitStatus:
        if daily_limit is None:
            return DailyLimitStatus(daily_limit=None, assigned_today=0, is_reached=False)

        try:
            daily_limit_int = int(daily_limit)
        except (TypeError, ValueError):
            return DailyLimitStatus(daily_limit=daily_limit, assigned_today=0, is_reached=False)

        if daily_limit_int < 0:
            return DailyLimitStatus(daily_limit=daily_limit_int, assigned_today=0, is_reached=False)

        assigned_today = self.count_assigned_today(
            tenant=tenant,
            user_identifier=user_identifier,
            now=now,
            entity_type=entity_type,
            type_q=type_q,
        )

        is_reached = assigned_today >= daily_limit_int and not debug
        return DailyLimitStatus(
            daily_limit=daily_limit_int,
            assigned_today=assigned_today,
            is_reached=is_reached,
        )

