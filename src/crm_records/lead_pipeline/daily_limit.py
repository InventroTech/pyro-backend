from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from django.utils import timezone
from django.db.models import QuerySet

from crm_records.models import Record


@dataclass(frozen=True)
class DailyLimitStatus:
    daily_limit: Optional[int]
    assigned_today: int
    is_reached: bool


class DailyLimitChecker:
    """
    Extracted from GetNextLeadView Step 2.5.
    """

    def check(self, *, tenant, user_identifier: str, daily_limit: Optional[int], now, debug: bool) -> DailyLimitStatus:
        if daily_limit is None:
            return DailyLimitStatus(daily_limit=None, assigned_today=0, is_reached=False)

        try:
            daily_limit_int = int(daily_limit)
        except (TypeError, ValueError):
            return DailyLimitStatus(daily_limit=daily_limit, assigned_today=0, is_reached=False)

        if daily_limit_int < 0:
            return DailyLimitStatus(daily_limit=daily_limit_int, assigned_today=0, is_reached=False)

        if timezone.is_aware(now):
            start_of_day = timezone.localtime(now).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

        assigned_today = Record.objects.filter(tenant=tenant, entity_type="lead").extra(
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

        is_reached = assigned_today >= daily_limit_int and not debug
        return DailyLimitStatus(daily_limit=daily_limit_int, assigned_today=assigned_today, is_reached=is_reached)

