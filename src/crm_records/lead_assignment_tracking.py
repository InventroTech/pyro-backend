"""
Lead JSON fields for the 12-hour reassignment rule (NOT_CONNECTED segregation).

Business rule (flowchart): "Has it been 12 hours since the lead assigned for the
first time TODAY?":

- On every transition **unassigned → assigned**, we set an anchor timestamp
  ``first_assigned_today_at`` (UTC ISO) and ``first_assignment_today_date`` (local
  calendar date ``YYYY-MM-DD``) for observability.

- The background job releases NOT_CONNECTED leads when
  ``first_assigned_today_at + 12 hours <= now``, then clears ``assigned_to`` while
  keeping ``lead_stage`` = NOT_CONNECTED (pool / segregation).

``first_assigned_at`` / ``first_assigned_to`` remain for daily-limit semantics
(unchanged). Legacy ``not_connected_unassign_at`` is still honored in the job
only when ``first_assigned_today_at`` is absent (migration-friendly).
"""

from __future__ import annotations

from typing import Any, Optional

from django.utils import timezone

# Keys stored on Record.data (JSON)
FIRST_ASSIGNED_TODAY_AT = "first_assigned_today_at"
FIRST_ASSIGNMENT_TODAY_DATE = "first_assignment_today_date"


def set_first_assignment_today_anchor(*, now=None) -> dict:
    """
    Return fields to merge into ``record.data`` when the lead becomes assigned
    from an unassigned state (any path: pool, retry, rule engine, partner).
    """
    if now is None:
        now = timezone.now()
    local = timezone.localtime(now)
    return {
        FIRST_ASSIGNED_TODAY_AT: now.isoformat(),
        FIRST_ASSIGNMENT_TODAY_DATE: local.date().isoformat(),
    }


def merge_first_assignment_today_anchor(target: dict, now=None) -> None:
    """In-place merge of anchor fields into ``target`` (e.g. resolved_updates or data)."""
    target.update(set_first_assignment_today_anchor(now=now))


def is_assigned_value(value: Optional[Any]) -> bool:
    if value is None:
        return False
    s = str(value).strip()
    return s not in ("", "null", "None", "none")


def is_unassigned_value(value: Optional[Any]) -> bool:
    return not is_assigned_value(value)
