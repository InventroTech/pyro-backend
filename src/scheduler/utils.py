from datetime import timedelta
import re
from django.utils import timezone

_INTERVAL = re.compile(r"^\s*(\d+)\s*(s|m|h|d)\s*$", re.I)

def apply_interval(base_dt, interval_str):
    """
    '30m' -> base_dt + 30 minutes; supports s/m/h/d.
    """
    m = _INTERVAL.match(interval_str)
    if not m:
        raise ValueError(f"Bad interval: {interval_str}")
    n, unit = int(m.group(1)), m.group(2).lower()
    if unit == "s": delta = timedelta(seconds=n)
    elif unit == "m": delta = timedelta(minutes=n)
    elif unit == "h": delta = timedelta(hours=n)
    else: delta = timedelta(days=n)
    return base_dt + delta

def next_due_from_policy(now, policy, attempt_index_zero_based):
    """
    Given attempt index (0 for 1st retry window), compute next due_at.
    If attempt_index >= len(intervals), repeat last interval.
    """
    intervals = policy.intervals or []
    idx = min(attempt_index_zero_based, max(0, len(intervals) - 1))
    interval = intervals[idx] if intervals else "24h"
    return apply_interval(now, interval)