import re
from datetime import timedelta
from django.utils import timezone
from .policy import CALL_POLICY_INTERVALS

_RX = re.compile(r"^\s*(\d+)\s*(s|m|h|d)\s*$", re.I)

def add_interval(base, spec):
    n, u = _RX.match(spec).groups()
    n = int(n); u = u.lower()
    if u == "s": delta = timedelta(seconds=n)
    elif u == "m": delta = timedelta(minutes=n)
    elif u == "h": delta = timedelta(hours=n)
    else: delta = timedelta(days=n)
    return base + delta

def next_due(now, attempt_index_zero_based):
    """
    attempt_index_zero_based: 0 for first retry window after attempt #1, etc.
    Clamp to last interval if we run out.
    """
    ivals = CALL_POLICY_INTERVALS or ["24h"]
    idx = min(attempt_index_zero_based, len(ivals) - 1)
    return add_interval(now, ivals[idx])
