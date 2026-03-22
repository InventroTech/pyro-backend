from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from django.utils import timezone

try:
    # Optional; matches current view behavior.
    from dateutil import parser as date_parser  # type: ignore
except ImportError:  # pragma: no cover
    date_parser = None

from datetime import timezone as std_utc


@dataclass
class CandidateSelector:
    """
    Cooldown/cadence check extracted from GetNextLeadView.
    """

    def is_due_for_call(self, lead_data: Any, now) -> bool:
        if not isinstance(lead_data, dict):
            return True

        try:
            call_attempts_int = int(lead_data.get("call_attempts") or 0)
        except (TypeError, ValueError):
            call_attempts_int = 0

        # Fresh leads are always due.
        if call_attempts_int == 0:
            return True

        raw = lead_data.get("next_call_at")
        if raw is None or raw == "" or raw == "null":
            return False

        try:
            if isinstance(raw, datetime):
                next_call_at = raw
            elif date_parser:
                next_call_at = date_parser.parse(str(raw))
            else:
                next_call_at = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))

            # Normalize timezone so comparisons behave like current code.
            if now.tzinfo is None and next_call_at.tzinfo:
                next_call_at = next_call_at.replace(tzinfo=None)
            elif now.tzinfo and next_call_at.tzinfo is None:
                next_call_at = next_call_at.replace(tzinfo=std_utc.utc)

            return next_call_at <= now
        except Exception:
            return False

