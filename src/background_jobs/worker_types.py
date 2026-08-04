"""Job-type groupings for dedicated worker processes."""
from __future__ import annotations

# Mixpanel jobs have been moved to pyro_jobs — no dedicated bg worker pool needed.
MIXPANEL_JOB_TYPES: tuple[str, ...] = ()
