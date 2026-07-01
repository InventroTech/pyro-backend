"""Job-type groupings for dedicated worker processes."""
from __future__ import annotations

from .models import JobType

# All jobs that call Mixpanel APIs — drain on a separate worker pool.
MIXPANEL_JOB_TYPES: tuple[str, ...] = (
    JobType.SEND_MIXPANEL_EVENT,
    JobType.SEND_RM_ASSIGNED_EVENT,
    JobType.SEND_CSE_ASSIGNED_EVENT,
)
