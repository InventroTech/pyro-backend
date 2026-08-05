"""
Close Stale Self-Trial Support Tickets Job
==========================================
Closes expired support tickets across all tenants:

- Self Trial: created_at at least ``days`` old (default 15).
- Non-Self Trial: first_assigned_at at least ``other_days`` old (default 3).

Payload (all optional):
  days (int): Self Trial age from ticket creation (default 15).
  other_days (int): Non-Self Trial age from first assignment (default 3).
"""
from __future__ import annotations

import logging

from django.utils import timezone

logger = logging.getLogger(__name__)

_DEFAULT_DAYS = 15
_DEFAULT_OTHER_DAYS = 3


def run_close_stale_self_trial_support_tickets(payload: dict) -> dict:
    from background_jobs.job_handlers import (
        _close_stale_non_self_trial_by_first_assign_all_tenants,
        _close_stale_self_trial_support_tickets_all_tenants,
    )

    payload = payload or {}
    days = int(payload.get("days", _DEFAULT_DAYS))
    other_days = int(payload.get("other_days", _DEFAULT_OTHER_DAYS))
    if days < 1 or other_days < 1:
        raise ValueError("days and other_days must be >= 1")

    st_updated, n_tenants, st_cutoff = _close_stale_self_trial_support_tickets_all_tenants(days)
    other_updated, _, other_cutoff = _close_stale_non_self_trial_by_first_assign_all_tenants(
        days=other_days
    )
    records_updated = st_updated + other_updated

    logger.info(
        "[CloseStaleSelfTrialSupportTickets] all tenants self_trial=%s other=%s "
        "tenants=%s days=%s other_days=%s",
        st_updated,
        other_updated,
        n_tenants,
        days,
        other_days,
    )
    return {
        "success": True,
        "updated": records_updated,
        "self_trial_updated": st_updated,
        "other_updated": other_updated,
        "days": days,
        "other_days": other_days,
        "self_trial_cutoff": st_cutoff,
        "other_cutoff": other_cutoff,
        "tenants_processed": n_tenants,
        "timestamp": timezone.now().isoformat(),
    }
