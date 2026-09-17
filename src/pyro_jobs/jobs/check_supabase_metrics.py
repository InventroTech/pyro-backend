"""
Check Supabase Metrics Job
==========================
Polls Supabase's Prometheus metrics endpoint (CPU, memory) and emails an
alert when a threshold is crossed. Runs every 5 minutes (see
pyro_jobs.pyro_job_creator.SCHEDULE). The actual fetching/threshold/email
logic lives in background_jobs.supabase_metrics_monitor.

Note: CPU usage is a rate computed from two consecutive scrapes, so the
first run after a restart reports memory only — CPU shows up starting the
second run, 5 minutes later.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def run_check_supabase_metrics(payload: dict) -> dict:
    from background_jobs.supabase_metrics_monitor import check_supabase_metrics

    result = check_supabase_metrics()
    logger.debug("[CheckSupabaseMetrics] result=%s", result)
    return {"success": True, **result}
