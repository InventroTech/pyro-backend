"""
Runs every 5 minutes (see pyro_jobs.pyro_job_creator.SCHEDULE). Fetch/threshold/
email logic lives in pyro_jobs.supabase_metrics_monitor. CPU is a rate
from two scrapes, so the first run after a restart reports memory only.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def run_check_supabase_metrics(payload: dict) -> dict:
    from pyro_jobs.supabase_metrics_monitor import check_supabase_metrics

    result = check_supabase_metrics()
    logger.debug("[CheckSupabaseMetrics] result=%s", result)
    return {"success": True, **result}
