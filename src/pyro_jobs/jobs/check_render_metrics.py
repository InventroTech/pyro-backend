"""
Runs every 5 minutes (see pyro_jobs.pyro_job_creator.SCHEDULE). Fetch/threshold/
email logic lives in pyro_jobs.render_metrics_monitor.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def run_check_render_metrics(payload: dict) -> dict:
    from pyro_jobs.render_metrics_monitor import check_render_metrics

    result = check_render_metrics()
    logger.debug("[CheckRenderMetrics] result=%s", result)
    return {"success": True, **result}
