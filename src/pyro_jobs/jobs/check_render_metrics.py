"""
Check Render Metrics Job
========================
Polls the Render REST API (CPU, memory, HTTP P95 latency) and emails an
alert when a threshold is crossed. Runs every 5 minutes (see
pyro_jobs.pyro_job_creator.SCHEDULE). The actual fetching/threshold/email
logic lives in background_jobs.render_metrics_monitor.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def run_check_render_metrics(payload: dict) -> dict:
    from background_jobs.render_metrics_monitor import check_render_metrics

    result = check_render_metrics()
    logger.debug("[CheckRenderMetrics] result=%s", result)
    return {"success": True, **result}
