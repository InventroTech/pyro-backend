"""
Gunicorn configuration file for background job worker startup.

This ensures background job worker threads start in each Gunicorn worker process,
not in the master process (important when using --preload flag).
"""

import logging
import sys

logger = logging.getLogger("background_jobs")


def post_fork(server, worker):
    """Start background job processor threads after Gunicorn forks a worker."""
    try:
        from background_jobs.worker_bootstrap import start_background_job_worker_threads

        start_background_job_worker_threads()
    except Exception as e:
        error_msg = f"Failed to start background job workers in Gunicorn worker: {e}"
        logger.error(error_msg, exc_info=True)
        print(f"[BACKGROUND_JOBS] ERROR: {error_msg}", flush=True, file=sys.stderr)
