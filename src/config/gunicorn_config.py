"""
Gunicorn configuration file for background job worker startup.

This ensures background job worker threads start in each Gunicorn worker process,
not in the master process (important when using --preload flag).
"""

import logging
import os
import sys

logger = logging.getLogger("background_jobs")

workers = int(os.environ.get("WEB_CONCURRENCY", "2"))
bind = os.environ.get("GUNICORN_BIND", "0.0.0.0:8000")
timeout = int(os.environ.get("GUNICORN_TIMEOUT", "30"))
keepalive = int(os.environ.get("GUNICORN_KEEPALIVE", "2"))
max_requests = int(os.environ.get("GUNICORN_MAX_REQUESTS", "1000"))
max_requests_jitter = int(os.environ.get("GUNICORN_MAX_REQUESTS_JITTER", "100"))
graceful_timeout = int(os.environ.get("GUNICORN_GRACEFUL_TIMEOUT", "30"))


def post_fork(server, worker):
    """Start general background job threads (Mixpanel runs on Render Background Worker)."""
    try:
        from background_jobs.worker_bootstrap import start_background_job_worker_threads

        start_background_job_worker_threads()
    except Exception as e:
        error_msg = f"Failed to start background job workers in Gunicorn worker: {e}"
        logger.error(error_msg, exc_info=True)
        print(f"[BACKGROUND_JOBS] ERROR: {error_msg}", flush=True, file=sys.stderr)
