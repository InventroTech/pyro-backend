"""
Gunicorn configuration file for background job worker startup.

This ensures background job worker threads start in each Gunicorn worker process,
not in the master process (important when using --preload flag).

Optional Prometheus multiprocess support:
  If PROMETHEUS_MULTIPROC_DIR is set in the environment, prepare the directory
  and mark workers dead on exit so scrapes can aggregate across workers.
"""

import logging
import os
import sys

logger = logging.getLogger("background_jobs")

workers = int(os.environ.get("WEB_CONCURRENCY", "2"))
bind = os.environ.get("GUNICORN_BIND", "0.0.0.0:8000")
timeout = int(os.environ.get("GUNICORN_TIMEOUT", "30"))
keepalive = int(os.environ.get("GUNICORN_KEEPALIVE", "2"))


def on_starting(server):
    """Prepare Prometheus multiprocess directory when explicitly enabled."""
    multiproc_dir = (os.environ.get("PROMETHEUS_MULTIPROC_DIR") or "").strip()
    if not multiproc_dir:
        return

    os.makedirs(multiproc_dir, exist_ok=True)
    for name in os.listdir(multiproc_dir):
        path = os.path.join(multiproc_dir, name)
        try:
            if os.path.isfile(path):
                os.unlink(path)
        except OSError:
            pass


def child_exit(server, worker):
    """Mark dead worker so Prometheus multiprocess registry drops its samples."""
    if not (os.environ.get("PROMETHEUS_MULTIPROC_DIR") or "").strip():
        return
    try:
        from prometheus_client import multiprocess

        multiprocess.mark_process_dead(worker.pid)
    except Exception:
        pass


def post_fork(server, worker):
    """Start general background job threads (Mixpanel runs on Render Background Worker)."""
    try:
        from background_jobs.worker_bootstrap import start_background_job_worker_threads

        start_background_job_worker_threads()
    except Exception as e:
        error_msg = f"Failed to start background job workers in Gunicorn worker: {e}"
        logger.error(error_msg, exc_info=True)
        print(f"[BACKGROUND_JOBS] ERROR: {error_msg}", flush=True, file=sys.stderr)
