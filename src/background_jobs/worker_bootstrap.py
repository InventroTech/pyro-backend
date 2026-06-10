"""
Start configurable background job processor threads for Gunicorn and runserver.
"""
from __future__ import annotations

import logging
import os
import socket
import threading
from typing import List

from django.conf import settings

from .job_processor import JobProcessor

logger = logging.getLogger(__name__)


def _worker_settings() -> tuple[int, float, int]:
    thread_count = max(1, int(getattr(settings, "BACKGROUND_JOB_WORKER_THREADS", 1)))
    poll_interval = float(getattr(settings, "BACKGROUND_JOB_POLL_INTERVAL", 1.0))
    batch_size = max(1, int(getattr(settings, "BACKGROUND_JOB_BATCH_SIZE", 10)))
    return thread_count, poll_interval, batch_size


def start_background_job_worker_threads(*, process_label: str | None = None) -> List[threading.Thread]:
    """
    Spawn N daemon threads that drain ``background_jobs``.

    Thread 0 runs cron schedulers; other threads only process jobs (more throughput
    without multiplying cron enqueue ticks).
    """
    thread_count, poll_interval, batch_size = _worker_settings()
    host = socket.gethostname()
    pid = os.getpid()
    label = process_label or f"{host}-{pid}"
    started: List[threading.Thread] = []

    for index in range(thread_count):
        worker_id = f"{label}-t{index}"
        processor = JobProcessor(worker_id=worker_id)
        worker_thread = threading.Thread(
            target=processor.start_worker_loop,
            kwargs={
                "poll_interval": poll_interval,
                "batch_size": batch_size,
                "run_schedulers": index == 0,
            },
            daemon=True,
            name=f"BackgroundJobWorker-{index}",
        )
        worker_thread.start()
        started.append(worker_thread)

    log_msg = (
        f"Started {len(started)} background job worker thread(s) "
        f"(poll={poll_interval}s batch={batch_size}) label={label}"
    )
    logger.info(log_msg)
    print(f"[BACKGROUND_JOBS] {log_msg}", flush=True)
    return started
