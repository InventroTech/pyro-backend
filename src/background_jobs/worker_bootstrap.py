"""
Start configurable background job processor threads for Gunicorn and runserver.
"""
from __future__ import annotations

import logging
import os
import socket
import threading
from typing import List, Optional, Sequence, Tuple

from django.conf import settings

from .job_processor import JobProcessor

logger = logging.getLogger(__name__)


def parse_job_type_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _worker_settings(
    *,
    thread_count_override: Optional[int] = None,
    poll_interval_override: Optional[float] = None,
    batch_size_override: Optional[int] = None,
    settings_prefix: str = "BACKGROUND_JOB",
) -> tuple[int, float, int]:
    thread_count = max(
        1,
        int(
            thread_count_override
            if thread_count_override is not None
            else getattr(settings, f"{settings_prefix}_WORKER_THREADS", 1)
        ),
    )
    poll_interval = float(
        poll_interval_override
        if poll_interval_override is not None
        else getattr(settings, f"{settings_prefix}_POLL_INTERVAL", 1.0)
    )
    batch_size = max(
        1,
        int(
            batch_size_override
            if batch_size_override is not None
            else getattr(settings, f"{settings_prefix}_BATCH_SIZE", 10)
        ),
    )
    return thread_count, poll_interval, batch_size


def start_background_job_worker_threads(
    *,
    process_label: str | None = None,
    run_schedulers: bool = True,
    thread_count: Optional[int] = None,
    poll_interval: Optional[float] = None,
    batch_size: Optional[int] = None,
    job_types: Optional[Sequence[str]] = None,
    exclude_job_types: Optional[Sequence[str]] = None,
    settings_prefix: str = "BACKGROUND_JOB",
    blocking: bool = False,
) -> List[threading.Thread] | Tuple[List[threading.Thread], List[JobProcessor]]:
    """
    Spawn N threads that drain ``background_jobs``.

    Pass ``job_types`` for a dedicated pool (e.g. Mixpanel-only). Pass
    ``exclude_job_types`` on general workers so they leave those jobs for
    the dedicated pool.
    """
    resolved_thread_count, resolved_poll, resolved_batch = _worker_settings(
        thread_count_override=thread_count,
        poll_interval_override=poll_interval,
        batch_size_override=batch_size,
        settings_prefix=settings_prefix,
    )

    resolved_exclude = (
        tuple(exclude_job_types)
        if exclude_job_types is not None
        else parse_job_type_csv(getattr(settings, "BACKGROUND_JOB_EXCLUDE_JOB_TYPES", ""))
    )
    resolved_job_types = tuple(job_types) if job_types else None

    host = socket.gethostname()
    pid = os.getpid()
    label = process_label or f"{host}-{pid}"
    started: List[threading.Thread] = []
    processors: List[JobProcessor] = []

    for index in range(resolved_thread_count):
        worker_id = f"{label}-t{index}"
        processor = JobProcessor(
            worker_id=worker_id,
            job_types=resolved_job_types,
            exclude_job_types=resolved_exclude if resolved_job_types is None else None,
        )
        processors.append(processor)
        schedulers_on = run_schedulers and index == 0 and resolved_job_types is None
        worker_thread = threading.Thread(
            target=processor.start_worker_loop,
            kwargs={
                "poll_interval": resolved_poll,
                "batch_size": resolved_batch,
                "run_schedulers": schedulers_on,
            },
            daemon=not blocking,
            name=f"BackgroundJobWorker-{index}",
        )
        worker_thread.start()
        started.append(worker_thread)

    filter_bits = []
    if resolved_job_types:
        filter_bits.append(f"only={','.join(resolved_job_types)}")
    elif resolved_exclude:
        filter_bits.append(f"exclude={','.join(resolved_exclude)}")
    filter_label = f" filters=[{'; '.join(filter_bits)}]" if filter_bits else ""
    sched_label = "on" if run_schedulers and resolved_job_types is None else "off"

    log_msg = (
        f"Started {len(started)} background job worker thread(s) "
        f"(poll={resolved_poll}s batch={resolved_batch} schedulers={sched_label}) "
        f"label={label}{filter_label}"
    )
    logger.info(log_msg)
    print(f"[BACKGROUND_JOBS] {log_msg}", flush=True)
    if blocking:
        return started, processors
    return started
