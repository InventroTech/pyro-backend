"""
Entry point for Render Background Worker (no Django management command required).

Render Docker Command: python run_mixpanel_workers.py
"""
from __future__ import annotations

import os
import signal
import sys
import time

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from background_jobs.worker_bootstrap import start_background_job_worker_threads
from background_jobs.worker_types import MIXPANEL_JOB_TYPES


def main() -> None:
    threads, processors = start_background_job_worker_threads(
        process_label="mixpanel",
        run_schedulers=False,
        job_types=MIXPANEL_JOB_TYPES,
        settings_prefix="MIXPANEL_JOB",
        blocking=True,
    )

    def _shutdown(signum, _frame):
        print(f"Stopping Mixpanel workers (signal {signum})...", flush=True)
        for processor in processors:
            processor.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    types = ", ".join(MIXPANEL_JOB_TYPES)
    print(
        f"Running {len(threads)} Mixpanel worker thread(s) for [{types}]. "
        "SIGTERM to stop.",
        flush=True,
    )

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
