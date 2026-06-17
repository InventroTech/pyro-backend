import time
import logging
import threading
from datetime import timedelta
from django.utils import timezone
from django.db import transaction

logger = logging.getLogger(__name__)

RETRY_DELAYS = [60, 300]


def fetch_and_lock_job(PyroJob):
    """
    Atomically fetch one due job and lock it so no other worker can pick it up.

    How it works:
      - transaction.atomic() opens a DB transaction
      - select_for_update() tells Postgres: lock this row
      - skip_locked=True tells Postgres: if the row is already locked by
        another worker, skip it and move to the next one
      - We immediately set status=RUNNING inside the same transaction
      - When the transaction commits, the lock is released but status is
        already RUNNING so other workers won't touch it
    """
    with transaction.atomic():
        job = (
            PyroJob.objects
            .select_for_update(skip_locked=True)
            .filter(
                status=PyroJob.STATUS_PENDING,
                is_deleted=False,
                run_at__lte=timezone.now()
            )
            .first()
        )

        if job:
            # claim the job inside the same transaction
            # this is atomic — no other worker can sneak in between
            job.status     = PyroJob.STATUS_RUNNING
            job.started_at = timezone.now()
            job.attempts   = job.attempts + 1
            job.save(update_fields=["status", "started_at", "attempts"])

    return job


def run_vishnu_loop():
    time.sleep(10)

    while True:
        try:
            from pyro_jobs.models import PyroJob
            from pyro_jobs.handlers import JOB_HANDLERS

            # keep picking up jobs until there are none left
            while True:
                job = fetch_and_lock_job(PyroJob)

                # no more due jobs right now — break inner loop
                if not job:
                    break

                try:
                    handler = JOB_HANDLERS.get(job.job_name)

                    if handler:
                        logger.info(
                            "[Vishnu] Running: %s (attempt %s/%s)",
                            job.job_name, job.attempts, job.max_attempts
                        )
                        handler(job.payload)

                        job.status       = PyroJob.STATUS_COMPLETED
                        job.completed_at = timezone.now()
                        job.is_deleted   = True
                        job.save(update_fields=["status", "completed_at", "is_deleted"])
                        logger.info("[Vishnu] Completed: %s", job.job_name)

                    else:
                        logger.error("[Vishnu] No handler found for: %s", job.job_name)
                        job.status     = PyroJob.STATUS_FAILED
                        job.error      = f"No handler registered for: {job.job_name}"
                        job.is_deleted = True
                        job.save(update_fields=["status", "error", "is_deleted"])

                except Exception as e:
                    logger.error(
                        "[Vishnu] Job failed: %s → %s (attempt %s/%s)",
                        job.job_name, e, job.attempts, job.max_attempts
                    )
                    job.error = str(e)

                    if job.attempts < job.max_attempts:
                        delay      = RETRY_DELAYS[min(job.attempts - 1, len(RETRY_DELAYS) - 1)]
                        job.status = PyroJob.STATUS_PENDING
                        job.run_at = timezone.now() + timedelta(seconds=delay)
                        job.save(update_fields=["status", "error", "run_at", "attempts"])
                        logger.info(
                            "[Vishnu] Retry in %ss: %s (attempt %s/%s)",
                            delay, job.job_name, job.attempts, job.max_attempts
                        )
                    else:
                        job.status     = PyroJob.STATUS_FAILED
                        job.is_deleted = True
                        job.save(update_fields=["status", "error", "is_deleted", "attempts"])
                        logger.error(
                            "[Vishnu] Permanent failure: %s after %s attempts",
                            job.job_name, job.attempts
                        )

        except Exception as e:
            logger.error("[Vishnu] Loop error: %s", e)

        time.sleep(5)


def start_vishnu():
    thread = threading.Thread(
        target=run_vishnu_loop,
        daemon=True,
        name="vishnu"
    )
    thread.start()
    logger.info("[Vishnu] Thread started")
