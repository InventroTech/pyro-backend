import time
import logging
import threading
from datetime import timedelta
from django.utils import timezone
from django.db import transaction

logger = logging.getLogger(__name__)

SCHEDULE = {
    "dispatch_data_sync": {"every_minutes": 480},
}


def run_brahma_loop():
    time.sleep(15)

    while True:
        try:
            from pyro_jobs.models import PyroJob

            for job_name, config in SCHEDULE.items():

                with transaction.atomic():
                    """
                    get_or_create is atomic — Postgres guarantees only ONE
                    worker creates the row even if all 3 workers run this
                    at the exact same millisecond.

                    How it works:
                      - All 3 workers try get_or_create at the same time
                      - Postgres lets only 1 through to INSERT
                      - The other 2 get the existing row back (created=False)
                      - So only 1 job row is ever created per schedule slot
                    """

                    # check if a future pending/running job already exists
                    already_scheduled = PyroJob.objects.filter(
                        job_name=job_name,
                        is_deleted=False,
                        status__in=[PyroJob.STATUS_PENDING, PyroJob.STATUS_RUNNING],
                        run_at__gt=timezone.now()
                    ).exists()

                    if already_scheduled:
                        continue

                    # find last completed run to anchor the next schedule
                    last_completed = PyroJob.objects.filter(
                        job_name=job_name,
                        status=PyroJob.STATUS_COMPLETED,
                    ).order_by("-completed_at").first()

                    if last_completed:
                        # next_run = last run time + interval (no drift)
                        next_run = last_completed.run_at + timedelta(minutes=config["every_minutes"])

                        # if we missed the window (e.g. server was down) → run immediately
                        if next_run < timezone.now():
                            next_run = timezone.now()
                    else:
                        # first time ever → run immediately
                        next_run = timezone.now()

                    # run_at is in defaults (not the lookup key) so that two
                    # workers racing through the same check always hit the
                    # same row — one creates, the other gets the existing one.
                    _, created = PyroJob.objects.get_or_create(
                        job_name=job_name,
                        status=PyroJob.STATUS_PENDING,
                        is_deleted=False,
                        defaults={"run_at": next_run, "payload": {}}
                    )

                    if created:
                        logger.info("[Brahma] Scheduled: %s → %s", job_name, next_run)

        except Exception as e:
            logger.error("[Brahma] Loop error: %s", e)

        time.sleep(60)


def start_brahma():
    thread = threading.Thread(
        target=run_brahma_loop,
        daemon=True,
        name="brahma"
    )
    thread.start()
    logger.info("[Brahma] Thread started")


def schedule_once(job_name, payload, run_at):
    from pyro_jobs.models import PyroJob
    PyroJob.objects.create(job_name=job_name, payload=payload, run_at=run_at)
    logger.info("[Brahma] One-off scheduled: %s → %s", job_name, run_at)
