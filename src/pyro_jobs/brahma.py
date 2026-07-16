import time
import logging
import threading
from datetime import timedelta
from django.core.exceptions import MultipleObjectsReturned
from django.utils import timezone
from django.db import transaction, close_old_connections, ProgrammingError, OperationalError
from django.db.utils import InterfaceError

logger = logging.getLogger(__name__)

SCHEDULE = {
    "dispatch_data_sync":                {"every_minutes": 480},
    "purge_old_log_tables":              {"every_minutes": 1440},
    "snoozed_to_not_connected_midnight": {"daily_at_utc": "17:30"},
}


def _next_occurrence_utc(time_str: str, now):
    """Return the next UTC datetime for a HH:MM clock time (today if not yet passed, else tomorrow)."""
    h, m = (int(x) for x in time_str.split(":"))
    candidate = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def run_brahma_loop():
    time.sleep(15)

    while True:
        try:
            close_old_connections()
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

                    # check if any pending/running job already exists (regardless of run_at)
                    # run_at is intentionally excluded: a RUNNING job has a past run_at but
                    # must still block Brahma from scheduling a duplicate
                    already_scheduled = PyroJob.objects.filter(
                        job_name=job_name,
                        is_deleted=False,
                        status__in=[PyroJob.STATUS_PENDING, PyroJob.STATUS_RUNNING],
                    ).exists()

                    if already_scheduled:
                        continue

                    # find last completed run to anchor the next schedule
                    last_completed = PyroJob.objects.filter(
                        job_name=job_name,
                        status=PyroJob.STATUS_COMPLETED,
                    ).order_by("-completed_at").first()

                    now = timezone.now()

                    if "daily_at_utc" in config:
                        # Pin to a specific UTC clock time each day.
                        # Subsequent runs anchor from last run_at + 1 day (no drift).
                        # First run (or missed window) schedules the next occurrence.
                        if last_completed:
                            next_run = last_completed.run_at + timedelta(days=1)
                            if next_run < now:
                                next_run = _next_occurrence_utc(config["daily_at_utc"], now)
                        else:
                            next_run = _next_occurrence_utc(config["daily_at_utc"], now)
                    else:
                        if last_completed:
                            # next_run = last run time + interval (no drift)
                            next_run = last_completed.run_at + timedelta(minutes=config["every_minutes"])

                            # if we missed the window (e.g. server was down) → run immediately
                            if next_run < now:
                                next_run = now
                        else:
                            # first time ever → run immediately
                            next_run = now

                    # run_at is in defaults (not the lookup key) so that two
                    # workers racing through the same check always hit the
                    # same row — one creates, the other gets the existing one.
                    # MultipleObjectsReturned can occur in a tight race where
                    # N workers all pass the already_scheduled check before any
                    # of them commits — treat it as "already scheduled".
                    try:
                        _, created = PyroJob.objects.get_or_create(
                            job_name=job_name,
                            status=PyroJob.STATUS_PENDING,
                            is_deleted=False,
                            defaults={"run_at": next_run, "payload": {}}
                        )
                        if created:
                            logger.info("[Brahma] Scheduled: %s → %s", job_name, next_run)
                    except MultipleObjectsReturned:
                        logger.debug(
                            "[Brahma] Multiple pending rows detected for %s due to race; treating as already scheduled.",
                            job_name,
                        )

        except ProgrammingError as e:
            if "pyro_job" in str(e):
                logger.warning("[Brahma] pyro_job table not ready yet, waiting for migrations...")
                time.sleep(30)
                continue
            logger.error("[Brahma] Loop error: %s", e)
        except (InterfaceError, OperationalError) as e:
            logger.warning("[Brahma] Database connection error, reconnecting: %s", e)
            close_old_connections()
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
