"""
Cross-process scheduler guards using PostgreSQL advisory locks.

Prevents multiple Gunicorn workers from enqueueing the same cron tick at once.
Does not cancel or skip any existing queued jobs.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from django.db import connection

logger = logging.getLogger(__name__)

SCHEDULER_LOCK_LEAD_CRON = 9_000_001
SCHEDULER_LOCK_DUMP_TICKETS = 9_000_002
SCHEDULER_LOCK_LOG_RETENTION = 9_000_003
SCHEDULER_LOCK_SNOOZED_MIDNIGHT = 9_000_004
SCHEDULER_LOCK_DISPATCH_SYNC = 9_000_005


@contextmanager
def scheduler_lock(lock_key: int) -> Iterator[bool]:
    acquired = False
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", [lock_key])
            acquired = bool(cursor.fetchone()[0])
        yield acquired
    finally:
        if acquired:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
            except Exception as exc:
                logger.warning("Failed to release scheduler lock %s: %s", lock_key, exc)
