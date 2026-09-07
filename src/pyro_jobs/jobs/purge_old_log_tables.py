"""
Purge Old Log Tables Job
========================
Hard-deletes rows older than LOG_RETENTION_DAYS (default 30) from:
  - ObjectHistory  (only persistent_history=False rows)
  - EventLog
  - RuleExecutionLog
  - BackgroundJob  (only COMPLETED / FAILED rows — active queue rows are kept)
  - PyroJob        (only COMPLETED / FAILED rows — active queue rows are kept)

Deletes ALL matching rows per table (not capped by row count); each table is
bounded only by a time budget so this daily job can't run indefinitely.

The regular schedule (SCHEDULE["purge_old_log_tables"]["every_minutes"] = 1440
in pyro_job_creator.py) is untouched by this. Instead, if any table's time
budget runs out before it's fully drained (has_more=True), this handler
schedules a one-off follow-up run 1 hour later via schedule_once() — so a
large backlog gets chipped away hourly instead of waiting a full day between
increments. Once every table catches up (has_more=False), no follow-up is
scheduled and the job reverts to its normal daily cadence.

Payload (all optional):
  days (int): override LOG_RETENTION_DAYS setting for this run
  chunk_size (int): rows per delete statement (default LOG_RETENTION_CHUNK_SIZE)
  max_runtime_seconds (int): per-table time budget in seconds, 0 = unbounded
    (default LOG_RETENTION_MAX_RUNTIME_SECONDS)
"""
from __future__ import annotations

import logging
from datetime import timedelta

from django.utils import timezone

from core.log_retention import (
    get_log_retention_chunk_size,
    get_log_retention_days,
    get_log_retention_max_runtime_seconds,
    purge_old_log_rows,
)

logger = logging.getLogger(__name__)

FOLLOW_UP_DELAY = timedelta(hours=1)


def run_purge_old_log_tables(payload: dict) -> None:
    payload = payload or {}

    if "days" in payload:
        days = int(payload["days"])
    else:
        days = get_log_retention_days()

    if days < 1:
        raise ValueError("days must be >= 1")

    chunk_size = (
        int(payload["chunk_size"])
        if "chunk_size" in payload
        else get_log_retention_chunk_size()
    )
    max_runtime_seconds = (
        int(payload["max_runtime_seconds"])
        if "max_runtime_seconds" in payload
        else get_log_retention_max_runtime_seconds()
    )
    if chunk_size < 1 or max_runtime_seconds < 0:
        raise ValueError("chunk_size must be >= 1 and max_runtime_seconds must be >= 0")

    logger.info("[PurgeOldLogTables] Starting — retention=%s days", days)
    stats = purge_old_log_rows(
        days=days,
        chunk_size=chunk_size,
        max_runtime_seconds=max_runtime_seconds,
    )
    logger.info("[PurgeOldLogTables] Done — %s", stats)

    if stats.get("has_more"):
        from pyro_jobs.pyro_job_creator import schedule_once

        follow_up_run_at = timezone.now() + FOLLOW_UP_DELAY
        schedule_once(
            "purge_old_log_tables",
            payload={
                "days": days,
                "chunk_size": chunk_size,
                "max_runtime_seconds": max_runtime_seconds,
            },
            run_at=follow_up_run_at,
        )
        logger.info(
            "[PurgeOldLogTables] has_more=True — follow-up run scheduled for %s",
            follow_up_run_at.isoformat(),
        )

    return {"success": True, **stats}
