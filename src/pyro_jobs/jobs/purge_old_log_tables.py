"""
Purge Old Log Tables Job
========================
Hard-deletes rows older than LOG_RETENTION_DAYS (default 30) from:
  - ObjectHistory  (only persistent_history=False rows)
  - EventLog
  - RuleExecutionLog
  - BackgroundJob  (only COMPLETED / FAILED rows — active queue rows are kept)

Payload (all optional):
  days (int): override LOG_RETENTION_DAYS setting for this run
  chunk_size (int): rows per delete chunk (default LOG_RETENTION_CHUNK_SIZE)
  max_chunks_per_table (int): max chunks per table per run (default LOG_RETENTION_MAX_CHUNKS_PER_TABLE)
"""
from __future__ import annotations

import logging

from core.log_retention import get_log_retention_days, purge_old_log_rows

logger = logging.getLogger(__name__)


def run_purge_old_log_tables(payload: dict) -> None:
    payload = payload or {}

    if "days" in payload:
        days = int(payload["days"])
    else:
        days = get_log_retention_days()

    if days < 1:
        raise ValueError("days must be >= 1")

    from core.log_retention import (
        get_log_retention_chunk_size,
        get_log_retention_max_chunks_per_table,
    )

    chunk_size = (
        int(payload["chunk_size"])
        if "chunk_size" in payload
        else get_log_retention_chunk_size()
    )
    max_chunks_per_table = (
        int(payload["max_chunks_per_table"])
        if "max_chunks_per_table" in payload
        else get_log_retention_max_chunks_per_table()
    )

    logger.info("[PurgeOldLogTables] Starting — retention=%s days", days)
    stats = purge_old_log_rows(
        days=days,
        chunk_size=chunk_size,
        max_chunks_per_table=max_chunks_per_table,
    )
    logger.info("[PurgeOldLogTables] Done — %s", stats)
    return {"success": True, **stats}
