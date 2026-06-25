"""
Hard-delete rows older than a cutoff from high-volume log and audit tables (object
history, event logs, rule execution logs, and finished background jobs). Configured
via :setting:`LOG_RETENTION_DAYS`.

**Background jobs** are only purged when ``status`` is **COMPLETED** or **FAILED**,
so PENDING / PROCESSING / RETRYING rows are never removed.

**Object history**: rows with ``persistent_history=True`` are never removed by this job.
That flag is set at insert time from :class:`core.models.TenantSettings.persistent_object_history`.
"""
from __future__ import annotations

import logging
from datetime import timedelta

from django.conf import settings
from django.db import models
from django.utils import timezone

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 500
DEFAULT_MAX_CHUNKS_PER_TABLE = 20


def get_log_retention_days() -> int:
    return int(getattr(settings, "LOG_RETENTION_DAYS", 30))


def get_log_retention_chunk_size() -> int:
    return max(1, int(getattr(settings, "LOG_RETENTION_CHUNK_SIZE", DEFAULT_CHUNK_SIZE)))


def get_log_retention_max_chunks_per_table() -> int:
    return max(
        1,
        int(
            getattr(
                settings,
                "LOG_RETENTION_MAX_CHUNKS_PER_TABLE",
                DEFAULT_MAX_CHUNKS_PER_TABLE,
            )
        ),
    )


def purge_old_log_rows(
    *,
    days: int | None = None,
    chunk_size: int | None = None,
    max_chunks_per_table: int | None = None,
    tenant_id: str | None = None,
) -> dict[str, int | str | bool]:
    """
    Permanently remove rows with ``created_at`` strictly before
    ``now - timedelta(days)`` from ``ObjectHistory`` (only ``persistent_history=False``),
    ``EventLog``, ``RuleExecutionLog``, and finished ``BackgroundJob`` rows (uses
    ``all_objects`` + :meth:`~core.soft_delete.SoftDeleteQuerySet.hard_delete`).

    Deletes at most ``chunk_size * max_chunks_per_table`` rows per table per call.
    When the cap is hit and more matching rows remain, ``has_more`` is ``True`` so
    callers can enqueue a follow-up job.

    Returns counts, the cutoff ISO timestamp used, and ``has_more``.
    """
    if days is None:
        days = get_log_retention_days()
    if days < 1:
        raise ValueError("days must be >= 1")
    if chunk_size is None:
        chunk_size = get_log_retention_chunk_size()
    if max_chunks_per_table is None:
        max_chunks_per_table = get_log_retention_max_chunks_per_table()

    from object_history.models import ObjectHistory
    from crm_records.models import EventLog, RuleExecutionLog
    from background_jobs.models import BackgroundJob, JobStatus

    before = timezone.now() - timedelta(days=days)
    tenant_filter: dict = {}
    if tenant_id:
        tenant_filter = {"tenant_id": tenant_id}

    table_specs: list[tuple[str, type[models.Model], dict]] = [
        (
            "object_history",
            ObjectHistory,
            {"created_at__lt": before, "persistent_history": False, **tenant_filter},
        ),
        (
            "event_logs",
            EventLog,
            {"created_at__lt": before, **tenant_filter},
        ),
        (
            "rule_exec_logs",
            RuleExecutionLog,
            {"created_at__lt": before, **tenant_filter},
        ),
        (
            "background_jobs",
            BackgroundJob,
            {
                "created_at__lt": before,
                "status__in": [JobStatus.COMPLETED, JobStatus.FAILED],
                **tenant_filter,
            },
        ),
    ]

    out: dict[str, int | str | bool] = {
        "cutoff": before.isoformat(),
        "days": days,
        "has_more": False,
    }
    for key, model, filter_kw in table_specs:
        deleted, has_more = _chunked_hard_delete(
            model,
            filter_kw,
            chunk_size,
            max_chunks_per_table,
        )
        out[key] = deleted
        if has_more:
            out["has_more"] = True

    logger.info(
        "[log_retention] purged before=%s days=%s has_more=%s counts=%s",
        before.isoformat(),
        days,
        out["has_more"],
        {k: v for k, v in out.items() if k not in ("cutoff", "days", "has_more")},
    )
    return out


def _chunked_hard_delete(
    model: type[models.Model],
    filter_kw: dict,
    chunk_size: int,
    max_chunks: int,
) -> tuple[int, bool]:
    """
    Delete up to ``chunk_size * max_chunks`` rows matching ``filter_kw``.

    Rows are selected in ``created_at`` order so tenant/time indexes can be used
    (``ORDER BY pk`` forces a costly sort on large tables).
    """
    total = 0
    for _chunk_index in range(max_chunks):
        pks = list(
            model.all_objects.filter(**filter_kw)
            .order_by("created_at", "pk")
            .values_list("pk", flat=True)[:chunk_size]
        )
        if not pks:
            return total, False
        n, _ = model.all_objects.filter(pk__in=pks).hard_delete()
        total += n
        if len(pks) < chunk_size:
            return total, False
    return total, True
