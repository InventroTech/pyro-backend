"""
Hard-delete rows older than a cutoff from high-volume log and audit tables (object
history, event logs, rule execution logs, finished background jobs, and finished
pyro jobs). Configured via :setting:`LOG_RETENTION_DAYS`.

Deletes ALL matching rows per table (chunked, not row-count-capped), stopping
early only on a per-table time budget (:setting:`LOG_RETENTION_MAX_RUNTIME_SECONDS`,
default 300s) so the daily job can't run indefinitely. ``has_more=True`` in the
result means the time budget ran out before the table was fully drained; the
next scheduled run continues where this one left off. For a one-off backlog
clear-out, use ``manage.py purge_old_logs`` (unbounded runtime).

**Background jobs** and **pyro jobs** are only purged when ``status`` is
**COMPLETED** or **FAILED**, so still-active rows are never removed.

**Object history**: rows with ``persistent_history=True`` are never removed by this job.
That flag is set at insert time from :class:`core.models.TenantSettings.persistent_object_history`.

**Pyro jobs** (``pyro_jobs.models.PyroJob``) are tenant-agnostic (no ``tenant_id``
column) and are not ``SoftDeleteMixin``-based, so they're purged via the plain
``objects`` manager and a regular queryset ``.delete()`` rather than
``all_objects`` + ``hard_delete()``.
"""
from __future__ import annotations

import logging
import time
from datetime import timedelta

from django.conf import settings
from django.db import models
from django.utils import timezone

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1000
# Per-table safety valve: stop after this many seconds even if rows remain,
# rather than after a fixed row count. The job runner has no execution
# timeout of its own (see pyro_jobs/pyro_job_executor.py) other than a
# 30-minute stale-job reaper, so this must stay comfortably under that
# across all tables combined.
DEFAULT_MAX_RUNTIME_SECONDS_PER_TABLE = 300


def get_log_retention_days() -> int:
    return int(getattr(settings, "LOG_RETENTION_DAYS", 30))


def get_log_retention_chunk_size() -> int:
    return max(1, int(getattr(settings, "LOG_RETENTION_CHUNK_SIZE", DEFAULT_CHUNK_SIZE)))


def get_log_retention_max_runtime_seconds() -> int:
    return max(
        1,
        int(
            getattr(
                settings,
                "LOG_RETENTION_MAX_RUNTIME_SECONDS",
                DEFAULT_MAX_RUNTIME_SECONDS_PER_TABLE,
            )
        ),
    )


def purge_old_log_rows(
    *,
    days: int | None = None,
    chunk_size: int | None = None,
    max_runtime_seconds: int | None = None,
    tenant_id: str | None = None,
) -> dict[str, int | str | bool]:
    """
    Permanently remove rows with ``created_at`` strictly before
    ``now - timedelta(days)`` from ``ObjectHistory`` (only ``persistent_history=False``),
    ``EventLog``, ``RuleExecutionLog``, finished ``BackgroundJob`` rows, and finished
    ``PyroJob`` rows (the first four use ``all_objects`` +
    :meth:`~core.soft_delete.SoftDeleteQuerySet.hard_delete`; ``PyroJob`` has no
    soft-delete manager or ``tenant_id``, so it's purged via the plain ``objects``
    manager and a regular queryset ``.delete()``).

    Deletes ALL matching rows per table, chunked at ``chunk_size`` rows per
    statement, until either no rows remain or ``max_runtime_seconds`` elapses
    for that table. When the time budget runs out and more matching rows
    remain, ``has_more`` is ``True`` so callers can enqueue a follow-up job
    (the daily schedule will pick it back up automatically). Pass
    ``max_runtime_seconds=0`` for no time limit at all (run until fully done —
    intended for the one-off ``purge_old_logs`` management command, not the
    recurring job).

    Returns counts, the cutoff ISO timestamp used, and ``has_more``.
    """
    if days is None:
        days = get_log_retention_days()
    if days < 1:
        raise ValueError("days must be >= 1")
    if chunk_size is None:
        chunk_size = get_log_retention_chunk_size()
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    if max_runtime_seconds is None:
        max_runtime_seconds = get_log_retention_max_runtime_seconds()
    if max_runtime_seconds < 0:
        raise ValueError("max_runtime_seconds must be >= 0")

    from object_history.models import ObjectHistory
    from crm_records.models import EventLog, RuleExecutionLog
    from background_jobs.models import BackgroundJob, JobStatus
    from pyro_jobs.models import PyroJob

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
        (
            "pyro_jobs",
            PyroJob,
            {
                "created_at__lt": before,
                "status__in": [PyroJob.STATUS_COMPLETED, PyroJob.STATUS_FAILED],
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
            max_runtime_seconds,
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


def _manager(model: type[models.Model]) -> models.Manager:
    """``all_objects`` for SoftDeleteMixin models, else the plain default manager
    (e.g. ``PyroJob``, which has no soft-delete manager)."""
    return getattr(model, "all_objects", model.objects)


def _chunked_hard_delete(
    model: type[models.Model],
    filter_kw: dict,
    chunk_size: int,
    max_runtime_seconds: int,
) -> tuple[int, bool]:
    """
    Delete ALL rows matching ``filter_kw``, one ``chunk_size``-row DELETE at a
    time, until no rows remain or ``max_runtime_seconds`` elapses (0 = no
    limit — loops until fully done).

    Rows are selected in ``created_at`` order so tenant/time indexes can be used
    (``ORDER BY pk`` forces a costly sort on large tables).
    """
    manager = _manager(model)
    total = 0
    deadline = None if max_runtime_seconds == 0 else time.monotonic() + max_runtime_seconds
    while True:
        pks = list(
            manager.filter(**filter_kw)
            .order_by("created_at", "pk")
            .values_list("pk", flat=True)[:chunk_size]
        )
        if not pks:
            return total, False
        qs = manager.filter(pk__in=pks)
        n, _ = qs.hard_delete() if hasattr(qs, "hard_delete") else qs.delete()
        total += n
        if len(pks) < chunk_size:
            return total, False
        if deadline is not None and time.monotonic() >= deadline:
            return total, True
