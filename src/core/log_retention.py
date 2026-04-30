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

DEFAULT_CHUNK_SIZE = 2_000


def get_log_retention_days() -> int:
    return int(getattr(settings, "LOG_RETENTION_DAYS", 30))


def purge_old_log_rows(
    *,
    days: int | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, int | str]:
    """
    Permanently remove rows with ``created_at`` strictly before
    ``now - timedelta(days)`` from ``ObjectHistory`` (only ``persistent_history=False``),
    ``EventLog``, ``RuleExecutionLog``, and finished ``BackgroundJob`` rows (uses
    ``all_objects`` + :meth:`~core.soft_delete.SoftDeleteQuerySet.hard_delete`).

    Returns counts and the cutoff ISO timestamp used.
    """
    if days is None:
        days = get_log_retention_days()
    if days < 1:
        raise ValueError("days must be >= 1")

    from object_history.models import ObjectHistory
    from crm_records.models import EventLog, RuleExecutionLog
    from background_jobs.models import BackgroundJob, JobStatus

    before = timezone.now() - timedelta(days=days)
    out: dict[str, int | str] = {
        "cutoff": before.isoformat(),
        "days": days,
        "object_history": _chunked_hard_delete(
            ObjectHistory,
            {"created_at__lt": before, "persistent_history": False},
            chunk_size,
        ),
        "event_logs": _chunked_hard_delete(
            EventLog, {"created_at__lt": before}, chunk_size
        ),
        "rule_exec_logs": _chunked_hard_delete(
            RuleExecutionLog, {"created_at__lt": before}, chunk_size
        ),
        "background_jobs": _chunked_hard_delete(
            BackgroundJob,
            {
                "created_at__lt": before,
                "status__in": [JobStatus.COMPLETED, JobStatus.FAILED],
            },
            chunk_size,
        ),
    }
    logger.info(
        "[log_retention] purged before=%s days=%s counts=%s",
        before.isoformat(),
        days,
        {k: v for k, v in out.items() if k not in ("cutoff", "days")},
    )
    return out


def _chunked_hard_delete(
    model: type[models.Model],
    filter_kw: dict,
    chunk_size: int,
) -> int:
    total = 0
    while True:
        pks = list(
            model.all_objects.filter(**filter_kw)
            .order_by("pk")
            .values_list("pk", flat=True)[:chunk_size]
        )
        if not pks:
            return total
        n, _ = model.all_objects.filter(pk__in=pks).hard_delete()
        total += n
