"""
Helpers for scheduling :data:`~background_jobs.models.JobType.PURGE_OLD_LOG_TABLES`.
"""
from __future__ import annotations

from datetime import timedelta

from django.db.models import F
from django.utils import timezone

from .models import BackgroundJob, JobStatus, JobType


def _active_purge_jobs_qs(*, tenant_id: str, exclude_job_id: int | None = None):
    qs = BackgroundJob.objects.filter(
        job_type=JobType.PURGE_OLD_LOG_TABLES,
        tenant_id=str(tenant_id),
        status__in=(JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.RETRYING),
    ).exclude(
        status=JobStatus.PENDING,
        attempts__gte=F("max_attempts"),
    )
    if exclude_job_id is not None:
        qs = qs.exclude(pk=exclude_job_id)
    return qs


def tenant_should_enqueue_purge(
    tenant_id: str,
    *,
    now=None,
    interval_seconds: int,
) -> bool:
    """
    Return whether the daily scheduler should enqueue a purge job for ``tenant_id``.

    Skips when an active purge job already exists, or when a purge completed
    within ``interval_seconds`` without ``has_more`` in the result (continuation
    jobs are enqueued by the handler, not the daily scheduler).
    """
    tid = str(tenant_id)
    now = now or timezone.now()

    if _active_purge_jobs_qs(tenant_id=tid).exists():
        return False

    recent = (
        BackgroundJob.objects.filter(
            job_type=JobType.PURGE_OLD_LOG_TABLES,
            tenant_id=tid,
            status=JobStatus.COMPLETED,
            completed_at__gte=now - timedelta(seconds=interval_seconds),
        )
        .order_by("-completed_at")
        .first()
    )
    if recent is None:
        return True

    result = recent.result if isinstance(recent.result, dict) else {}
    return bool(result.get("has_more"))


def tenant_has_active_purge_job(
    tenant_id: str,
    *,
    exclude_job_id: int | None = None,
) -> bool:
    return _active_purge_jobs_qs(
        tenant_id=tenant_id,
        exclude_job_id=exclude_job_id,
    ).exists()
