"""
Helpers for enqueueing background jobs with tenant_id set on each row.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Union
from uuid import UUID

from core.models import Tenant

from .models import BackgroundJob
from .queue_service import QueueService

logger = logging.getLogger(__name__)

TenantId = Union[str, UUID]


def iter_active_tenant_ids():
    """Yield all tenant primary keys."""
    return Tenant.objects.values_list("id", flat=True).iterator()


def enqueue_for_all_tenants(
    queue: QueueService,
    *,
    job_type: str,
    payload: Optional[Dict[str, Any]] = None,
    priority: int = 0,
    payload_for_tenant: Optional[Callable[[TenantId], Dict[str, Any]]] = None,
) -> List[BackgroundJob]:
    """
    Enqueue one job per tenant, each with ``tenant_id`` set on the BackgroundJob row.

    ``payload_for_tenant``, when provided, is called with each tenant id and should return
    the payload dict for that job (merged on top of ``payload`` if both are given).
    """
    base_payload = dict(payload or {})
    jobs: List[BackgroundJob] = []
    for tid in iter_active_tenant_ids():
        job_payload = dict(base_payload)
        if payload_for_tenant is not None:
            job_payload.update(payload_for_tenant(tid))
        job = queue.enqueue_job(
            job_type=job_type,
            payload=job_payload,
            priority=priority,
            tenant_id=str(tid),
        )
        jobs.append(job)
    logger.debug(
        "Enqueued %s jobs type=%s for %s tenant(s)",
        len(jobs),
        job_type,
        len(jobs),
    )
    return jobs
