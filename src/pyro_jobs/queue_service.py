"""
High-level helpers for listing, enqueueing, inspecting, and re-running pyro jobs.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from django.db import IntegrityError, transaction
from django.utils import timezone

from pyro_jobs.handlers import JOB_HANDLERS
from pyro_jobs.models import PyroJob

logger = logging.getLogger(__name__)


class PyroQueueService:
    def list_job_types(self) -> list:
        return sorted(JOB_HANDLERS.keys())

    def _execute_job_now(self, job: PyroJob) -> Dict[str, Any]:
        """
        Run one pending/running pyro job synchronously (same outcome as Vishnu).
        """
        now = timezone.now()
        if job.status == PyroJob.STATUS_PENDING:
            job.status = PyroJob.STATUS_RUNNING
            job.started_at = now
            job.attempts = (job.attempts or 0) + 1
            job.save(update_fields=["status", "started_at", "attempts"])

        handler = JOB_HANDLERS.get(job.job_name)
        if not handler:
            job.status = PyroJob.STATUS_FAILED
            job.error = f"No handler registered for: {job.job_name}"
            job.is_deleted = True
            job.completed_at = timezone.now()
            job.save(update_fields=["status", "error", "is_deleted", "completed_at"])
            return {
                "id": job.id,
                "status": job.status,
                "error": job.error,
            }

        try:
            result = handler(job.payload or {})
            job.status = PyroJob.STATUS_COMPLETED
            job.completed_at = timezone.now()
            job.is_deleted = True
            job.result = result if isinstance(result, dict) else None
            job.error = None
            job.save(
                update_fields=["status", "completed_at", "is_deleted", "result", "error"]
            )
            logger.info("Manually ran pending pyro job %s (%s) to completion", job.id, job.job_name)
            return {
                "id": job.id,
                "status": job.status,
                "result": job.result,
            }
        except Exception as e:
            job.status = PyroJob.STATUS_FAILED
            job.error = str(e)
            job.is_deleted = True
            job.completed_at = timezone.now()
            job.save(update_fields=["status", "error", "is_deleted", "completed_at"])
            logger.exception(
                "Manual run of pending pyro job %s (%s) failed: %s",
                job.id,
                job.job_name,
                e,
            )
            return {
                "id": job.id,
                "status": job.status,
                "error": job.error,
            }

    def _run_existing_pending_jobs(self, job_name: str) -> List[Dict[str, Any]]:
        """
        Execute all open PENDING rows for job_name so a new PENDING can be created.

        Staging/prod enforce unique_pending_job_per_name.
        """
        ran: List[Dict[str, Any]] = []
        pending_ids = list(
            PyroJob.objects.filter(
                job_name=job_name,
                status=PyroJob.STATUS_PENDING,
                is_deleted=False,
            )
            .order_by("id")
            .values_list("id", flat=True)
        )
        for job_id in pending_ids:
            job = PyroJob.objects.get(pk=job_id)
            # Skip if another worker already claimed it.
            if job.status != PyroJob.STATUS_PENDING or job.is_deleted:
                continue
            ran.append(self._execute_job_now(job))
        return ran

    def enqueue_job(
        self,
        job_name: str,
        payload: Optional[Dict[str, Any]] = None,
        run_at: Optional[datetime] = None,
        max_attempts: Optional[int] = None,
    ) -> Tuple[PyroJob, List[Dict[str, Any]]]:
        """
        Enqueue a pyro job as a new PENDING row.

        If a PENDING row already exists for that job_name, run it now
        (complete/fail + soft-delete) then create a fresh PENDING job.

        Returns:
            (job, ran_pending_jobs)
        """
        if job_name not in JOB_HANDLERS:
            raise ValueError(f"Invalid job name: {job_name}. No handler registered.")

        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")

        payload = dict(payload)
        effective_run_at = run_at or timezone.now()
        effective_max_attempts = max_attempts or 3

        ran_pending = self._run_existing_pending_jobs(job_name)

        try:
            with transaction.atomic():
                job = PyroJob.objects.create(
                    job_name=job_name,
                    payload=payload,
                    run_at=effective_run_at,
                    status=PyroJob.STATUS_PENDING,
                    max_attempts=effective_max_attempts,
                    is_deleted=False,
                )
        except IntegrityError:
            # Rare race: another PENDING appeared while we were creating.
            ran_pending.extend(self._run_existing_pending_jobs(job_name))
            job = PyroJob.objects.create(
                job_name=job_name,
                payload=payload,
                run_at=effective_run_at,
                status=PyroJob.STATUS_PENDING,
                max_attempts=effective_max_attempts,
                is_deleted=False,
            )

        logger.info(
            "Enqueued pyro job %s: name=%s run_at=%s ran_pending=%s",
            job.id,
            job_name,
            job.run_at,
            [r.get("id") for r in ran_pending],
        )
        return job, ran_pending

    def get_job_status(self, job_id: int) -> Dict[str, Any]:
        job = PyroJob.objects.get(pk=job_id)
        return {
            "id": job.id,
            "job_name": job.job_name,
            "status": job.status,
            "payload": job.payload,
            "result": job.result,
            "error": job.error,
            "attempts": job.attempts,
            "max_attempts": job.max_attempts,
            "run_at": job.run_at.isoformat() if job.run_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "is_deleted": job.is_deleted,
        }

    def requeue_job(
        self,
        job_id: int,
        *,
        max_attempts: Optional[int] = None,
    ) -> Tuple[PyroJob, List[Dict[str, Any]]]:
        """
        Re-run a pyro job by cloning payload into a new PENDING row.

        Runs any existing PENDING for that job_name first.

        Returns:
            (job, ran_pending_jobs)
        """
        source = PyroJob.objects.get(pk=job_id)
        if source.job_name not in JOB_HANDLERS:
            raise ValueError(
                f"Cannot requeue job {job_id}: no handler for name {source.job_name}"
            )

        payload = dict(source.payload or {})
        job, ran_pending = self.enqueue_job(
            job_name=source.job_name,
            payload=payload,
            run_at=timezone.now(),
            max_attempts=max_attempts or source.max_attempts or 3,
        )
        logger.info(
            "Manually requeued pyro job %s as new job %s (name=%s ran_pending=%s)",
            job_id,
            job.id,
            source.job_name,
            [r.get("id") for r in ran_pending],
        )
        return job, ran_pending


_queue_service: Optional[PyroQueueService] = None


def get_pyro_queue_service() -> PyroQueueService:
    global _queue_service
    if _queue_service is None:
        _queue_service = PyroQueueService()
    return _queue_service
