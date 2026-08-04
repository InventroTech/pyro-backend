from django.utils import timezone

from pyro_jobs.models import PyroJob


def enqueue_now(job_name: str, payload: dict | None = None) -> PyroJob:
    """Enqueue a PyroJob for immediate execution by Vishnu."""
    return PyroJob.objects.create(
        job_name=job_name,
        payload=payload or {},
        run_at=timezone.now(),
    )
