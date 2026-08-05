"""
API views for manual pyro job control.
"""
import logging
from datetime import datetime

from django.utils.dateparse import parse_datetime
from drf_spectacular.utils import OpenApiExample, OpenApiResponse, extend_schema
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from authz.permissions import IsTenantAuthenticated
from config.supabase_auth import SupabaseJWTAuthentication
from pyro_jobs.models import PyroJob
from pyro_jobs.queue_service import get_pyro_queue_service

logger = logging.getLogger(__name__)


class _TenantPyroJobAPIView(APIView):
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]


class PyroJobTypesView(_TenantPyroJobAPIView):
    """
    List registered pyro job names that can be manually enqueued.
    GET /pyro-jobs/types/
    """

    @extend_schema(
        summary="List runnable pyro job types",
        description="Returns registered pyro job names available for manual enqueue.",
        responses={200: OpenApiResponse(description="Job type list")},
        tags=["Pyro Jobs"],
    )
    def get(self, request):
        queue_service = get_pyro_queue_service()
        return Response({"job_types": queue_service.list_job_types()})


class PyroEnqueueJobView(_TenantPyroJobAPIView):
    """
    Manually enqueue a new pyro job.
    POST /pyro-jobs/enqueue/
    """

    @extend_schema(
        summary="Manually enqueue a pyro job",
        description=(
            "Create a PENDING pyro_job row for any registered job name. "
            "PyroJobExecutor picks it up when run_at is due."
        ),
        request={
            "application/json": {
                "type": "object",
                "properties": {
                    "job_name": {"type": "string", "example": "purge_old_log_tables"},
                    "payload": {
                        "type": "object",
                        "example": {
                            "days": 30,
                            "chunk_size": 1000,
                            "max_chunks_per_table": 20,
                        },
                    },
                    "run_at": {"type": "string", "format": "date-time"},
                    "max_attempts": {"type": "integer", "default": 3},
                },
                "required": ["job_name"],
            }
        },
        responses={
            201: OpenApiResponse(description="Job enqueued"),
            400: OpenApiResponse(description="Invalid job name or payload"),
        },
        tags=["Pyro Jobs"],
    )
    def post(self, request):
        job_name = request.data.get("job_name")
        payload = request.data.get("payload", {})
        run_at_raw = request.data.get("run_at")
        max_attempts = request.data.get("max_attempts", 3)

        if not job_name or not isinstance(job_name, str):
            return Response(
                {"error": "job_name is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            return Response(
                {"error": "payload must be a JSON object"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        run_at = None
        if run_at_raw not in (None, ""):
            if isinstance(run_at_raw, datetime):
                run_at = run_at_raw
            else:
                run_at = parse_datetime(str(run_at_raw))
                if run_at is None:
                    return Response(
                        {"error": "run_at must be a valid ISO datetime"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

        try:
            max_attempts = int(max_attempts)
        except (TypeError, ValueError):
            return Response(
                {"error": "max_attempts must be an integer"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if max_attempts < 1:
            return Response(
                {"error": "max_attempts must be >= 1"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        queue_service = get_pyro_queue_service()
        try:
            job, ran_pending = queue_service.enqueue_job(
                job_name=job_name,
                payload=payload,
                run_at=run_at,
                max_attempts=max_attempts,
            )
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        logger.info(
            "Manual pyro enqueue job_id=%s job_name=%s ran_pending=%s",
            job.id,
            job.job_name,
            [r.get("id") for r in ran_pending],
        )
        return Response(
            {
                "id": job.id,
                "job_name": job.job_name,
                "status": job.status,
                "payload": job.payload,
                "run_at": job.run_at.isoformat() if job.run_at else None,
                "ran_pending_jobs": ran_pending,
                "message": (
                    "Job enqueued"
                    if not ran_pending
                    else (
                        "Ran existing pending job(s) "
                        f"{[r.get('id') for r in ran_pending]} then enqueued new job"
                    )
                ),
            },
            status=status.HTTP_201_CREATED,
        )


class PyroJobDetailView(_TenantPyroJobAPIView):
    """
    API endpoint for viewing pyro job details.
    GET /pyro-jobs/<job_id>/
    """

    @extend_schema(
        summary="Get pyro job details",
        description="Returns detailed information about a specific pyro job.",
        responses={
            200: OpenApiResponse(
                description="Job details",
                examples=[
                    OpenApiExample(
                        name="Pyro Job Details",
                        value={
                            "id": 1,
                            "job_name": "dispatch_data_sync",
                            "status": "COMPLETED",
                            "payload": {},
                            "result": {"success": True},
                            "error": None,
                        },
                    )
                ],
            ),
            404: OpenApiResponse(description="Job not found"),
        },
        tags=["Pyro Jobs"],
    )
    def get(self, request, job_id):
        queue_service = get_pyro_queue_service()
        try:
            job_status = queue_service.get_job_status(job_id)
        except PyroJob.DoesNotExist:
            return Response(
                {"error": "Job not found"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(job_status)


class PyroRerunJobView(_TenantPyroJobAPIView):
    """
    Clone any existing pyro job as a new PENDING job.
    POST /pyro-jobs/<job_id>/rerun/
    """

    @extend_schema(
        summary="Re-run an existing pyro job",
        description=(
            "Creates a new PENDING pyro job with the same name and payload as the "
            "source job. Original job is unchanged."
        ),
        responses={
            201: OpenApiResponse(description="Job requeued"),
            400: OpenApiResponse(description="Cannot requeue"),
            404: OpenApiResponse(description="Job not found"),
        },
        tags=["Pyro Jobs"],
    )
    def post(self, request, job_id):
        queue_service = get_pyro_queue_service()
        try:
            source = PyroJob.objects.get(pk=job_id)
        except PyroJob.DoesNotExist:
            return Response(
                {"error": "Job not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        try:
            job, ran_pending = queue_service.requeue_job(source.id)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except PyroJob.DoesNotExist:
            return Response(
                {"error": "Job not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        logger.info(
            "Manual pyro rerun source_job_id=%s new_job_id=%s job_name=%s ran_pending=%s",
            source.id,
            job.id,
            source.job_name,
            [r.get("id") for r in ran_pending],
        )
        return Response(
            {
                "id": job.id,
                "source_job_id": source.id,
                "job_name": job.job_name,
                "status": job.status,
                "payload": job.payload,
                "run_at": job.run_at.isoformat() if job.run_at else None,
                "ran_pending_jobs": ran_pending,
                "message": (
                    "Job requeued"
                    if not ran_pending
                    else (
                        "Ran existing pending job(s) "
                        f"{[r.get('id') for r in ran_pending]} then requeued"
                    )
                ),
            },
            status=status.HTTP_201_CREATED,
        )
