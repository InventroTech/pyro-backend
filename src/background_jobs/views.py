"""
API views for background job management.

Provides endpoints for monitoring and managing background jobs.
"""
import logging
from datetime import timedelta

from django.utils import timezone
from drf_spectacular.utils import extend_schema, OpenApiExample, OpenApiResponse
from rest_framework import generics, status
from rest_framework.response import Response
from rest_framework.views import APIView

from authz.permissions import IsTenantAuthenticated
from config.supabase_auth import SupabaseJWTAuthentication
from core.models import Tenant
from core.pagination import MetaPageNumberPagination

from .models import BackgroundJob, JobStatus, JobType
from .queue_service import get_queue_service


# Manual enqueue must not accept arbitrary pickled callables.
_MANUAL_ENQUEUE_BLOCKED_TYPES = frozenset({JobType.EXECUTE_FUNCTION})
_CROSS_TENANT_ROLE_KEYS = frozenset({"PYRO_ADMIN", "GM", "ASM", "OWNER", "ADMIN"})
logger = logging.getLogger(__name__)


class _TenantJobAPIView(APIView):
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]


def _current_tenant_id(request):
    if hasattr(request, "tenant") and request.tenant:
        return str(request.tenant.id)
    return None


def _can_override_tenant(request):
    role_key = str(getattr(request.user, "role_key", "") or "").strip().upper()
    if role_key in _CROSS_TENANT_ROLE_KEYS:
        return True

    # Fallback: permissions may have authenticated via membership without
    # populating role_key on the user object.
    tenant = getattr(request, "tenant", None)
    user = getattr(request, "user", None)
    supabase_uid = getattr(user, "supabase_uid", None) if user else None
    if not tenant or not supabase_uid:
        return False

    from authz.models import TenantMembership

    membership = (
        TenantMembership.objects.filter(
            tenant=tenant,
            user_id=supabase_uid,
            is_active=True,
        )
        .select_related("role")
        .first()
    )
    if not membership or not membership.role:
        return False
    role_key = str(membership.role.key or "").strip().upper()
    if hasattr(request, "user") and request.user is not None:
        request.user.role_key = membership.role.key
    return role_key in _CROSS_TENANT_ROLE_KEYS


def _resolve_requested_tenant_id(request):
    tenant_id = request.query_params.get("tenant_id")
    if tenant_id is None and hasattr(request, "data"):
        tenant_id = request.data.get("tenant_id")
    if tenant_id in (None, ""):
        return None, None

    tenant_id = str(tenant_id).strip()
    try:
        tenant = Tenant.objects.only("id", "slug", "name").get(id=tenant_id)
    except Tenant.DoesNotExist:
        return None, Response(
            {"error": f"Tenant with id {tenant_id} not found"},
            status=status.HTTP_404_NOT_FOUND,
        )
    except (TypeError, ValueError):
        return None, Response(
            {"error": f"Invalid tenant_id format: {tenant_id}"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    return tenant, None


def _get_job_for_manual_control(request, job_id, requested_tenant=None):
    """
    Resolve a background job for detail/rerun.

    Privileged roles can look up by job ID across tenants when no explicit
    tenant_id is provided. Non-privileged users remain scoped to request.tenant.
    """
    if requested_tenant and not _can_override_tenant(request):
        return None, Response(
            {"error": "You do not have permission to access jobs for another tenant"},
            status=status.HTTP_403_FORBIDDEN,
        )

    if requested_tenant:
        try:
            job = BackgroundJob.objects.get(pk=job_id, tenant_id=requested_tenant.id)
            return job, None
        except BackgroundJob.DoesNotExist:
            return None, Response(
                {"error": "Job not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

    if _can_override_tenant(request):
        try:
            job = BackgroundJob.objects.get(pk=job_id)
            return job, None
        except BackgroundJob.DoesNotExist:
            return None, Response(
                {"error": "Job not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

    tenant_id = _current_tenant_id(request)
    queryset = BackgroundJob.objects.all()
    if tenant_id:
        queryset = queryset.filter(tenant_id=tenant_id)
    try:
        job = queryset.get(pk=job_id)
        return job, None
    except BackgroundJob.DoesNotExist:
        return None, Response(
            {"error": "Job not found"},
            status=status.HTTP_404_NOT_FOUND,
        )


class JobQueueStatusView(_TenantJobAPIView):
    """
    API endpoint for checking job queue status.
    GET /jobs/status/ - Get queue statistics
    """
    @extend_schema(
        summary="Get job queue status",
        description="Returns statistics about the job queue including pending, "
                   "processing, failed, and completed job counts.",
        responses={
            200: OpenApiResponse(
                description="Queue status statistics",
                examples=[
                    OpenApiExample(
                        name="Queue Status",
                        value={
                            "pending": 5,
                            "processing": 2,
                            "failed": 1,
                            "completed_24h": 150
                        }
                    )
                ]
            )
        },
        tags=["Background Jobs"]
    )
    def get(self, request):
        tenant_id = None
        if hasattr(request, 'tenant') and request.tenant:
            tenant_id = str(request.tenant.id)
        
        queryset = BackgroundJob.objects.all()
        if tenant_id:
            queryset = queryset.filter(tenant_id=tenant_id)
        
        stats = {
            'pending': queryset.filter(status=JobStatus.PENDING).count(),
            'processing': queryset.filter(status=JobStatus.PROCESSING).count(),
            'failed': queryset.filter(status=JobStatus.FAILED).count(),
            'completed_24h': queryset.filter(
                status=JobStatus.COMPLETED,
                completed_at__gte=timezone.now() - timedelta(hours=24)
            ).count(),
        }
        
        return Response(stats)


class JobDetailView(_TenantJobAPIView):
    """
    API endpoint for viewing job details.
    GET /jobs/<job_id>/ - Get job details
    """
    @extend_schema(
        summary="Get job details",
        description="Returns detailed information about a specific job including "
                   "payload, result, errors, and status.",
        responses={
            200: OpenApiResponse(
                description="Job details",
                examples=[
                    OpenApiExample(
                        name="Job Details",
                        value={
                            "id": 1,
                            "job_type": "send_mixpanel_event",
                            "status": "COMPLETED",
                            "priority": 0,
                            "attempts": 1,
                            "max_attempts": 3,
                            "created_at": "2025-01-01T00:00:00Z",
                            "completed_at": "2025-01-01T00:00:01Z",
                            "payload": {"user_id": "123", "event_name": "test_event"},
                            "result": {"success": True},
                            "last_error": None
                        }
                    )
                ]
            ),
            404: OpenApiResponse(description="Job not found")
        },
        tags=["Background Jobs"]
    )
    def get(self, request, job_id):
        tenant_id = _current_tenant_id(request)
        requested_tenant, tenant_error = _resolve_requested_tenant_id(request)
        if tenant_error:
            return tenant_error

        job, error = _get_job_for_manual_control(request, job_id, requested_tenant)
        if error:
            return error

        logger.info(
            "Job detail lookup job_id=%s resolved_job_tenant=%s request_tenant=%s role=%s can_override=%s",
            job_id,
            job.tenant_id,
            tenant_id,
            getattr(request.user, "role_key", None),
            _can_override_tenant(request),
        )

        queue_service = get_queue_service()
        job_status = queue_service.get_job_status(job.id)
        return Response(job_status)


class RetryJobView(_TenantJobAPIView):
    """
    API endpoint for manually retrying failed jobs.
    POST /jobs/<job_id>/retry/ - Retry a failed job
    """
    @extend_schema(
        summary="Retry a failed job",
        description="Manually retry a failed or retrying job by resetting it to pending status.",
        responses={
            200: OpenApiResponse(
                description="Job retried successfully",
                examples=[
                    OpenApiExample(
                        name="Retry Success",
                        value={
                            "id": 1,
                            "status": "PENDING",
                            "message": "Job queued for retry"
                        }
                    )
                ]
            ),
            400: OpenApiResponse(description="Job cannot be retried"),
            404: OpenApiResponse(description="Job not found")
        },
        tags=["Background Jobs"]
    )
    def post(self, request, job_id):
        tenant_id = None
        if hasattr(request, 'tenant') and request.tenant:
            tenant_id = str(request.tenant.id)
        
        try:
            queryset = BackgroundJob.objects.all()
            if tenant_id:
                queryset = queryset.filter(tenant_id=tenant_id)
            
            job = queryset.get(pk=job_id)
            queue_service = get_queue_service()
            
            try:
                retried_job = queue_service.retry_failed_job(job.id)
                return Response({
                    "id": retried_job.id,
                    "status": retried_job.status,
                    "message": "Job queued for retry"
                })
            except ValueError as e:
                return Response(
                    {"error": str(e)},
                    status=status.HTTP_400_BAD_REQUEST
                )
        except BackgroundJob.DoesNotExist:
            return Response(
                {"error": "Job not found"},
                status=status.HTTP_404_NOT_FOUND
            )


class FailedJobsView(generics.ListAPIView):
    """
    API endpoint for listing failed jobs.
    GET /jobs/failed/ - List failed jobs with pagination
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination

    @extend_schema(
        summary="List failed jobs",
        description="Retrieves a paginated list of failed jobs for the current tenant. "
                   "Useful for debugging and manual retry operations.",
        parameters=[
            {
                'name': 'job_type',
                'in': 'query',
                'description': 'Filter by job type',
                'required': False,
                'schema': {'type': 'string'},
                'example': 'send_mixpanel_event'
            }
        ],
        responses={
            200: OpenApiResponse(
                description="List of failed jobs",
                examples=[
                    OpenApiExample(
                        name="Failed Jobs",
                        value={
                            "count": 3,
                            "next": None,
                            "previous": None,
                            "results": [
                                {
                                    "id": 1,
                                    "job_type": "send_mixpanel_event",
                                    "status": "FAILED",
                                    "attempts": 3,
                                    "last_error": "Connection timeout",
                                    "created_at": "2025-01-01T00:00:00Z"
                                }
                            ]
                        }
                    )
                ]
            )
        },
        tags=["Background Jobs"]
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        """Filter failed jobs by tenant and optional job type"""
        tenant_id = None
        if hasattr(self.request, 'tenant') and self.request.tenant:
            tenant_id = str(self.request.tenant.id)
        
        queryset = BackgroundJob.objects.filter(status=JobStatus.FAILED)
        
        if tenant_id:
            queryset = queryset.filter(tenant_id=tenant_id)
        
        # Filter by job type
        job_type = self.request.query_params.get('job_type')
        if job_type:
            queryset = queryset.filter(job_type=job_type)
        
        return queryset.order_by('-created_at')
    
    def list(self, request, *args, **kwargs):
        """Custom list to return simplified job data"""
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)
        
        if page is not None:
            jobs_data = []
            for job in page:
                jobs_data.append({
                    'id': job.id,
                    'job_type': job.job_type,
                    'status': job.status,
                    'priority': job.priority,
                    'attempts': job.attempts,
                    'max_attempts': job.max_attempts,
                    'last_error': job.last_error,
                    'created_at': job.created_at.isoformat() if job.created_at else None,
                    'completed_at': job.completed_at.isoformat() if job.completed_at else None,
                })
            
            return self.get_paginated_response(jobs_data)
        
        # Fallback if no pagination
        jobs_data = []
        for job in queryset:
            jobs_data.append({
                'id': job.id,
                'job_type': job.job_type,
                'status': job.status,
                'priority': job.priority,
                'attempts': job.attempts,
                'max_attempts': job.max_attempts,
                'last_error': job.last_error,
                'created_at': job.created_at.isoformat() if job.created_at else None,
                'completed_at': job.completed_at.isoformat() if job.completed_at else None,
            })
        
        return Response(jobs_data)


class BulkRetryJobsView(_TenantJobAPIView):
    """
    API endpoint for bulk retrying failed jobs.
    POST /jobs/bulk-retry/ - Retry multiple failed jobs
    """
    @extend_schema(
        summary="Bulk retry failed jobs",
        description="Manually retry multiple failed or retrying jobs by resetting them to pending status.",
        request={
            'application/json': {
                'type': 'object',
                'properties': {
                    'job_ids': {
                        'type': 'array',
                        'items': {'type': 'integer'},
                        'description': 'List of job IDs to retry'
                    }
                },
                'required': ['job_ids']
            }
        },
        responses={
            200: OpenApiResponse(
                description="Bulk retry results",
                examples=[
                    OpenApiExample(
                        name="Bulk Retry Success",
                        value={
                            "retried_count": 3,
                            "failed_count": 1,
                            "errors": ["Job 5: Job cannot be retried"]
                        }
                    )
                ]
            ),
            400: OpenApiResponse(description="Invalid request")
        },
        tags=["Background Jobs"]
    )
    def post(self, request):
        tenant_id = None
        if hasattr(request, 'tenant') and request.tenant:
            tenant_id = str(request.tenant.id)
        
        job_ids = request.data.get('job_ids', [])
        
        if not isinstance(job_ids, list) or not job_ids:
            return Response(
                {"error": "job_ids must be a non-empty list"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        queryset = BackgroundJob.objects.filter(id__in=job_ids)
        if tenant_id:
            queryset = queryset.filter(tenant_id=tenant_id)
        
        queue_service = get_queue_service()
        retried_count = 0
        failed_count = 0
        errors = []
        
        for job in queryset:
            if job.status in [JobStatus.FAILED, JobStatus.RETRYING]:
                try:
                    queue_service.retry_failed_job(job.id)
                    retried_count += 1
                except ValueError as e:
                    errors.append(f"Job {job.id}: {str(e)}")
                    failed_count += 1
                except Exception as e:
                    errors.append(f"Job {job.id}: {str(e)}")
                    failed_count += 1
            else:
                errors.append(f"Job {job.id}: Cannot retry job with status {job.status}")
                failed_count += 1
        
        return Response({
            "retried_count": retried_count,
            "failed_count": failed_count,
            "errors": errors
        })


class JobTypesView(_TenantJobAPIView):
    """
    List registered job types that can be manually enqueued.
    GET /jobs/types/
    """
    @extend_schema(
        summary="List runnable job types",
        description="Returns registered background job types available for manual enqueue.",
        responses={200: OpenApiResponse(description="Job type list")},
        tags=["Background Jobs"],
    )
    def get(self, request):
        queue_service = get_queue_service()
        job_types = [
            jt
            for jt in queue_service.list_job_types()
            if jt not in _MANUAL_ENQUEUE_BLOCKED_TYPES
        ]
        return Response({"job_types": job_types})


class EnqueueJobView(_TenantJobAPIView):
    """
    Manually enqueue a new background job.
    POST /jobs/enqueue/
    """
    @extend_schema(
        summary="Manually enqueue a background job",
        description=(
            "Create a PENDING job of any registered type with a custom payload. "
            "Workers pick it up like a normal job. "
            "Example: send_cse_assigned_event or send_to_praja / save_resolved_ticket."
        ),
        request={
            "application/json": {
                "type": "object",
                "properties": {
                    "job_type": {"type": "string", "example": "send_cse_assigned_event"},
                    "payload": {
                        "type": "object",
                        "example": {
                            "user_id": 3181247,
                            "cse_email": "cse@example.com",
                        },
                    },
                    "priority": {"type": "integer", "default": 0},
                    "max_attempts": {"type": "integer", "default": 3},
                },
                "required": ["job_type", "payload"],
            }
        },
        responses={
            201: OpenApiResponse(description="Job enqueued"),
            400: OpenApiResponse(description="Invalid job type or payload"),
        },
        tags=["Background Jobs"],
    )
    def post(self, request):
        job_type = request.data.get("job_type")
        payload = request.data.get("payload")
        priority = request.data.get("priority", 0)
        max_attempts = request.data.get("max_attempts", 3)
        requested_tenant, tenant_error = _resolve_requested_tenant_id(request)
        if tenant_error:
            return tenant_error

        if not job_type or not isinstance(job_type, str):
            return Response(
                {"error": "job_type is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if not isinstance(payload, dict):
            return Response(
                {"error": "payload must be a JSON object"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if job_type in _MANUAL_ENQUEUE_BLOCKED_TYPES:
            return Response(
                {"error": f"Manual enqueue of job type '{job_type}' is not allowed"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            priority = int(priority)
            max_attempts = int(max_attempts)
        except (TypeError, ValueError):
            return Response(
                {"error": "priority and max_attempts must be integers"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if max_attempts < 1:
            return Response(
                {"error": "max_attempts must be >= 1"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        tenant_id = _current_tenant_id(request)
        if requested_tenant:
            if not _can_override_tenant(request):
                return Response(
                    {"error": "You do not have permission to enqueue jobs for another tenant"},
                    status=status.HTTP_403_FORBIDDEN,
                )
            tenant_id = str(requested_tenant.id)

        queue_service = get_queue_service()
        try:
            job = queue_service.enqueue_job(
                job_type=job_type,
                payload=payload,
                priority=priority,
                tenant_id=tenant_id,
                max_attempts=max_attempts,
            )
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        logger.info(
            "Manual enqueue created job_id=%s tenant_id=%s request_tenant=%s role=%s",
            job.id,
            job.tenant_id,
            _current_tenant_id(request),
            getattr(request.user, "role_key", None),
        )

        return Response(
            {
                "id": job.id,
                "job_type": job.job_type,
                "status": job.status,
                "priority": job.priority,
                "payload": job.payload,
                "tenant_id": str(job.tenant_id) if job.tenant_id else None,
                "message": "Job enqueued",
            },
            status=status.HTTP_201_CREATED,
        )


class RerunJobView(_TenantJobAPIView):
    """
    Clone any existing job (including COMPLETED) as a new PENDING job.
    POST /jobs/<job_id>/rerun/
    """
    @extend_schema(
        summary="Re-run an existing background job",
        description=(
            "Creates a new PENDING job with the same type and payload as the source job. "
            "Works for COMPLETED, FAILED, or any other status. Original job is unchanged."
        ),
        responses={
            201: OpenApiResponse(description="Job requeued"),
            400: OpenApiResponse(description="Cannot requeue"),
            404: OpenApiResponse(description="Job not found"),
        },
        tags=["Background Jobs"],
    )
    def post(self, request, job_id):
        tenant_id = _current_tenant_id(request)
        requested_tenant, tenant_error = _resolve_requested_tenant_id(request)
        if tenant_error:
            return tenant_error

        source, error = _get_job_for_manual_control(request, job_id, requested_tenant)
        if error:
            return error

        if source.job_type in _MANUAL_ENQUEUE_BLOCKED_TYPES:
            return Response(
                {
                    "error": (
                        f"Manual re-run of job type '{source.job_type}' is not allowed"
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        queue_service = get_queue_service()
        try:
            job = queue_service.requeue_job(source.id)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except BackgroundJob.DoesNotExist:
            return Response(
                {"error": "Job not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        logger.info(
            "Manual rerun source_job_id=%s new_job_id=%s source_tenant=%s request_tenant=%s role=%s",
            source.id,
            job.id,
            source.tenant_id,
            tenant_id,
            getattr(request.user, "role_key", None),
        )

        return Response(
            {
                "id": job.id,
                "source_job_id": source.id,
                "job_type": job.job_type,
                "status": job.status,
                "payload": job.payload,
                "tenant_id": str(job.tenant_id) if job.tenant_id else None,
                "message": "Job requeued",
            },
            status=status.HTTP_201_CREATED,
        )
