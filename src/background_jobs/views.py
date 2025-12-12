"""
API views for background job management.

Provides endpoints for monitoring and managing background jobs.
"""
from rest_framework import generics, status
from rest_framework.response import Response
from rest_framework.views import APIView
from django.utils import timezone
from datetime import timedelta
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiExample
from authz.permissions import IsTenantAuthenticated
from core.pagination import MetaPageNumberPagination
from .models import BackgroundJob, JobStatus
from .queue_service import get_queue_service


class JobQueueStatusView(APIView):
    """
    API endpoint for checking job queue status.
    GET /jobs/status/ - Get queue statistics
    """
    permission_classes = [IsTenantAuthenticated]

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


class JobDetailView(APIView):
    """
    API endpoint for viewing job details.
    GET /jobs/<job_id>/ - Get job details
    """
    permission_classes = [IsTenantAuthenticated]

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
        tenant_id = None
        if hasattr(request, 'tenant') and request.tenant:
            tenant_id = str(request.tenant.id)
        
        try:
            queryset = BackgroundJob.objects.all()
            if tenant_id:
                queryset = queryset.filter(tenant_id=tenant_id)
            
            job = queryset.get(pk=job_id)
            queue_service = get_queue_service()
            job_status = queue_service.get_job_status(job.id)
            
            return Response(job_status)
        except BackgroundJob.DoesNotExist:
            return Response(
                {"error": "Job not found"},
                status=status.HTTP_404_NOT_FOUND
            )


class RetryJobView(APIView):
    """
    API endpoint for manually retrying failed jobs.
    POST /jobs/<job_id>/retry/ - Retry a failed job
    """
    permission_classes = [IsTenantAuthenticated]

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


class BulkRetryJobsView(APIView):
    """
    API endpoint for bulk retrying failed jobs.
    POST /jobs/bulk-retry/ - Retry multiple failed jobs
    """
    permission_classes = [IsTenantAuthenticated]

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


