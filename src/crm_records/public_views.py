from rest_framework import generics, status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import AllowAny
from core.pagination import MetaPageNumberPagination
from core.models import Tenant
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
import logging

logger = logging.getLogger(__name__)
from .models import Record
from .serializers import RecordSerializer
from .mixins import TenantScopedMixin


class PublicJobsView(TenantScopedMixin, generics.ListAPIView):
    """
    Public jobs listing endpoint - NO authentication required.
    
    Uses TenantScopedMixin to automatically filter by tenant.
    Requires ?tenant=slug parameter - no default tenant fallback.
    
    GET: List job postings for specific tenant only (READ-ONLY)
    
    No JWT token required, but tenant slug is mandatory!
    """
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    authentication_classes = []  # Disable all authentication - anyone can access
    permission_classes = [AllowAny]  # Explicitly allow anonymous users
    pagination_class = MetaPageNumberPagination  # Enable pagination for large job lists
    
    def get_queryset(self):
        """
        Get job postings for the specified tenant only.
        Uses TenantScopedMixin for automatic tenant filtering.
        
        REQUIREMENTS FROM TECH LEAD:
        - Use TenantScopedMixin to fetch only tenant data
        - No default tenant fallback
        - Only show data for the specific tenant
        - No default slug usage
        """
        # Get tenant-scoped queryset from TenantScopedMixin
        # This automatically filters by the tenant from ?tenant=slug parameter
        queryset = super().get_queryset()
        
        # Filter for jobs only (entity_type='job')
        queryset = queryset.filter(entity_type='job')
        
        # Apply additional filters from URL parameters
        query_params = self.request.query_params
        
        # Filter by job data fields (department, location, title, etc.)
        # Example: ?department=engineering&location=Bangalore
        for key, value in query_params.items():
            # Skip pagination and system parameters
            if key not in ['tenant', 'page', 'page_size', 'ordering']:
                # Use JSONB field lookup to search inside the 'data' column
                queryset = queryset.filter(**{f'data__{key}__icontains': value})
        
        # Apply ordering (newest first by default)
        ordering = query_params.get('ordering', '-created_at')
        queryset = queryset.order_by(ordering)
        
        return queryset

    @extend_schema(
        summary="List Public Job Postings",
        description="Get job postings without authentication. Requires tenant slug parameter. No default tenant fallback.",
        parameters=[
            OpenApiParameter("tenant", OpenApiTypes.STR, description="REQUIRED: Tenant slug to filter jobs", required=True),
            OpenApiParameter("department", OpenApiTypes.STR, description="Optional: Filter by department"),
            OpenApiParameter("location", OpenApiTypes.STR, description="Optional: Filter by location"),
            OpenApiParameter("title", OpenApiTypes.STR, description="Optional: Filter by job title"),
        ]
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)


class PublicJobApplicationView(TenantScopedMixin, generics.CreateAPIView):
    """
    Public job application submission endpoint - NO authentication required.
    
    Uses TenantScopedMixin for automatic tenant assignment.
    Requires ?tenant=slug parameter - no default tenant fallback.
    
    POST: Submit job applications to specific tenant only
    
    PAYLOAD: Same as CRM records POST request format:
    {
        "name": "Application: John Doe - Software Engineer",
        "data": {
            "applicant_name": "John Doe",
            "email": "john@example.com",
            "phone": "+91-9876543210",
            "job_title": "Software Engineer",
            "resume_url": "https://example.com/resume.pdf",
            "cover_letter": "I am excited to apply...",
            // ... any other application fields
        }
    }
    
    No JWT token required, but tenant slug is mandatory!
    """
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    authentication_classes = []  # Disable authentication - anyone can apply
    permission_classes = [AllowAny]  # Allow anonymous job applications
    
    def perform_create(self, serializer):
        """
        Create job application with automatic tenant assignment from TenantScopedMixin.
        
        REQUIREMENTS FROM TECH LEAD:
        - Use TenantScopedMixin for tenant assignment
        - No default tenant fallback
        - Only create for the specific tenant from ?tenant=slug
        """
        # TenantScopedMixin automatically handles tenant assignment
        # No need for manual tenant lookup or default fallback
        # The mixin gets tenant from ?tenant=slug parameter
        
        # Force entity_type to 'application' so it's categorized correctly
        serializer.save(entity_type='application')
        
        # Log the creation (tenant info available via self.request.tenant if needed)
        tenant_slug = self.request.query_params.get('tenant', 'unknown')
        logger.info(f"[PublicApplications] Created application for tenant: {tenant_slug}")

    @extend_schema(
        summary="Submit Job Application",
        description="Submit job application without authentication. Requires tenant slug parameter. No default tenant fallback.",
        parameters=[
            OpenApiParameter("tenant", OpenApiTypes.STR, description="REQUIRED: Tenant slug for application submission", required=True),
        ],
        request=RecordSerializer,
        responses={201: RecordSerializer}
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)
