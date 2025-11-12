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


class PublicJobsView(generics.ListAPIView):
    """
    Public jobs listing endpoint - NO authentication required.
    
    This endpoint allows anyone to view job postings without any authentication.
    Perfect for public job boards, career pages, or third-party integrations.
    
    GET: List all job postings (READ-ONLY)
    
    No JWT token, no tenant headers required!
    """
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    authentication_classes = []  # Disable all authentication - anyone can access
    permission_classes = [AllowAny]  # Explicitly allow anonymous users
    pagination_class = MetaPageNumberPagination  # Enable pagination for large job lists
    
    def get_queryset(self):
        """
        Get job postings from all tenants or a specific tenant.
        Filters for entity_type='job' automatically.
        
        WHY TENANT SLUG IS IMPORTANT:
        - Multi-tenancy: Different companies use the same system
        - Data isolation: Company A shouldn't see Company B's jobs
        - Flexibility: Can show jobs from specific company or default company
        
        Examples:
        - ?tenant=google -> Shows only Google's jobs
        - ?tenant=microsoft -> Shows only Microsoft's jobs  
        - No tenant param -> Shows default company's jobs
        """
        # STEP 1: Start with all job records only (filter out leads, applications, etc.)
        queryset = Record.objects.filter(entity_type='job')
        
        # STEP 2: Handle tenant filtering - WHO'S JOBS TO SHOW?
        tenant_slug = self.request.query_params.get('tenant')  # Get ?tenant=company-name from URL
        
        if tenant_slug:
            # User specified a specific company's jobs to view
            try:
                tenant = Tenant.objects.get(slug=tenant_slug)  # Find the company by slug
                queryset = queryset.filter(tenant=tenant)      # Show only that company's jobs
                logger.info(f"[PublicJobs] Filtering by tenant: {tenant_slug}")
            except Tenant.DoesNotExist:
                # Company doesn't exist - return empty results
                logger.warning(f"[PublicJobs] Tenant '{tenant_slug}' not found")
                return Record.objects.none()
        else:
            # No specific company requested - use default company
            default_tenant = self._get_default_tenant()
            if default_tenant:
                queryset = queryset.filter(tenant=default_tenant)  # Show default company's jobs
                logger.info(f"[PublicJobs] Using default tenant: {default_tenant.slug}")
            else:
                logger.warning("[PublicJobs] No default tenant found")
        
        # STEP 3: Log how many jobs we found for debugging
        job_count = queryset.count()
        logger.info(f"[PublicJobs] Found {job_count} job records")
        
        # STEP 4: Apply additional filters from URL parameters
        query_params = self.request.query_params
        
        # Filter by job data fields (department, location, title, etc.)
        # Example: ?department=engineering&location=Bangalore
        for key, value in query_params.items():
            # Skip pagination and system parameters
            if key not in ['tenant', 'page', 'page_size', 'ordering']:
                # Use JSONB field lookup to search inside the 'data' column
                # data__department__icontains means: data.department contains "engineering"
                queryset = queryset.filter(**{f'data__{key}__icontains': value})
                logger.info(f"[PublicJobs] Filtered by {key}='{value}', found {queryset.count()} records")
        
        # STEP 5: Apply ordering (newest first by default)
        ordering = query_params.get('ordering', '-created_at')  # Default: newest jobs first
        queryset = queryset.order_by(ordering)
        
        return queryset
    
    def _get_default_tenant(self):
        """
        Get default tenant from settings.
        
        WHY WE NEED THIS:
        - When no specific company is requested, we need a fallback
        - Prevents errors when tenant parameter is missing
        - Provides a "main" company for the job board
        
        FALLBACK STRATEGY:
        1. Try to get company from DEFAULT_TENANT_SLUG setting
        2. If that doesn't exist, use the first company in database
        3. If no companies exist, return None (will cause error)
        """
        from django.conf import settings
        
        # Get default company slug from environment variables (.env file)
        # Example: DEFAULT_TENANT_SLUG=bibhab-thepyro-ai
        default_slug = getattr(settings, 'DEFAULT_TENANT_SLUG', 'bibhab-thepyro-ai')
        
        try:
            # Try to find the default company
            return Tenant.objects.get(slug=default_slug)
        except Tenant.DoesNotExist:
            # Default company doesn't exist - use any company as fallback
            return Tenant.objects.first()

    @extend_schema(
        summary="List Public Job Postings",
        description="Get job postings without authentication. No JWT token required! READ-ONLY endpoint.",
        parameters=[
            OpenApiParameter("tenant", OpenApiTypes.STR, description="Optional: Filter by tenant slug"),
            OpenApiParameter("department", OpenApiTypes.STR, description="Filter by department"),
            OpenApiParameter("location", OpenApiTypes.STR, description="Filter by location"),
            OpenApiParameter("title", OpenApiTypes.STR, description="Filter by job title"),
        ]
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)


class PublicJobApplicationView(generics.CreateAPIView):
    """
    Public job application submission endpoint - NO authentication required.
    
    This allows job seekers to submit applications without creating accounts.
    Perfect for career pages where people can apply directly.
    
    POST: Submit job applications
    
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
    
    No JWT token, no tenant headers required!
    """
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    authentication_classes = []  # Disable authentication - anyone can apply
    permission_classes = [AllowAny]  # Allow anonymous job applications
    
    def perform_create(self, serializer):
        """
        Create job application with automatic tenant assignment.
        
        This runs when someone submits a job application.
        We need to decide which company receives this application.
        
        TENANT ASSIGNMENT LOGIC:
        1. If ?tenant=company-name in URL -> Application goes to that company
        2. If no tenant specified -> Application goes to default company
        3. This ensures applications reach the right HR department
        """
        # STEP 1: Determine which company should receive this application
        tenant_slug = self.request.query_params.get('tenant')  # Check URL for ?tenant=company-name
        
        if tenant_slug:
            # User specified which company they're applying to
            try:
                tenant = Tenant.objects.get(slug=tenant_slug)  # Find the company
            except Tenant.DoesNotExist:
                # Company doesn't exist - reject the application
                raise ValidationError(f"Tenant '{tenant_slug}' not found")
        else:
            # No company specified - send to default company
            tenant = self._get_default_tenant()
            if not tenant:
                raise ValidationError("No default tenant available")
        
        # STEP 2: Save the application with the determined company
        # Force entity_type to 'application' so it's categorized correctly
        serializer.save(tenant=tenant, entity_type='application')
        logger.info(f"[PublicApplications] Created application in tenant: {tenant.slug}")
    
    def _get_default_tenant(self):
        """
        Get default tenant from settings.
        
        Same logic as PublicJobsView - provides fallback company
        when no specific company is mentioned in the application.
        """
        from django.conf import settings
        
        # Get default company from environment settings
        default_slug = getattr(settings, 'DEFAULT_TENANT_SLUG', 'bibhab-thepyro-ai')
        
        try:
            # Try to find the default company
            return Tenant.objects.get(slug=default_slug)
        except Tenant.DoesNotExist:
            # Default company doesn't exist - use first available company
            return Tenant.objects.first()

    @extend_schema(
        summary="Submit Job Application",
        description="Submit job application without authentication. No JWT token required!",
        request=RecordSerializer,
        responses={201: RecordSerializer}
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)
