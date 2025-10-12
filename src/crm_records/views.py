from rest_framework import generics, status
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from authz.permissions import IsTenantAuthenticated
from core.pagination import MetaPageNumberPagination
from .models import Record
from .serializers import RecordSerializer
from .mixins import TenantScopedMixin


class RecordListCreateView(TenantScopedMixin, generics.ListCreateAPIView):
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination
    
    def get_queryset(self):
        """
        Filter records by tenant and optionally by entity_type.
        Supports dynamic entity filtering for config-driven UIs.
        """
        queryset = super().get_queryset()
        
        # Get entity_type from query params (optional)
        entity_type = self.request.query_params.get('entity_type')
        if entity_type:
            queryset = queryset.filter(entity_type=entity_type)
            
        return queryset
    
    def perform_create(self, serializer):
        """
        Create record with tenant and entity_type assignment.
        entity_type can come from query params or request body.
        """
        # Get entity_type from query params or request data
        entity_type = self.request.query_params.get('entity_type')
        if not entity_type:
            entity_type = self.request.data.get('entity_type')
        
        if not entity_type:
            raise ValidationError({
                'entity_type': 'This field is required. Provide it in query params or request body.'
            })
        
        serializer.save(
            tenant=self.request.tenant,
            entity_type=entity_type
        )


class RecordDetailView(TenantScopedMixin, generics.RetrieveUpdateAPIView):
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    permission_classes = [IsTenantAuthenticated]


class EntityProxyView(TenantScopedMixin, generics.ListCreateAPIView):
    """
    Proxy view for entity-specific endpoints (e.g., /leads/, /tickets/).
    Provides friendly URLs while reusing the same Record logic.
    """
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination
    entity_type = None  # Set this in URL configuration
    
    def get_queryset(self):
        """Filter by tenant and the specific entity type."""
        queryset = super().get_queryset()
        if self.entity_type:
            queryset = queryset.filter(entity_type=self.entity_type)
        return queryset
    
    def perform_create(self, serializer):
        """Create record with tenant and the specific entity type."""
        serializer.save(
            tenant=self.request.tenant,
            entity_type=self.entity_type
        )
