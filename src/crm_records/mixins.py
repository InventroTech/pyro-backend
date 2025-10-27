from rest_framework import status
from rest_framework.response import Response
from django.core.exceptions import PermissionDenied


class TenantScopedMixin:
    """
    Mixin to automatically scope all queries to the current tenant.
    Ensures tenant isolation across all API endpoints.
    """
    
    def get_queryset(self):
        """
        Filter queryset to only include records for the current tenant.
        """
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            # If no tenant is set, return empty queryset
            return self.queryset.none()
        
        return self.queryset.filter(tenant=self.request.tenant)
    
    def perform_create(self, serializer):
        """
        Automatically assign the current tenant to new records.
        """
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            raise PermissionDenied("No tenant context available")
        
        serializer.save(tenant=self.request.tenant)
    
    def perform_update(self, serializer):
        """
        Ensure updates are scoped to the current tenant.
        """
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            raise PermissionDenied("No tenant context available")
        
        # Ensure the record belongs to the current tenant
        if serializer.instance.tenant != self.request.tenant:
            raise PermissionDenied("Cannot modify records from other tenants")
        
        serializer.save()
    
    def perform_destroy(self, instance):
        """
        Ensure deletions are scoped to the current tenant.
        """
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            raise PermissionDenied("No tenant context available")
        
        # Ensure the record belongs to the current tenant
        if instance.tenant != self.request.tenant:
            raise PermissionDenied("Cannot delete records from other tenants")
        
        instance.delete()
    
    def get_object(self):
        """
        Override get_object to ensure single record access is tenant-scoped.
        """
        obj = super().get_object()
        
        # Additional check for single object access
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            raise PermissionDenied("No tenant context available")
        
        if obj.tenant != self.request.tenant:
            raise PermissionDenied("Cannot access records from other tenants")
        
        return obj
