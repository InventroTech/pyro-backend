from rest_framework import status
from rest_framework.generics import ListCreateAPIView, RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from authz.permissions import IsTenantAuthenticated
from .models import WhatsAppTemplate
from .serializers import WhatsAppTemplateSerializer


class WhatsAppTemplateListCreateView(ListCreateAPIView):
    """
    List all WhatsApp templates for the current tenant or create a new template.
    GET /whatsapp/templates/ - List all templates
    POST /whatsapp/templates/ - Create a new template
    """
    serializer_class = WhatsAppTemplateSerializer
    permission_classes = [IsTenantAuthenticated]

    def get_queryset(self):
        """Filter templates by current tenant"""
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            return WhatsAppTemplate.objects.none()
        return WhatsAppTemplate.objects.filter(tenant=self.request.tenant)

    def perform_create(self, serializer):
        """Automatically assign the current tenant to new templates"""
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            from rest_framework.exceptions import PermissionDenied
            raise PermissionDenied("No tenant context available")
        serializer.save(tenant=self.request.tenant)


class WhatsAppTemplateDetailView(RetrieveUpdateDestroyAPIView):
    """
    Retrieve, update or delete a WhatsApp template.
    GET /whatsapp/templates/<pk>/ - Retrieve a template
    PUT /whatsapp/templates/<pk>/ - Update a template
    PATCH /whatsapp/templates/<pk>/ - Partially update a template
    DELETE /whatsapp/templates/<pk>/ - Delete a template
    """
    serializer_class = WhatsAppTemplateSerializer
    permission_classes = [IsTenantAuthenticated]
    lookup_field = 'pk'

    def get_queryset(self):
        """Filter templates by current tenant"""
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            return WhatsAppTemplate.objects.none()
        return WhatsAppTemplate.objects.filter(tenant=self.request.tenant)

    def perform_update(self, serializer):
        """Ensure updates are scoped to the current tenant"""
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            from rest_framework.exceptions import PermissionDenied
            raise PermissionDenied("No tenant context available")
        
        # Ensure the template belongs to the current tenant
        if serializer.instance.tenant != self.request.tenant:
            from rest_framework.exceptions import PermissionDenied
            raise PermissionDenied("Cannot modify templates from other tenants")
        
        serializer.save()

    def perform_destroy(self, instance):
        """Ensure deletions are scoped to the current tenant"""
        if not hasattr(self.request, 'tenant') or not self.request.tenant:
            from rest_framework.exceptions import PermissionDenied
            raise PermissionDenied("No tenant context available")
        
        if instance.tenant != self.request.tenant:
            from rest_framework.exceptions import PermissionDenied
            raise PermissionDenied("Cannot delete templates from other tenants")
        
        instance.delete()
