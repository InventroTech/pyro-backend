from rest_framework import generics, status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError
from django.shortcuts import get_object_or_404
from django.db import transaction
from uuid import UUID

from .models import CustomTable, CustomColumn, CustomRow, Page, Card
from .serializers import (
    CustomTableSerializer, CustomColumnSerializer, CustomRowSerializer,
    PageSerializer, CardSerializer
)
from authz.permissions import IsTenantAuthenticated


class CustomTablesView(generics.ListCreateAPIView):
    """List and create custom tables"""
    serializer_class = CustomTableSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        # Handle both tenant from auth middleware and tenant_id from query params
        tenant = getattr(self.request, 'tenant', None)
        if not tenant:
            tenant_id = self.request.query_params.get('tenant_id')
            if tenant_id:
                from core.models import Tenant
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            return CustomTable.objects.none()
            
        return CustomTable.objects.filter(tenant=tenant).order_by('-created_at')
    
    def perform_create(self, serializer):
        # Handle both tenant from auth middleware and tenant_id from request data
        tenant = getattr(self.request, 'tenant', None)
        if not tenant:
            tenant_id = self.request.data.get('tenant_id')
            if tenant_id:
                from core.models import Tenant
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            raise ValidationError("Tenant is required")
            
        serializer.save(tenant=tenant)


class CustomTableDetailView(generics.RetrieveUpdateDestroyAPIView):
    """Get, update, delete custom table"""
    serializer_class = CustomTableSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        return CustomTable.objects.filter(tenant=self.request.tenant)


class CustomColumnsView(generics.ListCreateAPIView):
    """List and create custom columns"""
    serializer_class = CustomColumnSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        table_id = self.request.query_params.get('table_id')
        if not table_id:
            return CustomColumn.objects.none()
        
        # Verify table belongs to tenant
        table = get_object_or_404(CustomTable, id=table_id, tenant=self.request.tenant)
        return CustomColumn.objects.filter(table=table).order_by('ordinal_position')
    
    def perform_create(self, serializer):
        table_id = self.request.data.get('table_id')
        table = get_object_or_404(CustomTable, id=table_id, tenant=self.request.tenant)
        serializer.save(table=table)


class CustomColumnDetailView(generics.RetrieveUpdateDestroyAPIView):
    """Get, update, delete custom column"""
    serializer_class = CustomColumnSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        return CustomColumn.objects.filter(table__tenant=self.request.tenant)


class CustomRowsView(generics.ListCreateAPIView):
    """List and create custom rows"""
    serializer_class = CustomRowSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        table_id = self.request.query_params.get('table_id')
        if not table_id:
            return CustomRow.objects.none()
        
        # Verify table belongs to tenant
        table = get_object_or_404(CustomTable, id=table_id, tenant=self.request.tenant)
        return CustomRow.objects.filter(table=table).order_by('-created_at')
    
    def perform_create(self, serializer):
        table_id = self.request.data.get('table_id')
        table = get_object_or_404(CustomTable, id=table_id, tenant=self.request.tenant)
        serializer.save(table=table)


class CustomRowDetailView(generics.RetrieveUpdateDestroyAPIView):
    """Get, update, delete custom row"""
    serializer_class = CustomRowSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        return CustomRow.objects.filter(table__tenant=self.request.tenant)


class PagesView(generics.ListCreateAPIView):
    """List and create pages"""
    serializer_class = PageSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        from core.models import Tenant
        
        # Handle both tenant from auth middleware and tenant_id from query params
        tenant = getattr(self.request, 'tenant', None)
        if not tenant:
            tenant_id = self.request.query_params.get('tenant_id')
            if tenant_id:
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            return Page.objects.none()
        
        user_id = self.request.query_params.get('user_id')
        role = self.request.query_params.get('role')
        
        queryset = Page.objects.filter(tenant=tenant)
        
        if user_id:
            queryset = queryset.filter(user_id=user_id)
        if role:
            # Filter by role if needed - you may need to adjust this based on your role implementation
            queryset = queryset.filter(user__role__name=role)
            
        return queryset.order_by('-created_at')
    
    def perform_create(self, serializer):
        from core.models import Tenant
        
        # Handle both tenant from auth middleware and tenant_id from request data
        tenant = getattr(self.request, 'tenant', None)
        if not tenant:
            tenant_id = self.request.data.get('tenant_id')
            if tenant_id:
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            raise ValidationError("Tenant is required")
            
        serializer.save(user=self.request.user, tenant=tenant)


class PageDetailView(generics.RetrieveUpdateDestroyAPIView):
    """Get, update, delete page"""
    serializer_class = PageSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        from core.models import Tenant
        
        # Handle both tenant from auth middleware and tenant_id from query params
        tenant = getattr(self.request, 'tenant', None)
        if not tenant:
            tenant_id = self.request.query_params.get('tenant_id')
            if tenant_id:
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            return Page.objects.none()
            
        return Page.objects.filter(tenant=tenant)


class CardsView(generics.ListCreateAPIView):
    """List and create cards"""
    serializer_class = CardSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        return Card.objects.filter(tenant=self.request.tenant).order_by('-created_at')
    
    def perform_create(self, serializer):
        serializer.save(user=self.request.user, tenant=self.request.tenant)


class CardDetailView(generics.RetrieveUpdateDestroyAPIView):
    """Get, update, delete card"""
    serializer_class = CardSerializer
    permission_classes = [IsTenantAuthenticated]
    
    def get_queryset(self):
        return Card.objects.filter(tenant=self.request.tenant)


class TenantUsersView(APIView):
    """Get tenant users"""
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        from authz.models import TenantMembership
        from core.models import Tenant
        
        # Handle both tenant from auth middleware and tenant_id from query params
        tenant = getattr(request, 'tenant', None)
        if not tenant:
            tenant_id = request.query_params.get('tenant_id')
            if tenant_id:
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            return Response({
                'success': False,
                'error': 'Tenant not found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Get active tenant memberships
        memberships = TenantMembership.objects.filter(
            tenant=tenant,
            is_active=True
        ).select_related('role')
        
        users = []
        for membership in memberships:
            users.append({
                'id': str(membership.user_id) if membership.user_id else None,
                'email': membership.email,
                'role': membership.role.name if membership.role else None,
                'is_active': membership.is_active
            })
        
        return Response({
            'success': True,
            'data': users
        })


class TenantView(APIView):
    """Get tenant info"""
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        from core.models import Tenant
        
        # Handle both tenant from auth middleware and tenant_id from query params
        tenant = getattr(request, 'tenant', None)
        if not tenant:
            tenant_id = request.query_params.get('tenant_id')
            if tenant_id:
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            return Response({
                'success': False,
                'error': 'Tenant not found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response({
            'success': True,
            'data': {
                'id': str(tenant.id),
                'name': tenant.name,
                'slug': tenant.slug
            }
        })


class RolesView(APIView):
    """Get and create roles for a tenant"""
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        from authz.models import Role
        from core.models import Tenant
        
        # Handle both tenant from auth middleware and tenant_id from query params
        tenant = getattr(request, 'tenant', None)
        if not tenant:
            tenant_id = request.query_params.get('tenant_id')
            if tenant_id:
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            return Response({
                'success': False,
                'error': 'Tenant not found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        roles = Role.objects.filter(tenant=tenant).values('id', 'name')
        return Response({
            'success': True,
            'data': list(roles)
        })
    
    def post(self, request):
        from authz.models import Role
        from core.models import Tenant
        
        # Handle both tenant from auth middleware and tenant_id from request data
        tenant = getattr(request, 'tenant', None)
        if not tenant:
            tenant_id = request.data.get('tenant_id')
            if tenant_id:
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            return Response({
                'success': False,
                'error': 'Tenant not found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        name = request.data.get('name')
        if not name:
            return Response({
                'success': False,
                'error': 'Role name is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            role = Role.objects.create(
                name=name,
                tenant=tenant
            )
            
            return Response({
                'success': True,
                'data': {
                    'id': str(role.id),
                    'name': role.name
                }
            })
        except Exception as e:
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class UsersView(APIView):
    """Get users for a tenant"""
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        from authz.models import TenantMembership
        from core.models import Tenant
        
        # Handle both tenant from auth middleware and tenant_id from query params
        tenant = getattr(request, 'tenant', None)
        if not tenant:
            tenant_id = request.query_params.get('tenant_id')
            if tenant_id:
                tenant = get_object_or_404(Tenant, id=tenant_id)
        
        if not tenant:
            return Response({
                'success': False,
                'error': 'Tenant not found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Get active tenant memberships with role info
        memberships = TenantMembership.objects.filter(
            tenant=tenant,
            is_active=True
        ).select_related('role')
        
        users = []
        for membership in memberships:
            users.append({
                'uid': str(membership.user_id) if membership.user_id else None,
                'name': membership.email,  # You might want to get actual name from user table
                'email': membership.email,
                'role_id': str(membership.role.id) if membership.role else None,
                'role': {
                    'id': str(membership.role.id) if membership.role else None,
                    'name': membership.role.name if membership.role else None
                },
                'created_at': membership.created_at.isoformat() if membership.created_at else None
            })
        
        return Response({
            'success': True,
            'data': users
        })


class UserByEmailView(APIView):
    """Get user by email"""
    permission_classes = [IsTenantAuthenticated]
    
    def post(self, request):
        email = request.data.get('email')
        if not email:
            return Response({
                'success': False,
                'error': 'Email is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        from authz.models import TenantMembership
        
        # Find user by email in tenant memberships
        membership = TenantMembership.objects.filter(
            email=email,
            is_active=True
        ).select_related('tenant', 'role').first()
        
        if not membership:
            return Response({
                'success': False,
                'error': 'User not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        return Response({
            'success': True,
            'data': {
                'user_id': str(membership.user_id) if membership.user_id else None,
                'email': membership.email,
                'tenant_id': str(membership.tenant.id),
                'role_id': str(membership.role.id) if membership.role else None,
                'role_name': membership.role.name if membership.role else None
            }
        })


class RemoveTenantUserView(APIView):
    """Remove a user from tenant"""
    permission_classes = [IsTenantAuthenticated]
    
    def delete(self, request):
        user_id = request.data.get('user_id')
        if not user_id:
            return Response({
                'success': False,
                'error': 'user_id is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        from authz.models import TenantMembership
        
        # Find and deactivate the membership
        membership = TenantMembership.objects.filter(
            user_id=user_id,
            tenant=request.tenant
        ).first()
        
        if not membership:
            return Response({
                'success': False,
                'error': 'User membership not found'
            }, status=status.HTTP_404_NOT_FOUND)
        
        membership.is_active = False
        membership.save()
        
        return Response({
            'success': True,
            'message': 'User removed from tenant'
        })


class LeadsView(APIView):
    """Get leads"""
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        from crm.models import Lead
        
        leads = Lead.objects.filter(tenant=request.tenant).values(
            'id', 'name', 'email', 'phone', 'company', 'status', 'created_at'
        )
        
        return Response({
            'success': True,
            'data': list(leads)
        })


class LeadDetailView(APIView):
    """Get, update, delete individual lead"""
    permission_classes = [IsTenantAuthenticated]
    
    def delete(self, request, lead_id):
        from crm.models import Lead
        
        try:
            lead = get_object_or_404(Lead, id=lead_id, tenant=request.tenant)
            lead.delete()
            
            return Response({
                'success': True,
                'message': 'Lead deleted successfully'
            })
        except Exception as e:
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def put(self, request, lead_id):
        from crm.models import Lead
        
        try:
            lead = get_object_or_404(Lead, id=lead_id, tenant=request.tenant)
            
            # Update lead fields
            for field in ['name', 'email', 'phone', 'company', 'status']:
                if field in request.data:
                    setattr(lead, field, request.data[field])
            
            lead.save()
            
            return Response({
                'success': True,
                'data': {
                    'id': str(lead.id),
                    'name': lead.name,
                    'email': lead.email,
                    'phone': lead.phone,
                    'company': lead.company,
                    'status': lead.status
                }
            })
        except Exception as e:
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class CheckExistingLeadsView(APIView):
    """Check if leads with given emails/phones already exist"""
    permission_classes = [IsTenantAuthenticated]
    
    def post(self, request):
        emails = request.data.get('emails', [])
        phones = request.data.get('phones', [])
        
        from crm.models import Lead
        
        existing_emails = []
        existing_phones = []
        
        if emails:
            existing_emails = list(Lead.objects.filter(
                email__in=emails,
                tenant=request.tenant
            ).values_list('email', flat=True))
        
        if phones:
            existing_phones = list(Lead.objects.filter(
                phone__in=phones,
                tenant=request.tenant
            ).values_list('phone', flat=True))
        
        return Response({
            'success': True,
            'data': {
                'emails': existing_emails,
                'phones': existing_phones
            }
        })


class CreateLeadsView(APIView):
    """Create multiple leads"""
    permission_classes = [IsTenantAuthenticated]
    
    def post(self, request):
        leads_data = request.data
        if not isinstance(leads_data, list):
            return Response({
                'success': False,
                'error': 'Leads data must be a list'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        from crm.models import Lead
        
        created_leads = []
        for lead_data in leads_data:
            lead = Lead.objects.create(
                tenant=request.tenant,
                name=lead_data.get('name', ''),
                email=lead_data.get('email', ''),
                phone=lead_data.get('phone', ''),
                company=lead_data.get('company', ''),
                status=lead_data.get('status', 'new')
            )
            created_leads.append({
                'id': str(lead.id),
                'name': lead.name,
                'email': lead.email,
                'phone': lead.phone,
                'company': lead.company,
                'status': lead.status
            })
        
        return Response({
            'success': True,
            'data': created_leads
        })


class CreateLeadsTableRecordView(APIView):
    """Create a single lead record"""
    permission_classes = [IsTenantAuthenticated]
    
    def post(self, request):
        from crm.models import Lead
        
        lead = Lead.objects.create(
            tenant=request.tenant,
            name=request.data.get('name', ''),
            email=request.data.get('email', ''),
            phone=request.data.get('phone', ''),
            company=request.data.get('company', ''),
            status=request.data.get('status', 'new')
        )
        
        return Response({
            'success': True,
            'data': {
                'id': str(lead.id),
                'name': lead.name,
                'email': lead.email,
                'phone': lead.phone,
                'company': lead.company,
                'status': lead.status
            }
        })


class UpdateLeadsTableRecordView(APIView):
    """Update a lead record"""
    permission_classes = [IsTenantAuthenticated]
    
    def put(self, request, pk):
        from crm.models import Lead
        
        try:
            lead = get_object_or_404(Lead, id=pk, tenant=request.tenant)
            
            # Update lead fields
            for field in ['name', 'email', 'phone', 'company', 'status']:
                if field in request.data:
                    setattr(lead, field, request.data[field])
            
            lead.save()
            
            return Response({
                'success': True,
                'data': {
                    'id': str(lead.id),
                    'name': lead.name,
                    'email': lead.email,
                    'phone': lead.phone,
                    'company': lead.company,
                    'status': lead.status
                }
            })
        except Exception as e:
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ReorderCustomColumnsView(APIView):
    """Reorder custom columns"""
    permission_classes = [IsTenantAuthenticated]
    
    def put(self, request):
        column_ids = request.data.get('columnIds', [])
        if not column_ids:
            return Response({
                'success': False,
                'error': 'columnIds is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            with transaction.atomic():
                for index, column_id in enumerate(column_ids):
                    CustomColumn.objects.filter(
                        id=column_id,
                        table__tenant=request.tenant
                    ).update(ordinal_position=index)
            
            return Response({
                'success': True,
                'message': 'Columns reordered successfully'
            })
        except Exception as e:
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
