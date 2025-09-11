from django.db import transaction, connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from accounts.serializers import LegacyUserCreateSerializer
from .models import LegacyUser, LegacyRole
from authz.permissions import IsTenantAuthenticated, HasTenantRole
from authz.service import get_authz_role_from_legacy_role
from authz.models import TenantMembership
from rest_framework.permissions import IsAuthenticated

from django.db.models import Subquery
from .serializers import LegacyUserLiteSerializer

class LegacyUserCreateView(APIView):
    """
    GM adds a row to public.users
    Body: { name, email, [company_name], [role_id], [uid] }
    """
    # permission_classes = [IsTenantAuthenticated, HasTenantRole("GM")]
    permission_classes = [IsAuthenticated]
    def post(self, request):
        ser = LegacyUserCreateSerializer(data = request.data, context={'request':request})
        ser.is_valid(raise_exception=True)
        tenant = ser.validated_data["_tenant"]
        name = ser.validated_data["name"].strip()
        email = ser.validated_data["email"]
        company_name = ser.validated_data.get("company_name")
        role_id = ser.validated_data.get("role_id")
        uid = ser.validated_data.get("uid")

        with transaction.atomic():
            user_row = LegacyUser.objects.create(
                name = name,
                email = email,
                tenant = tenant,
                company_name=company_name,
                role_id=role_id,
                uid=uid  # Set UID if provided
            )
            
            # Create TenantMembership entry if role_id is provided
            if role_id:
                try:
                    # Get the corresponding authz role
                    authz_role = get_authz_role_from_legacy_role(role_id, tenant)
                    
                    # Create or update TenantMembership
                    membership, created = TenantMembership.objects.get_or_create(
                        tenant=tenant,
                        email=email,
                        defaults={
                            'role': authz_role,
                            'user_id': uid,  # Link to UID if provided
                            'is_active': bool(uid)  # Activate if UID is provided
                        }
                    )
                    
                    # If membership already exists, update it
                    if not created:
                        membership.role = authz_role
                        if uid:
                            membership.user_id = uid
                            membership.is_active = True
                        membership.save()
                        
                except Exception as e:
                    # Log the error but don't fail the user creation
                    # This allows the system to work even if authz roles aren't set up
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Failed to create TenantMembership for user {email}: {str(e)}")

        return Response({
            'id': user_row.id,
            'name': user_row.name,
            'email': user_row.email,
            'tenant_id': str(tenant.id),
            'company_name': user_row.company_name,
            'role_id': str(role_id) if role_id else None,
            'uid': str(uid) if uid else None,
            'tenant_membership_created': bool(role_id),  # Indicate if TenantMembership was created
        }, status=status.HTTP_201_CREATED)
    



class AssigneesByRoleView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        role_name = (request.query_params.get('role') or '').strip()
        tenant_id = request.tenant.id
        if not tenant_id:
            return Response({'error': "Unable to determine tenant for current user."}, status=403)
        if role_name:
            role_ids = LegacyRole.objects.filter(
                tenant_id=tenant_id,
                name__iexact=role_name
            ).values('id') 
        else:
            role_ids = LegacyRole.objects.filter(
                tenant_id=tenant_id
            ).values('id') 

        qs = (
            LegacyUser.objects
            .filter(tenant_id=tenant_id, role_id__in=Subquery(role_ids))
            .order_by('name', 'id')
        )

        data = LegacyUserLiteSerializer(qs, many=True).data
        return Response({'count': len(data), 'results': data})
