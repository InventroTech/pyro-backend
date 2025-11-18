from django.db import transaction, connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, serializers
from django.conf import settings
from accounts.serializers import LegacyUserCreateSerializer
from .models import LegacyUser, LegacyRole
from authz.permissions import IsTenantAuthenticated, HasTenantRole
from authz.service import get_authz_role_from_legacy_role
from authz.models import TenantMembership
from rest_framework.permissions import IsAuthenticated

from django.db.models import Subquery
from .serializers import LegacyUserLiteSerializer

from authz.service import link_user_uid_and_activate
import logging
from .serializers import LinkUserUidSerializer
from rest_framework.permissions import AllowAny
from authz.permissions import IsTenantAuthenticated, HasTenantRole
from accounts.serializers import DeleteUserEverywhereSerializer
from accounts.services.delete_user_everywhere import delete_user_everywhere

logger = logging.getLogger(__name__)

class LegacyUserCreateView(APIView):
    """
    GM adds a row to public.users
    Body: { name, email, [company_name], [role_id], [uid] }
    """
    # permission_classes = [IsTenantAuthenticated, HasTenantRole("GM")]
    permission_classes = [IsTenantAuthenticated]
    def post(self, request):
        ser = LegacyUserCreateSerializer(data = request.data, context={'request':request})
        ser.is_valid(raise_exception=True)
        tenant = request.tenant
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


class LinkUserUidView(APIView):
    """
    POST: Link Supabase UID to a user and activate tenant memberships.
    """
    permission_classes = [AllowAny]

    def post(self, request):
        try:
            serializer = LinkUserUidSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)

            email = serializer.validated_data["email"]
            uid = serializer.validated_data["uid"]

            result = link_user_uid_and_activate(email, uid)

            if result.get("success"):
                return Response(result, status=status.HTTP_200_OK)
            return Response(result, status=status.HTTP_400_BAD_REQUEST)

        except serializers.ValidationError as ve:
            return Response({"error": ve.detail}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Error in LinkUserUidView.post: {e}", exc_info=True)
            return Response(
                {"error": "Internal server error", "message": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )



class DeleteUserEverywhereView(APIView):
    """
    DELETE /api/accounts/delete-user
    Body:
      - Preferred: { "uid": "<uuid>" }
      - Or:        { "email": "user@example.com", "role_id": "<uuid>" }

    Behavior:
      - Requires authenticated, tenant-scoped caller.
      - GM/OWNER (or whichever role you prefer) can delete users in their tenant.
      - Deletes rows in:
          1) auth.users (by uid)  [cascades public.users via FK]
          2) public.users (any leftovers)
          3) public.authz_tenantmembership (scoped to tenant)
      - Idempotent; returns counts of actually deleted rows.
    """
    permission_classes = [IsTenantAuthenticated, HasTenantRole("GM")]  # adjust to your policy

    def delete(self, request):
        try:
            ser = DeleteUserEverywhereSerializer(data=request.data, context={"request": request})
            ser.is_valid(raise_exception=True)

            tenant = ser.validated_data["_tenant"]
            uid = ser.validated_data.get("uid")
            email = ser.validated_data.get("email")
            role_id = ser.validated_data.get("role_id")

            report = delete_user_everywhere(tenant=tenant, uid=uid, email=email, role_id=role_id)

            # 204 is also OK for delete
            return Response(
                {
                    "success": True,
                    "matched_by": report.get("matched_by"),
                    "tenant_id": report.get("tenant_id"),
                    "resolved_uid": report.get("resolved_uid"),
                    "deleted": report.get("deleted"),
                    "notes": report.get("notes", []),
                },
                status=status.HTTP_200_OK,
            )

        except Exception as e:
            logger.error("DeleteUserEverywhereView error", exc_info=True, extra={"message": str(e)})
            return Response(
                {"success": False, "error": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )