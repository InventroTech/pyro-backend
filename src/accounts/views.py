from django.db import transaction, connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, serializers
from django.conf import settings
from accounts.serializers import LegacyUserCreateSerializer
from .models import LegacyUser, LegacyRole  # DEPRECATED: Keep for backward compatibility during transition
from authz.permissions import IsTenantAuthenticated, HasTenantRole
from authz.service import get_authz_role_from_legacy_role  # DEPRECATED: Will be removed
from authz.models import TenantMembership, Role
from rest_framework.permissions import IsAuthenticated, AllowAny

from django.db.models import Subquery
from .serializers import LegacyUserLiteSerializer  # DEPRECATED: Will be replaced
from authz.serializers import TenantMembershipUserSerializer

from authz.service import link_user_uid_and_activate, drop_permissions_cache
import logging
from .serializers import LinkUserUidSerializer, DeleteUserEverywhereSerializer
from accounts.services.delete_user_everywhere import delete_user_everywhere

logger = logging.getLogger(__name__)

class LegacyUserCreateView(APIView):
    """
    NEW: Creates TenantMembership directly (no longer creates LegacyUser).
    Body: { name, email, [company_name], [role_id], [uid] }
    
    DEPRECATED: LegacyUser creation removed. This endpoint now only creates TenantMembership.
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

        if not role_id:
            return Response({
                'error': 'role_id is required to create TenantMembership'
            }, status=status.HTTP_400_BAD_REQUEST)

        with transaction.atomic():
            try:
                # Get the authz role (role_id should be AuthZ role ID, not legacy)
                # Try to get by ID first (assume it's AuthZ role ID)
                try:
                    authz_role = Role.objects.get(id=role_id, tenant=tenant)
                except Role.DoesNotExist:
                    # Fallback: try legacy role mapping (for backward compatibility during transition)
                    try:
                        authz_role = get_authz_role_from_legacy_role(role_id, tenant)
                    except Exception as e:
                        logger.error(f"Failed to find role {role_id} for tenant {tenant.id}: {e}")
                        return Response({
                            'error': f'Role with ID {role_id} not found for this tenant'
                        }, status=status.HTTP_400_BAD_REQUEST)
                
                # Create or update TenantMembership directly (no LegacyUser)
                membership, created = TenantMembership.objects.get_or_create(
                    tenant=tenant,
                    email=email,
                    role=authz_role,
                    defaults={
                        'name': name,
                        'company_name': company_name,
                        'user_id': uid,
                        'is_active': bool(uid)
                    }
                )
                
                # If membership already exists, update it
                if not created:
                    membership.name = name
                    if company_name:
                        membership.company_name = company_name
                    membership.role = authz_role
                    if uid:
                        membership.user_id = uid
                        membership.is_active = True
                    membership.save()
                
                # Invalidate permissions cache so newly updated role (e.g. GM) is seen immediately
                # without user having to re-login (permission checks use cached role with 10min TTL)
                if membership.user_id:
                    drop_permissions_cache(str(membership.user_id), membership.tenant)
                
                return Response({
                    'id': str(membership.id),
                    'name': membership.name,
                    'email': membership.email,
                    'tenant_id': str(tenant.id),
                    'company_name': membership.company_name,
                    'role_id': str(membership.role.id),
                    'uid': str(membership.user_id) if membership.user_id else None,
                    'is_active': membership.is_active,
                    'created': created
                }, status=status.HTTP_201_CREATED)
                        
            except Exception as e:
                logger.error(f"Failed to create TenantMembership for user {email}: {str(e)}", exc_info=True)
                return Response({
                    'error': f'Failed to create user: {str(e)}'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    



class AssigneesByRoleView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        """
        NEW: Using TenantMembership instead of LegacyUser
        Returns assignees filtered by role key (e.g., 'CSE', 'RM')
        """
        role_key = (request.query_params.get('role') or '').strip().upper()
        tenant = request.tenant
        
        if not tenant:
            return Response({'error': "Unable to determine tenant for current user."}, status=403)
        
        # NEW: Query TenantMembership filtered by Role key
        try:
            if role_key:
                # Find role by key (case-insensitive)
                role = Role.objects.filter(tenant=tenant, key__iexact=role_key).first()
                if not role:
                    # Fallback: try to find by name for backward compatibility
                    role = Role.objects.filter(tenant=tenant, name__iexact=role_key).first()
                
                if role:
                    qs = TenantMembership.objects.filter(
                        tenant=tenant,
                        role=role,
                        is_active=True
                    ).select_related('role').order_by('email', 'id')
                else:
                    qs = TenantMembership.objects.none()
            else:
                # Return all active memberships if no role specified
                qs = TenantMembership.objects.filter(
                    tenant=tenant,
                    is_active=True
                ).select_related('role').order_by('email', 'id')
            
            # NEW: Serialize using TenantMembership.name directly (no LegacyUser fallback)
            data = []
            for membership in qs:
                # Use TenantMembership.name field (migrated from LegacyUser)
                name = membership.name or membership.email.split('@')[0] if membership.email else 'Unknown'
                
                data.append({
                    'id': str(membership.id),
                    'name': name,
                    'email': membership.email,
                    'company_name': membership.company_name,  # Include company_name
                    'uid': str(membership.user_id) if membership.user_id else None,
                    'role': {
                        'id': str(membership.role.id),
                        'key': membership.role.key,
                        'name': membership.role.name
                    }
                })
            
            return Response({'count': len(data), 'results': data})
            
        except Exception as e:
            logger.error(f"Error in AssigneesByRoleView: {e}", exc_info=True)
            # Return empty result on error (no legacy fallback)
            return Response({'count': 0, 'results': []}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


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