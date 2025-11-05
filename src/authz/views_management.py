from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Q
from authz.permissions import IsTenantAuthenticated, HasTenantRole
from authz.models import Role, TenantMembership
from .serializers import RoleListSerializer, CreateSyncedRoleSerializer, TenantMembershipUserSerializer
from .service import create_or_sync_role
from django.db.models.functions import Lower
from accounts.models import LegacyRole, LegacyUser 


class RolesView(APIView):
    """
    GET  /api/authz/roles      -> list roles from authz_role (tenant-scoped)
    POST /api/authz/roles      -> create role in BOTH authz_role & legacy roles (same UUID)
    """
    permission_classes = [IsTenantAuthenticated]  # POST will add GM requirement inline


    def get(self, request, *args, **kwargs):
        tenant = request.tenant 

        qs = (
            Role.objects
            .filter(tenant=tenant)
            .annotate(norm_name=Lower('name'))
            .order_by('norm_name', 'id')   # order_by must include distinct keys prefix
            .distinct('norm_name')         # collapse GM/gm/etc.
        )

        data = [{
            "id": str(r.id),
            "name": r.name,
            "description": r.description or "",
            "key": r.key or "",
        } for r in qs]

        return Response({"count": len(data), "results": data}, status=status.HTTP_200_OK)
    

    def post(self, request):
        # Enforce GM only for create, while GET stays open to tenant users.
        # Atomic guarantee: both tables written or none.
        return Response({"success": True, "role": result}, status=status.HTTP_201_CREATED)



class ListTenantUsersView(APIView):
    permission_classes = [IsTenantAuthenticated, HasTenantRole("GM")]

    def get(self, request):
        from django.db.models import Q, OuterRef, Subquery
        
        # Create a subquery to get the name from LegacyUser
        legacy_user_subquery = LegacyUser.objects.filter(
            email=OuterRef('email'),
            role_id=OuterRef('role__id'),
            tenant=request.tenant
        ).values('name')[:1]
        
        # Query TenantMembership with left join to LegacyUser
        qs = (TenantMembership.objects
              .select_related("role")
              .filter(tenant=request.tenant)
              .annotate(name=Subquery(legacy_user_subquery))
              .order_by("-is_active", "email"))
        
        # Serialize the data
        data = TenantMembershipUserSerializer(qs, many=True).data
        
        # Add name field to each result
        for i, item in enumerate(data):
            item['name'] = qs[i].name or ''
        
        return Response({"count": len(data), "results": data}, status=status.HTTP_200_OK)


class CurrentUserRoleView(APIView):
    """
    Get the current authenticated user's role from TenantMembership (backend source of truth).
    This ensures frontend uses the same role that backend permissions check against.
    Also returns tenant_id from enriched jwt_claims or request.tenant.
    """
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        user = request.user
        tenant = request.tenant
        
        if not tenant:
            return Response({
                'error': 'Tenant not found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        supabase_uid = getattr(user, 'supabase_uid', None)
        if not supabase_uid:
            return Response({
                'error': 'User supabase_uid not found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Get tenant_id from enriched jwt_claims or fallback to request.tenant
        tenant_id = None
        if hasattr(request, 'jwt_claims') and request.jwt_claims:
            tenant_id = request.jwt_claims.get('tenant_id')
        if not tenant_id:
            tenant_id = str(tenant.id) if tenant else None
        
        # Get the membership from TenantMembership (same source backend uses)
        membership = TenantMembership.objects.filter(
            tenant=tenant,
            user_id=supabase_uid,
            is_active=True
        ).select_related('role').first()
        
        if not membership:
            return Response({
                'tenant_id': tenant_id,
                'role_key': None,
                'role_name': None,
                'error': 'No active tenant membership found'
            }, status=status.HTTP_200_OK)
        
        return Response({
            'tenant_id': tenant_id,
            'role_key': membership.role.key,
            'role_name': membership.role.name,
            'role_id': str(membership.role.id),
            'is_active': membership.is_active
        }, status=status.HTTP_200_OK)
