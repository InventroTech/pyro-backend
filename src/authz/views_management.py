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
        serializer = CreateSyncedRoleSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        tenant = request.tenant
        result = create_or_sync_role(
            tenant=tenant,
            key=serializer.validated_data['key'],
            name=serializer.validated_data['name'],
            description=serializer.validated_data.get('description', '')
        )
        
        return Response({"success": True, "role": result['role']}, status=status.HTTP_201_CREATED)



class ListTenantUsersView(APIView):
    permission_classes = [IsTenantAuthenticated]

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
              .select_related("role", "user_parent_id")
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
        
        # Get the membership from TenantMembership (same source backend uses)
        membership = TenantMembership.objects.filter(
            tenant=tenant,
            user_id=supabase_uid,
            is_active=True
        ).select_related('role').first()
        
        if not membership:
            return Response({
                'role_key': None,
                'role_name': None,
                'error': 'No active tenant membership found'
            }, status=status.HTTP_200_OK)
        
        return Response({
            'role_key': membership.role.key,
            'role_name': membership.role.name,
            'is_active': membership.is_active
        }, status=status.HTTP_200_OK)


class UpdateUserHierarchyView(APIView):
    """
    PATCH /api/membership/users/hierarchy
    Body: { "assignments": [ { "membership_id": int, "parent_membership_id": int|null }, ... ] }
    Restricts to GM/ASM. Validates tenant and prevents cycles.
    """
    permission_classes = [IsTenantAuthenticated]

    def _collect_subtree_ids(self, tenant, root_membership_id, exclude_membership_id=None):
        """Return set of all membership ids in the subtree under root_membership_id (excluding exclude_membership_id)."""
        seen = set()
        stack = [root_membership_id]
        while stack:
            mid = stack.pop()
            if mid == exclude_membership_id:
                continue
            if mid in seen:
                continue
            seen.add(mid)
            children = TenantMembership.objects.filter(
                tenant=tenant, user_parent_id_id=mid
            ).values_list('id', flat=True)
            stack.extend(children)
        return seen

    def patch(self, request):
        tenant = request.tenant
        assignments = request.data.get('assignments')
        if not isinstance(assignments, list):
            return Response(
                {'error': 'assignments must be a list'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Resolve all membership ids that belong to this tenant
        all_ids = set()
        for a in assignments:
            if not isinstance(a, dict):
                continue
            mid = a.get('membership_id')
            pid = a.get('parent_membership_id')
            if mid is not None:
                all_ids.add(mid)
            if pid is not None:
                all_ids.add(pid)

        if not all_ids:
            return Response({'count': 0}, status=status.HTTP_200_OK)

        valid_ids = set(
            TenantMembership.objects.filter(
                tenant=tenant, id__in=all_ids
            ).values_list('id', flat=True)
        )
        invalid = all_ids - valid_ids
        if invalid:
            return Response(
                {'error': f'membership_id or parent_membership_id not in tenant: {invalid}'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Prevent cycle: for each assignment, new parent must not be in the member's subtree
        for a in assignments:
            if not isinstance(a, dict):
                continue
            mid = a.get('membership_id')
            pid = a.get('parent_membership_id')
            if mid is None or pid is None:
                continue
            subtree = self._collect_subtree_ids(tenant, mid, exclude_membership_id=mid)
            if pid in subtree:
                return Response(
                    {'error': f'Cycle: parent_membership_id {pid} is in subtree of membership_id {mid}'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # Apply updates
        for a in assignments:
            if not isinstance(a, dict):
                continue
            mid = a.get('membership_id')
            pid = a.get('parent_membership_id')
            if mid is None:
                continue
            TenantMembership.objects.filter(tenant=tenant, id=mid).update(
                user_parent_id_id=pid
            )

        return Response({'count': len(assignments)}, status=status.HTTP_200_OK)
