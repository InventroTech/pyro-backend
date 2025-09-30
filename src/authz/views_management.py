from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Q
from authz.permissions import IsTenantAuthenticated, HasTenantRole
from authz.models import Role, TenantMembership
from .serializers import RoleListSerializer, CreateSyncedRoleSerializer, TenantMembershipUserSerializer
from .service import create_or_sync_role
from django.db.models.functions import Lower
from accounts.models import LegacyRole 


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
        gm_only = HasTenantRole("GM")()
        if not gm_only.has_permission(request, self):
            return Response({"detail": "GM role required."}, status=status.HTTP_403_FORBIDDEN)

        ser = CreateSyncedRoleSerializer(data=request.data)
        ser.is_valid(raise_exception=True)

        result = create_or_sync_role(
            tenant=request.tenant,
            key=ser.validated_data["key"],
            name=ser.validated_data["name"],
            description=ser.validated_data.get("description", ""),
        )
        # Atomic guarantee: both tables written or none.
        return Response({"success": True, "role": result}, status=status.HTTP_201_CREATED)



class ListTenantUsersView(APIView):
    permission_classes = [IsTenantAuthenticated, HasTenantRole("GM")]

    def get(self, request):
        qs = (TenantMembership.objects
              .select_related("role")
              .filter(tenant=request.tenant)
              .order_by("-is_active", "email"))
        data = TenantMembershipUserSerializer(qs, many=True).data
        return Response({"count": len(data), "results": data}, status=status.HTTP_200_OK)

