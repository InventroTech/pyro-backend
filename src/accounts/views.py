from django.db import transaction, connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from accounts.serializers import LegacyUserCreateSerializer
from .models import LegacyUser, LegacyRole
from authz.permissions import IsTenantAuthenticated, HasTenantRole
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
                role_id=role_id
            )

        return Response({
            'id': user_row.id,
            'name': user_row.name,
            'email': user_row.email,
            'tenant_id': str(tenant.id),
            'company_name': user_row.company_name,
            'role_id': str(role_id) if role_id else None,
            'uid': str(uid) if uid else None,
        }, status=status.HTTP_201_CREATED)
    


def _tenant_id_from_user(user):
    return getattr(getattr(user, 'tenant', None), 'id', None) or \
           getattr(user, 'tenant_id', None) 

class AssigneesByRoleView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        print(request.user)
        role_name = (request.query_params.get('role') or '').strip()
        if not role_name:
            return Response({'error': "Missing required query param 'role'."}, status=400)

        tenant_id = _tenant_id_from_user(request)
        if not tenant_id:
            return Response({'error': "Unable to determine tenant for current user."}, status=403)

        role_ids = LegacyRole.objects.filter(
            tenant_id=tenant_id,
            name__iexact=role_name
        ).values('id')  # could be multiple if not unique per tenant

        qs = (
            LegacyUser.objects
            .filter(tenant_id=tenant_id, role_id__in=Subquery(role_ids))
            .order_by('name', 'id')
        )

        data = LegacyUserLiteSerializer(qs, many=True).data
        return Response({'count': len(data), 'results': data})
