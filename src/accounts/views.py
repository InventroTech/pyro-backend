from django.db import transaction, connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from accounts.serializers import LegacyUserCreateSerializer
from accounts.models import LegacyUser
from authz.permissions import IsTenantAuthenticated, HasTenantRole
from rest_framework.permissions import IsAuthenticated

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