from rest_framework import serializers
from authz.models import TenantMembership, Role
from authz.models import Role


class RoleListSerializer(serializers.ModelSerializer):
    class Meta:
        model = Role
        fields = ("id", "key", "name", "description")


class TenantMembershipUserSerializer(serializers.ModelSerializer):
    role = RoleListSerializer()
    class Meta:
        model = TenantMembership
        fields = ("email", "user_id", "is_active", "created_at", "role")


class CreateSyncedRoleSerializer(serializers.Serializer):
    key = serializers.CharField(max_length=64)
    name = serializers.CharField(max_length=128)
    description = serializers.CharField(required=False, allow_blank=True)
