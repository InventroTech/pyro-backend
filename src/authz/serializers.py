from rest_framework import serializers
from authz.models import TenantMembership, Role
from authz.models import Role


class RoleListSerializer(serializers.ModelSerializer):
    class Meta:
        model = Role
        fields = ("id", "key", "name", "description")


class TenantMembershipUserSerializer(serializers.ModelSerializer):
    """
    NEW: Includes name and company_name fields (migrated from LegacyUser).
    """
    role = RoleListSerializer()
    user_parent_id = serializers.SerializerMethodField()
    user_parent_email = serializers.SerializerMethodField()

    class Meta:
        model = TenantMembership
        fields = ("id", "email", "name", "company_name", "department", "user_id", "is_active", "created_at", "role", "user_parent_id", "user_parent_email")

    def get_user_parent_id(self, obj):
        if obj.user_parent_id_id is None:
            return None
        return obj.user_parent_id_id

    def get_user_parent_email(self, obj):
        if obj.user_parent_id is None:
            return None
        return obj.user_parent_id.email


class CreateSyncedRoleSerializer(serializers.Serializer):
    key = serializers.CharField(max_length=64)
    name = serializers.CharField(max_length=128)
    description = serializers.CharField(required=False, allow_blank=True)
