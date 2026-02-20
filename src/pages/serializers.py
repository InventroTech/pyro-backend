from rest_framework import serializers
from .models import Page


class PageSerializer(serializers.ModelSerializer):
    """Read serializer: id, name, config, role (id), created_at, updated_at."""
    role = serializers.SerializerMethodField()

    class Meta:
        model = Page
        fields = ['id', 'name', 'config', 'role', 'created_at', 'updated_at']
        read_only_fields = ['id', 'created_at', 'updated_at']

    def get_role(self, obj):
        if obj.role_id is None:
            return None
        return str(obj.role_id)


class PageCreateUpdateSerializer(serializers.ModelSerializer):
    """Write serializer: name, config, role (optional UUID)."""
    role = serializers.UUIDField(required=False, allow_null=True)

    class Meta:
        model = Page
        fields = ['name', 'config', 'role']

    def validate_role(self, value):
        if value is None:
            return None
        from authz.models import Role
        tenant = self.context.get('tenant')
        if not tenant:
            return value
        if not Role.objects.filter(id=value, tenant=tenant).exists():
            raise serializers.ValidationError('Role not found in this tenant.')
        return value

    def create(self, validated_data):
        role_id = validated_data.pop('role', None)
        tenant = self.context['tenant']
        user_id = self.context['user_id']
        page = Page.objects.create(
            tenant=tenant,
            user_id=user_id,
            role_id=role_id,
            **validated_data,
        )
        return page

    def update(self, instance, validated_data):
        if 'role' in validated_data:
            instance.role_id = validated_data.pop('role')
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()
        return instance
