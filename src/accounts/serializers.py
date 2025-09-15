from rest_framework import serializers
from accounts.models import LegacyUser

class LegacyUserCreateSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=255)
    email = serializers.EmailField()
    company_name = serializers.CharField(required=False, allow_blank=True)
    role_id=serializers.UUIDField(required=False,allow_null=True)
    uid=serializers.UUIDField(required=False,allow_null=True)

    def validate(self, attrs):
        req = self.context['request']
        tenant = getattr(req, 'tenant', None)
        if not tenant:
            raise serializers.ValidationError("Tenant is not resolved")
        attrs["_tenant"] = tenant
        email = attrs["email"].strip().lower()
        attrs["email"] = email

        if LegacyUser.objects.filter(tenant=tenant, email__iexact=email).exists():
            raise serializers.ValidationError({'email': 'User with this email already exists in this tenant.'})
        return attrs


class LegacyUserLiteSerializer(serializers.ModelSerializer):
    class Meta:
        model = LegacyUser
        fields = ['id', 'name', 'email', 'company_name', 'uid']


class LinkUserUidSerializer(serializers.Serializer):
    email = serializers.EmailField()
    uid = serializers.CharField(max_length=64)