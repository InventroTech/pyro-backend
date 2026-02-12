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

        # NEW: Check TenantMembership instead of LegacyUser
        from authz.models import TenantMembership
        role_id = attrs.get("role_id")
        
        if role_id:
            # Check for duplicate TenantMembership with same tenant + email + role
            if TenantMembership.objects.filter(
                tenant=tenant,
                email=email,
                role_id=role_id
            ).exists():
                raise serializers.ValidationError({
                    'email': f'User with this email and role already exists in this tenant.'
                })
        else:
            # If no role_id, check if any TenantMembership exists with this email
            if TenantMembership.objects.filter(
                tenant=tenant,
                email=email
            ).exists():
                raise serializers.ValidationError({
                    'email': 'User with this email already exists in this tenant.'
                })
        
        # DEPRECATED: Legacy check - remove after migration complete
        # Keep for backward compatibility during transition
        if LegacyUser.objects.filter(tenant=tenant, email__iexact=email).exists():
            # Don't fail, just log a warning - LegacyUser is being phased out
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"LegacyUser found for {email} in tenant {tenant.id}, but proceeding with TenantMembership creation")
        
        return attrs


class LegacyUserLiteSerializer(serializers.ModelSerializer):
    class Meta:
        model = LegacyUser
        fields = ['id', 'name', 'email', 'company_name', 'uid']


class LinkUserUidSerializer(serializers.Serializer):
    email = serializers.EmailField()
    uid = serializers.CharField(max_length=64)


class DeleteUserEverywhereSerializer(serializers.Serializer):
    """
    Identify a user by:
      A) uid (Supabase auth.users.id) or
      B) (email + role_id) scoped to request.tenant

    role_id can be either the Legacy Role ID (public.roles.id) OR the AuthZ Role ID.
    We attempt to resolve both to maximize match success.
    """
    uid = serializers.UUIDField(required=False)
    email = serializers.EmailField(required=False)
    role_id = serializers.UUIDField(required=False)

    def validate(self, attrs):
        req = self.context["request"]

        tenant = getattr(req, "tenant", None)
        if not tenant:
            raise serializers.ValidationError("Tenant not resolved on request.")

        uid = attrs.get("uid")
        email = attrs.get("email")
        role_id = attrs.get("role_id")

        # Normalize email
        if email:
            attrs["email"] = email.strip().lower()

        # Must have either uid OR (email & role_id)
        if not uid and not (email and role_id):
            raise serializers.ValidationError(
                "Provide either 'uid' or both 'email' and 'role_id'."
            )

        attrs["_tenant"] = tenant
        return attrs
