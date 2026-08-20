from rest_framework import serializers


class TenantMembershipCreateSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=255)
    email = serializers.EmailField()
    company_name = serializers.CharField(required=False, allow_blank=True)
    department = serializers.CharField(required=False, allow_blank=True, allow_null=True, max_length=255)
    role_id = serializers.UUIDField(required=False, allow_null=True)
    uid = serializers.UUIDField(required=False, allow_null=True)
    lead_group_name = serializers.CharField(required=False, allow_blank=True, allow_null=True, max_length=255)
    daily_target = serializers.IntegerField(required=False, allow_null=True, min_value=0)
    daily_limit = serializers.IntegerField(required=False, allow_null=True, min_value=0)

    def validate(self, attrs):
        req = self.context['request']
        tenant = getattr(req, 'tenant', None)
        if not tenant:
            raise serializers.ValidationError("Tenant is not resolved")
        attrs["_tenant"] = tenant
        email = attrs["email"].strip().lower()
        attrs["email"] = email

        from authz.models import TenantMembership
        role_id = attrs.get("role_id")
        
        if role_id:
            if TenantMembership.objects.filter(
                tenant=tenant,
                email=email,
                role_id=role_id
            ).exists():
                raise serializers.ValidationError({
                    'email': f'User with this email and role already exists in this tenant.'
                })
        else:
            if TenantMembership.objects.filter(
                tenant=tenant,
                email=email
            ).exists():
                raise serializers.ValidationError({
                    'email': 'User with this email already exists in this tenant.'
                })

        return attrs


class TenantMembershipUpdateSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=255)
    email = serializers.EmailField()
    department = serializers.CharField(required=False, allow_blank=True, allow_null=True, max_length=255)
    role_id = serializers.UUIDField(required=True)
    original_email = serializers.EmailField(required=True)
    original_role_id = serializers.UUIDField(required=True)
    lead_group_name = serializers.CharField(required=False, allow_blank=True, allow_null=True, max_length=255)
    daily_target = serializers.IntegerField(required=False, allow_null=True, min_value=0)
    daily_limit = serializers.IntegerField(required=False, allow_null=True, min_value=0)
    
    # Optional manager fields added to prevent them from being stripped during updates
    manager_email = serializers.EmailField(required=False, allow_blank=True, allow_null=True)
    manager_name = serializers.CharField(required=False, allow_blank=True, allow_null=True, max_length=255)

    def validate(self, attrs):
        req = self.context["request"]
        tenant = getattr(req, "tenant", None)
        if not tenant:
            raise serializers.ValidationError("Tenant is not resolved")

        from authz.models import TenantMembership

        attrs["_tenant"] = tenant
        attrs["email"] = attrs["email"].strip().lower()
        attrs["original_email"] = attrs["original_email"].strip().lower()

        existing_membership = TenantMembership.objects.filter(
            tenant=tenant,
            email=attrs["original_email"],
            role_id=attrs["original_role_id"],
        ).first()

        if not existing_membership:
            raise serializers.ValidationError({
                "original_email": "Original user membership not found for this tenant."
            })

        conflict = TenantMembership.objects.filter(
            tenant=tenant,
            email=attrs["email"],
            role_id=attrs["role_id"],
        ).exclude(id=existing_membership.id).exists()
        if conflict:
            raise serializers.ValidationError({
                "email": "User with this email and role already exists in this tenant."
            })

        attrs["_membership"] = existing_membership
        return attrs


class LinkUserUidSerializer(serializers.Serializer):
    email = serializers.EmailField()
    uid = serializers.CharField(max_length=64)


class DeleteUserEverywhereSerializer(serializers.Serializer):
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

        if email:
            attrs["email"] = email.strip().lower()

        if not uid and not (email and role_id):
            raise serializers.ValidationError(
                "Provide either 'uid' or both 'email' and 'role_id'."
            )

        attrs["_tenant"] = tenant
        return attrs