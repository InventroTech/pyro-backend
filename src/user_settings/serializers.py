from rest_framework import serializers
from .models import Group, TenantMemberSetting


class TenantMemberSettingSerializer(serializers.ModelSerializer):
    """Serializer for dedicated per-tenant-member settings table."""

    class Meta:
        model = TenantMemberSetting
        fields = ["id", "tenant", "tenant_membership", "key", "value", "created_at", "updated_at"]
        read_only_fields = ["id", "tenant", "created_at", "updated_at"]


class LeadTypeAssignmentSerializer(serializers.Serializer):
    """Serializer specifically for lead type assignments"""
    user_id = serializers.CharField()  # Accept both UUID and integer ID
    lead_types = serializers.ListField(
        child=serializers.CharField(max_length=100),
        allow_empty=True,
        help_text="List of lead types (affiliated_party) assigned to the user"
    )
    lead_sources = serializers.ListField(
        child=serializers.CharField(max_length=200),
        allow_empty=True,
        required=False,
        default=list,
        help_text="List of lead sources assigned to the user; only these leads will be directed to the RM"
    )
    lead_statuses = serializers.ListField(
        child=serializers.CharField(max_length=200),
        allow_empty=True,
        required=False,
        default=list,
        help_text="List of lead statuses assigned to the user; only these leads will be directed to the RM"
    )
    daily_target = serializers.IntegerField(
        required=False,
        allow_null=True,
        min_value=0,
        help_text="Daily target for the user"
    )
    daily_limit = serializers.IntegerField(
        required=False,
        allow_null=True,
        min_value=0,
        help_text="Daily lead pull limit for the user (max leads they can fetch per day)"
    )
    def validate_user_id(self, value):
        """Validate and normalize user_id - can be UUID string or integer string"""
        # Try to parse as UUID first
        try:
            import uuid
            return str(uuid.UUID(value))
        except (ValueError, AttributeError):
            # If not a valid UUID, assume it's an integer ID
            try:
                int_id = int(value)
                # Store as-is for lookup - we'll handle conversion in the view
                return str(int_id)
            except (ValueError, TypeError):
                raise serializers.ValidationError("user_id must be a valid UUID or integer ID")

    def validate_lead_types(self, value):
        """Validate lead types list"""
        if not isinstance(value, list):
            raise serializers.ValidationError("Lead types must be a list")
        return [lt.strip() for lt in value if lt.strip()]

    def validate_lead_sources(self, value):
        """Validate lead sources list"""
        if not isinstance(value, list):
            return []
        return [ls.strip() for ls in value if ls.strip()]

    def validate_lead_statuses(self, value):
        """Validate lead statuses list"""
        if not isinstance(value, list):
            return []
        return [lstatus.strip() for lstatus in value if lstatus.strip()]

    def validate_daily_target(self, value):
        """Validate daily_target"""
        if value is not None and value < 0:
            raise serializers.ValidationError("Daily target must be a non-negative integer")
        return value

    def validate_daily_limit(self, value):
        """Validate daily_limit"""
        if value is not None and value < 0:
            raise serializers.ValidationError("Daily limit must be a non-negative integer")
        return value


class UserCoreKVSettingsPatchSerializer(serializers.Serializer):
    """PATCH body for CSE support daily limits / resolve-rate goal on core-kv-settings."""

    support_daily_limit_self_trial = serializers.IntegerField(
        required=False,
        allow_null=True,
        min_value=0,
        help_text="Max fresh Self Trial tickets per day; null clears the cap",
    )
    support_daily_limit_other = serializers.IntegerField(
        required=False,
        allow_null=True,
        min_value=0,
        help_text="Max fresh non–Self Trial tickets per day; null clears the cap",
    )
    support_resolve_rate_goal = serializers.IntegerField(
        required=False,
        allow_null=True,
        min_value=0,
        max_value=100,
        help_text="Overall resolve-rate goal percent (0–100); null clears",
    )

    def validate(self, attrs):
        if not attrs:
            raise serializers.ValidationError(
                "Provide at least one of support_daily_limit_self_trial, "
                "support_daily_limit_other, support_resolve_rate_goal"
            )
        return attrs


class GroupSerializer(serializers.ModelSerializer):
    """Serializer for tenant groups."""

    fresh_leads_count = serializers.SerializerMethodField()

    class Meta:
        model = Group
        fields = ["id", "tenant", "name", "group_data", "fresh_leads_count", "created_at", "updated_at"]
        read_only_fields = ["id", "tenant", "created_at", "updated_at", "fresh_leads_count"]

    def get_fresh_leads_count(self, obj: Group):
        counts = self.context.get("fresh_leads_counts") or {}
        if obj.id in counts:
            return counts[obj.id]
        return None

    def validate_name(self, value: str) -> str:
        cleaned = (value or "").strip()
        if not cleaned:
            raise serializers.ValidationError("Group name is required")
        return cleaned

    def validate_group_data(self, value):
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise serializers.ValidationError("group_data must be a JSON object")
        return value
