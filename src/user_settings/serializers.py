from rest_framework import serializers
from .models import UserSettings, RoutingRule


class UserSettingsSerializer(serializers.ModelSerializer):
    """Serializer for UserSettings model"""
    
    class Meta:
        model = UserSettings
        fields = ['id', 'tenant', 'user_id', 'key', 'value', 'daily_target', 'daily_limit', 'created_at', 'updated_at']
        read_only_fields = ['id', 'created_at', 'updated_at']

    def validate_key(self, value):
        """Validate that key is not empty and follows naming conventions"""
        if not value or not value.strip():
            raise serializers.ValidationError("Key cannot be empty")
        return value.strip().upper()

    def validate_value(self, value):
        """Validate that value is not None"""
        if value is None:
            raise serializers.ValidationError("Value cannot be None")
        return value


class UserSettingsCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating UserSettings"""
    
    class Meta:
        model = UserSettings
        fields = ['user_id', 'key', 'value', 'daily_target', 'daily_limit']

    def validate_key(self, value):
        """Validate that key is not empty and follows naming conventions"""
        if not value or not value.strip():
            raise serializers.ValidationError("Key cannot be empty")
        return value.strip().upper()

    def validate_value(self, value):
        """Validate that value is not None"""
        if value is None:
            raise serializers.ValidationError("Value cannot be None")
        return value


class LeadTypeAssignmentSerializer(serializers.Serializer):
    """Serializer specifically for lead type assignments"""
    user_id = serializers.CharField()  # Accept both UUID and integer ID
    lead_types = serializers.ListField(
        child=serializers.CharField(max_length=100),
        allow_empty=True,
        help_text="List of lead types assigned to the user"
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


class RoutingRuleSerializer(serializers.ModelSerializer):
    """
    Simple serializer for RoutingRule.

    v1 keeps conditions very free-form on the backend, with light validation that
    it's a dict and (optionally) has a 'filters' key.
    """

    class Meta:
        model = RoutingRule
        fields = [
            "id",
            "tenant",
            "user_id",
            "queue_type",
            "is_active",
            "conditions",
            "name",
            "description",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "tenant", "created_at", "updated_at"]

    def validate_queue_type(self, value: str) -> str:
        value = (value or "").strip().lower()
        if value not in {RoutingRule.QUEUE_TYPE_TICKET, RoutingRule.QUEUE_TYPE_LEAD}:
            raise serializers.ValidationError("queue_type must be 'ticket' or 'lead'")
        return value

    def validate_conditions(self, value):
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise serializers.ValidationError("conditions must be a JSON object")
        filters = value.get("filters")
        if filters is not None and not isinstance(filters, (list, tuple)):
            raise serializers.ValidationError("'filters' must be a list when provided")
        return value
    
    def validate(self, attrs):
        """
        Ensure conditions is always present and is a dict, never None.
        """
        # Ensure conditions is always a dict
        if 'conditions' not in attrs or attrs.get('conditions') is None:
            attrs['conditions'] = {}
        return attrs
    
