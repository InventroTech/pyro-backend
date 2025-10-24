from rest_framework import serializers
from .models import UserSettings


class UserSettingsSerializer(serializers.ModelSerializer):
    """Serializer for UserSettings model"""
    
    class Meta:
        model = UserSettings
        fields = ['id', 'tenant', 'user_id', 'key', 'value', 'created_at', 'updated_at']
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
        fields = ['user_id', 'key', 'value']

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
    user_id = serializers.UUIDField()
    lead_types = serializers.ListField(
        child=serializers.CharField(max_length=100),
        allow_empty=True,
        help_text="List of lead types assigned to the user"
    )

    def validate_lead_types(self, value):
        """Validate lead types list"""
        if not isinstance(value, list):
            raise serializers.ValidationError("Lead types must be a list")
        return [lt.strip() for lt in value if lt.strip()]
