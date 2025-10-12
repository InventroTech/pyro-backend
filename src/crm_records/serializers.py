from rest_framework import serializers
from .models import Record


class RecordSerializer(serializers.ModelSerializer):
    """
    Serializer for Record model with tenant isolation.
    Prevents tenant spoofing by making tenant_id read-only.
    """
    tenant_id = serializers.UUIDField(read_only=True, source='tenant.id')
    
    class Meta:
        model = Record
        fields = [
            "id", 
            "tenant_id", 
            "entity_type", 
            "name", 
            "data", 
            "created_at", 
            "updated_at"
        ]
        read_only_fields = [
            "id", 
            "tenant_id", 
            "created_at", 
            "updated_at"
        ]
    
    def validate_data(self, value):
        """
        Validate that data is a dictionary/object.
        """
        if not isinstance(value, dict):
            raise serializers.ValidationError("Data must be a valid JSON object.")
        return value
    
    def validate_entity_type(self, value):
        """
        Validate entity_type is not empty and has reasonable length.
        """
        if not value or not value.strip():
            raise serializers.ValidationError("Entity type cannot be empty.")
        if len(value) > 100:
            raise serializers.ValidationError("Entity type cannot exceed 100 characters.")
        return value.strip()
