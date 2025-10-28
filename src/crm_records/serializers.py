from rest_framework import serializers
from .models import Record, EventLog, RuleSet, RuleExecutionLog


class RecordSerializer(serializers.ModelSerializer):
    """
    Serializer for Record model with tenant isolation.
    Prevents tenant spoofing by making tenant_id read-only.
    """
    tenant_id = serializers.UUIDField(read_only=True)
    
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


class EventLogSerializer(serializers.ModelSerializer):
    """
    Serializer for EventLog model with tenant isolation.
    Controls event creation payload and output format for API responses.
    """
    tenant_id = serializers.UUIDField(read_only=True)
    record_id = serializers.IntegerField(read_only=True)
    
    class Meta:
        model = EventLog
        fields = [
            "id",
            "record_id", 
            "tenant_id",
            "event",
            "payload",
            "timestamp"
        ]
        read_only_fields = [
            "id",
            "record_id",
            "tenant_id", 
            "timestamp"
        ]
    
    def validate_event(self, value):
        """
        Validate event name is not empty and has reasonable length.
        """
        if not value or not value.strip():
            raise serializers.ValidationError("Event name cannot be empty.")
        if len(value) > 100:
            raise serializers.ValidationError("Event name cannot exceed 100 characters.")
        return value.strip()
    
    def validate_payload(self, value):
        """
        Validate that payload is a dictionary/object.
        """
        if not isinstance(value, dict):
            raise serializers.ValidationError("Payload must be a valid JSON object.")
        return value


class RuleSetSerializer(serializers.ModelSerializer):
    """
    Serializer for RuleSet model with tenant isolation.
    Allows tenant implementors to manage rule configurations.
    """
    tenant_id = serializers.UUIDField(read_only=True)
    
    class Meta:
        model = RuleSet
        fields = [
            "id",
            "tenant_id",
            "event_name",
            "condition",
            "actions",
            "enabled",
            "description",
            "created_at",
            "updated_at"
        ]
        read_only_fields = [
            "id",
            "tenant_id",
            "created_at",
            "updated_at"
        ]
    
    def validate_event_name(self, value):
        """
        Validate event name is not empty and has reasonable length.
        """
        if not value or not value.strip():
            raise serializers.ValidationError("Event name cannot be empty.")
        if len(value) > 100:
            raise serializers.ValidationError("Event name cannot exceed 100 characters.")
        return value.strip()
    
    def validate_condition(self, value):
        """
        Validate that condition is a dictionary/object.
        """
        if not isinstance(value, dict):
            raise serializers.ValidationError("Condition must be a valid JSON object.")
        return value
    
    def validate_actions(self, value):
        """
        Validate that actions is a list of action objects.
        """
        if not isinstance(value, list):
            raise serializers.ValidationError("Actions must be a list.")
        
        for i, action in enumerate(value):
            if not isinstance(action, dict):
                raise serializers.ValidationError(f"Action {i} must be a dictionary.")
            if 'action' not in action:
                raise serializers.ValidationError(f"Action {i} must have an 'action' field.")
        
        return value


class RuleExecutionLogSerializer(serializers.ModelSerializer):
    """
    Serializer for RuleExecutionLog model with tenant isolation.
    Provides read-only access to rule execution history for debugging.
    """
    tenant_id = serializers.UUIDField(read_only=True)
    record_id = serializers.IntegerField(read_only=True)
    rule_id = serializers.IntegerField(read_only=True, allow_null=True)
    
    class Meta:
        model = RuleExecutionLog
        fields = [
            "id",
            "tenant_id",
            "record_id",
            "rule_id",
            "event_name",
            "matched",
            "actions",
            "errors",
            "duration_ms",
            "created_at"
        ]
        read_only_fields = [
            "id",
            "tenant_id",
            "record_id",
            "rule_id",
            "event_name",
            "matched",
            "actions",
            "errors",
            "duration_ms",
            "created_at"
        ]
