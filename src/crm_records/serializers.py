from rest_framework import serializers
from .models import Record, EventLog, RuleSet, RuleExecutionLog, EntityTypeSchema, CallAttemptMatrix


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
            "data", 
            "pyro_data",
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
    
    def validate_pyro_data(self, value):
        """
        Validate that pyro_data is a dictionary/object or None.
        """
        if value is None:
            return None
        if not isinstance(value, dict):
            raise serializers.ValidationError("Pyro data must be a valid JSON object or null.")
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


class EntityTypeSchemaSerializer(serializers.ModelSerializer):
    """
    Serializer for EntityTypeSchema model.
    Stores entity type definitions with their attribute lists.
    """
    tenant_id = serializers.UUIDField(read_only=True)
    
    class Meta:
        model = EntityTypeSchema
        fields = [
            "id",
            "tenant_id",
            "entity_type",
            "attributes",
            "rules",
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
    
    def validate_entity_type(self, value):
        """Validate entity_type is not empty and has reasonable length."""
        if not value or not value.strip():
            raise serializers.ValidationError("Entity type cannot be empty.")
        if len(value) > 100:
            raise serializers.ValidationError("Entity type cannot exceed 100 characters.")
        return value.strip()
    
    def validate_attributes(self, value):
        """Validate attributes is a list of strings."""
        if not isinstance(value, list):
            raise serializers.ValidationError("Attributes must be a list.")
        
        for i, attr in enumerate(value):
            if not isinstance(attr, str):
                raise serializers.ValidationError(f"Attribute at index {i} must be a string.")
            if not attr.strip():
                raise serializers.ValidationError(f"Attribute at index {i} cannot be empty.")
        
        # Remove duplicates and sort
        unique_attrs = sorted(list(set([attr.strip() for attr in value if attr.strip()])))
        return unique_attrs
    
    def validate_rules(self, value):
        """Validate rules is a list of rule objects."""
        if not isinstance(value, list):
            raise serializers.ValidationError("Rules must be a list.")
        
        for i, rule in enumerate(value):
            if not isinstance(rule, dict):
                raise serializers.ValidationError(f"Rule at index {i} must be a dictionary.")
            
            # Validate required fields
            required_fields = ['attr', 'operator', 'value', 'weight']
            for field in required_fields:
                if field not in rule:
                    raise serializers.ValidationError(f"Rule at index {i} is missing required field: {field}")
        
        return value


class ScoringRuleSerializer(serializers.Serializer):
    """
    Serializer for individual scoring rule.
    """
    attr = serializers.CharField(help_text="Attribute path (e.g., 'data.assigned_to', 'data.affiliated_party')")
    operator = serializers.ChoiceField(
        choices=['==', '!=', '>', '<', '>=', '<=', 'contains', 'in'],
        help_text="Comparison operator"
    )
    value = serializers.CharField(help_text="Value to compare against")
    weight = serializers.FloatField(help_text="Weight/score to add if rule matches")


class LeadScoringRequestSerializer(serializers.Serializer):
    """
    Serializer for lead scoring request payload.
    """
    rules = ScoringRuleSerializer(many=True, help_text="List of scoring rules")
    
    def validate_rules(self, value):
        """Validate that rules list is not empty."""
        if not value or len(value) == 0:
            raise serializers.ValidationError("At least one rule is required.")
        return value


class CallAttemptMatrixSerializer(serializers.ModelSerializer):
    """
    Serializer for CallAttemptMatrix model with tenant isolation.
    """
    tenant_id = serializers.UUIDField(read_only=True)
    
    class Meta:
        model = CallAttemptMatrix
        fields = [
            "id",
            "tenant_id",
            "lead_type",
            "max_call_attempts",
            "sla_days",
            "min_time_between_calls_hours",
            "created_at",
            "updated_at"
        ]
        read_only_fields = [
            "id",
            "tenant_id",
            "created_at",
            "updated_at"
        ]
    
    def validate_lead_type(self, value):
        """Validate lead_type is not empty and has reasonable length."""
        if not value or not value.strip():
            raise serializers.ValidationError("Lead type cannot be empty.")
        if len(value) > 100:
            raise serializers.ValidationError("Lead type cannot exceed 100 characters.")
        return value.strip()
    
    def validate_max_call_attempts(self, value):
        """Validate max_call_attempts is positive."""
        if value <= 0:
            raise serializers.ValidationError("Max call attempts must be greater than 0.")
        if value > 100:
            raise serializers.ValidationError("Max call attempts cannot exceed 100.")
        return value
    
    def validate_sla_days(self, value):
        """Validate sla_days is positive."""
        if value <= 0:
            raise serializers.ValidationError("SLA days must be greater than 0.")
        if value > 365:
            raise serializers.ValidationError("SLA days cannot exceed 365.")
        return value
    
    def validate_min_time_between_calls_hours(self, value):
        """Validate min_time_between_calls_hours is positive."""
        if value <= 0:
            raise serializers.ValidationError("Minimum time between calls must be greater than 0.")
        if value > 168:  # 1 week
            raise serializers.ValidationError("Minimum time between calls cannot exceed 168 hours (1 week).")
        return value
