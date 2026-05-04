from typing import Optional

from rest_framework import serializers
from object_history.models import ObjectHistory
from .models import Record, EventLog, RuleSet, RuleExecutionLog, EntityTypeSchema, CallAttemptMatrix, ScoringRule
from .assignee_display import build_assigned_to_display_map, _is_empty_assigned


class RecordListSerializer(serializers.ListSerializer):
    """
    One query to resolve all assigned_to identifiers on a page of records.
    """

    def to_representation(self, data):
        iterable = data.all() if hasattr(data, "all") else data
        records = list(iterable)
        tenant = None
        if records:
            tenant = getattr(records[0], "tenant", None)
        if not tenant:
            tenant = self.context.get("tenant")

        assigned_ids = []
        if tenant and records:
            for r in records:
                d = r.data if isinstance(r.data, dict) else {}
                v = d.get("assigned_to")
                if not _is_empty_assigned(v):
                    assigned_ids.append(str(v).strip())

        self.child.context["assigned_to_display_map"] = (
            build_assigned_to_display_map(tenant, assigned_ids) if tenant else {}
        )
        return super().to_representation(records)


class RecordSerializer(serializers.ModelSerializer):
    """
    Serializer for Record model with tenant isolation.
    Prevents tenant spoofing by making tenant_id read-only.
    """

    tenant_id = serializers.UUIDField(read_only=True)
    assigned_to_display = serializers.SerializerMethodField()

    class Meta:
        model = Record
        fields = [
            "id",
            "tenant_id",
            "entity_type",
            "data",
            "pyro_data",
            "assigned_to_display",
            "created_at",
            "updated_at",
        ]
        read_only_fields = [
            "id",
            "tenant_id",
            "assigned_to_display",
            "created_at",
            "updated_at",
        ]
        list_serializer_class = RecordListSerializer
    
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

    def get_assigned_to_display(self, obj):
        d = obj.data if isinstance(obj.data, dict) else {}
        raw = d.get("assigned_to")
        if _is_empty_assigned(raw):
            return None
        key = str(raw).strip()
        m = self.context.get("assigned_to_display_map")
        if m is not None and key in m:
            return m[key]
        tenant = self.context.get("tenant") or getattr(obj, "tenant", None)
        if tenant:
            one = build_assigned_to_display_map(tenant, [key])
            return one.get(key, key)
        return key


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


class ScoringRuleModelSerializer(serializers.ModelSerializer):
    """
    ModelSerializer for ScoringRule model - supports CRUD operations.
    
    Flexible structure: 'data' field can contain any structure (operator, value, etc.)
    """
    class Meta:
        model = ScoringRule
        fields = [
            'id',
            'entity_type',
            'attribute',
            'data',
            'weight',
            'order',
            'is_active',
            'description',
            'created_at',
            'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
    
    def validate_data(self, value):
        """Validate data is a dictionary."""
        if not isinstance(value, dict):
            raise serializers.ValidationError("Data must be a valid JSON object.")
        return value
    
    def validate_weight(self, value):
        """Validate weight is a valid number."""
        # Allow None for partial updates (PATCH)
        if value is None:
            return value
        try:
            return float(value)
        except (ValueError, TypeError):
            raise serializers.ValidationError("Weight must be a valid number")
    
    def validate_data(self, value):
        """Validate data is a dictionary."""
        # Allow None for partial updates (PATCH)
        if value is None:
            return value
        if not isinstance(value, dict):
            raise serializers.ValidationError("Data must be a valid JSON object.")
        return value


class LeadScoringRequestSerializer(serializers.Serializer):
    """
    Serializer for lead scoring request payload.
    
    Rules can be empty if rules already exist in ScoringRule table (for individual rule management).
    """
    rules = ScoringRuleSerializer(many=True, required=False, help_text="List of scoring rules (optional if rules exist in ScoringRule table)")
    
    def validate_rules(self, value):
        """Validate rules - allow empty list if rules exist in database."""
        # Empty rules are allowed - backend will check ScoringRule table first
        return value or []


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


def _actor_display_name(user) -> Optional[str]:
    if not user:
        return None
    meta = user.raw_user_meta_data
    if isinstance(meta, dict):
        return meta.get("full_name") or meta.get("name")
    return None


class RecordHistoryEntrySerializer(serializers.ModelSerializer):
    """
    Lean payload for record audit UI: field-level diff only, no full snapshots.
    """

    actor = serializers.SerializerMethodField()

    class Meta:
        model = ObjectHistory
        fields = ["id", "action", "version", "created_at", "actor", "changes"]
        read_only_fields = fields

    def get_actor(self, obj):
        user = getattr(obj, "actor_user", None)
        label = obj.actor_label
        if user:
            return {
                "id": str(user.id),
                "email": user.email,
                "name": _actor_display_name(user),
                "label": label,
            }
        return {
            "id": None,
            "email": None,
            "name": None,
            "label": label,
        }
