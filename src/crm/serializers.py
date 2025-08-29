from rest_framework import serializers
from .models import Lead
from scheduler.models import AttemptOutcome


# Keep your existing statuses
ALLOWED_STATUSES = {
    "won", "lost", "closed", "call_later",
    "scheduled", "in_queue",
    "assigned"
    }

class LeadSerializer(serializers.ModelSerializer):
    assigned_to_email = serializers.SerializerMethodField()

    class Meta:
        model = Lead
        fields = (
            "id", "name", "phone_no", "lead_status", "lead_score",
            "assigned_to_email",
            "attempt_count", "last_call_outcome", "next_call_at", "do_not_call",
            "created_at", "updated_at",
        )
        read_only_fields = (
            "id", "assigned_to_email", "attempt_count", "created_at", "updated_at",
        )

    def get_assigned_to_email(self, obj):
        return obj.assigned_to.email if obj.assigned_to else None


class LeadCreateSerializer(serializers.ModelSerializer):
    """
    Create serializer: client can set safe, descriptive fields.
    Server sets assigned_to/tenant in the view's perform_create.
    """
    class Meta:
        model = Lead
        fields = [
            "name",
            "phone_no",
            "user_id",
            "lead_description",
            "other_description",
            "lead_creation_date",
            "praja_dashboard_user_link",
            "lead_score",        
            "atleast_paid_once",
            "reason",
            "badge",
            "display_pic_url",
            "lead_status",
            "last_call_outcome",
            "next_call_at",
            "do_not_call",
        ]
        read_only_fields = ("id",)

    def validate_lead_status(self, value):
        if value and value not in ALLOWED_STATUSES:
            raise serializers.ValidationError(f"Invalid status: {value}")
        return value


class LeadUpdateSerializer(serializers.ModelSerializer):
    """
    Update serializer: fields an agent UI can change.
    - We DO NOT allow updating assigned_to, attempt_count, or lead_score here.
    - Business rule: if do_not_call becomes True -> force status Lost.
    """
    class Meta:
        model = Lead
        fields = [
            "name",
            "phone_no",            
            "user_id",
            "lead_description",
            "other_description",
            "lead_creation_date",
            "praja_dashboard_user_link",
            "atleast_paid_once",
            "reason",
            "badge",
            "display_pic_url",
            "lead_status",
            "last_call_outcome",
            "next_call_at",
            "do_not_call",
        ]
        read_only_fields = ("id", "assigned_to", "attempt_count")

    def validate_lead_status(self, value):
        if value and value not in ALLOWED_STATUSES:
            raise serializers.ValidationError(f"Invalid status: {value}")
        return value

    def validate(self, attrs):
        
        dnc = attrs.get("do_not_call", getattr(self.instance, "do_not_call", False))
        if dnc:
            attrs["lead_status"] = "Lost"
        return attrs


class LeadScoreUpdateSerializer(serializers.ModelSerializer):
    """
    Minimal serializer for internal/ops scoring updates.
    Keeps lead_score writes separate from general updates.
    """
    class Meta:
        model = Lead
        fields = ["lead_score"]
        read_only_fields = ("id",)


class LeadCallOutcomeRequest(serializers.Serializer): 
    outcome = serializers.ChoiceField(choices=AttemptOutcome.choices) 
    callback_at = serializers.DateTimeField(required=False)





OUTCOME_CHOICES = ("won", "lost", "call_later")

class LeadOutcomeRequestSerializer(serializers.Serializer):
    outcome = serializers.ChoiceField(choices=OUTCOME_CHOICES)
    # Keep the client-facing key as `callbackAt` if FE already uses it
    callbackAt = serializers.DateTimeField(required=False, allow_null=True)

    def validate(self, attrs):
        outcome = attrs.get("outcome")
        callback_at = attrs.get("callbackAt")
        return attrs

class LeadOutcomeResponseSerializer(serializers.Serializer):
    ok = serializers.BooleanField()
    attempt_count = serializers.IntegerField()
    next_call_at = serializers.DateTimeField(allow_null=True)

class ErrorSerializer(serializers.Serializer):
    error = serializers.CharField()
