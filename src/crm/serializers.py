from rest_framework import serializers
from .models import Lead

ALLOWED_STATUSES = {"New", "Pending", "Follow-up", "WIP", "Resolved", "Won", "Lost", "Can't Resolve"}

class LeadSerializer(serializers.ModelSerializer):
    """Read serializer (returns everything)."""
    class Meta:
        model = Lead
        fields = "__all__"


class LeadCreateSerializer(serializers.ModelSerializer):
    """
    Write serializer: only allow client to set safe fields.
    Server will set tenant_id, assigned_to, created_at/updated_at.
    """
    class Meta:
        model = Lead
        fields = [
            "name", "phone_no", "user_id",
            "lead_description", "other_description",
            "lead_creation_date", "praja_dashboard_user_link",
            "lead_score", "atleast_paid_once", "reason",
            "badge", "display_pic_url", "lead_status",
        ]

    def validate_lead_status(self, value):
        if value and value not in ALLOWED_STATUSES:
            raise serializers.ValidationError(f"Invalid status: {value}")
        return value
