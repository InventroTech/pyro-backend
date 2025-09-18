from rest_framework import serializers
from analytics.models import SupportTicket
from .models import SupportTicketDump


class TicketDumpWebhookSerializer(serializers.Serializer):
    """
    Serializer for incoming ticket webhook data - matches edge function ALLOWED_FIELDS exactly
    """
    # Required field
    tenant_id = serializers.UUIDField(required=True)
    
    # Optional fields (matching ALLOWED_FIELDS from edge function)
    ticket_date = serializers.DateTimeField(required=False, allow_null=True)
    user_id = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)
    name = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)
    phone = serializers.CharField(max_length=50, required=False, allow_blank=True, allow_null=True)
    reason = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    rm_name = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    layout_status = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)
    badge = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)
    poster = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)
    subscription_status = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    atleast_paid_once = serializers.BooleanField(required=False, allow_null=True)
    source = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)
    praja_dashboard_user_link = serializers.URLField(required=False, allow_blank=True, allow_null=True)
    display_pic_url = serializers.URLField(required=False, allow_blank=True, allow_null=True)

    def validate(self, data):
        """Clean the data - only include non-null, non-undefined values like edge function"""
        cleaned_data = {}
        
        # tenant_id is required
        if not data.get('tenant_id'):
            raise serializers.ValidationError("Missing required field: tenant_id")
        
        # Only include fields that are not null/undefined (like edge function logic)
        for field, value in data.items():
            if value is not None and value != '':
                cleaned_data[field] = value
        
        # Set default ticket_date if not present
        if 'ticket_date' not in cleaned_data:
            from django.utils import timezone
            cleaned_data['ticket_date'] = timezone.now()
        
        return cleaned_data


class TicketUpdateWebhookSerializer(serializers.Serializer):
    """
    Serializer for ticket update webhook data
    """
    ticket_id = serializers.IntegerField(required=False, allow_null=True)
    user_id = serializers.CharField(max_length=255, required=False, allow_blank=True)
    tenant_id = serializers.UUIDField(required=False, allow_null=True)
    
    # Updatable fields
    resolution_status = serializers.CharField(max_length=255, required=False, allow_blank=True)
    resolution_time = serializers.CharField(max_length=255, required=False, allow_blank=True)
    cse_name = serializers.CharField(max_length=255, required=False, allow_blank=True)
    cse_remarks = serializers.CharField(required=False, allow_blank=True)
    call_status = serializers.CharField(max_length=255, required=False, allow_blank=True)
    call_attempts = serializers.IntegerField(required=False, allow_null=True)
    assigned_to = serializers.UUIDField(required=False, allow_null=True)
    completed_at = serializers.DateTimeField(required=False, allow_null=True)
    snooze_until = serializers.DateTimeField(required=False, allow_null=True)
    layout_status = serializers.CharField(max_length=255, required=False, allow_blank=True)

    def validate(self, data):
        """Validate that either ticket_id or both user_id and tenant_id are provided"""
        ticket_id = data.get('ticket_id')
        user_id = data.get('user_id')
        tenant_id = data.get('tenant_id')
        
        if not ticket_id and not (user_id and tenant_id):
            raise serializers.ValidationError(
                "Either 'ticket_id' or both 'user_id' and 'tenant_id' must be provided"
            )
        
        return data


class SupportTicketResponseSerializer(serializers.ModelSerializer):
    """
    Serializer for SupportTicket response
    """
    class Meta:
        model = SupportTicket
        fields = [
            'id', 'created_at', 'ticket_date', 'user_id', 'name', 'phone',
            'source', 'subscription_status', 'atleast_paid_once', 'reason',
            'badge', 'poster', 'tenant_id', 'assigned_to', 'layout_status',
            'resolution_status', 'resolution_time', 'cse_name', 'cse_remarks',
            'call_status', 'call_attempts', 'completed_at', 'dumped_at'
        ]
        read_only_fields = ['id', 'created_at', 'dumped_at']
