from rest_framework import serializers

from .events import resolve_support_ticket_record


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


class SaveAndContinueSerializer(serializers.Serializer):
    """
    Serializer for save-and-continue API request - matches the edge function parameters
    """
    ticketId = serializers.IntegerField(required=True)
    resolutionStatus = serializers.CharField(max_length=255, required=False, allow_blank=True)
    callStatus = serializers.CharField(max_length=255, required=False, allow_blank=True)
    cseRemarks = serializers.CharField(required=False, allow_blank=True)
    resolutionTime = serializers.CharField(max_length=255, required=False, allow_blank=True)
    otherReasons = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_empty=True
    )
    ticketStartTime = serializers.CharField(required=False, allow_blank=True)
    isReadOnly = serializers.BooleanField(default=False)
    reviewRequested = serializers.BooleanField(required=False, allow_null=True)

    def validate_ticketId(self, value):
        """Validate that the ticket record exists for the current tenant."""
        request = self.context.get('request')
        tenant = getattr(request, 'tenant', None) if request else None
        if tenant is None or not resolve_support_ticket_record(tenant=tenant, ticket_id=value):
            raise serializers.ValidationError("Ticket not found")
        return value

    def validate_isReadOnly(self, value):
        """Validate that the ticket is not read-only"""
        if value:
            raise serializers.ValidationError("This ticket is read-only")
        return value


class SaveAndContinueResponseSerializer(serializers.Serializer):
    """
    Serializer for save-and-continue API response
    """
    success = serializers.BooleanField()
    message = serializers.CharField()
    updatedTicket = serializers.DictField()
    userId = serializers.CharField()
    userEmail = serializers.CharField()
    totalResolutionTime = serializers.CharField()


class GetNextTicketResponseSerializer(serializers.Serializer):
    """
    Serializer for get-next-ticket API response.

    ``ticket`` is a flattened dict built from a support ``Record`` row.
    """

    ticket = serializers.DictField(required=False, allow_null=True)

    def to_representation(self, instance):
        if not instance.get("ticket"):
            return {}
        return super().to_representation(instance)
    


class UpdateCallStatusRequestSerializer(serializers.Serializer):
    ticketId = serializers.IntegerField(required=True)
    callStatus = serializers.CharField(required=True, allow_blank=False)

    def validate_ticketId(self, value):
        request = self.context.get("request")
        tenant = getattr(request, "tenant", None) if request else None
        if tenant is None or not resolve_support_ticket_record(tenant=tenant, ticket_id=value):
            raise serializers.ValidationError("Ticket not found")
        return value
    resolutionStatus = serializers.CharField(required=False, allow_blank=True)
    cseRemarks = serializers.CharField(required=False, allow_blank=True)
    resolutionTime = serializers.CharField(required=False, allow_blank=True)
    otherReasons = serializers.ListField(
        child=serializers.CharField(), required=False, allow_empty=True
    )
    assignedTo = serializers.UUIDField(required=False, allow_null=True)


class SupportTicketUpdateSerializer(serializers.Serializer):
    """
    Serializer for updating support tickets - specifically for admin assignment
    """
    ticket_id = serializers.IntegerField(required=True)
    assigned_to = serializers.UUIDField(required=False, allow_null=True)
    resolution_status = serializers.CharField(max_length=255, required=False, allow_blank=True)
    layout_status = serializers.CharField(max_length=255, required=False, allow_blank=True)
    cse_name = serializers.CharField(max_length=255, required=False, allow_blank=True)
    cse_remarks = serializers.CharField(required=False, allow_blank=True)
    call_status = serializers.CharField(max_length=255, required=False, allow_blank=True)
    snooze_until = serializers.DateTimeField(required=False, allow_null=True)
    review_requested = serializers.BooleanField(required=False, allow_null=True)
    
    def validate_ticket_id(self, value):
        request = self.context.get("request")
        tenant = getattr(request, "tenant", None) if request else None
        if tenant is None or not resolve_support_ticket_record(tenant=tenant, ticket_id=value):
            raise serializers.ValidationError("Ticket not found")
        return value
    
    def validate(self, data):
        """Validate that at least one field is being updated"""
        ticket_id = data.get('ticket_id')
        update_fields = {k: v for k, v in data.items() if k != 'ticket_id'}
        
        if not update_fields:
            raise serializers.ValidationError("At least one field must be provided for update")
        
        return data

#
class TakeBreakSerializer(serializers.Serializer):
    """
    Serializer for the take-break API request
    """
    ticketId = serializers.IntegerField(required=True)
    resolutionStatus = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)

    def validate_ticketId(self, value):
        request = self.context.get("request")
        tenant = getattr(request, "tenant", None) if request else None
        if tenant is None or not resolve_support_ticket_record(tenant=tenant, ticket_id=value):
            raise serializers.ValidationError("Ticket not found")
        return value
