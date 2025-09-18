from django.db import models
import uuid


class SupportTicketDump(models.Model):
    """
    Temporary dump table for incoming ticket webhooks before processing.
    Matches the support_ticket_dump table structure from Supabase.
    """
    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    # Required fields
    tenant_id = models.UUIDField()
    ticket_date = models.DateTimeField()
    
    # Optional fields (matching the ALLOWED_FIELDS from edge function)
    user_id = models.CharField(max_length=255, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=50, null=True, blank=True)
    reason = models.TextField(null=True, blank=True)
    rm_name = models.TextField(null=True, blank=True)
    layout_status = models.CharField(max_length=255, null=True, blank=True)
    badge = models.CharField(max_length=255, null=True, blank=True)
    poster = models.CharField(max_length=255, null=True, blank=True)
    subscription_status = models.TextField(null=True, blank=True)
    atleast_paid_once = models.BooleanField(null=True, blank=True)
    source = models.CharField(max_length=255, null=True, blank=True)
    praja_dashboard_user_link = models.TextField(null=True, blank=True)
    display_pic_url = models.TextField(null=True, blank=True)
    
    # Processing status (for cron job)
    is_processed = models.BooleanField(default=False)
    processed_at = models.DateTimeField(null=True, blank=True)
    processing_error = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "support_ticket_dump"
        indexes = [
            models.Index(fields=["tenant_id", "created_at"], name="std_tenant_created"),
            models.Index(fields=["is_processed", "created_at"], name="std_processed_created"),
        ]

    def __str__(self):
        return f"Dump {self.id} - {self.tenant_id} - {self.created_at}"
