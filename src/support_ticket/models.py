from django.db import models
from django.contrib.postgres.fields import ArrayField
import uuid


class SupportTicketDump(models.Model):
    """
    Temporary staging table for support tickets before they're processed.
    This matches the structure expected by the DumpTicketWebhookView.
    """
    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    ticket_date = models.DateTimeField(null=True, blank=True)
    user_id = models.CharField(max_length=255, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=50, null=True, blank=True)
    source = models.CharField(max_length=255, null=True, blank=True)
    subscription_status = models.TextField(null=True, blank=True)
    atleast_paid_once = models.BooleanField(null=True, blank=True)
    reason = models.TextField(null=True, blank=True)
    badge = models.CharField(max_length=255, null=True, blank=True)
    poster = models.CharField(max_length=255, null=True, blank=True)
    tenant_id = models.UUIDField()  # Required field
    layout_status = models.CharField(max_length=255, null=True, blank=True)
    praja_dashboard_user_link = models.TextField(null=True, blank=True)
    display_pic_url = models.TextField(null=True, blank=True)
    is_processed = models.BooleanField(default=False)  # For cron job tracking

    class Meta:
        db_table = "support_ticket_dump"
        managed = True
        indexes = [
            models.Index(fields=["tenant_id", "-created_at"], name="std_tn_cr_desc"),
            models.Index(fields=["is_processed", "-created_at"], name="std_proc_cr"),
        ]

    def __str__(self):
        return f"SupportTicketDump {self.id} - {self.name or 'Unknown'} ({self.tenant_id})"
