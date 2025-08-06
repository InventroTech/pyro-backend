from django.db import models
from django.utils import timezone


class SupportTicketDump(models.Model):
    """
    Model to store tickets from webhook before processing.
    Matches the support_ticket_dump table structure exactly.
    """
    id = models.BigAutoField(primary_key=True)
    is_processed = models.BooleanField(default=False)
    created_at = models.DateTimeField(null=True, blank=True)
    ticket_date = models.DateTimeField(null=True, blank=True)
    user_id = models.CharField(max_length=255, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=255, null=True, blank=True)
    source = models.CharField(max_length=255, null=True, blank=True)
    subscription_status = models.TextField(null=True, blank=True)
    atleast_paid_once = models.BooleanField(null=True, blank=True)
    reason = models.TextField(null=True, blank=True)
    badge = models.CharField(max_length=255, null=True, blank=True)
    poster = models.CharField(max_length=255, null=True, blank=True)
    tenant_id = models.UUIDField(null=True, blank=True)
    assigned_to = models.UUIDField(null=True, blank=True)
    layout_status = models.CharField(max_length=255, null=True, blank=True)
    resolution_status = models.CharField(max_length=255, null=True, blank=True)
    resolution_time = models.TextField(null=True, blank=True)
    cse_name = models.CharField(max_length=255, null=True, blank=True)
    cse_remarks = models.TextField(null=True, blank=True)
    call_status = models.CharField(max_length=255, null=True, blank=True)
    call_attempts = models.IntegerField(null=True, blank=True)
    rm_name = models.TextField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    snooze_until = models.DateTimeField(null=True, blank=True)
    dumped_at = models.DateTimeField(default=timezone.now)
    praja_dashboard_user_link = models.TextField(null=True, blank=True)
    display_pic_url = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'support_ticket_dump'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['is_processed']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return f"Ticket {self.id} - {self.name} ({self.user_id})"

    def mark_as_processed(self):
        """Mark the ticket as processed"""
        self.is_processed = True
        self.save(update_fields=['is_processed'])
