from django.db import models

class SupportTicket(models.Model):
    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField()
    ticket_date = models.DateTimeField(null=True, blank=True)
    user_id = models.CharField(max_length=255, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=50, null=True, blank=True)
    source = models.CharField(max_length=255, null=True, blank=True)
    subscription_status = models.TextField(null=True, blank=True)
    atleast_paid_once = models.BooleanField(null=True, blank=True)
    reason = models.TextField(null=True, blank=True)
    other_reasons = models.JSONField(null=True, blank=True)
    badge = models.CharField(max_length=255, null=True, blank=True)
    poster = models.CharField(max_length=255, null=True, blank=True)
    tenant_id = models.UUIDField(null=True, blank=True)
    assigned_to = models.UUIDField(null=True, blank=True)
    layout_status = models.CharField(max_length=255, null=True, blank=True)
    resolution_status = models.CharField(max_length=255, null=True, blank=True)
    resolution_time = models.CharField(max_length=255, null=True, blank=True)
    cse_name = models.CharField(max_length=255, null=True, blank=True)
    cse_remarks = models.TextField(null=True, blank=True)
    call_status = models.CharField(max_length=255, null=True, blank=True)
    call_attempts = models.IntegerField(null=True, blank=True)
    rm_name = models.TextField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    snooze_until = models.DateTimeField(null=True, blank=True)
    praja_dashboard_user_link = models.TextField(null=True, blank=True)
    display_pic_url = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'support_ticket'
        managed = False  # IMPORTANT: Don't let Django try to create or alter this table!
