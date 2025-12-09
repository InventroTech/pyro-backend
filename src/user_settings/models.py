from django.db import models
from core.models import Tenant


class UserSettings(models.Model):
    """
    User settings model to store key-value pairs for users within tenants.
    Used for storing settings like lead type assignments for RMs.
    """
    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        db_column='tenant_id',
        help_text="The tenant this setting belongs to"
    )
    user_id = models.UUIDField(
        help_text="The user ID this setting belongs to"
    )
    key = models.CharField(
        max_length=100,
        help_text="The setting key (e.g., 'LEAD_TYPE_ASSIGNMENT')"
    )
    value = models.JSONField(
        help_text="The setting value (e.g., ['LEAD_TYPE_1', 'LEAD_TYPE_2'])"
    )
    assigned_leads_count = models.IntegerField(
        null=True,
        blank=True,
        help_text="Number of leads assigned to the user (for LEAD_TYPE_ASSIGNMENT key)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'user_settings'
        unique_together = ['tenant', 'user_id', 'key']
        indexes = [
            models.Index(fields=['tenant', 'user_id']),
            models.Index(fields=['tenant', 'key']),
        ]

    def __str__(self):
        return f"{self.tenant.name} - {self.user_id} - {self.key}: {self.value}"