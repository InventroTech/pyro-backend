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
    tenant_membership = models.ForeignKey(
        'authz.TenantMembership',
        on_delete=models.CASCADE,
        db_column='tenant_membership_id',
        help_text="The tenant membership this setting belongs to"
    )
    key = models.CharField(
        max_length=100,
        help_text="The setting key (e.g., 'LEAD_TYPE_ASSIGNMENT')"
    )
    value = models.JSONField(
        help_text="The setting value (e.g., ['LEAD_TYPE_1', 'LEAD_TYPE_2'])"
    )
    daily_target = models.IntegerField(
        null=True,
        blank=True,
        help_text="Daily target for the user"
    )
    daily_limit = models.IntegerField(
        null=True,
        blank=True,
        help_text="Daily lead pull limit for the user (max leads they can fetch per day)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'user_settings'
        unique_together = ['tenant', 'tenant_membership', 'key']
        indexes = [
            models.Index(fields=['tenant', 'tenant_membership']),
            models.Index(fields=['tenant', 'key']),
        ]

    def __str__(self):
        return f"{self.tenant.name} - {self.tenant_membership.id} - {self.key}: {self.value}"