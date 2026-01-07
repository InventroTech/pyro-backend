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


class RoutingRule(models.Model):
    """
    Simple per-user routing rule for queueable objects (tickets, leads, etc.).
    v1: one active rule per (tenant, user_id, queue_type) with a small set of
    allowed conditions (e.g. state, poster) stored as JSON.
    """

    QUEUE_TYPE_TICKET = "ticket"
    QUEUE_TYPE_LEAD = "lead"

    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        db_column="tenant_id",
        help_text="The tenant this routing rule belongs to",
    )
    user_id = models.UUIDField(
        help_text="The user ID this routing rule applies to (Supabase user UUID)"
    )
    queue_type = models.CharField(
        max_length=50,
        help_text="Type of queue this rule applies to, e.g. 'ticket' or 'lead'",
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this rule is currently active",
    )
    conditions = models.JSONField(
        default=dict,
        blank=True,
        help_text=(
            "Simple condition config, e.g.: "
            "{'filters': [{'field': 'state', 'op': 'equals', 'value': 'Tamil Nadu'}]}"
        ),
    )
    name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Optional human-readable name for this rule",
    )
    description = models.TextField(
        null=True,
        blank=True,
        help_text="Optional description for this rule",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "routing_rules"
        unique_together = [["tenant", "user_id", "queue_type"]]
        indexes = [
            models.Index(fields=["tenant", "queue_type", "user_id"]),
            models.Index(fields=["tenant", "queue_type", "is_active"]),
        ]

    def __str__(self) -> str:
        return (
            f"RoutingRule(tenant={self.tenant_id}, user={self.user_id}, "
            f"queue_type={self.queue_type}, active={self.is_active})"
        )
