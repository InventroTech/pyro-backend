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
    group_id = models.BigIntegerField(
        null=True,
        blank=True,
        help_text="Assigned Group id for this user's lead assignment"
    )
    lead_sources = models.JSONField(
        null=True,
        blank=True,
        help_text="List of lead sources assigned to this user (for key=LEAD_TYPE_ASSIGNMENT); only these leads are directed to the RM"
    )
    lead_statuses = models.JSONField(
        null=True,
        blank=True,
        help_text="List of lead statuses assigned to this user (for key=LEAD_TYPE_ASSIGNMENT); only these leads are directed to the RM"
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
    Routing rule for queueable objects (tickets, leads), keyed by authz.TenantMembership.
    One active rule per (tenant, tenant_membership, queue_type). Works even when the
    membership has no linked auth user yet (user_id null). user_id is denormalized
    from the membership for backward compatibility.
    """

    QUEUE_TYPE_TICKET = "ticket"
    QUEUE_TYPE_LEAD = "lead"

    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        db_column="tenant_id",
        help_text="The tenant this routing rule belongs to",
    )
    tenant_membership = models.ForeignKey(
        "authz.TenantMembership",
        on_delete=models.CASCADE,
        db_column="tenant_membership_id",
        help_text="The tenant membership this rule applies to (primary key for the rule).",
    )
    user_id = models.UUIDField(
        null=True,
        blank=True,
        help_text="Denormalized from TenantMembership.user_id when set; may be null if membership has no linked auth user yet.",
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
        unique_together = [["tenant", "tenant_membership", "queue_type"]]
        indexes = [
            models.Index(fields=["tenant", "queue_type", "tenant_membership"]),
            models.Index(fields=["tenant", "queue_type", "is_active"]),
        ]

    def __str__(self) -> str:
        return (
            f"RoutingRule(tenant={self.tenant_id}, tenant_membership={self.tenant_membership_id}, "
            f"queue_type={self.queue_type}, active={self.is_active})"
        )


class Group(models.Model):
    """Tenant-scoped lead assignment group configuration."""

    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        db_column="tenant_id",
        help_text="The tenant this group belongs to",
    )
    name = models.CharField(
        max_length=255,
        help_text="Human-readable group name",
    )
    group_data = models.JSONField(
        default=dict,
        blank=True,
        help_text="Arbitrary group payload (party, lead sources, statuses, limits, etc.)",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "groups"
        unique_together = [["tenant", "name"]]
        indexes = [
            models.Index(fields=["tenant", "name"]),
        ]

    def __str__(self) -> str:
        return f"Group(tenant={self.tenant_id}, name={self.name})"
