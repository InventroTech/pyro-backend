from django.db import models
from core.models import BaseModel, Tenant
from core.soft_delete import alive_q


class Group(BaseModel):
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

    class Meta(BaseModel.Meta):
        db_table = "groups"
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "name"],
                condition=alive_q(),
                name="user_settings_groups_tenant_name_uniq_alive",
            ),
        ]
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["tenant", "name"]),
        ]

    def __str__(self) -> str:
        return f"Group(tenant={self.tenant_id}, name={self.name})"


class TenantMemberSetting(BaseModel):
    """
    Dedicated key/value table for core per-user settings like:
      - GROUP (group id)
      - DAILY_LIMIT
      - DAILY_TARGET
    """

    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        db_column="tenant_id",
        help_text="The tenant this setting belongs to",
    )
    tenant_membership = models.ForeignKey(
        "authz.TenantMembership",
        on_delete=models.CASCADE,
        db_column="tenant_membership_id",
        help_text="The tenant membership this setting belongs to",
    )
    key = models.CharField(max_length=100, help_text="Setting key (e.g., 'GROUP', 'DAILY_LIMIT')")
    value = models.JSONField(null=True, blank=True, help_text="Setting value (JSON)")

    class Meta(BaseModel.Meta):
        db_table = "user_kv_settings"
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "tenant_membership", "key"],
                condition=alive_q(),
                name="user_kv_tenant_mship_key_uniq_alive",
            ),
        ]
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["tenant", "tenant_membership", "key"]),
            models.Index(fields=["tenant", "key"]),
        ]

    def __str__(self) -> str:
        return f"{self.tenant_id} - {self.tenant_membership_id} - {self.key}: {self.value}"
