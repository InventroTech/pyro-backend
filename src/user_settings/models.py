from django.db import models
from core.models import BaseModel, Tenant
from core.soft_delete import alive_q
from object_history.models import HistoryTrackedModel


class Group(HistoryTrackedModel, BaseModel):
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


class TenantMemberSetting(HistoryTrackedModel, BaseModel):
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


class RmDailyTarget(HistoryTrackedModel, BaseModel):
    """
    A frozen snapshot of a specific RM's DAILY_TARGET (TenantMemberSetting)
    for one specific calendar date that has already ended.

    Written automatically once a day by RmDailyTargetSnapshotJobHandler, so
    a later edit to the flat DAILY_TARGET setting never rewrites a past
    day's history in RM PRD analytics. Today and future days have no row
    here yet and read DAILY_TARGET live (see
    user_settings.services.get_rm_daily_targets_sum).
    """

    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        db_column="tenant_id",
        help_text="The tenant this target belongs to",
    )
    tenant_membership = models.ForeignKey(
        "authz.TenantMembership",
        on_delete=models.CASCADE,
        db_column="tenant_membership_id",
        help_text="The RM this target is for",
    )
    date = models.DateField(help_text="Calendar date this target applies to")
    target = models.PositiveIntegerField(help_text="Trial target for this RM on this date")

    class Meta(BaseModel.Meta):
        db_table = "rm_daily_targets"
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "tenant_membership", "date"],
                condition=alive_q(),
                name="rm_daily_targets_tenant_mship_date_uniq_alive",
            ),
        ]
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["tenant", "tenant_membership", "date"]),
        ]

    def __str__(self) -> str:
        return f"RmDailyTarget({self.tenant_id}, membership={self.tenant_membership_id}, {self.date}={self.target})"
