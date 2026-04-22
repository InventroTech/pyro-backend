import uuid
from django.db import models
from django.utils import timezone
from django.core.validators import RegexValidator

from authz.models import Role


class TimeStampedModel(models.Model):
    """
    Adds created_at / updated_at and a default ordering by -created_at.
    """
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        abstract = True
        ordering = ['-created_at']  # children inherit unless overridden


class Tenant(models.Model):
    """
    Unmanaged mirror of public.tenants.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.TextField()
    created_at = models.DateTimeField(default=timezone.now)
    slug = models.CharField(
        max_length=64,    
        unique=True,
        validators=[RegexValidator(regex=r'^[a-z0-9]+(-[a-z0-9]+)*$')],
    )

    class Meta:
        db_table = "tenants"   
        managed = False

    def __str__(self):
        return f"{self.name} ({self.slug})"


class TenantModel(models.Model):
    """
    Standard tenant scoping FK pointing to public.tenants(id).
    Maps to the existing column name tenant_id.
    """
    tenant = models.ForeignKey(
        Tenant,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,      # keep records if tenant deleted
        db_index=True,
        db_column='tenant_id',          # use existing column name
        related_name='%(app_label)s_%(class)s_set',  # avoid reverse name clashes
    )

    class Meta:
        abstract = True


class RoleModel(TenantModel):
    """
    Tenant-scoped role (authz_role is per-tenant). Adds role_id; tenant comes from TenantModel.
    Use with BaseModel for tenant + timestamps + role.
    """
    role = models.ForeignKey(
        Role,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        db_index=True,
        db_column='role_id',
        related_name='%(app_label)s_%(class)s_set',
    )

    class Meta:
        abstract = True


class BaseModel(TimeStampedModel, TenantModel):
    """
    One-stop base: timestamps + tenant + sensible indexes.
    """
    class Meta(TimeStampedModel.Meta):
        abstract = True
        indexes = [
            
            models.Index(fields=['tenant', '-created_at']),
        ]


class SystemSettings(TimeStampedModel):
    """
    Global system settings storage.
    NOT tenant-scoped - these are application-wide settings.
    """
    setting_key = models.CharField(
        max_length=255,
        db_index=True,
        unique=True,
        help_text="Unique key for this setting"
    )
    setting_value = models.JSONField(
        default=dict,
        help_text="JSON value for this setting"
    )
    description = models.TextField(
        blank=True,
        help_text="Description of what this setting is for"
    )

    class Meta:
        db_table = "system_settings"
        verbose_name_plural = "System Settings"

    def __str__(self):
        return f"{self.setting_key}"


class RecordAggregator(BaseModel):
    """
    Stores the aggregated schema for each (tenant, entity_type) combination.
    Captures field names and their occurrence counts from the records table.
    """
    entity_type = models.CharField(
        max_length=100,
        db_index=True,
        help_text="Entity type (e.g., 'lead', 'invoice', 'inventory_item')"
    )
    schema_snapshot = models.JSONField(
        default=dict,
        help_text="Schema snapshot: {field_name: {count, field_type}}"
    )
    total_records_processed = models.BigIntegerField(
        default=0,
        help_text="Total records processed for this entity type"
    )
    last_aggregation_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Timestamp of last aggregation run"
    )

    class Meta:
        db_table = "record_aggregators"
        unique_together = [('tenant', 'entity_type')]
        indexes = [
            models.Index(fields=['tenant', 'entity_type']),
            models.Index(fields=['tenant', '-last_aggregation_at']),
        ]

    def __str__(self):
        return f"{self.entity_type} ({self.tenant.slug})"
