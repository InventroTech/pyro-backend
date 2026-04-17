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


class SystemSettings(models.Model):
    """
    Global system settings for tracking processing state across the application.
    Stores the last processed record ID for the record aggregator job.
    
    Key (setting_key) examples:
    - "record_aggregator_last_processed_id": Last record ID processed by the aggregator job
    """
    setting_key = models.CharField(
        max_length=255,
        unique=True,
        db_index=True,
        help_text="Unique key for the setting (e.g., 'record_aggregator_last_processed_id')"
    )
    setting_value = models.JSONField(
        default=dict,
        blank=True,
        help_text="The value of the setting, can be any JSON-serializable type"
    )
    description = models.TextField(
        null=True,
        blank=True,
        help_text="Description of what this setting controls"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "system_settings"
        verbose_name = "System Setting"
        verbose_name_plural = "System Settings"

    def __str__(self):
        return f"{self.setting_key}: {self.setting_value}"


class RecordAggregator(BaseModel):
    """
    Tracks entity schema snapshots per tenant.
    One row per (tenant, entity_type) combination.
    
    The aggregator job runs every minute scanning the records table.
    For each (tenant, entity_type) combination, it:
    1. Reads records from last_processed_record_id onwards (from SystemSettings)
    2. Captures all attribute names and their distinct values from the data JSON
    3. Updates this record with the schema snapshot showing all fields and value variations
    4. Updates the last_processed_record_id in SystemSettings for the next run
    
    This eliminates manual schema definitions - schema is automatically discovered from actual record data.
    """
    entity_type = models.CharField(
        max_length=100,
        db_index=True,
        help_text="The entity type (e.g., 'lead', 'ticket', 'vendor', 'request', 'product')"
    )
    schema_snapshot = models.JSONField(
        default=dict,
        blank=True,
        help_text="Schema snapshot: {field_name: {values: [...distinct_values...], count: int, field_type: str}}"
    )
    total_records_processed = models.BigIntegerField(
        default=0,
        help_text="Total count of records processed for this entity type"
    )
    last_aggregation_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Timestamp of last aggregation run for this entity"
    )

    class Meta:
        db_table = "record_aggregators"
        unique_together = [['tenant', 'entity_type']]
        indexes = [
            models.Index(fields=['tenant', 'entity_type']),
            models.Index(fields=['tenant', '-last_aggregation_at']),
        ]

    def __str__(self):
        fields_count = len(self.schema_snapshot) if isinstance(self.schema_snapshot, dict) else 0
        return f"{self.tenant.slug if self.tenant else 'N/A'}.{self.entity_type} ({fields_count} fields)"
