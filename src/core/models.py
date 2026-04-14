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


class Entity(TenantModel):
    """
    Stores the schema/blueprint of record types (e.g., 'lead', 'ticket') 
    for a specific tenant, and tracks the last processed record for the background sync job.
    """
    tenant = models.ForeignKey('core.Tenant', on_delete=models.CASCADE, related_name='entities')
    name = models.CharField(max_length=255, help_text="The type of entity (e.g., lead, ticket)")
    
    # Stores the snapshot of all discovered fields and their data types
    schema = models.JSONField(default=dict, blank=True) 
    
    # Cursor for the background job to know where it left off
    last_processed_record_id = models.BigIntegerField(default=0) 
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "core_entity"
        # A tenant should only have one Entity blueprint per record type
        unique_together = ('tenant', 'name')

    def __str__(self):
        return f"{self.name} Schema for {self.tenant}"
