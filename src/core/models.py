import uuid
from django.db import models
from django.utils import timezone
from django.core.validators import RegexValidator

from core.soft_delete import SoftDeleteMixin


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
    Use :class:`BaseModel` or :class:`RoleBaseModel` for application tables.
    """
    role = models.ForeignKey(
        'authz.Role',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        db_index=True,
        db_column='role_id',
        related_name='%(app_label)s_%(class)s_set',
    )

    class Meta:
        abstract = True


class BaseModel(SoftDeleteMixin, TimeStampedModel, TenantModel):
    """
    Primary concrete base: timestamps + tenant FK + soft-delete (``is_deleted``,
    ``deleted_at``) + filtered ``objects`` / ``all_objects``.

    Prefer this over inheriting :class:`core.soft_delete.SoftDeleteMixin` directly.
    Add ``UniqueConstraint(..., condition=alive_q())`` where uniqueness should ignore
    soft-deleted rows.
    """
    class Meta(TimeStampedModel.Meta):
        abstract = True
        indexes = [
            models.Index(fields=['tenant', '-created_at']),
        ]


class RoleBaseModel(SoftDeleteMixin, TimeStampedModel, RoleModel):
    """
    Same as :class:`BaseModel` plus the authz ``role`` FK (e.g. dashboard ``Page``).
    """
    class Meta(TimeStampedModel.Meta):
        abstract = True
        indexes = [
            models.Index(fields=['tenant', '-created_at']),
        ]
