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


class TenantSettings(models.Model):
    """
    Per-tenant product settings stored in the app DB (unlike :class:`Tenant`, which
    mirrors an external ``tenants`` table).

    ``persistent_object_history``: when ``True``, :class:`object_history.models.ObjectHistory`
    rows for this tenant are stamped ``persistent_history=True`` and are skipped by
    :func:`core.log_retention.purge_old_log_rows`.
    """

    tenant = models.OneToOneField(
        Tenant,
        on_delete=models.CASCADE,
        db_column="tenant_id",
        related_name="app_settings",
        primary_key=True,
    )
    persistent_object_history = models.BooleanField(
        default=False,
        db_index=True,
        help_text="If True, object history for this tenant is not purged by retention.",
    )

    class Meta:
        db_table = "core_tenant_settings"

    def __str__(self) -> str:
        return f"TenantSettings({self.tenant_id})"

    @classmethod
    def object_history_should_persist(cls, tenant, *, using=None) -> bool:
        if tenant is None:
            return False
        tid = getattr(tenant, "pk", None)
        if tid is None:
            return False
        qs = cls.objects if using is None else cls.objects.db_manager(using)
        return qs.filter(tenant_id=tid, persistent_object_history=True).exists()

    def delete(self, *args, **kwargs):
        tenant_id = self.tenant_id
        was_persistent = bool(self.persistent_object_history)
        result = super().delete(*args, **kwargs)
        if was_persistent and tenant_id:
            from object_history.models import ObjectHistory

            ObjectHistory.all_objects.filter(tenant_id=tenant_id).update(
                persistent_history=False
            )
        return result

    def save(self, *args, **kwargs):
        was_persistent = False
        if self.tenant_id:
            was_persistent = (
                type(self)
                .objects.filter(tenant_id=self.tenant_id)
                .values_list("persistent_object_history", flat=True)
                .first()
                is True
            )
        super().save(*args, **kwargs)
        from object_history.models import ObjectHistory

        if self.persistent_object_history:
            ObjectHistory.all_objects.filter(tenant_id=self.tenant_id).update(
                persistent_history=True
            )
        elif was_persistent:
            ObjectHistory.all_objects.filter(tenant_id=self.tenant_id).update(
                persistent_history=False
            )


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
