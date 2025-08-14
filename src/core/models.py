import uuid
from django.db import models
from django.utils import timezone
from django.core.validators import RegexValidator


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
    Tenant Model.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.TextField()
    slug = models.CharField(
        max_length=255,
        unique=True,
        validators=[RegexValidator(regex=r'^[a-z0-9]+(-[a-z0-9]+)*$')],
    )
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = 'tenants'
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


class BaseModel(TimeStampedModel, TenantModel):
    """
    One-stop base: timestamps + tenant + sensible indexes.
    """
    class Meta(TimeStampedModel.Meta):
        abstract = True
        indexes = [
            
            models.Index(fields=['tenant', '-created_at']),
        ]
