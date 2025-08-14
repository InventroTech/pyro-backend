# core/models.py
from django.db import models
from django.utils import timezone

class TimeStampedModel(models.Model):
    """
    Adds created_at / updated_at and a default ordering by -created_at.
    """
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        abstract = True
        ordering = ['-created_at']  # children inherit unless overridden


class TenantOwnedModel(models.Model):
    """
    Standard tenant scoping field.
    """
    tenant_id = models.UUIDField(null=True, blank=True, db_index=True)

    class Meta:
        abstract = True


class BaseModel(TimeStampedModel, TenantOwnedModel):
    """
    One-stop base: timestamps + tenant + sensible indexes.
    """
    class Meta(TimeStampedModel.Meta):
        abstract = True
        indexes = [
            models.Index(fields=['tenant_id', '-created_at']),
        ]
