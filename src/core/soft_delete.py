"""
Reusable soft-delete for Django models.

Use for any table that should keep rows but hide them from normal queries:

    from core.soft_delete import SoftDeleteModel

    class MyModel(SoftDeleteModel):
        ...

- Default manager ``objects`` excludes rows where ``deleted_at`` is set.
- Use ``all_objects`` to query including soft-deleted rows.
- Call ``.delete()`` to soft-delete; ``.hard_delete()`` for a real DB delete;
  ``.restore()`` to undo a soft delete.

Partial unique constraints should scope to non-deleted rows, e.g.::

    condition=Q(deleted_at__isnull=True)

or use :func:`not_deleted_q`.
"""

from __future__ import annotations

from django.db import models
from django.db.models import Q
from django.utils import timezone


def not_deleted_q() -> Q:
    """Use in ``UniqueConstraint(condition=...)`` so uniqueness ignores soft-deleted rows."""
    return Q(deleted_at__isnull=True)


class SoftDeleteQuerySet(models.QuerySet):
    def delete(self):
        count = self.update(is_deleted=True, deleted_at=timezone.now())
        return count, {self.model._meta.label: count}

    def hard_delete(self):
        return super().delete()


class SoftDeleteManager(models.Manager):
    """Excludes soft-deleted rows (``deleted_at`` is null)."""

    def get_queryset(self):
        return SoftDeleteQuerySet(self.model, using=self._db).filter(
            deleted_at__isnull=True
        )


class AllObjectsManager(models.Manager):
    """Includes soft-deleted rows."""

    def get_queryset(self):
        return SoftDeleteQuerySet(self.model, using=self._db)


class SoftDeleteModel(models.Model):
    """
    Abstract base: ``is_deleted``, ``deleted_at``, soft-delete managers, and helpers.

    Subclasses add their own fields and ``Meta``; add indexes on ``is_deleted`` /
    ``deleted_at`` per table if you filter on them often.
    """

    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True, default=None)

    objects = SoftDeleteManager()
    all_objects = AllObjectsManager()

    class Meta:
        abstract = True

    def delete(self, using=None, keep_parents=False):
        self.is_deleted = True
        self.deleted_at = timezone.now()
        self.save(update_fields=["is_deleted", "deleted_at"])

    def hard_delete(self, using=None, keep_parents=False):
        return super().delete(using=using, keep_parents=keep_parents)

    def restore(self):
        self.is_deleted = False
        self.deleted_at = None
        self.save(update_fields=["is_deleted", "deleted_at"])
