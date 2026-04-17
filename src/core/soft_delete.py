"""
Reusable soft-delete for Django models.

**Default manager** ``objects`` returns only *alive* rows: ``is_deleted=False`` and
``deleted_at`` is null. Use ``all_objects`` only when you intentionally need
soft-deleted rows (audits, restore flows, admin). Misuse of ``all_objects`` is
the main foot-gun; keep business logic on ``objects``.

**Uniqueness**: use :func:`alive_q` as ``UniqueConstraint(condition=...)`` so
uniqueness applies only to alive rows (both ``is_deleted`` and ``deleted_at``).

**Delete behavior**: instance ``.delete()`` and ``QuerySet.delete()`` soft-delete
(bulk ``UPDATE``). ``.hard_delete()`` performs a real ``DELETE``.

    from core.soft_delete import SoftDeleteModel, alive_q

    class MyModel(SoftDeleteModel):
        class Meta:
            constraints = [
                models.UniqueConstraint(
                    fields=("slug",),
                    condition=alive_q(),
                    name="uniq_mymodel_slug_alive",
                ),
            ]
"""

from __future__ import annotations

from django.db import models
from django.db.models import Q
from django.utils import timezone


def alive_q() -> Q:
    """
    Rows that are not soft-deleted. Use for partial unique constraints and
    filtered querysets so constraints match the default manager semantics.
    """
    return Q(is_deleted=False) & Q(deleted_at__isnull=True)


def not_deleted_q() -> Q:
    """Backward-compatible alias for :func:`alive_q`."""
    return alive_q()


class SoftDeleteQuerySet(models.QuerySet):
    """QuerySet whose ``delete()`` soft-deletes instead of SQL ``DELETE``."""

    def delete(self):
        now = timezone.now()
        count = self.update(is_deleted=True, deleted_at=now)
        return count, {self.model._meta.label: count}

    def hard_delete(self):
        """Permanent delete (SQL ``DELETE``)."""
        return super().delete()


class SoftDeleteManager(models.Manager):
    """
    Default manager: only *alive* rows (``is_deleted=False``, ``deleted_at`` unset).
    This is what application code should use almost everywhere.
    """

    def get_queryset(self):
        return SoftDeleteQuerySet(self.model, using=self._db).filter(
            is_deleted=False,
            deleted_at__isnull=True,
        )


class AllObjectsManager(models.Manager):
    """
    Unfiltered manager: includes soft-deleted rows. Use only when you explicitly
    need deleted data (e.g. restore, compliance, debugging).
    """

    def get_queryset(self):
        return SoftDeleteQuerySet(self.model, using=self._db)


class SoftDeleteModel(models.Model):
    """
    Abstract base with ``is_deleted``, ``deleted_at``, soft-delete managers,
    and ``delete`` / ``restore`` / ``hard_delete``.

    The first declared manager is ``objects`` (filtered); ``all_objects`` is
    intentionally separate so "see deleted rows" is an explicit choice.
    """

    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True, default=None)

    objects = SoftDeleteManager()
    all_objects = AllObjectsManager()

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        # Keep is_deleted / deleted_at consistent if one is set without the other.
        if self.is_deleted and self.deleted_at is None:
            self.deleted_at = timezone.now()
        if not self.is_deleted:
            self.deleted_at = None
        update_fields = kwargs.get("update_fields")
        if update_fields is not None:
            uf = list(update_fields)
            if "is_deleted" in uf and "deleted_at" not in uf:
                uf.append("deleted_at")
                kwargs["update_fields"] = uf
        super().save(*args, **kwargs)

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
