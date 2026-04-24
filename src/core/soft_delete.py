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

**Cascade**: set ``soft_delete_cascade`` on a concrete model to a tuple of *reverse*
relation accessor names (``ForeignKey`` / ``OneToOne`` from child to parent). On
``delete()`` / ``QuerySet.delete()``, those related rows are soft-deleted first
(recursively if they also define ``soft_delete_cascade``). Same accessors are
used for ``hard_delete()`` so children are removed before the parent SQL
``DELETE``. Restore does not cascade.

    from core.models import BaseModel
    from core.soft_delete import alive_q

    class MyModel(BaseModel):
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

from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS, models
from django.db.models import Q
from django.utils import timezone


def _related_queryset_for_cascade(
    parent_model: type[models.Model],
    accessor_name: str,
    parent_pks: list,
    using: str,
):
    """
    Queryset of related rows (default manager) for a reverse FK / O2O accessor.
    """
    for rel in parent_model._meta.related_objects:
        if rel.get_accessor_name() == accessor_name:
            field = rel.field
            child_model = field.model
            if not issubclass(child_model, SoftDeleteMixin):
                raise ImproperlyConfigured(
                    f"{parent_model.__name__}.soft_delete_cascade[{accessor_name!r}] "
                    f"resolves to {child_model.__name__}, which must inherit SoftDeleteMixin "
                    f"(or BaseModel / RoleBaseModel)."
                )
            kw = {f"{field.name}__in": parent_pks}
            return child_model.objects.using(using).filter(**kw)
    names = [r.get_accessor_name() for r in parent_model._meta.related_objects]
    raise ImproperlyConfigured(
        f"{parent_model.__name__} has no reverse relation {accessor_name!r} for "
        f"soft_delete_cascade. Valid names: {names}"
    )


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
        model = self.model
        cascade = getattr(model, "soft_delete_cascade", ()) or ()
        if cascade:
            parent_ids = list(self.values_list("pk", flat=True))
            if parent_ids:
                using = self.db
                for accessor_name in cascade:
                    child_qs = _related_queryset_for_cascade(
                        model, accessor_name, parent_ids, using
                    )
                    child_qs.delete()
        now = timezone.now()
        count = self.update(is_deleted=True, deleted_at=now)
        return count, {model._meta.label: count}

    def hard_delete(self):
        """Permanent delete (SQL ``DELETE``), with optional cascade."""
        model = self.model
        cascade = getattr(model, "soft_delete_cascade", ()) or ()
        if cascade:
            parent_ids = list(self.values_list("pk", flat=True))
            if parent_ids:
                using = self.db
                for accessor_name in cascade:
                    child_qs = _related_queryset_for_cascade(
                        model, accessor_name, parent_ids, using
                    )
                    child_qs.hard_delete()
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


class SoftDeleteMixin(models.Model):
    """
    Soft-delete fields, managers, and ``delete`` / ``restore`` / ``hard_delete``.

    **Prefer :class:`core.models.BaseModel`** (tenant + timestamps + soft-delete) for
    new tables. Use this mixin only for models that cannot extend ``BaseModel`` or
    :class:`core.models.RoleBaseModel` (e.g. authz tables without timestamps, or
    analytics rows without a tenant FK).

    Optional ``soft_delete_cascade``: tuple of reverse relation names whose rows
    are soft-deleted before this row (see module docstring).

    The first declared manager is ``objects`` (filtered); ``all_objects`` is
    intentionally separate so "see deleted rows" is an explicit choice.
    """

    soft_delete_cascade: tuple[str, ...] = ()

    # Per-table indexes; avoid repeating models.Index in each Meta.
    is_deleted = models.BooleanField(default=False, db_index=True)
    deleted_at = models.DateTimeField(
        null=True, blank=True, default=None, db_index=True
    )

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
        cascade = getattr(self.__class__, "soft_delete_cascade", ()) or ()
        db = using or getattr(self._state, "db", None) or DEFAULT_DB_ALIAS
        if cascade and self.pk is not None:
            for accessor_name in cascade:
                child_qs = _related_queryset_for_cascade(
                    self.__class__, accessor_name, [self.pk], db
                )
                child_qs.delete()
        self.is_deleted = True
        self.deleted_at = timezone.now()
        self.save(update_fields=["is_deleted", "deleted_at"], using=using)

    def hard_delete(self, using=None, keep_parents=False):
        cascade = getattr(self.__class__, "soft_delete_cascade", ()) or ()
        db = using or getattr(self._state, "db", None) or DEFAULT_DB_ALIAS
        if cascade and self.pk is not None:
            for accessor_name in cascade:
                child_qs = _related_queryset_for_cascade(
                    self.__class__, accessor_name, [self.pk], db
                )
                child_qs.hard_delete()
        return super().delete(using=using, keep_parents=keep_parents)

    def restore(self):
        self.is_deleted = False
        self.deleted_at = None
        self.save(update_fields=["is_deleted", "deleted_at"])
