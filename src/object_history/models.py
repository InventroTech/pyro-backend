from __future__ import annotations

from django.db import models
from django.contrib.contenttypes.models import ContentType

from accounts.models import SupabaseAuthUser
from core.models import BaseModel
from core.soft_delete import (
    alive_q,
    SoftDeleteQuerySet,
    SoftDeleteManager,
    AllObjectsManager,
)
from .tracking import HistoryTrackedModel


class ObjectHistoryQuerySet(SoftDeleteQuerySet):
    def for_instance(self, instance):
        """Filter history entries for a specific model instance."""
        content_type = ContentType.objects.get_for_model(instance.__class__)
        return self.filter(content_type=content_type, object_id=str(instance.pk))


class ObjectHistoryManager(SoftDeleteManager):
    def get_queryset(self):
        return ObjectHistoryQuerySet(self.model, using=self._db).filter(
            is_deleted=False,
            deleted_at__isnull=True,
        )


class ObjectHistoryAllObjectsManager(AllObjectsManager):
    def get_queryset(self):
        return ObjectHistoryQuerySet(self.model, using=self._db)


class ObjectHistory(BaseModel):
    """
    Generic history storage for any tracked model.
    """

    content_type = models.ForeignKey(
        ContentType,
        on_delete=models.CASCADE,
        related_name="object_history_entries",
    )
    object_id = models.TextField()
    object_repr = models.TextField()
    action = models.CharField(max_length=32)
    actor_user = models.ForeignKey(
        SupabaseAuthUser,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="object_history_entries",
    )
    actor_label = models.CharField(max_length=255, null=True, blank=True)
    version = models.PositiveIntegerField()
    changes = models.JSONField(default=dict, blank=True)
    before_state = models.JSONField(null=True, blank=True)
    after_state = models.JSONField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    # Copied from core.TenantSettings.persistent_object_history at write time; retained rows are
    # excluded from LOG_RETENTION_DAYS purge (see core.log_retention).
    persistent_history = models.BooleanField(default=False, db_index=True)

    objects = ObjectHistoryManager()
    all_objects = ObjectHistoryAllObjectsManager()

    class Meta(BaseModel.Meta):
        db_table = "object_history"
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(
                fields=["tenant", "content_type", "object_id", "-created_at"],
                name="object_hist_obj_lookup",
            ),
            models.Index(
                fields=["content_type", "action", "-created_at"],
                name="object_hist_action_lookup",
            ),
            models.Index(
                fields=["persistent_history", "created_at"],
                name="object_hist_persist_cr_idx",
            ),
            models.Index(
                fields=["tenant", "created_at"],
                name="object_hist_retention_idx",
                condition=models.Q(
                    persistent_history=False,
                    is_deleted=False,
                    deleted_at__isnull=True,
                ),
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["content_type", "object_id", "version"],
                condition=alive_q(),
                name="object_hist_unique_version",
            ),
        ]

    def __str__(self) -> str:
        return f"[{self.action}] {self.object_repr} (v{self.version})"


__all__ = ["ObjectHistory", "HistoryTrackedModel"]
