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


class HistoryTrackedModel(models.Model):
    """
    Abstract mixin that auto-captures history around save/delete.
    """

    class Meta:
        abstract = True

    def save(
        self,
        *args,
        history_action: str | None = None,
        history_actor: str | None = None,
        history_actor_user=None,
        history_force: bool = False,
        history_metadata: dict | None = None,
        **kwargs,
    ):
        from .engine import HistoryEngine  # local import to avoid circular deps

        HistoryEngine.capture_before(self)
        result = super().save(*args, **kwargs)
        HistoryEngine.capture_after(
            self,
            action=history_action,
            actor=history_actor,
            actor_user=history_actor_user,
            force=history_force,
            extra_metadata=history_metadata,
        )
        return result

    def delete(
        self,
        *args,
        history_actor: str | None = None,
        history_actor_user=None,
        history_metadata: dict | None = None,
        **kwargs,
    ):
        from .engine import HistoryEngine

        HistoryEngine.capture_before(self, for_delete=True)
        result = super().delete(*args, **kwargs)
        HistoryEngine.capture_after(
            self,
            action="deleted",
            actor=history_actor,
            actor_user=history_actor_user,
            force=True,  # deletions should always persist
            include_after=False,
            extra_metadata=history_metadata,
        )
        return result


__all__ = ["ObjectHistory", "HistoryTrackedModel"]
