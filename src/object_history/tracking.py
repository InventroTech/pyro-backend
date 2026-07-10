from __future__ import annotations

from django.db import models


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
