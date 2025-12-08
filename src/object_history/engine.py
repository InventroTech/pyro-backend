from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Optional

from django.contrib.contenttypes.models import ContentType
from django.db import transaction

from .models import ObjectHistory
from .registry import get_config
from .serializers import compute_diff, redact_payload, serialize_instance

logger = logging.getLogger(__name__)

_state = threading.local()


def _get_thread_state() -> Dict[str, Any]:
    if not hasattr(_state, "before"):
        _state.before = {}
    return _state.before


def _set_before(instance, payload: Dict[str, Any]):
    state = _get_thread_state()
    state[id(instance)] = payload


def _pop_before(instance) -> Optional[Dict[str, Any]]:
    state = _get_thread_state()
    return state.pop(id(instance), None)


def set_manual_context(
    *,
    actor_user=None,
    actor_label: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
):
    """
    Allow scripts / tests to push context without a Django request.
    """
    _state.request_context = {
        "actor_user": actor_user,
        "actor_label": actor_label,
        "metadata": metadata or {},
    }


def set_request_context(request):
    """
    Adapter used by middleware to bridge request data into thread-local storage.
    """
    actor_user = getattr(request, "_actor_user", None)
    actor_label = getattr(request, "_actor_identifier", None)
    metadata = getattr(request, "_request_metadata", None) or {}
    
    logger.debug(
        f"HistoryEngine.set_request_context: Setting context from request | "
        f"path={getattr(request, 'path', 'N/A')} | "
        f"actor_user_id={actor_user.id if actor_user else None} | "
        f"actor_label={actor_label}"
    )
    
    set_manual_context(
        actor_user=actor_user,
        actor_label=actor_label,
        metadata=metadata,
    )


def clear_request_context():
    if hasattr(_state, "request_context"):
        delattr(_state, "request_context")


def get_request_context():
    return getattr(
        _state,
        "request_context",
        {"actor_user": None, "actor_label": None, "metadata": {}},
    )


class HistoryEngine:
    @staticmethod
    def capture_before(instance, *, for_delete: bool = False):
        config = get_config(instance.__class__)
        if not config:
            return
        payload: Dict[str, Any] = {}
        if for_delete:
            payload = serialize_instance(instance, config)
        else:
            pk = getattr(instance, "pk", None)
            if pk:
                original = instance.__class__.objects.filter(pk=pk).first()
                if original:
                    payload = serialize_instance(original, config)
        _set_before(instance, payload)

    @staticmethod
    def capture_after(
        instance,
        *,
        action: Optional[str] = None,
        actor: Optional[str] = None,
        actor_user=None,
        force: bool = False,
        include_after: bool = True,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ):
        config = get_config(instance.__class__)
        if not config:
            return

        before = _pop_before(instance) or {}
        after = serialize_instance(instance, config) if include_after else {}
        diff = compute_diff(before, after, config.redact_fields)
        if not diff and not force and before:
            return

        request_ctx = get_request_context()
        resolved_actor_user = actor_user or request_ctx.get("actor_user")
        actor_label = actor or request_ctx.get("actor_label")
        
        # Log actor resolution for debugging
        logger.debug(
            f"HistoryEngine.capture_after: Resolving actor | "
            f"instance={instance.__class__.__name__}#{instance.pk} | "
            f"explicit_actor_user={actor_user is not None} | "
            f"explicit_actor={actor} | "
            f"context_actor_user={request_ctx.get('actor_user') is not None} | "
            f"context_actor_label={request_ctx.get('actor_label')}"
        )
        
        # Ensure actor_label is set - prefer from actor_user, then from context
        if resolved_actor_user and not actor_label:
            actor_label = getattr(resolved_actor_user, "email", None) or str(resolved_actor_user.id)
        # If still no label but we have context, use it
        if not actor_label and request_ctx.get("actor_label"):
            actor_label = request_ctx.get("actor_label")
        
        # Final logging of resolved actor
        logger.info(
            f"HistoryEngine.capture_after: Writing history | "
            f"instance={instance.__class__.__name__}#{instance.pk} | "
            f"action={action or ('created' if not before else 'updated')} | "
            f"actor_user_id={resolved_actor_user.id if resolved_actor_user else None} | "
            f"actor_label={actor_label}"
        )
        
        metadata = dict(request_ctx.get("metadata") or {})
        if extra_metadata:
            metadata.update(extra_metadata)

        HistoryEngine._write_history(
            instance,
            action=action or ("created" if not before else "updated"),
            actor_user=resolved_actor_user,
            actor_label=actor_label,
            metadata=metadata,
            before_state=before,
            after_state=after if include_after else None,
            changes=diff,
            config=config,
            include_after=include_after,
        )

    @staticmethod
    def _write_history(
        instance,
        *,
        action: str,
        actor_user,
        actor_label: Optional[str],
        metadata: Dict[str, Any],
        before_state: Dict[str, Any],
        after_state: Optional[Dict[str, Any]],
        changes: Dict[str, Dict[str, Any]],
        config,
        include_after: bool,
    ):
        tenant = getattr(instance, "tenant", None)
        content_type = ContentType.objects.get_for_model(instance.__class__)
        object_repr = str(instance)
        object_id = instance.pk

        version = HistoryEngine._next_version(content_type, object_id)

        before_snapshot = before_state
        after_snapshot = after_state or {}
        if config.snapshot_strategy == "minimal":
            changed_fields = set(changes.keys())
            before_snapshot = {
                field: before_state.get(field)
                for field in changed_fields
                if field in before_state
            }
            after_snapshot = {
                field: after_snapshot.get(field)
                for field in changed_fields
                if field in after_snapshot
            }

        ObjectHistory.objects.create(
            tenant=tenant,
            content_type=content_type,
            object_id=str(object_id),
            object_repr=object_repr,
            action=action,
            actor_user=actor_user,
            actor_label=actor_label,
            version=version,
            changes=changes,
            before_state=redact_payload(before_snapshot, config.redact_fields),
            after_state=redact_payload(after_snapshot, config.redact_fields)
            if include_after
            else None,
            metadata=metadata,
        )

    @staticmethod
    @transaction.atomic
    def _next_version(content_type, object_id) -> int:
        latest = (
            ObjectHistory.objects.select_for_update()
            .filter(content_type=content_type, object_id=str(object_id))
            .order_by("-version")
            .first()
        )
        if not latest:
            return 1
        return latest.version + 1


__all__ = [
    "HistoryEngine",
    "set_request_context",
    "set_manual_context",
    "clear_request_context",
]


