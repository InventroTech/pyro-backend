from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.conf import settings

from .event_loop import get_main_event_loop, schedule_on_main_loop

logger = logging.getLogger(__name__)

_skip_broadcast = threading.local()
_recent_lock = threading.Lock()
_recent_broadcasts: dict[tuple[str, str], float] = {}
_DEDUP_TTL_SECONDS = 2.0
# Merge rapid updates for the same record before fan-out (last payload wins).
_COALESCE_SECONDS = 0.4
_SLIM_DATA_KEYS = ("tasks", "reject_reason")

_pending_lock = threading.Lock()
_pending_coalesce: dict[str, dict[str, Any]] = {}


def tenant_group_name(tenant_id) -> str:
    return f"tenant_{str(tenant_id)}"


def user_group_name(user_id) -> str:
    return f"user_{user_id}"


def tenant_oversight_group_name(tenant_id) -> str:
    """Managers/admins who should see all lead/ticket live updates for a tenant."""
    return f"tenant_{str(tenant_id)}_oversight"


def is_realtime_oversight_role(role_key: str | None) -> bool:
    """True when this membership role should receive tenant-wide record_updated."""
    if not role_key:
        return False
    normalized = str(role_key).strip().lower().replace("-", "_").replace(" ", "_")
    if not normalized:
        return False

    configured = getattr(settings, "REALTIME_OVERSIGHT_ROLE_KEYS", None) or []
    for raw in configured:
        key = str(raw).strip().lower().replace("-", "_").replace(" ", "_")
        if not key:
            continue
        if normalized == key:
            return True
        # Allow keys like "sales_manager" / "team_lead_north" to match "manager" / "team_lead"
        if key in normalized or normalized in key:
            return True
    return False


@contextmanager
def skip_realtime_broadcast() -> Iterator[None]:
    """Disable post_save websocket fan-out (e.g. bulk night jobs)."""
    previous = getattr(_skip_broadcast, "active", False)
    _skip_broadcast.active = True
    try:
        yield
    finally:
        _skip_broadcast.active = previous


def realtime_broadcast_skipped() -> bool:
    return bool(getattr(_skip_broadcast, "active", False))


async def _group_send(channel_layer, group: str, message: dict) -> None:
    await channel_layer.group_send(group, message)


def _send_to_group(group: str, data: dict[str, Any], *, label: str) -> None:
    channel_layer = get_channel_layer()
    if channel_layer is None:
        logger.debug("No channel layer configured; skipping %s broadcast", label)
        return

    payload = {"type": "notify", "data": data}
    try:
        loop = get_main_event_loop()
        if loop is not None and loop.is_running():
            if schedule_on_main_loop(_group_send(channel_layer, group, payload)):
                return
        async_to_sync(channel_layer.group_send)(group, payload)
    except Exception:
        logger.exception("Failed to broadcast realtime event to %s (%s)", label, group)


def broadcast_to_tenant(tenant_id, data: dict[str, Any]) -> None:
    if not tenant_id:
        return
    _send_to_group(tenant_group_name(tenant_id), data, label=f"tenant {tenant_id}")


def broadcast_to_user(user_id, data: dict[str, Any]) -> None:
    if not user_id:
        return
    _send_to_group(user_group_name(user_id), data, label=f"user {user_id}")


def broadcast_to_tenant_oversight(tenant_id, data: dict[str, Any]) -> None:
    if not tenant_id:
        return
    _send_to_group(
        tenant_oversight_group_name(tenant_id),
        data,
        label=f"tenant oversight {tenant_id}",
    )


def _lead_stage_from_record(record) -> str | None:
    data = getattr(record, "data", None)
    if not isinstance(data, dict):
        return None
    stage = data.get("lead_stage") or data.get("lead_status") or data.get("status")
    return str(stage) if stage is not None else None


def _slim_record_data(data: dict[str, Any]) -> dict[str, Any] | None:
    slim = {key: data[key] for key in _SLIM_DATA_KEYS if key in data}
    return slim or None


def _already_broadcast(record_id, updated_at) -> bool:
    key = (str(record_id), str(updated_at or ""))
    now = time.monotonic()
    with _recent_lock:
        stale = [k for k, seen_at in _recent_broadcasts.items() if now - seen_at > _DEDUP_TTL_SECONDS]
        for stale_key in stale:
            del _recent_broadcasts[stale_key]
        if key in _recent_broadcasts:
            return True
        _recent_broadcasts[key] = now
        return False


def _resolve_assignee_user_pk(tenant, assigned_to) -> Any | None:
    """Map lead.data.assigned_to → User.pk used by ``user_{pk}`` WS groups."""
    if assigned_to in (None, "", "null", "None"):
        return None
    try:
        # Lazy import: notifications.service imports broadcast_to_user from here.
        from notifications.service import resolve_user_pk_for_assigned_to

        return resolve_user_pk_for_assigned_to(tenant, assigned_to)
    except Exception:
        logger.exception(
            "Failed resolving assigned_to=%s for realtime fan-out", assigned_to
        )
        return None


def _dispatch_record_updated(
    tenant_id,
    payload: dict[str, Any],
    *,
    assigned_to=None,
    tenant=None,
) -> None:
    """
    Role-based fan-out:
    - always notify tenant oversight (managers/admins)
    - also notify the assigned RM when resolvable
    Regular RMs do not get other people's lead updates.
    """
    broadcast_to_tenant_oversight(tenant_id, payload)

    user_pk = _resolve_assignee_user_pk(tenant if tenant is not None else tenant_id, assigned_to)
    if user_pk:
        logger.debug(
            "Broadcasting record_updated to user_pk=%s + oversight record=%s",
            user_pk,
            payload.get("record_id"),
        )
        broadcast_to_user(user_pk, payload)
        return

    logger.debug(
        "Broadcasting record_updated to oversight only (unassigned) record=%s",
        payload.get("record_id"),
    )


def _flush_coalesced_record(record_id: str) -> None:
    with _pending_lock:
        entry = _pending_coalesce.pop(record_id, None)
    if not entry:
        return
    _dispatch_record_updated(
        entry["tenant_id"],
        entry["payload"],
        assigned_to=entry.get("assigned_to"),
        tenant=entry.get("tenant"),
    )


def flush_pending_record_broadcasts() -> None:
    """Send any queued coalesced broadcasts immediately (tests / shutdown)."""
    with _pending_lock:
        keys = list(_pending_coalesce.keys())
        for key in keys:
            entry = _pending_coalesce.get(key)
            timer = entry.get("timer") if entry else None
            if timer is not None:
                timer.cancel()
    for key in keys:
        _flush_coalesced_record(key)


def _enqueue_coalesced_broadcast(
    tenant_id,
    record_id: str,
    payload: dict[str, Any],
    *,
    assigned_to=None,
    tenant=None,
) -> None:
    """Last-write-wins coalesce so cascade saves become one WS fan-out."""
    if _COALESCE_SECONDS <= 0:
        _dispatch_record_updated(
            tenant_id, payload, assigned_to=assigned_to, tenant=tenant
        )
        return

    with _pending_lock:
        existing = _pending_coalesce.get(record_id)
        if existing is not None:
            timer = existing.get("timer")
            if timer is not None:
                timer.cancel()

        timer = threading.Timer(_COALESCE_SECONDS, _flush_coalesced_record, args=(record_id,))
        timer.daemon = True
        _pending_coalesce[record_id] = {
            "tenant_id": tenant_id,
            "payload": payload,
            "assigned_to": assigned_to,
            "tenant": tenant,
            "timer": timer,
        }
        timer.start()


def broadcast_record_updated(record, *, created: bool = False) -> None:
    """Push CRM record ids/stage so clients can patch UI without the full JSONB blob."""
    if realtime_broadcast_skipped():
        return

    tenant_id = getattr(record, "tenant_id", None)
    if not tenant_id:
        return

    if _already_broadcast(getattr(record, "id", None), getattr(record, "updated_at", None)):
        return

    data = record.data if isinstance(getattr(record, "data", None), dict) else {}
    assigned_to = data.get("assigned_to")
    lead_stage = _lead_stage_from_record(record)
    slim_data = _slim_record_data(data)
    tenant = getattr(record, "tenant", None)

    payload = {
        "event": "record_updated",
        "record_id": str(record.id),
        "entity_type": record.entity_type,
        "lead_stage": lead_stage,
        "assigned_to": str(assigned_to) if assigned_to is not None else None,
        "created": created,
        "updated_at": record.updated_at.isoformat() if record.updated_at else None,
    }
    if slim_data:
        payload["data"] = slim_data

    logger.debug(
        "Broadcasting record_updated tenant=%s record=%s stage=%s assigned_to=%s",
        tenant_id,
        record.id,
        lead_stage,
        assigned_to,
    )

    # Creates should appear immediately; updates coalesce cascade writes.
    if created or _COALESCE_SECONDS <= 0:
        _dispatch_record_updated(
            tenant_id, payload, assigned_to=assigned_to, tenant=tenant
        )
        return

    _enqueue_coalesced_broadcast(
        tenant_id,
        str(record.id),
        payload,
        assigned_to=assigned_to,
        tenant=tenant,
    )


def broadcast_job_status(job) -> None:
    """Push background job status updates to the job's tenant."""
    tenant_id = getattr(job, "tenant_id", None)
    if not tenant_id:
        return

    broadcast_to_tenant(
        tenant_id,
        {
            "event": "job_status",
            "job_id": str(job.id),
            "job_type": job.job_type,
            "status": job.status,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "last_error": job.last_error,
        },
    )
