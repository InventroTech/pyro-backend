from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from .event_loop import get_main_event_loop, schedule_on_main_loop

logger = logging.getLogger(__name__)

_skip_broadcast = threading.local()
_recent_lock = threading.Lock()
_recent_broadcasts: dict[tuple[str, str], float] = {}
_DEDUP_TTL_SECONDS = 2.0
_SLIM_DATA_KEYS = ("tasks", "reject_reason")


def tenant_group_name(tenant_id) -> str:
    return f"tenant_{str(tenant_id)}"


def user_group_name(user_id) -> str:
    return f"user_{user_id}"


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


def broadcast_to_tenant(tenant_id, data: dict[str, Any]) -> None:
    if not tenant_id:
        return

    channel_layer = get_channel_layer()
    if channel_layer is None:
        logger.warning("No channel layer configured; skipping tenant broadcast")
        return

    group = tenant_group_name(tenant_id)
    payload = {"type": "notify", "data": data}

    try:
        loop = get_main_event_loop()
        if loop is not None and loop.is_running():
            if schedule_on_main_loop(_group_send(channel_layer, group, payload)):
                return
        async_to_sync(channel_layer.group_send)(group, payload)
    except Exception:
        logger.exception("Failed to broadcast realtime event to tenant %s", tenant_id)


def broadcast_to_user(user_id, data: dict[str, Any]) -> None:
    if not user_id:
        return

    channel_layer = get_channel_layer()
    if channel_layer is None:
        logger.debug("No channel layer configured; skipping user broadcast")
        return

    group = user_group_name(user_id)
    payload = {"type": "notify", "data": data}

    try:
        loop = get_main_event_loop()
        if loop is not None and loop.is_running():
            if schedule_on_main_loop(_group_send(channel_layer, group, payload)):
                return
        async_to_sync(channel_layer.group_send)(group, payload)
    except Exception:
        logger.exception("Failed to broadcast realtime event to user %s", user_id)


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
        "Broadcasting record_updated tenant=%s record=%s stage=%s",
        tenant_id,
        record.id,
        lead_stage,
    )
    broadcast_to_tenant(tenant_id, payload)


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
