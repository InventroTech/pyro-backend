"""
Support ticket button events — state changes are applied by tenant ``RuleSet`` rows
in the database (configured per tenant, same as leads).

This module normalizes API payloads for rule templates and enqueues Mixpanel events.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional
from uuid import UUID

from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

from background_jobs.models import JobType
from background_jobs.queue_service import get_queue_service
from crm_records.models import EventLog, Record
from support_ticket.constants import (
    SUPPORT_EVENT_CALL_LATER,
    SUPPORT_EVENT_CANNOT_RESOLVE,
    SUPPORT_EVENT_NOT_CONNECTED,
    SUPPORT_EVENT_TAKE_BREAK,
    SUPPORT_EVENT_RESOLVED,
    SUPPORT_EVENT_TO_PRAJA_RESOLUTION_STATUS,
    SUPPORT_TICKET_BUTTON_EVENTS,
    SUPPORT_TICKET_ENTITY_TYPE,
    SUPPORT_TICKET_PRAJA_SYNC_RESOLUTION_STATUSES,
)
from support_ticket.mixpanel_properties import support_ticket_button_event_mixpanel_properties
from support_ticket.services import TicketTimeService

logger = logging.getLogger(__name__)

_CAMEL_TO_SNAKE = {
    "cseRemarks": "cse_remarks",
    "callStatus": "call_status",
    "resolutionTime": "resolution_time",
    "otherReasons": "other_reasons",
    "reviewRequested": "review_requested",
    "resolutionStatus": "resolution_status",
    "snoozeUntil": "snooze_until",
    "nextCallAt": "next_call_at",
}


def _payload_get(payload: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return default


def _accumulate_resolution_time(
    record: Record,
    payload: Dict[str, Any],
) -> Optional[str]:
    # Only accumulate from the API field; prepared payloads already have snake_case.
    incoming = payload.get("resolutionTime")
    if incoming is None:
        return None
    if not str(incoming).strip() or ":" not in str(incoming):
        return (record.data or {}).get("resolution_time") or "0:00"
    current = (record.data or {}).get("resolution_time") or "0:00"
    return TicketTimeService.add_time_strings(current, str(incoming))


def prepare_support_ticket_event_payload(
    record: Record,
    payload: Optional[Dict[str, Any]] = None,
    *,
    event_name: Optional[str] = None,
    actor_user_id: Optional[str] = None,
    actor_email: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build ``payload`` keys expected by production support ``RuleSet`` templates.
    """
    out = dict(payload or {})

    # Accumulate session time before camelCase→snake_case (resolutionTime is removed there).
    accumulated = _accumulate_resolution_time(record, out)
    if accumulated is not None:
        out["resolution_time"] = accumulated
        out.pop("resolutionTime", None)

    for camel, snake in _CAMEL_TO_SNAKE.items():
        if camel not in out:
            continue
        if snake not in out:
            out[snake] = out[camel]
        del out[camel]

    email = _payload_get(out, "cse_name", "actor_email", "cse_email", "userEmail") or actor_email
    if email and not out.get("cse_name"):
        out["cse_name"] = email

    # Take break unassigns the ticket — do not inject the actor as assignee.
    if event_name != SUPPORT_EVENT_TAKE_BREAK:
        assignee = _payload_get(out, "assigned_to", "actor_user_id", "userId") or actor_user_id
        if assignee and not out.get("assigned_to"):
            try:
                out["assigned_to"] = str(UUID(str(assignee)))
            except (ValueError, AttributeError, TypeError):
                out["assigned_to"] = str(assignee)

    reason = _payload_get(out, "reason")
    if reason is not None:
        out["reason"] = reason

    return out


def resolve_support_ticket_record(*, tenant, ticket_id: int) -> Optional[Record]:
    """Resolve a support ticket ``Record`` by record id or legacy ``support_ticket_id``."""
    base_qs = Record.objects.filter(
        tenant=tenant,
        entity_type=SUPPORT_TICKET_ENTITY_TYPE,
    )
    record = base_qs.filter(id=ticket_id).first()
    if record:
        return record
    return base_qs.filter(data__support_ticket_id=ticket_id).first()


def _enqueue_mixpanel_event(
    *,
    user_id: Any,
    event_name: str,
    properties: Dict[str, Any],
    tenant_id: Any = None,
) -> None:
    if not user_id:
        return
    try:
        get_queue_service().enqueue_job(
            job_type=JobType.SEND_MIXPANEL_EVENT,
            payload={
                "user_id": str(user_id),
                "event_name": event_name,
                "properties": properties or {},
            },
            tenant_id=str(tenant_id) if tenant_id else None,
            priority=0,
            max_attempts=3,
        )
    except Exception as exc:
        logger.error(
            "Failed to enqueue Mixpanel event=%s user_id=%s: %s",
            event_name,
            user_id,
            exc,
            exc_info=True,
        )


def enqueue_support_ticket_mixpanel(
    record: Record,
    event_name: str,
    payload: Dict[str, Any],
) -> None:
    """Mixpanel side-effects after rules have updated the record."""
    data = record.data or {}
    customer_user_id = data.get("user_id")
    if not customer_user_id:
        return

    props = support_ticket_button_event_mixpanel_properties(record, payload)
    tenant_id = record.tenant_id

    if event_name == SUPPORT_EVENT_NOT_CONNECTED:
        _enqueue_mixpanel_event(
            user_id=customer_user_id,
            event_name="pyro_st_not_connected",
            properties=props,
            tenant_id=tenant_id,
        )
        return

    outcome_by_event = {
        SUPPORT_EVENT_CALL_LATER: "pyro_st_call_later",
        SUPPORT_EVENT_RESOLVED: "pyro_st_resolve",
        SUPPORT_EVENT_CANNOT_RESOLVE: "pyro_st_cannot_resolve",
    }
    outcome = outcome_by_event.get(event_name)
    if not outcome:
        return

    _enqueue_mixpanel_event(
        user_id=customer_user_id,
        event_name="pyro_st_connected",
        properties=props,
        tenant_id=tenant_id,
    )
    _enqueue_mixpanel_event(
        user_id=customer_user_id,
        event_name=outcome,
        properties=props,
        tenant_id=tenant_id,
    )


def _enqueue_praja_save_resolved_ticket(
    *,
    record: Record,
    resolution_status: Optional[str] = None,
) -> None:
    from support_ticket.constants import SUPPORT_RESOLUTION_STATUS_OPEN
    from support_ticket.services import SaveResolvedTicketPrajaService

    service = SaveResolvedTicketPrajaService()
    payload = service.build_payload(record, resolution_status=resolution_status)
    if not payload:
        logger.warning(
            "[Praja] Skipping save_resolved_ticket enqueue — empty payload "
            "record_id=%s resolution_status=%s",
            record.id,
            resolution_status,
        )
        return
    is_open = resolution_status == SUPPORT_RESOLUTION_STATUS_OPEN or (
        str(payload.get("ticket_status") or "").upper() == "OPEN"
    )
    try:
        get_queue_service().enqueue_job(
            job_type=JobType.SEND_TO_PRAJA,
            payload={
                "object_type": "save_resolved_ticket",
                **payload,
            },
            tenant_id=str(record.tenant_id) if record.tenant_id else None,
            priority=0,
            max_attempts=3,
        )
        if is_open:
            logger.info(
                "[Praja] Enqueued OPEN save_support_ticket record_id=%s "
                "user_id=%s ticket_id=%s ticket_type=%s ticket_status=%s "
                "all_tasks_completed=%s",
                record.id,
                payload.get("user_id"),
                payload.get("ticket_id"),
                payload.get("ticket_type"),
                payload.get("ticket_status"),
                payload.get("all_tasks_completed"),
            )
        else:
            logger.info(
                "[Praja] Enqueued save_resolved_ticket record_id=%s "
                "ticket_status=%s ticket_id=%s",
                record.id,
                payload.get("ticket_status"),
                payload.get("ticket_id"),
            )
    except Exception as exc:
        logger.error(
            "Failed to enqueue Praja save_resolved_ticket record_id=%s "
            "resolution_status=%s: %s",
            record.id,
            resolution_status,
            exc,
            exc_info=True,
        )


def enqueue_praja_for_terminal_resolution(
    record: Record,
    *,
    resolution_status: Optional[str] = None,
) -> None:
    """Enqueue Praja when latest object_history (or explicit status) is terminal."""
    from support_ticket.records import resolution_status_from_latest_object_history

    status = resolution_status or resolution_status_from_latest_object_history(record)
    if status not in SUPPORT_TICKET_PRAJA_SYNC_RESOLUTION_STATUSES:
        return
    data = record.data or {}
    if not data.get("user_id"):
        logger.warning(
            "Skipping Praja save_resolved_ticket for record_id=%s — missing user_id",
            record.id,
        )
        return
    _enqueue_praja_save_resolved_ticket(record=record, resolution_status=status)


def enqueue_praja_for_open_ticket(
    record: Record,
    dump_data: Optional[Mapping[str, Any]] = None,
) -> None:
    """
    Enqueue Praja when a new open ticket row is created from ``process_dumped_tickets``.

    At dump ingest we read ``resolution_status`` from the dump payload / creation-time
    record data (not a separate query on ``records`` for terminal statuses).
    """
    from support_ticket.constants import SUPPORT_RESOLUTION_STATUS_OPEN

    data = record.data or {}
    dump_status = (dump_data or {}).get("resolution_status")
    record_status = data.get("resolution_status")
    status = _open_resolution_status_at_dump_ingest(record, dump_data)
    if status != SUPPORT_RESOLUTION_STATUS_OPEN:
        logger.info(
            "[Praja] Skipping OPEN save_support_ticket — not Open at dump ingest "
            "record_id=%s dump_resolution_status=%r record_resolution_status=%r "
            "resolved_status=%r user_id=%s",
            record.id,
            dump_status,
            record_status,
            status,
            data.get("user_id"),
        )
        return
    if not data.get("user_id"):
        logger.warning(
            "[Praja] Skipping OPEN save_support_ticket for record_id=%s — missing user_id "
            "dump_resolution_status=%r record_resolution_status=%r",
            record.id,
            dump_status,
            record_status,
        )
        return
    logger.info(
        "[Praja] OPEN ticket dump ingest → enqueue save_support_ticket "
        "record_id=%s user_id=%s dump_resolution_status=%r record_resolution_status=%r "
        "support_ticket_type=%r",
        record.id,
        data.get("user_id"),
        dump_status,
        record_status,
        data.get("support_ticket_type") or data.get("poster"),
    )
    _enqueue_praja_save_resolved_ticket(
        record=record,
        resolution_status=SUPPORT_RESOLUTION_STATUS_OPEN,
    )


def _open_resolution_status_at_dump_ingest(
    record: Record,
    dump_data: Optional[Mapping[str, Any]] = None,
) -> Optional[str]:
    """Resolve Open status at dump→record create time (dump payload first)."""
    from support_ticket.constants import SUPPORT_RESOLUTION_STATUS_OPEN
    from support_ticket.records import resolution_status_from_latest_object_history

    for source in (
        (dump_data or {}).get("resolution_status"),
        (record.data or {}).get("resolution_status"),
        resolution_status_from_latest_object_history(record),
    ):
        if source == SUPPORT_RESOLUTION_STATUS_OPEN:
            return SUPPORT_RESOLUTION_STATUS_OPEN
    return None


def enqueue_support_ticket_praja_sync(record: Record, event_name: str) -> None:
    """POST to Praja using object_history from the button-click save (not records.data)."""
    from support_ticket.records import resolution_status_from_latest_object_history

    status = resolution_status_from_latest_object_history(record)
    if status is None:
        status = SUPPORT_EVENT_TO_PRAJA_RESOLUTION_STATUS.get(event_name)
    enqueue_praja_for_terminal_resolution(record, resolution_status=status)


def dispatch_support_ticket_event(
    event_name: str,
    record: Record,
    payload: Dict[str, Any],
) -> None:
    """Normalize payload, run tenant rules, then Mixpanel."""
    from crm_records.rule_engine import execute_rules

    if record.entity_type != SUPPORT_TICKET_ENTITY_TYPE:
        raise ValueError(f"Record {record.id} is not a support ticket")
    if event_name not in SUPPORT_TICKET_BUTTON_EVENTS:
        raise ValueError(f"Unsupported support ticket event: {event_name}")

    prepared = prepare_support_ticket_event_payload(record, payload, event_name=event_name)
    cache.delete(f"rules:{record.tenant_id}:{event_name}")
    execute_rules(event_name, record, prepared, str(record.tenant_id))
    record.refresh_from_db()
    enqueue_support_ticket_mixpanel(record, event_name, prepared)
    enqueue_support_ticket_praja_sync(record, event_name)


def log_and_dispatch_support_ticket_event(
    *,
    record: Record,
    tenant,
    event_name: str,
    payload: Optional[Dict[str, Any]] = None,
    actor_user_id: Optional[str] = None,
    actor_email: Optional[str] = None,
) -> Record:
    """Create EventLog + dispatch through RuleSet engine."""
    enriched = prepare_support_ticket_event_payload(
        record,
        payload,
        event_name=event_name,
        actor_user_id=actor_user_id,
        actor_email=actor_email,
    )

    with transaction.atomic():
        EventLog.objects.create(
            record=record,
            tenant=tenant,
            event=event_name,
            payload=enriched,
            timestamp=timezone.now(),
        )

    dispatch_support_ticket_event(event_name, record, enriched)
    record.refresh_from_db()
    return record
