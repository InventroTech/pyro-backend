"""
Support ticket button events — state changes are applied by tenant ``RuleSet`` rows
in the database (configured per tenant, same as leads).

This module normalizes API payloads for rule templates and enqueues Mixpanel events.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
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
    SUPPORT_EVENT_RESOLVED,
    SUPPORT_EVENT_TAKE_BREAK,
    SUPPORT_TICKET_BUTTON_EVENTS,
    SUPPORT_TICKET_ENTITY_TYPE,
)
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
    incoming = _payload_get(payload, "resolution_time", "resolutionTime")
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
    actor_user_id: Optional[str] = None,
    actor_email: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build ``payload`` keys expected by production support ``RuleSet`` templates.
    """
    out = dict(payload or {})

    for camel, snake in _CAMEL_TO_SNAKE.items():
        if camel not in out:
            continue
        if snake not in out:
            out[snake] = out[camel]
        del out[camel]

    email = _payload_get(out, "cse_name", "actor_email", "cse_email", "userEmail") or actor_email
    if email and not out.get("cse_name"):
        out["cse_name"] = email

    assignee = _payload_get(out, "assigned_to", "actor_user_id", "userId") or actor_user_id
    if assignee and not out.get("assigned_to"):
        try:
            out["assigned_to"] = str(UUID(str(assignee)))
        except (ValueError, AttributeError, TypeError):
            out["assigned_to"] = str(assignee)

    accumulated = _accumulate_resolution_time(record, out)
    if accumulated is not None:
        out["resolution_time"] = accumulated

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


def _mixpanel_properties(record: Record, payload: Dict[str, Any]) -> Dict[str, Any]:
    data = record.data or {}
    legacy_id = data.get("support_ticket_id") or data.get("ticket_id") or record.id
    return {
        "support_ticket_id": legacy_id,
        "record_id": record.id,
        "ticket_id": legacy_id,
        "remarks": payload.get("cse_remarks") or "",
        "cse_email_id": payload.get("cse_name"),
        "reasons": payload.get("other_reasons") or [],
        "review_requested": payload.get("review_requested"),
        "poster": data.get("poster"),
        "source": data.get("source"),
        "resolution_status": data.get("resolution_status"),
        "call_status": data.get("call_status"),
        "call_attempts": data.get("call_attempts"),
    }


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

    props = _mixpanel_properties(record, payload)
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

    prepared = prepare_support_ticket_event_payload(record, payload)
    cache.delete(f"rules:{record.tenant_id}:{event_name}")
    execute_rules(event_name, record, prepared, str(record.tenant_id))
    record.refresh_from_db()
    enqueue_support_ticket_mixpanel(record, event_name, prepared)


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
