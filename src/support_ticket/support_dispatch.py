"""
Support workflow dispatch: records are primary; support_ticket is mirrored for rollback.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from uuid import UUID

from django.utils import timezone

from crm_records.events import dispatch_event
from crm_records.models import EventLog, Record
from crm_records.support_record_mirror import (
    get_mirrored_record_for_ticket,
    sync_record_from_ticket,
    upsert_support_record_from_ticket,
)
from support_ticket.models import SupportTicket

logger = logging.getLogger(__name__)


def _apply_ticket_field_updates(ticket: SupportTicket, updates: Dict[str, Any]) -> None:
    field_map = {
        "resolution_status": "resolution_status",
        "call_status": "call_status",
        "cse_remarks": "cse_remarks",
        "cse_name": "cse_name",
        "resolution_time": "resolution_time",
        "call_attempts": "call_attempts",
        "other_reasons": "other_reasons",
        "review_requested": "review_requested",
        "snooze_until": "snooze_until",
        "completed_at": "completed_at",
    }
    for key, attr in field_map.items():
        if key not in updates:
            continue
        val = updates[key]
        if key == "completed_at" and isinstance(val, str):
            from django.utils.dateparse import parse_datetime
            val = parse_datetime(val) or timezone.now()
        if key == "snooze_until" and isinstance(val, str):
            from django.utils.dateparse import parse_datetime
            val = parse_datetime(val)
        setattr(ticket, attr, val)
    if "assigned_to_id" in updates:
        at = updates["assigned_to_id"]
        if at in (None, "", "null"):
            ticket.assigned_to_id = None
        else:
            try:
                ticket.assigned_to_id = UUID(str(at))
            except (ValueError, TypeError):
                ticket.assigned_to_id = None
    elif "assigned_to" in updates:
        at = updates["assigned_to"]
        if at in (None, "", "null"):
            ticket.assigned_to_id = None
        else:
            try:
                ticket.assigned_to_id = UUID(str(at))
            except (ValueError, TypeError):
                ticket.assigned_to_id = None


def ensure_support_record(*, ticket: SupportTicket, tenant) -> Record:
    """Ensure a mirrored record exists and reflects the ticket baseline before rules run."""
    record = get_mirrored_record_for_ticket(ticket=ticket, tenant=tenant)
    if record:
        sync_record_from_ticket(ticket, tenant=tenant)
        record.refresh_from_db()
        return record
    return upsert_support_record_from_ticket(ticket=ticket, tenant=tenant)


def _tenant_has_rules_for_event(tenant, event_name: str) -> bool:
    from crm_records.models import RuleSet

    return RuleSet.objects.filter(
        tenant_id=tenant.id,
        event_name=event_name,
        enabled=True,
    ).exists()


def _apply_simulated_support_workflow(
    *,
    record: Record,
    event_name: str,
    payload: Dict[str, Any],
) -> Record:
    """Apply built-in support workflow when tenant has no RuleSet rows (tests / pre-seed)."""
    from crm_records.events import simulate_workflow_actions
    from crm_records.rule_engine import action_update_fields

    simulated = simulate_workflow_actions(event_name, record, payload)
    if simulated.get("action") != "update_fields":
        logger.warning("No fallback workflow for support event %s", event_name)
        return record

    ctx = {
        "record": record,
        "payload": payload,
        "event": event_name,
        "record_data": record.data or {},
    }
    action_update_fields(
        ctx,
        simulated.get("updates") or {},
        increments=simulated.get("increments"),
    )
    return record


def dispatch_support_record_event(
    *,
    tenant,
    record: Record,
    event_name: str,
    payload: Dict[str, Any],
    log_event: bool = True,
) -> Record:
    """
    Run rules on the record (primary), then mirror results to support_ticket.
    """
    if log_event:
        EventLog.objects.create(
            record=record,
            tenant=tenant,
            event=event_name,
            payload=payload,
            timestamp=timezone.now(),
        )
    if _tenant_has_rules_for_event(tenant, event_name):
        dispatch_event(event_name, record, payload)
    else:
        _apply_simulated_support_workflow(record=record, event_name=event_name, payload=payload)
    record.refresh_from_db()

    ticket_id = (record.data or {}).get("support_ticket_id")
    if ticket_id:
        mirror_record_to_ticket(record, ticket_id=int(ticket_id))

    return record


def dispatch_support_event(
    *,
    ticket: SupportTicket,
    event_name: str,
    payload: Optional[Dict[str, Any]] = None,
    tenant=None,
    request_user=None,
    log_event: bool = True,
) -> Optional[Record]:
    """
    Legacy support_ticket view entrypoint: seed record from ticket, run rules, mirror back.
    """
    payload = dict(payload or {})
    tenant = tenant or ticket.tenant
    if not tenant:
        logger.warning("dispatch_support_event: no tenant for ticket %s", ticket.id)
        return None

    record = ensure_support_record(ticket=ticket, tenant=tenant)
    return dispatch_support_record_event(
        tenant=tenant,
        record=record,
        event_name=event_name,
        payload=payload,
        log_event=log_event,
    )


def mirror_record_to_ticket(record: Record, *, ticket_id: int) -> Optional[SupportTicket]:
    """Sync records.data fields back to legacy support_ticket row."""
    try:
        ticket = SupportTicket.objects.get(id=ticket_id)
    except SupportTicket.DoesNotExist:
        logger.warning("mirror_record_to_ticket: ticket %s not found", ticket_id)
        return None

    data = record.data or {}
    updates: Dict[str, Any] = {}
    for key in (
        "resolution_status",
        "call_status",
        "cse_remarks",
        "cse_name",
        "resolution_time",
        "call_attempts",
        "other_reasons",
        "review_requested",
    ):
        if key in data:
            updates[key] = data[key]

    if "snooze_until" in data:
        from django.utils.dateparse import parse_datetime
        raw = data["snooze_until"]
        updates["snooze_until"] = parse_datetime(raw) if isinstance(raw, str) else raw

    if "completed_at" in data:
        from django.utils.dateparse import parse_datetime
        raw = data["completed_at"]
        updates["completed_at"] = parse_datetime(raw) if isinstance(raw, str) else raw

    assigned = data.get("assigned_to")
    if assigned in (None, "", "null"):
        updates["assigned_to_id"] = None
    elif assigned:
        try:
            updates["assigned_to_id"] = UUID(str(assigned))
        except (ValueError, TypeError):
            pass

    _apply_ticket_field_updates(ticket, updates)
    ticket.save()
    return ticket
