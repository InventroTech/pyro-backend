"""Mirror support_ticket rows to records (entity_type=support_ticket) for unified pull."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, Optional

from django.db.models import Q
from django.utils import timezone

from crm_records.models import Record
from support_ticket.models import SupportTicket
from support_ticket.utils import ticket_to_mixpanel_data

logger = logging.getLogger(__name__)

OPEN_RESOLUTION_Q = Q(resolution_status__isnull=True) | Q(resolution_status="Snoozed")


def ticket_data_from_model(ticket: SupportTicket) -> Dict[str, Any]:
    """Full mirror of support_ticket columns into records.data."""
    data = ticket_to_mixpanel_data(ticket)
    data["support_ticket_id"] = ticket.id
    if ticket.snooze_until:
        data["snooze_until"] = ticket.snooze_until.isoformat()
        data["next_call_at"] = ticket.snooze_until.isoformat()
    elif ticket.snooze_until is None and "next_call_at" not in data:
        data.pop("next_call_at", None)
    if ticket.assigned_to_id:
        data["assigned_to"] = str(ticket.assigned_to_id)
    else:
        data.pop("assigned_to", None)
    return data


def delete_open_support_records_for_user(*, tenant, user_id: str) -> int:
    """Mirror ProcessDumpedTickets replace-open behavior on records."""
    if not user_id:
        return 0
    qs = Record.objects.filter(
        tenant=tenant,
        entity_type="support_ticket",
        data__user_id=user_id,
    ).extra(
        where=[
            """
            (
                data->>'resolution_status' IS NULL
                OR TRIM(COALESCE(data->>'resolution_status', '')) = ''
                OR data->>'resolution_status' = 'Snoozed'
            )
            """
        ]
    )
    count, _ = qs.delete()
    return count


def upsert_support_record_from_ticket(*, ticket: SupportTicket, tenant) -> Record:
    """Create or update mirrored Record for a SupportTicket."""
    data = ticket_data_from_model(ticket)
    existing = Record.objects.filter(
        tenant=tenant,
        entity_type="support_ticket",
        data__support_ticket_id=ticket.id,
    ).first()
    if existing:
        merged = existing.data.copy() if existing.data else {}
        merged.update(data)
        existing.data = merged
        existing.updated_at = timezone.now()
        existing.save(update_fields=["data", "updated_at"])
        return existing
    return Record.objects.create(
        tenant=tenant,
        entity_type="support_ticket",
        data=data,
    )


def get_mirrored_record_for_ticket(*, ticket: SupportTicket, tenant) -> Optional[Record]:
    return Record.objects.filter(
        tenant=tenant,
        entity_type="support_ticket",
        data__support_ticket_id=ticket.id,
    ).first()


def sync_record_from_ticket(ticket: SupportTicket, *, tenant) -> Optional[Record]:
    """Update mirrored record from ticket after action."""
    record = get_mirrored_record_for_ticket(ticket=ticket, tenant=tenant)
    if not record:
        return None
    record.data = ticket_data_from_model(ticket)
    record.updated_at = timezone.now()
    record.save(update_fields=["data", "updated_at"])
    return record


def compute_support_not_connected_updates(
    ticket: SupportTicket,
    *,
    now=None,
) -> Dict[str, Any]:
    """Match UpdateCallStatusView not-connected semantics."""
    now = now or timezone.now()
    attempts = (ticket.call_attempts or 0) + 1
    updates: Dict[str, Any] = {
        "call_attempts": attempts,
        "completed_at": now.isoformat(),
    }
    if attempts == 1:
        snooze = now + timedelta(hours=1)
        updates["resolution_status"] = "Snoozed"
        updates["snooze_until"] = snooze.isoformat()
        updates["next_call_at"] = snooze.isoformat()
    else:
        snooze = now + timedelta(days=365 * 10)
        updates["resolution_status"] = "Closed"
        updates["snooze_until"] = snooze.isoformat()
        updates["next_call_at"] = None
    return updates
