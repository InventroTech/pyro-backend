"""Shared Mixpanel property builders for support ticket events."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Optional

from crm_records.models import Record


def _iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat()
    if isinstance(value, str):
        return value
    return str(value)


def support_ticket_mixpanel_properties(
    record: Record,
    **overrides: Any,
) -> Dict[str, Any]:
    """
    Full support ticket snapshot for Mixpanel — same shape as ``pyro_st_ticket_created``.
    Pass ``overrides`` for event-specific fields (e.g. assignment actor).
    """
    data = record.data or {}
    ticket_id = data.get("support_ticket_id") or data.get("ticket_id")
    props: Dict[str, Any] = {
        "ticket_id": ticket_id,
        "support_ticket_id": ticket_id,
        "record_id": record.id,
        "tenant_id": str(record.tenant_id) if record.tenant_id else data.get("tenant_id"),
        "created_at": _iso_or_none(record.created_at),
        "ticket_date": _iso_or_none(data.get("ticket_date")),
        "user_id": data.get("user_id"),
        "name": data.get("name"),
        "phone": data.get("phone"),
        "source": data.get("source"),
        "subscription_status": data.get("subscription_status"),
        "atleast_paid_once": data.get("atleast_paid_once"),
        "reason": data.get("reason"),
        "other_reasons": data.get("other_reasons") or [],
        "badge": data.get("badge"),
        "poster": data.get("poster"),
        "support_ticket_type": data.get("support_ticket_type"),
        "release_build_number": data.get("release_build_number"),
        "assigned_to": data.get("assigned_to"),
        "layout_status": data.get("layout_status"),
        "state": data.get("state"),
        "resolution_status": data.get("resolution_status"),
        "resolution_time": data.get("resolution_time"),
        "cse_name": data.get("cse_name"),
        "cse_remarks": data.get("cse_remarks"),
        "call_status": data.get("call_status"),
        "call_attempts": data.get("call_attempts"),
        "rm_name": data.get("rm_name"),
        "completed_at": _iso_or_none(data.get("completed_at")),
        "snooze_until": _iso_or_none(data.get("snooze_until")),
        "praja_dashboard_user_link": data.get("praja_dashboard_user_link"),
        "display_pic_url": data.get("display_pic_url"),
        "dumped_at": _iso_or_none(data.get("dumped_at")),
        "review_requested": data.get("review_requested"),
    }
    props.update(overrides)
    return props


def support_ticket_button_event_mixpanel_properties(
    record: Record,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Connected / not-connected / resolve events: full ticket snapshot plus legacy
    aliases still used by downstream Mixpanel consumers.
    """
    props = support_ticket_mixpanel_properties(record)
    if payload:
        if payload.get("cse_remarks") is not None:
            props["cse_remarks"] = payload.get("cse_remarks")
        if payload.get("cse_name") is not None:
            props["cse_name"] = payload.get("cse_name")
        if payload.get("other_reasons") is not None:
            props["other_reasons"] = payload.get("other_reasons") or []
        if payload.get("review_requested") is not None:
            props["review_requested"] = payload.get("review_requested")
    props["remarks"] = props.get("cse_remarks") or ""
    props["cse_email_id"] = props.get("cse_name")
    props["reasons"] = props.get("other_reasons") or []
    return props
