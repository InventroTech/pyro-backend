
import logging
from django.conf import settings
import re
from typing import Optional, Dict, Any

import requests
logger = logging.getLogger(__name__)


def ticket_to_mixpanel_data(ticket) -> Dict[str, Any]:
    """
    Serialize a SupportTicket instance to a JSON-serializable dict with all column data.
    Used to attach full ticket data to Mixpanel event properties.
    """
    if ticket is None:
        return {}
    return {
        "ticket_id": ticket.id,
        "created_at": ticket.created_at.isoformat() if getattr(ticket, "created_at", None) else None,
        "ticket_date": ticket.ticket_date.isoformat() if getattr(ticket, "ticket_date", None) else None,
        "user_id": getattr(ticket, "user_id", None),
        "name": getattr(ticket, "name", None),
        "phone": getattr(ticket, "phone", None),
        "source": getattr(ticket, "source", None),
        "subscription_status": getattr(ticket, "subscription_status", None),
        "atleast_paid_once": getattr(ticket, "atleast_paid_once", None),
        "reason": getattr(ticket, "reason", None),
        "other_reasons": getattr(ticket, "other_reasons", None) or [],
        "badge": getattr(ticket, "badge", None),
        "poster": getattr(ticket, "poster", None),
        "tenant_id": str(ticket.tenant.id) if getattr(ticket, "tenant", None) else None,
        "assigned_to": str(ticket.assigned_to.id) if getattr(ticket, "assigned_to", None) else None,
        "layout_status": getattr(ticket, "layout_status", None),
        "state": getattr(ticket, "state", None),
        "resolution_status": getattr(ticket, "resolution_status", None),
        "resolution_time": getattr(ticket, "resolution_time", None),
        "cse_name": getattr(ticket, "cse_name", None),
        "cse_remarks": getattr(ticket, "cse_remarks", None),
        "call_status": getattr(ticket, "call_status", None),
        "call_attempts": getattr(ticket, "call_attempts", None),
        "rm_name": getattr(ticket, "rm_name", None),
        "completed_at": ticket.completed_at.isoformat() if getattr(ticket, "completed_at", None) else None,
        "snooze_until": ticket.snooze_until.isoformat() if getattr(ticket, "snooze_until", None) else None,
        "praja_dashboard_user_link": getattr(ticket, "praja_dashboard_user_link", None),
        "display_pic_url": getattr(ticket, "display_pic_url", None),
        "dumped_at": ticket.dumped_at.isoformat() if getattr(ticket, "dumped_at", None) else None,
        "review_requested": getattr(ticket, "review_requested", None),
    }



def send_to_mixpanel(user_id: str, event_name: str, properties: dict) -> None:
    """
    Non-blocking Mixpanel call.
    Uses the same contract as Edge function proxy:
      - POST JSON to MIXPANEL_API_URL
      - Bearer MIXPANEL_TOKEN
    """
    token = getattr(settings, "MIXPANEL_TOKEN", None)
    url = getattr(settings, "MIXPANEL_API_URL", None)
    if not token or not url:
        logger.warning("MIXPANEL env not configured; skipping Mixpanel event")
        return

    payload = {
        "user_id": int(user_id) if str(user_id).isdigit() else user_id,
        "event_name": event_name,
        "properties": properties or {},
    }
    try:
        r = requests.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=5,
        )
        if r.status_code >= 400:
            logger.error("Mixpanel API error %s %s: %s", r.status_code, r.reason, r.text[:500])
        else:
            logger.info("Mixpanel event sent: %s", event_name)
    except Exception as e:
        logger.exception("Error sending Mixpanel event: %s", e)




