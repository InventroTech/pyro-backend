import factory
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, Mapping

from django.utils import timezone

from support_ticket.models import SupportTicketDump


def serialize_dump_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        elif isinstance(value, date):
            out[key] = datetime.combine(value, datetime.min.time()).isoformat()
        else:
            out[key] = value
    return out


def dump_data(**overrides: Any) -> Dict[str, Any]:
    """Build a serialized dump ``data`` payload for tests."""
    base = {
        "ticket_date": timezone.now() - timedelta(hours=1),
        "user_id": f"user_{uuid.uuid4().hex[:12]}",
        "name": "Test User",
        "phone": "5551234567",
        "source": "mobile_app",
        "subscription_status": "active",
        "atleast_paid_once": True,
        "reason": "Test support ticket dump",
        "badge": "premium",
        "poster": "support_agent",
        "layout_status": "pending",
        "praja_dashboard_user_link": (
            f"https://www.thecircleapp.in/admin/users/{uuid.uuid4()}"
        ),
        "display_pic_url": "https://example.com/pic.jpg",
    }
    base.update(overrides)
    return serialize_dump_payload(base)


class SupportTicketDumpFactory(factory.django.DjangoModelFactory):
    """Factory for creating SupportTicketDump instances for testing."""

    class Meta:
        model = SupportTicketDump

    tenant_id = factory.LazyFunction(uuid.uuid4)
    data = factory.LazyFunction(dump_data)
    is_processed = False
    created_at = factory.LazyFunction(timezone.now)


class ProcessedSupportTicketDumpFactory(SupportTicketDumpFactory):
    """Factory for creating processed SupportTicketDump instances."""

    is_processed = True


class MinimalSupportTicketDumpFactory(SupportTicketDumpFactory):
    """Minimal dump row: tenant_id only, empty payload."""

    data = factory.LazyFunction(dict)
