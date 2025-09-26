from support_ticket.models import SupportTicket
from datetime import datetime, timedelta
import uuid
import jwt
from django.conf import settings


def create_support_ticket(
    dumped_at=None,
    completed_at=None,
    resolution_status="Resolved",
    tenant_id=None,
    name="Test User",
    phone="9999999999",
    user_id=None,
    source="test_source",
):
    dumped_at = dumped_at or datetime.now() - timedelta(days=2)
    completed_at = completed_at or datetime.now() - timedelta(days=1)
    tenant_id = tenant_id or uuid.uuid4()
    user_id = user_id or "test-user-id"

    return SupportTicket.objects.create(
        dumped_at=dumped_at,
        completed_at=completed_at,
        resolution_status=resolution_status,
        tenant_id=tenant_id,
        name=name,
        phone=phone,
        user_id=user_id,
        source=source
    )


def get_date_range(days=7):
    """Returns list of dates from today - days to today (inclusive)."""
    today = datetime.today().date()
    return [(today - timedelta(days=i)).isoformat() for i in range(days)][::-1]


def generate_supabase_jwt(uid="test-uid-123", email="test@example.com", tenant_id="demo-tenant", role="authenticated"):
    payload = {
        "sub": uid,
        "email": email,
        "tenant_id": tenant_id,
        "role": role,
        "aud": "authenticated"
    }
    token = jwt.encode(payload, settings.SUPABASE_JWT_SECRET, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token



def convert_timedelta(td, unit="hours"):
    seconds = td.total_seconds()
    if unit == "seconds":
        return round(seconds, 2)
    elif unit == "minutes":
        return round(seconds / 60, 2)
    elif unit == "hours":
        return round(seconds / 3600, 2)
    elif unit == "days":
        return round(seconds / 86400, 2)
    return round(seconds / 3600, 2)
