import time
from contextlib import contextmanager
from datetime import datetime, timedelta

import jwt
from django.conf import settings

from support_ticket.models import SupportTicket


def generate_supabase_jwt(
    uid="test-uid-123",
    email="test@example.com",
    tenant_id="demo-tenant",
    role="authenticated",
):
    payload = {
        "sub": uid,
        "email": email,
        "tenant_id": tenant_id,
        "role": role,
        "aud": "authenticated",
    }
    token = jwt.encode(payload, settings.SUPABASE_JWT_SECRET, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


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
    import uuid
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
        source=source,
    )


def get_date_range(days=7):
    """Returns list of ISO date strings from (today - days) to today (inclusive)."""
    today = datetime.today().date()
    return [(today - timedelta(days=i)).isoformat() for i in range(days)][::-1]


def convert_timedelta(td, unit="hours"):
    seconds = td.total_seconds()
    divisors = {"seconds": 1, "minutes": 60, "hours": 3600, "days": 86400}
    return round(seconds / divisors.get(unit, 3600), 2)


@contextmanager
def timed(label="operation", max_seconds=None):
    """
    Lightweight timing context manager.

    Usage:
        with timed("create record", max_seconds=0.3) as t:
            response = client.post(...)
        print(t.elapsed)           # seconds as float

    If max_seconds is provided and exceeded, raises AssertionError.
    """
    class _Timer:
        elapsed = 0.0

    timer = _Timer()
    start = time.perf_counter()
    yield timer
    timer.elapsed = time.perf_counter() - start
    if max_seconds is not None and timer.elapsed > max_seconds:
        raise AssertionError(f"{label} took {timer.elapsed:.3f}s, expected < {max_seconds}s")
