"""Helpers for tenant Zoho Mail connection rows (active + history)."""

from __future__ import annotations

from typing import Optional

from django.db.models import QuerySet
from django.utils import timezone

from core.models import Tenant

from .models import ZohoMailConnection


def active_connections_qs(*, tenant: Tenant) -> QuerySet[ZohoMailConnection]:
    return ZohoMailConnection.objects.filter(
        tenant=tenant,
        is_active=True,
        disconnected_at__isnull=True,
    )


def get_active_connection(*, tenant: Tenant) -> Optional[ZohoMailConnection]:
    return (
        active_connections_qs(tenant=tenant)
        .exclude(refresh_token="")
        .order_by("-created_at")
        .first()
    )


def deactivate_active_connections(*, tenant: Tenant) -> int:
    """
    Mark any active connection rows as disconnected (preserves mailbox identity).

    Used on explicit disconnect and before creating a new connection row.
    """
    now = timezone.now()
    return active_connections_qs(tenant=tenant).update(
        is_active=False,
        disconnected_at=now,
        refresh_token="",
        access_token="",
        access_token_expires_at=None,
        last_received_time_ms=None,
    )


def serialize_connection_row(conn: ZohoMailConnection) -> dict:
    return {
        "id": conn.id,
        "email_address": conn.email_address or "",
        "connected_at": conn.created_at.isoformat() if conn.created_at else None,
        "disconnected_at": conn.disconnected_at.isoformat() if conn.disconnected_at else None,
        "is_active": conn.is_active,
        "connected_by_email": conn.connected_by_email or "",
        "last_synced_at": conn.last_synced_at.isoformat() if conn.last_synced_at else None,
    }
