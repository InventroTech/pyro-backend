"""Tests for Zoho Mail connection history rows (connect / disconnect)."""

from __future__ import annotations

from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from email_protocol.models import ZohoMailConnection
from email_protocol.zoho_connections import deactivate_active_connections, get_active_connection
from tests.factories.core_factory import TenantFactory


class ZohoConnectionHistoryTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()

    def test_disconnect_preserves_email_and_sets_disconnected_at(self):
        conn = ZohoMailConnection.objects.create(
            tenant=self.tenant,
            refresh_token="refresh",
            access_token="access",
            access_token_expires_at=timezone.now() + timedelta(hours=1),
            email_address="first@example.com",
            account_id="acc1",
            inbox_folder_id="fold1",
            is_active=True,
        )
        self.assertEqual(deactivate_active_connections(tenant=self.tenant), 1)
        conn.refresh_from_db()
        self.assertFalse(conn.is_active)
        self.assertIsNotNone(conn.disconnected_at)
        self.assertEqual(conn.email_address, "first@example.com")
        self.assertEqual(conn.refresh_token, "")

    def test_reconnect_creates_new_row_and_keeps_old_history(self):
        first = ZohoMailConnection.objects.create(
            tenant=self.tenant,
            refresh_token="refresh1",
            email_address="first@example.com",
            is_active=True,
        )
        deactivate_active_connections(tenant=self.tenant)
        first.refresh_from_db()

        second = ZohoMailConnection.objects.create(
            tenant=self.tenant,
            refresh_token="refresh2",
            email_address="second@example.com",
            is_active=True,
        )

        rows = list(
            ZohoMailConnection.objects.filter(tenant=self.tenant).order_by("created_at")
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].email_address, "first@example.com")
        self.assertIsNotNone(rows[0].disconnected_at)
        self.assertFalse(rows[0].is_active)
        self.assertEqual(rows[1].email_address, "second@example.com")
        self.assertTrue(rows[1].is_active)
        self.assertIsNone(rows[1].disconnected_at)
        self.assertEqual(get_active_connection(tenant=self.tenant), second)
