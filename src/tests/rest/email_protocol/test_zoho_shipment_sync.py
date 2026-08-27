"""Tests for Zoho shipment email sync matching / apply (mocked Zoho)."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.test import TestCase, override_settings
from django.utils import timezone

from background_jobs.job_handlers import SyncZohoShipmentEmailsJobHandler
from background_jobs.models import JobType
from email_protocol.models import ZohoMailConnection, ZohoMailProcessedMessage
from email_protocol.zoho_oauth import build_oauth_state, parse_oauth_state
from email_protocol.zoho_shipment_sync import apply_tracking_to_record, match_record_for_email
from tests.factories.crm_records_factory import BackgroundJobFactory, RecordFactory
from tests.factories.core_factory import TenantFactory


class ZohoShipmentMatchTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()

    def test_match_by_record_id_in_email(self):
        record = RecordFactory(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={"status": "IN_SHIPPING"},
        )
        parsed = {
            "tracking_number": "AWB999888777",
            "tracking_link": None,
            "courier_name": "Delhivery",
            "match_keys": {"record_ids": [str(record.id)]},
        }
        matched, reason = match_record_for_email(tenant_id=self.tenant.id, parsed=parsed)
        self.assertEqual(matched.id, record.id)
        self.assertEqual(reason, "record_id")

    def test_match_by_po_number(self):
        record = RecordFactory(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={"status": "IN_SHIPPING", "po_number": "PO-7788"},
        )
        parsed = {
            "tracking_number": "AWB111222333",
            "tracking_link": None,
            "courier_name": None,
            "match_keys": {"record_ids": [], "po_number": "PO-7788"},
        }
        matched, reason = match_record_for_email(tenant_id=self.tenant.id, parsed=parsed)
        self.assertEqual(matched.id, record.id)
        self.assertEqual(reason, "po_number")

    def test_apply_fills_empty_tracking_only(self):
        record = RecordFactory(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={"status": "IN_SHIPPING", "tracking_number": "", "courier_name": ""},
        )
        changed = apply_tracking_to_record(
            record,
            {
                "tracking_number": "XYZ123456789",
                "tracking_link": "https://www.aftership.com/track/XYZ123456789",
                "courier_name": "FedEx",
                "eta": "2026-09-01",
            },
        )
        self.assertTrue(changed)
        record.refresh_from_db()
        self.assertEqual(record.data["tracking_number"], "XYZ123456789")
        self.assertEqual(record.data["courier_name"], "FedEx")
        self.assertTrue(record.data.get("tracking_updated_at"))


class ZohoOAuthStateTests(TestCase):
    def test_state_roundtrip(self):
        state = build_oauth_state(
            tenant_id="11111111-1111-1111-1111-111111111111", user_email="ops@x.com"
        )
        data = parse_oauth_state(state)
        self.assertEqual(data["tenant_id"], "11111111-1111-1111-1111-111111111111")
        self.assertEqual(data["user_email"], "ops@x.com")


class SyncZohoShipmentEmailsJobHandlerTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()
        self.handler = SyncZohoShipmentEmailsJobHandler()

    def test_skips_when_no_connection(self):
        job = BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.SYNC_ZOHO_SHIPMENT_EMAILS,
            payload={},
        )
        ok = self.handler.process(job)
        self.assertTrue(ok)
        self.assertEqual(job.result["skipped"], "no_zoho_connection")

    @override_settings(
        ZOHO_CLIENT_ID="cid",
        ZOHO_CLIENT_SECRET="sec",
        ZOHO_OAUTH_REDIRECT_URI="https://api.example.com/email/zoho/callback/",
    )
    def test_sync_applies_tracking_from_mocked_inbox(self):
        record = RecordFactory(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={"status": "IN_SHIPPING", "po_number": "PO-55"},
        )
        ZohoMailConnection.objects.create(
            tenant=self.tenant,
            refresh_token="refresh",
            access_token="access",
            access_token_expires_at=timezone.now() + timedelta(hours=1),
            account_id="acc1",
            inbox_folder_id="fold1",
            is_active=True,
        )
        job = BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.SYNC_ZOHO_SHIPMENT_EMAILS,
            payload={"max_messages": 10},
        )

        fake_messages = [
            {
                "messageId": "m1",
                "folderId": "fold1",
                "subject": "Shipped PO-55",
                "receivedTime": str(int(timezone.now().timestamp() * 1000)),
                "summary": "AWB",
            }
        ]
        fake_content = {
            "content": "PO number: PO-55<br>Tracking number: DELH12345678<br>Courier: Delhivery"
        }

        with patch("email_protocol.zoho_shipment_sync.ZohoMailClient") as MockClient:
            client = MagicMock()
            MockClient.return_value = client
            client.list_messages.return_value = fake_messages
            client.get_message_content.return_value = fake_content
            ok = self.handler.process(job)

        self.assertTrue(ok)
        self.assertEqual(job.result.get("applied"), 1)
        record.refresh_from_db()
        self.assertEqual(record.data.get("tracking_number"), "DELH12345678")
        self.assertTrue(
            ZohoMailProcessedMessage.objects.filter(message_id="m1", applied=True).exists()
        )
