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

    def test_match_by_item_name_in_email(self):
        record = RecordFactory(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={
                "status": "IN_SHIPPING",
                "item_name_freeform": "Bambu Lab P1S 3D Printer",
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={"status": "IN_SHIPPING", "item_name_freeform": "Drone"},
        )
        parsed = {
            "tracking_number": "AWB999888777",
            "tracking_link": None,
            "courier_name": "Delhivery",
            "email_text": "Your Bambu Lab P1S 3D Printer has been shipped. AWB AWB999888777",
        }
        matched, reason = match_record_for_email(tenant_id=self.tenant.id, parsed=parsed)
        self.assertEqual(matched.id, record.id)
        self.assertEqual(reason, "item_name")

    def test_prefers_longer_item_name(self):
        short = RecordFactory(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={"status": "IN_SHIPPING", "item_name_freeform": "Drone"},
        )
        long = RecordFactory(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={
                "status": "IN_SHIPPING",
                "item_name_freeform": "Drone with Dual 4K Camera for Adults",
            },
        )
        parsed = {
            "tracking_number": "AWB111222333",
            "email_text": "Shipped: Drone with Dual 4K Camera for Adults",
        }
        matched, reason = match_record_for_email(tenant_id=self.tenant.id, parsed=parsed)
        self.assertEqual(matched.id, long.id)
        self.assertNotEqual(matched.id, short.id)
        self.assertEqual(reason, "item_name")

    def test_ambiguous_same_item_name(self):
        RecordFactory(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={"status": "IN_SHIPPING", "item_name_freeform": "Drone"},
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={"status": "IN_SHIPPING", "item_name_freeform": "Drone"},
        )
        parsed = {
            "tracking_number": "AWB111222333",
            "email_text": "Your Drone order is in transit",
        }
        matched, reason = match_record_for_email(tenant_id=self.tenant.id, parsed=parsed)
        self.assertIsNone(matched)
        self.assertEqual(reason, "ambiguous_item_name")

    def test_no_item_match(self):
        RecordFactory(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={"status": "IN_SHIPPING", "item_name_freeform": "Nylon Sleeve"},
        )
        parsed = {
            "tracking_number": "AWB111222333",
            "email_text": "Your package is out for delivery",
        }
        matched, reason = match_record_for_email(tenant_id=self.tenant.id, parsed=parsed)
        self.assertIsNone(matched)
        self.assertEqual(reason, "no_item_match")

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
            data={
                "status": "IN_SHIPPING",
                "item_name_freeform": "Bambu Lab P1S 3D Printer",
            },
        )
        ZohoMailConnection.objects.create(
            tenant=self.tenant,
            refresh_token="refresh",
            access_token="access",
            access_token_expires_at=timezone.now() + timedelta(hours=1),
            account_id="acc1",
            inbox_folder_id="fold1",
            is_active=True,
            initial_backfill_completed=True,
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
                "subject": "Your Bambu Lab P1S 3D Printer has shipped",
                "fromAddress": "Delhivery <noreply@delhivery.com>",
                "receivedTime": str(int(timezone.now().timestamp() * 1000)),
                "summary": "AWB",
            }
        ]
        fake_content = {
            "content": (
                "Item: Bambu Lab P1S 3D Printer<br>"
                "Tracking number: DELH12345678<br>"
                "Courier: Delhivery"
            )
        }

        # Connection already has account_id/inbox_folder_id; skip Zoho account resolution.
        with patch("email_protocol.zoho_shipment_sync.ZohoMailClient") as MockClient, patch(
            "email_protocol.zoho_shipment_sync.ensure_account_and_inbox"
        ):
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

    @override_settings(
        ZOHO_CLIENT_ID="cid",
        ZOHO_CLIENT_SECRET="sec",
        ZOHO_OAUTH_REDIRECT_URI="https://api.example.com/email/zoho/callback/",
        ZOHO_MAIL_BACKFILL_PAGE_SIZE=2,
        ZOHO_MAIL_BACKFILL_MAX_MESSAGES_PER_RUN=10,
    )
    def test_initial_backfill_paginates_whole_inbox_without_time_cursor(self):
        conn = ZohoMailConnection.objects.create(
            tenant=self.tenant,
            refresh_token="refresh",
            access_token="access",
            access_token_expires_at=timezone.now() + timedelta(hours=1),
            account_id="acc1",
            inbox_folder_id="fold1",
            is_active=True,
            initial_backfill_completed=False,
            backfill_next_start=1,
        )
        page_one = [
            {
                "messageId": "m1",
                "folderId": "fold1",
                "subject": "Older mail",
                "fromAddress": "ops@example.com",
                "receivedTime": "1000",
            },
            {
                "messageId": "m2",
                "folderId": "fold1",
                "subject": "Middle mail",
                "fromAddress": "ops@example.com",
                "receivedTime": "2000",
            },
        ]
        page_two = [
            {
                "messageId": "m3",
                "folderId": "fold1",
                "subject": "Newest mail",
                "fromAddress": "ops@example.com",
                "receivedTime": "3000",
            },
        ]

        with patch("email_protocol.zoho_shipment_sync.ZohoMailClient") as MockClient, patch(
            "email_protocol.zoho_shipment_sync.ensure_account_and_inbox"
        ):
            client = MagicMock()
            MockClient.return_value = client
            client.list_messages.side_effect = [page_one, page_two]
            client.get_message_content.return_value = {"content": "hello"}

            from email_protocol.zoho_shipment_sync import sync_zoho_shipment_emails

            result = sync_zoho_shipment_emails(conn)

        conn.refresh_from_db()
        self.assertTrue(result["initial_backfill_completed"])
        self.assertEqual(client.list_messages.call_count, 2)
        self.assertEqual(
            ZohoMailProcessedMessage.objects.filter(connection=conn).count(),
            3,
        )
        self.assertEqual(conn.last_received_time_ms, 3000)
        self.assertEqual(conn.backfill_next_start, 1)

    @override_settings(
        ZOHO_CLIENT_ID="cid",
        ZOHO_CLIENT_SECRET="sec",
        ZOHO_OAUTH_REDIRECT_URI="https://api.example.com/email/zoho/callback/",
        ZOHO_MAIL_BACKFILL_PAGE_SIZE=2,
        ZOHO_MAIL_BACKFILL_MAX_MESSAGES_PER_RUN=2,
    )
    def test_inbox_scan_resumes_from_saved_start_index(self):
        conn = ZohoMailConnection.objects.create(
            tenant=self.tenant,
            refresh_token="refresh",
            access_token="access",
            access_token_expires_at=timezone.now() + timedelta(hours=1),
            account_id="acc1",
            inbox_folder_id="fold1",
            is_active=True,
            initial_backfill_completed=False,
            backfill_next_start=1,
        )
        page_one = [
            {
                "messageId": "m1",
                "folderId": "fold1",
                "subject": "Mail 1",
                "fromAddress": "ops@example.com",
                "receivedTime": "1000",
            },
            {
                "messageId": "m2",
                "folderId": "fold1",
                "subject": "Mail 2",
                "fromAddress": "ops@example.com",
                "receivedTime": "2000",
            },
        ]
        page_two = [
            {
                "messageId": "m3",
                "folderId": "fold1",
                "subject": "Mail 3",
                "fromAddress": "ops@example.com",
                "receivedTime": "3000",
            },
        ]

        with patch("email_protocol.zoho_shipment_sync.ZohoMailClient") as MockClient, patch(
            "email_protocol.zoho_shipment_sync.ensure_account_and_inbox"
        ):
            client = MagicMock()
            MockClient.return_value = client
            client.list_messages.side_effect = [page_one, page_two]
            client.get_message_content.return_value = {"content": "hello"}

            from email_protocol.zoho_shipment_sync import sync_zoho_shipment_emails

            first = sync_zoho_shipment_emails(conn)

        conn.refresh_from_db()
        self.assertFalse(first["initial_backfill_completed"])
        self.assertEqual(conn.backfill_next_start, 3)
        self.assertEqual(
            ZohoMailProcessedMessage.objects.filter(connection=conn).count(),
            2,
        )
        self.assertEqual(client.list_messages.call_args_list[0].kwargs["start"], 1)

        with patch("email_protocol.zoho_shipment_sync.ZohoMailClient") as MockClient, patch(
            "email_protocol.zoho_shipment_sync.ensure_account_and_inbox"
        ):
            client = MagicMock()
            MockClient.return_value = client
            client.list_messages.return_value = page_two
            client.get_message_content.return_value = {"content": "hello"}

            from email_protocol.zoho_shipment_sync import sync_zoho_shipment_emails

            second = sync_zoho_shipment_emails(conn)

        conn.refresh_from_db()
        self.assertTrue(second["initial_backfill_completed"])
        self.assertEqual(client.list_messages.call_args.kwargs["start"], 3)
        self.assertEqual(
            ZohoMailProcessedMessage.objects.filter(connection=conn).count(),
            3,
        )
