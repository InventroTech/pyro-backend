import uuid
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from lead_notifications.models import InAppNotification
from lead_notifications.service import (
    build_lead_called_back_payload,
    notify_lead_called_back,
    resolve_user_pk_for_assigned_to,
    should_notify_lead_called_back,
    was_call_received,
)
from realtime.broadcast import skip_realtime_broadcast
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RecordFactory, TenantMembershipFactory, UserFactory


class CallReceivedFlagTests(TestCase):
    def test_was_call_received_truthy_values(self):
        self.assertTrue(was_call_received({"wati_chatbot_call_received": True}))
        self.assertTrue(was_call_received({"wati_chatbot_call_received": "true"}))
        self.assertFalse(was_call_received({"wati_chatbot_call_received": False}))
        self.assertFalse(was_call_received({}))

    def test_should_notify_only_on_false_to_true(self):
        self.assertTrue(
            should_notify_lead_called_back(
                {"wati_chatbot_call_received": False},
                {"wati_chatbot_call_received": True},
            )
        )
        self.assertTrue(
            should_notify_lead_called_back(
                {},
                {"wati_chatbot_call_received": True},
            )
        )
        self.assertFalse(
            should_notify_lead_called_back(
                {"wati_chatbot_call_received": True},
                {"wati_chatbot_call_received": True},
            )
        )
        self.assertFalse(
            should_notify_lead_called_back(
                {},
                {"wati_chatbot_call_received": False},
            )
        )


class ResolveAssignedToTests(TestCase):
    def test_resolves_supabase_uid(self):
        user = UserFactory(supabase_uid="rm-uid-123")
        record = RecordFactory(data={"assigned_to": "rm-uid-123"})
        pk = resolve_user_pk_for_assigned_to(record.tenant, "rm-uid-123")
        self.assertEqual(pk, user.pk)

    def test_resolves_membership_user_id(self):
        uid = str(uuid.uuid4())
        user = UserFactory(supabase_uid=uid)
        record = RecordFactory()
        TenantMembershipFactory(
            tenant=record.tenant,
            user_id=uid,
        )
        pk = resolve_user_pk_for_assigned_to(record.tenant, uid)
        self.assertEqual(pk, user.pk)


class NotifyLeadCalledBackTests(TestCase):
    def test_broadcasts_payload_to_assigned_rm(self):
        rm = UserFactory(supabase_uid="rm-abc")
        # Keep flag false on create so post_save signal does not also notify.
        record = RecordFactory(
            data={
                "name": "Raj",
                "phone_number": "9876543210",
                "praja_id": "PRAJA123",
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": False,
            },
        )

        with patch("lead_notifications.service.broadcast_to_user") as broadcast:
            notify_lead_called_back(record)

        broadcast.assert_called_once()
        user_pk, payload = broadcast.call_args.args
        self.assertEqual(user_pk, rm.pk)
        self.assertEqual(payload["event"], "lead_called_back")
        self.assertEqual(payload["record_id"], str(record.id))
        self.assertEqual(payload["lead_name"], "Raj")
        self.assertEqual(payload["phone_number"], "9876543210")
        self.assertEqual(payload["praja_id"], "PRAJA123")
        self.assertTrue(payload["wati_chatbot_call_received"])
        self.assertIsNotNone(payload.get("notification_id"))

        notif = InAppNotification.objects.get(pk=payload["notification_id"])
        self.assertEqual(notif.user_id, rm.supabase_uid)
        self.assertEqual(notif.notification_type, "lead_called_back")
        self.assertEqual(notif.record_id, record.id)
        self.assertIsNone(notif.read_at)
        self.assertIn("Raj", notif.message)
        self.assertEqual(InAppNotification.objects.filter(record_id=record.id).count(), 1)

    def test_skips_when_no_assignee(self):
        record = RecordFactory(data={"wati_chatbot_call_received": False})
        with patch("lead_notifications.service.broadcast_to_user") as broadcast:
            notify_lead_called_back(record)
        broadcast.assert_not_called()
        self.assertEqual(InAppNotification.objects.filter(record_id=record.id).count(), 0)

    def test_skips_when_realtime_broadcast_disabled(self):
        rm = UserFactory()
        record = RecordFactory(
            data={
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": False,
            },
        )
        before = InAppNotification.objects.count()
        with patch("lead_notifications.service.broadcast_to_user") as broadcast:
            with skip_realtime_broadcast():
                notify_lead_called_back(record)
        broadcast.assert_not_called()
        self.assertEqual(InAppNotification.objects.count(), before)


class LeadCalledBackSignalTests(TestCase):
    def test_post_save_notifies_on_flag_transition(self):
        rm = UserFactory()
        record = RecordFactory(
            data={
                "name": "Raj",
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": False,
            },
        )

        with patch("lead_notifications.service.broadcast_to_user") as broadcast:
            record.data = {**record.data, "wati_chatbot_call_received": True}
            record.save(update_fields=["data"])

        broadcast.assert_called_once()
        self.assertEqual(broadcast.call_args.args[0], rm.pk)
        self.assertEqual(InAppNotification.objects.filter(user_id=rm.supabase_uid).count(), 1)

    def test_post_save_does_not_notify_when_already_true(self):
        rm = UserFactory()
        record = RecordFactory(
            data={
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": True,
                "name": "Old",
            },
        )

        with patch("lead_notifications.service.broadcast_to_user") as broadcast:
            record.data = {**record.data, "name": "New"}
            record.save(update_fields=["data"])

        broadcast.assert_not_called()

    def test_post_save_ignores_support_ticket(self):
        record = RecordFactory(
            entity_type="support_ticket",
            data={"wati_chatbot_call_received": False},
        )

        with patch("lead_notifications.service.broadcast_to_user") as broadcast:
            record.data = {**record.data, "wati_chatbot_call_received": True}
            record.save(update_fields=["data"])

        broadcast.assert_not_called()

    def test_build_payload_shape(self):
        record = RecordFactory(
            data={
                "name": "Raj",
                "phone_number": "9876543210",
                "praja_id": "PRAJA123",
                "assigned_to": "rm-1",
            },
        )
        payload = build_lead_called_back_payload(record)
        self.assertEqual(payload["event"], "lead_called_back")
        self.assertEqual(payload["entity_type"], "lead")
        self.assertEqual(payload["assigned_to"], "rm-1")


class InAppNotificationApiTests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.unread = InAppNotification.objects.create(
            user_id=self.supabase_uid,
            notification_type="lead_called_back",
            title="WhatsApp call back",
            message="Sneha Jain called back (9876543210) · Praja ID: 1793876",
            record_id=123,
            tenant_id=self.tenant.id,
        )
        self.read = InAppNotification.objects.create(
            user_id=self.supabase_uid,
            notification_type="lead_called_back",
            title="WhatsApp call back",
            message="Old lead called back",
            record_id=456,
            tenant_id=self.tenant.id,
            read_at=timezone.now(),
        )
        InAppNotification.objects.create(
            user_id=str(uuid.uuid4()),
            notification_type="lead_called_back",
            title="Other user",
            message="Should not appear",
            tenant_id=self.tenant.id,
        )

    def test_list_returns_only_unread_for_current_user(self):
        response = self.client.get(
            reverse("lead_notifications:list"),
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["id"], self.unread.id)
        self.assertFalse(response.data["results"][0]["is_read"])

    def test_mark_read_sets_read_at_and_hides_from_list(self):
        response = self.client.post(
            reverse("lead_notifications:mark-read", kwargs={"pk": self.unread.id}),
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data["is_read"])
        self.assertIsNotNone(response.data["read_at"])

        self.unread.refresh_from_db()
        self.assertIsNotNone(self.unread.read_at)

        listed = self.client.get(
            reverse("lead_notifications:list"),
            **self.auth_headers,
        )
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.data["count"], 0)
