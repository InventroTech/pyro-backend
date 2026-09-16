import uuid
from unittest.mock import patch

import pytest
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.test import APIClient, APIRequestFactory, force_authenticate

import authz.service as authz_service
from notifications.models import InAppNotification
from notifications.service import (
    build_lead_called_back_payload,
    notify_lead_called_back,
    resolve_user_pk_for_assigned_to,
    should_notify_lead_called_back,
    was_call_received,
)
from notifications.signals import _previous_data_by_pk
from notifications.views import (
    InAppNotificationListView,
    InAppNotificationMarkReadView,
)
from realtime.broadcast import skip_realtime_broadcast
from tests.base.test_setup import BaseAPITestCase
from tests.factories import (
    RecordFactory,
    TenantFactory,
    TenantMembershipFactory,
    UserFactory,
)


class CallReceivedFlagTests(TestCase):
    def test_was_call_received_truthy_values(self):
        self.assertTrue(was_call_received({"wati_chatbot_call_received": True}))
        self.assertTrue(was_call_received({"wati_chatbot_call_received": "true"}))
        self.assertTrue(was_call_received({"wati_chatbot_call_received": "TRUE"}))
        self.assertTrue(was_call_received({"wati_chatbot_call_received": "1"}))
        self.assertTrue(was_call_received({"wati_chatbot_call_received": "yes"}))
        self.assertFalse(was_call_received({"wati_chatbot_call_received": False}))
        self.assertFalse(was_call_received({"wati_chatbot_call_received": "false"}))
        self.assertFalse(was_call_received({"wati_chatbot_call_received": "0"}))
        self.assertFalse(was_call_received({"wati_chatbot_call_received": None}))
        self.assertFalse(was_call_received({}))
        self.assertFalse(was_call_received(None))

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
        self.assertTrue(
            should_notify_lead_called_back(
                None,
                {"wati_chatbot_call_received": "true"},
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
                {"wati_chatbot_call_received": True},
                {"wati_chatbot_call_received": False},
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

    def test_resolves_email(self):
        user = UserFactory(email="rm@example.com", supabase_uid="rm-email-uid")
        tenant = TenantFactory()
        pk = resolve_user_pk_for_assigned_to(tenant, "rm@example.com")
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

    def test_resolves_membership_pk(self):
        uid = str(uuid.uuid4())
        user = UserFactory(supabase_uid=uid)
        record = RecordFactory()
        membership = TenantMembershipFactory(
            tenant=record.tenant,
            user_id=uid,
        )
        pk = resolve_user_pk_for_assigned_to(record.tenant, membership.id)
        self.assertEqual(pk, user.pk)

    def test_returns_none_for_empty_or_unknown(self):
        tenant = TenantFactory()
        self.assertIsNone(resolve_user_pk_for_assigned_to(tenant, None))
        self.assertIsNone(resolve_user_pk_for_assigned_to(tenant, ""))
        self.assertIsNone(resolve_user_pk_for_assigned_to(tenant, "null"))
        # Non-UUID / non-email string must not raise (membership.user_id is UUIDField).
        self.assertIsNone(resolve_user_pk_for_assigned_to(tenant, "missing-user"))
        self.assertIsNone(
            resolve_user_pk_for_assigned_to(tenant, "00000000-0000-0000-0000-000000000099")
        )


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

        with patch("notifications.service.broadcast_to_user") as broadcast:
            notify_lead_called_back(record)

        broadcast.assert_called_once()
        user_pk, payload = broadcast.call_args.args
        self.assertEqual(user_pk, rm.pk)
        self.assertEqual(payload["event"], "lead_called_back")
        self.assertEqual(payload["record_id"], str(record.id))
        self.assertEqual(payload["lead_name"], "Raj")
        self.assertNotIn("phone_number", payload)
        self.assertEqual(payload["praja_id"], "PRAJA123")
        self.assertTrue(payload["wati_chatbot_call_received"])
        self.assertIsNotNone(payload.get("notification_id"))

        notif = InAppNotification.objects.get(pk=payload["notification_id"])
        self.assertEqual(notif.user_id, rm.supabase_uid)
        self.assertEqual(notif.notification_type, "lead_called_back")
        self.assertEqual(notif.record_id, record.id)
        self.assertIsNone(notif.read_at)
        self.assertIn("Raj", notif.message)
        self.assertIn("PRAJA123", notif.message)
        self.assertNotIn("9876543210", notif.message)
        self.assertEqual(InAppNotification.objects.filter(record_id=record.id).count(), 1)

    def test_skips_when_no_assignee(self):
        record = RecordFactory(data={"wati_chatbot_call_received": False})
        with patch("notifications.service.broadcast_to_user") as broadcast:
            notify_lead_called_back(record)
        broadcast.assert_not_called()
        self.assertEqual(InAppNotification.objects.filter(record_id=record.id).count(), 0)

    def test_skips_when_assignee_unresolvable(self):
        record = RecordFactory(
            data={
                "assigned_to": "no-such-rm",
                "wati_chatbot_call_received": False,
            },
        )
        with patch("notifications.service.broadcast_to_user") as broadcast:
            notify_lead_called_back(record)
        broadcast.assert_not_called()
        self.assertEqual(InAppNotification.objects.filter(record_id=record.id).count(), 0)

    def test_skips_non_lead_entity(self):
        rm = UserFactory()
        record = RecordFactory(
            entity_type="support_ticket",
            data={
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": True,
            },
        )
        with patch("notifications.service.broadcast_to_user") as broadcast:
            notify_lead_called_back(record)
        broadcast.assert_not_called()

    def test_skips_when_realtime_broadcast_disabled(self):
        rm = UserFactory()
        record = RecordFactory(
            data={
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": False,
            },
        )
        before = InAppNotification.objects.count()
        with patch("notifications.service.broadcast_to_user") as broadcast:
            with skip_realtime_broadcast():
                notify_lead_called_back(record)
        broadcast.assert_not_called()
        self.assertEqual(InAppNotification.objects.count(), before)

    def test_message_without_praja_id(self):
        rm = UserFactory(supabase_uid="rm-no-praja")
        record = RecordFactory(
            data={
                "name": "Only Name",
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": False,
            },
        )
        with patch("notifications.service.broadcast_to_user") as broadcast:
            notify_lead_called_back(record)
        notif = InAppNotification.objects.get(pk=broadcast.call_args.args[1]["notification_id"])
        self.assertEqual(notif.message, "Only Name called back")


class LeadCalledBackSignalTests(TestCase):
    def tearDown(self):
        _previous_data_by_pk.clear()
        super().tearDown()

    def test_post_save_notifies_on_flag_transition(self):
        rm = UserFactory()
        record = RecordFactory(
            data={
                "name": "Raj",
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": False,
            },
        )

        with patch("notifications.service.broadcast_to_user") as broadcast:
            record.data = {**record.data, "wati_chatbot_call_received": True}
            record.save(update_fields=["data"])

        broadcast.assert_called_once()
        self.assertEqual(broadcast.call_args.args[0], rm.pk)
        self.assertEqual(InAppNotification.objects.filter(user_id=rm.supabase_uid).count(), 1)
        self.assertNotIn(record.pk, _previous_data_by_pk)

    def test_post_save_does_not_notify_when_already_true(self):
        rm = UserFactory()
        record = RecordFactory(
            data={
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": True,
                "name": "Old",
            },
        )

        with patch("notifications.service.broadcast_to_user") as broadcast:
            record.data = {**record.data, "name": "New"}
            record.save(update_fields=["data"])

        broadcast.assert_not_called()

    def test_post_save_does_not_notify_on_true_to_false(self):
        rm = UserFactory()
        record = RecordFactory(
            data={
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": True,
            },
        )

        with patch("notifications.service.broadcast_to_user") as broadcast:
            record.data = {**record.data, "wati_chatbot_call_received": False}
            record.save(update_fields=["data"])

        broadcast.assert_not_called()

    def test_create_with_flag_true_notifies(self):
        rm = UserFactory()
        with patch("notifications.service.broadcast_to_user") as broadcast:
            RecordFactory(
                data={
                    "name": "New Lead",
                    "assigned_to": rm.supabase_uid,
                    "wati_chatbot_call_received": True,
                },
            )
        broadcast.assert_called_once()

    def test_post_save_ignores_support_ticket(self):
        record = RecordFactory(
            entity_type="support_ticket",
            data={"wati_chatbot_call_received": False},
        )

        with patch("notifications.service.broadcast_to_user") as broadcast:
            record.data = {**record.data, "wati_chatbot_call_received": True}
            record.save(update_fields=["data"])

        broadcast.assert_not_called()

    def test_non_lead_save_does_not_leak_signal_cache(self):
        record = RecordFactory(
            entity_type="support_ticket",
            data={"wati_chatbot_call_received": False},
        )
        record.data = {**record.data, "wati_chatbot_call_received": True}
        record.save(update_fields=["data"])

        self.assertNotIn(record.pk, _previous_data_by_pk)

    def test_lead_save_without_notify_still_clears_cache(self):
        rm = UserFactory()
        record = RecordFactory(
            data={
                "assigned_to": rm.supabase_uid,
                "wati_chatbot_call_received": False,
                "name": "A",
            },
        )
        record.data = {**record.data, "name": "B"}
        record.save(update_fields=["data"])
        self.assertNotIn(record.pk, _previous_data_by_pk)

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
        self.assertNotIn("phone_number", payload)


class InAppNotificationModelTests(TestCase):
    def test_mark_read_is_idempotent(self):
        notif = InAppNotification.objects.create(
            user_id="uid-1",
            notification_type="lead_called_back",
            title="WhatsApp call back",
            message="Lead called back",
        )
        self.assertFalse(notif.is_read)
        notif.mark_read()
        first_read_at = notif.read_at
        self.assertTrue(notif.is_read)
        notif.mark_read()
        self.assertEqual(notif.read_at, first_read_at)


class InAppNotificationApiTests(BaseAPITestCase):
    """
    Exercise list/mark-read views without HTTP JWT/tenant middleware.
    CI can return 403 when Bearer auth / tenant resolution disagree across jobs.
    """

    def setUp(self):
        super().setUp()
        authz_service._CACHE.clear()
        self.factory = APIRequestFactory()
        self.unread = InAppNotification.objects.create(
            user_id=self.supabase_uid,
            notification_type="lead_called_back",
            title="WhatsApp call back",
            message="Sneha Jain called back · Praja ID: 1793876",
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
        self.other_user_notif = InAppNotification.objects.create(
            user_id=str(uuid.uuid4()),
            notification_type="lead_called_back",
            title="Other user",
            message="Should not appear",
            tenant_id=self.tenant.id,
        )

    def _auth_request(self, method: str, path: str):
        request = getattr(self.factory, method)(path)
        request.tenant = self.tenant
        force_authenticate(request, user=self.user)
        return request

    def test_list_returns_only_unread_for_current_user(self):
        request = self._auth_request("get", "/notifications/")
        response = InAppNotificationListView.as_view()(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["id"], self.unread.id)
        self.assertFalse(response.data["results"][0]["is_read"])

    def test_list_include_read_returns_read_and_unread(self):
        request = self._auth_request("get", "/notifications/?include_read=true")
        response = InAppNotificationListView.as_view()(request)

        self.assertEqual(response.status_code, 200)
        ids = {row["id"] for row in response.data["results"]}
        self.assertEqual(ids, {self.unread.id, self.read.id})

    def test_mark_read_sets_read_at_and_hides_from_list(self):
        request = self._auth_request("post", f"/notifications/{self.unread.id}/read/")
        response = InAppNotificationMarkReadView.as_view()(request, pk=self.unread.id)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data["is_read"])
        self.assertIsNotNone(response.data["read_at"])

        self.unread.refresh_from_db()
        self.assertIsNotNone(self.unread.read_at)

        list_request = self._auth_request("get", "/notifications/")
        listed = InAppNotificationListView.as_view()(list_request)
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.data["count"], 0)

    def test_mark_read_idempotent(self):
        first = InAppNotificationMarkReadView.as_view()(
            self._auth_request("post", f"/notifications/{self.unread.id}/read/"),
            pk=self.unread.id,
        )
        second = InAppNotificationMarkReadView.as_view()(
            self._auth_request("post", f"/notifications/{self.unread.id}/read/"),
            pk=self.unread.id,
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.data["read_at"], second.data["read_at"])

    def test_mark_read_other_users_notification_returns_404(self):
        request = self._auth_request(
            "post",
            f"/notifications/{self.other_user_notif.id}/read/",
        )
        response = InAppNotificationMarkReadView.as_view()(
            request,
            pk=self.other_user_notif.id,
        )
        self.assertEqual(response.status_code, 404)
        self.other_user_notif.refresh_from_db()
        self.assertIsNone(self.other_user_notif.read_at)


@pytest.mark.django_db
@override_settings(
    PYRO_SECRET="test-pyro-secret-notifications",
    DEFAULT_TENANT_SLUG="notif-entity-tenant",
)
class PrajaEntityCallReceivedNotificationTests(TestCase):
    """PATCH /entity/ with wati_chatbot_call_received should merge into data and notify."""

    def setUp(self):
        self.tenant = TenantFactory(slug="notif-entity-tenant")
        self.rm = UserFactory(supabase_uid="praja-rm-uid")
        self.client = APIClient()
        self.entity_url = "/entity/"
        self.headers = {"HTTP_X_SECRET_PYRO": "test-pyro-secret-notifications"}
        self.record = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "praja_id": "PRAJA_CALL_1",
                "name": "Callback Lead",
                "assigned_to": self.rm.supabase_uid,
                "wati_chatbot_call_received": False,
            },
        )

    def test_patch_root_flag_merges_and_notifies_assigned_rm(self):
        with patch("notifications.service.broadcast_to_user") as broadcast:
            response = self.client.patch(
                f"{self.entity_url}?praja_id=PRAJA_CALL_1",
                {"wati_chatbot_call_received": True},
                format="json",
                **self.headers,
            )

        self.assertEqual(response.status_code, 200)
        self.record.refresh_from_db()
        self.assertTrue(self.record.data.get("wati_chatbot_call_received"))
        self.assertEqual(self.record.data.get("name"), "Callback Lead")
        self.assertEqual(self.record.data.get("assigned_to"), self.rm.supabase_uid)

        broadcast.assert_called_once()
        self.assertEqual(broadcast.call_args.args[0], self.rm.pk)
        self.assertEqual(
            InAppNotification.objects.filter(user_id=self.rm.supabase_uid).count(),
            1,
        )

    def test_patch_already_true_does_not_renotify(self):
        self.record.data = {**self.record.data, "wati_chatbot_call_received": True}
        self.record.save(update_fields=["data"])
        InAppNotification.objects.filter(user_id=self.rm.supabase_uid).delete()

        with patch("notifications.service.broadcast_to_user") as broadcast:
            response = self.client.patch(
                f"{self.entity_url}?praja_id=PRAJA_CALL_1",
                {"wati_chatbot_call_received": True, "name": "Still True"},
                format="json",
                **self.headers,
            )

        self.assertEqual(response.status_code, 200)
        broadcast.assert_not_called()
        self.assertEqual(InAppNotification.objects.filter(user_id=self.rm.supabase_uid).count(), 0)
