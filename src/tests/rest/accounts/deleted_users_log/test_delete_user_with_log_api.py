"""DELETE /accounts/delete-user-with-log/ deletes like before and records who, when and why."""

import uuid
from types import SimpleNamespace
from unittest.mock import patch

from django.db import ProgrammingError
from django.test import SimpleTestCase
from rest_framework import status

from accounts.deleted_users_log import _actor
from accounts.models import DeletedUserLog
from authz.models import TenantMembership
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory, TenantFactory, TenantMembershipFactory

URL = "/accounts/delete-user-with-log/"
REVOKE = "accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally"


@patch(REVOKE, return_value={"revoked": True})
class DeleteUserWithLogApiTests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.client.force_authenticate(user=self.user)
        self.rm_role = RoleFactory(tenant=self.tenant, key="rm", name="RM")
        self.manager = TenantMembershipFactory(
            tenant=self.tenant, role=self.rm_role, email="boss@example.com", name="Big Boss"
        )
        self.subject = TenantMembershipFactory(
            tenant=self.tenant,
            role=self.rm_role,
            email="rm-leaving@example.com",
            name="Leaving RM",
            user_id=uuid.uuid4(),
            user_parent_id=self.manager,
        )

    def delete_subject(self, **extra):
        body = {"email": self.subject.email, "role_id": str(self.rm_role.id), **extra}
        return self.client.delete(URL, body, format="json", **self.auth_headers)

    def only_log(self):
        return DeletedUserLog.objects.get(tenant=self.tenant)

    def test_deletes_the_user_like_the_existing_endpoint(self, _revoke):
        response = self.delete_subject()

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(response.data["success"])
        self.assertEqual(response.data["deleted"]["tenant_memberships"], 1)
        self.assertFalse(TenantMembership.objects.filter(id=self.subject.id).exists())
        self.assertTrue(TenantMembership.all_objects.get(id=self.subject.id).is_deleted)

    def test_writes_one_log_row(self, _revoke):
        response = self.delete_subject()

        self.assertEqual(response.data["logged"], 1)
        self.assertEqual(DeletedUserLog.objects.count(), 1)

    def test_log_snapshots_the_user_details(self, _revoke):
        self.delete_subject()

        log = self.only_log()
        self.assertEqual(log.membership_id, self.subject.id)
        self.assertEqual(log.name, "Leaving RM")
        self.assertEqual(log.email, "rm-leaving@example.com")
        self.assertEqual(log.role_id, self.rm_role.id)
        self.assertEqual(log.role_name, "RM")
        self.assertEqual(log.manager_name, "Big Boss")
        self.assertEqual(log.manager_email, "boss@example.com")
        self.assertEqual(log.account_created_at, self.subject.created_at)

    def test_log_records_who_deleted_and_when(self, _revoke):
        self.delete_subject()

        log = self.only_log()
        archived = TenantMembership.all_objects.get(id=self.subject.id)
        self.assertEqual(log.deleted_by, self.email)
        self.assertEqual(log.deleted_at, archived.deleted_at)

    def test_a_reason_sent_by_an_older_client_is_ignored(self, _revoke):
        response = self.delete_subject(reason="Left the company")

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["logged"], 1)
        self.assertFalse(hasattr(self.only_log(), "reason"))

    def test_rejects_a_request_without_user_details(self, _revoke):
        response = self.client.delete(URL, {}, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertFalse(DeletedUserLog.objects.exists())

    def test_rejects_a_request_without_login(self, _revoke):
        self.client.force_authenticate(user=None)
        response = self.client.delete(
            URL,
            {"email": self.subject.email, "role_id": str(self.rm_role.id)},
            format="json",
            **self.make_unauthenticated_headers(),
        )

        self.assertIn(response.status_code, (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN))
        self.assertTrue(TenantMembership.objects.filter(id=self.subject.id).exists())
        self.assertFalse(DeletedUserLog.objects.exists())

    def test_logs_nothing_for_an_unknown_user(self, _revoke):
        response = self.client.delete(
            URL, {"email": "nobody@example.com", "role_id": str(self.rm_role.id)}, format="json", **self.auth_headers
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["logged"], 0)
        self.assertFalse(DeletedUserLog.objects.exists())

    def test_logs_nothing_and_keeps_the_user_when_the_delete_fails(self, _revoke):
        with patch(
            "accounts.deleted_users_log.delete_user_everywhere", side_effect=RuntimeError("database went away")
        ):
            response = self.delete_subject()

        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        self.assertFalse(DeletedUserLog.objects.exists())
        self.assertTrue(TenantMembership.objects.filter(id=self.subject.id).exists())

    def test_does_not_touch_other_users(self, _revoke):
        other = TenantMembershipFactory(tenant=self.tenant, role=self.rm_role, email="stays@example.com")

        self.delete_subject()

        self.assertTrue(TenantMembership.objects.filter(id=other.id).exists())
        self.assertFalse(DeletedUserLog.objects.filter(email="stays@example.com").exists())

    def test_does_not_touch_the_same_email_in_another_tenant(self, _revoke):
        other_tenant = TenantFactory()
        theirs = TenantMembershipFactory(
            tenant=other_tenant, role=RoleFactory(tenant=other_tenant), email=self.subject.email
        )

        self.delete_subject()

        self.assertTrue(TenantMembership.objects.filter(id=theirs.id).exists())
        self.assertFalse(DeletedUserLog.objects.filter(tenant=other_tenant).exists())

    def test_user_without_a_manager_logs_an_empty_manager(self, _revoke):
        TenantMembership.objects.filter(id=self.subject.id).update(user_parent_id=None)

        self.delete_subject()

        log = self.only_log()
        self.assertEqual(log.manager_name, "")
        self.assertEqual(log.manager_email, "")

    def test_deleting_twice_logs_once(self, _revoke):
        first = self.delete_subject()
        second = self.delete_subject()

        self.assertEqual(first.data["logged"], 1)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual(second.data["logged"], 0)
        self.assertEqual(DeletedUserLog.objects.count(), 1)

    def test_only_delete_is_allowed(self, _revoke):
        for method in ("get", "post", "put", "patch"):
            with self.subTest(method=method):
                response = getattr(self.client, method)(URL, {}, format="json", **self.auth_headers)
                self.assertEqual(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_deletes_by_uid_and_logs_it(self, _revoke):
        response = self.client.delete(URL, {"uid": str(self.subject.user_id)}, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["matched_by"], "uid")
        self.assertEqual(response.data["logged"], 1)
        self.assertEqual(self.only_log().email, "rm-leaving@example.com")

    def test_logs_every_membership_removed_for_the_same_email(self, _revoke):
        gm_role = RoleFactory(tenant=self.tenant, key="gm", name="GM")
        second = TenantMembershipFactory(tenant=self.tenant, role=gm_role, email=self.subject.email, user_id=None)

        response = self.delete_subject()

        self.assertEqual(response.data["deleted"]["tenant_memberships"], 2)
        self.assertEqual(response.data["logged"], 2)
        self.assertEqual(
            set(DeletedUserLog.objects.values_list("membership_id", "role_name")),
            {(self.subject.id, "RM"), (second.id, "GM")},
        )

    def test_deleting_a_manager_moves_their_reports_up_and_logs_only_the_manager(self, _revoke):
        response = self.client.delete(
            URL, {"email": self.manager.email, "role_id": str(self.rm_role.id)}, format="json", **self.auth_headers
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["reports_reassigned"], 1)
        self.subject.refresh_from_db()
        self.assertIsNone(self.subject.user_parent_id)
        self.assertEqual(list(DeletedUserLog.objects.values_list("email", flat=True)), [self.manager.email])

    def test_a_reports_log_keeps_the_manager_it_had_when_deleted(self, _revoke):
        self.delete_subject()
        TenantMembership.objects.filter(id=self.manager.id).update(name="Renamed Boss")

        self.assertEqual(self.only_log().manager_name, "Big Boss")

    def test_rejects_an_invalid_role_id_and_deletes_nothing(self, _revoke):
        response = self.client.delete(
            URL, {"email": self.subject.email, "role_id": "not-a-uuid"}, format="json", **self.auth_headers
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertTrue(TenantMembership.objects.filter(id=self.subject.id).exists())
        self.assertFalse(DeletedUserLog.objects.exists())

    def test_deletes_the_user_even_when_the_log_cannot_be_saved(self, _revoke):
        missing_table = ProgrammingError('relation "accounts_deleted_user_log" does not exist')
        with patch.object(DeletedUserLog.objects, "bulk_create", side_effect=missing_table):
            response = self.delete_subject()

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(response.data["success"])
        self.assertEqual(response.data["logged"], 0)
        self.assertFalse(TenantMembership.objects.filter(id=self.subject.id).exists())
        self.assertTrue(TenantMembership.all_objects.get(id=self.subject.id).is_deleted)


class ActorTests(SimpleTestCase):
    """deleted_by is the admin's email, or their user id when the account has no email."""

    def test_uses_the_email(self):
        request = SimpleNamespace(user=SimpleNamespace(email="admin@praja.in", supabase_uid="uid-1"))
        self.assertEqual(_actor(request), "admin@praja.in")

    def test_falls_back_to_the_user_id(self):
        request = SimpleNamespace(user=SimpleNamespace(email="", supabase_uid="uid-1"))
        self.assertEqual(_actor(request), "uid-1")

    def test_is_empty_without_a_user(self):
        self.assertEqual(_actor(SimpleNamespace(user=None)), "")
