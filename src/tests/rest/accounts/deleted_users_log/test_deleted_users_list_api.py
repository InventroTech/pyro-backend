"""GET /accounts/deleted-users/ lists only deletions recorded in the deleted-users log."""

import uuid
from datetime import timedelta
from unittest.mock import patch

from django.db import ProgrammingError
from django.utils import timezone
from rest_framework import status

from accounts.models import DeletedUserLog
from authz.models import TenantMembership
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory, TenantFactory, TenantMembershipFactory

LIST_URL = "/accounts/deleted-users/"
DELETE_URL = "/accounts/delete-user-with-log/"
REVOKE = "accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally"


class DeletedUsersListApiTests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.client.force_authenticate(user=self.user)
        self.rm_role = RoleFactory(tenant=self.tenant, key="rm", name="RM")
        self.manager = TenantMembershipFactory(
            tenant=self.tenant, role=self.rm_role, email="boss@example.com", name="Big Boss"
        )

    def log(self, email, deleted_at=None, tenant=None, **extra):
        return DeletedUserLog.objects.create(
            tenant=tenant or self.tenant,
            email=email,
            name=extra.pop("name", "Gone User"),
            role_id=self.rm_role.id,
            role_name="RM",
            manager_name=extra.pop("manager_name", "Big Boss"),
            manager_email=extra.pop("manager_email", "boss@example.com"),
            account_created_at=timezone.now() - timedelta(days=90),
            deleted_at=deleted_at or timezone.now(),
            deleted_by=extra.pop("deleted_by", "admin@praja.in"),
            **extra,
        )

    def legacy_deleted(self, email, deleted_at=None, **extra):
        membership = TenantMembershipFactory(tenant=self.tenant, role=self.rm_role, email=email, **extra)
        TenantMembership.all_objects.filter(id=membership.id).update(
            is_deleted=True, deleted_at=deleted_at or timezone.now(), is_active=False, user_id=None
        )
        return membership

    def get_list(self, headers=None):
        return self.client.get(LIST_URL, **(headers or self.auth_headers))

    def test_lists_logged_deletions_with_every_field(self):
        log = self.log("gone@example.com")

        response = self.get_list()

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        row = response.data["results"][0]
        self.assertEqual(row["id"], f"log-{log.id}")
        self.assertEqual(row["name"], "Gone User")
        self.assertEqual(row["email"], "gone@example.com")
        self.assertEqual(row["role_id"], str(self.rm_role.id))
        self.assertEqual(row["role_name"], "RM")
        self.assertEqual(row["manager_name"], "Big Boss")
        self.assertEqual(row["manager_email"], "boss@example.com")
        self.assertIsNotNone(row["created_at"])
        self.assertIsNotNone(row["deleted_at"])
        self.assertEqual(row["deleted_by"], "admin@praja.in")

    def test_rows_have_no_reason_field(self):
        self.log("gone@example.com")

        row = self.get_list().data["results"][0]

        self.assertNotIn("deletion_reason", row)
        self.assertNotIn("reason", row)

    def test_empty_manager_and_deleted_by_come_back_as_null(self):
        self.log("gone@example.com", manager_name="", manager_email="", deleted_by="")

        row = self.get_list().data["results"][0]

        self.assertIsNone(row["manager_name"])
        self.assertIsNone(row["manager_email"])
        self.assertIsNone(row["deleted_by"])

    def test_does_not_list_users_deleted_before_the_log_existed(self):
        self.legacy_deleted("old@example.com", name="Old User", user_parent_id=self.manager)

        self.assertEqual(self.get_list().data, {"count": 0, "results": []})

    def test_does_not_list_a_logged_user_twice(self):
        membership = self.legacy_deleted("gone@example.com")
        self.log("gone@example.com", membership_id=membership.id)

        response = self.get_list()

        self.assertEqual(response.data["count"], 1)
        self.assertTrue(response.data["results"][0]["id"].startswith("log-"))

    def test_newest_deletion_comes_first(self):
        now = timezone.now()
        self.log("log-old@example.com", deleted_at=now - timedelta(days=5))
        self.log("log-new@example.com", deleted_at=now)
        self.log("log-mid@example.com", deleted_at=now - timedelta(days=2))

        emails = [r["email"] for r in self.get_list().data["results"]]

        self.assertEqual(emails, ["log-new@example.com", "log-mid@example.com", "log-old@example.com"])

    def test_does_not_list_active_users(self):
        self.log("gone@example.com")

        emails = [r["email"] for r in self.get_list().data["results"]]

        self.assertNotIn("boss@example.com", emails)
        self.assertNotIn(self.email, emails)

    def test_returns_an_empty_list_when_nobody_was_deleted(self):
        self.assertEqual(self.get_list().data, {"count": 0, "results": []})

    def test_does_not_show_another_tenants_deleted_users(self):
        other_tenant = TenantFactory()
        self.log("theirs@example.com", tenant=other_tenant)
        theirs = TenantMembershipFactory(tenant=other_tenant, role=RoleFactory(tenant=other_tenant))
        TenantMembership.all_objects.filter(id=theirs.id).update(is_deleted=True, deleted_at=timezone.now())

        self.assertEqual(self.get_list().data["results"], [])

    def test_rejects_a_request_without_login(self):
        self.client.force_authenticate(user=None)
        response = self.client.get(LIST_URL, **self.make_unauthenticated_headers())

        self.assertIn(response.status_code, (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN))

    def test_only_get_is_allowed(self):
        for method in ("post", "put", "patch", "delete"):
            with self.subTest(method=method):
                response = getattr(self.client, method)(LIST_URL, {}, format="json", **self.auth_headers)
                self.assertEqual(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    @patch(REVOKE, return_value={"revoked": True})
    def test_a_deleted_user_appears_in_the_list(self, _revoke):
        subject = TenantMembershipFactory(
            tenant=self.tenant, role=self.rm_role, email="rm-leaving@example.com", user_id=uuid.uuid4()
        )

        self.client.delete(
            DELETE_URL,
            {"email": subject.email, "role_id": str(self.rm_role.id)},
            format="json",
            **self.auth_headers,
        )
        row = self.get_list().data["results"][0]

        self.assertEqual(row["email"], "rm-leaving@example.com")
        self.assertEqual(row["deleted_by"], self.email)

    def test_count_matches_the_logged_rows_and_ignores_older_deletions(self):
        self.log("logged-1@example.com")
        self.log("logged-2@example.com")
        self.legacy_deleted("old@example.com")

        data = self.get_list().data

        self.assertEqual(data["count"], 2)
        self.assertEqual({r["email"] for r in data["results"]}, {"logged-1@example.com", "logged-2@example.com"})

    def test_every_row_comes_from_the_log(self):
        self.log("logged@example.com")
        self.legacy_deleted("old@example.com")

        ids = [r["id"] for r in self.get_list().data["results"]]

        self.assertTrue(ids)
        self.assertTrue(all(row_id.startswith("log-") for row_id in ids))

    def test_returns_an_empty_list_when_the_log_cannot_be_read(self):
        self.legacy_deleted("old@example.com")
        missing_table = ProgrammingError('relation "accounts_deleted_user_log" does not exist')

        with patch.object(DeletedUserLog.objects, "filter", side_effect=missing_table):
            response = self.get_list()

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data, {"count": 0, "results": []})
