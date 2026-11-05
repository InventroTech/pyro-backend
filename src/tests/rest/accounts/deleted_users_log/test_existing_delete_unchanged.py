"""The existing DELETE /accounts/delete-user/ keeps working exactly as before and writes no log."""

import uuid
from unittest.mock import patch

from rest_framework import status

from accounts.models import DeletedUserLog
from authz.models import TenantMembership
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory, TenantMembershipFactory

REVOKE = "accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally"


@patch(REVOKE, return_value={"revoked": True})
class ExistingDeleteUnchangedTests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.rm_role = RoleFactory(tenant=self.tenant, key="rm", name="RM")
        self.subject = TenantMembershipFactory(
            tenant=self.tenant, role=self.rm_role, email="rm-leaving@example.com", user_id=uuid.uuid4()
        )

    def test_old_endpoint_still_deletes(self, _revoke):
        response = self.client.delete(
            "/accounts/delete-user/",
            {"email": self.subject.email, "role_id": str(self.rm_role.id)},
            format="json",
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(TenantMembership.all_objects.get(id=self.subject.id).is_deleted)

    def test_old_endpoint_writes_no_log_and_returns_no_logged_count(self, _revoke):
        response = self.client.delete(
            "/accounts/delete-user/",
            {"email": self.subject.email, "role_id": str(self.rm_role.id), "reason": "ignored"},
            format="json",
            **self.auth_headers,
        )

        self.assertNotIn("logged", response.data)
        self.assertFalse(DeletedUserLog.objects.exists())
