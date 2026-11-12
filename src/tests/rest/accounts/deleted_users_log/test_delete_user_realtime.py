"""A logged delete tells every open screen in the tenant to refresh the deleted-users list."""

import uuid
from unittest.mock import patch

from rest_framework import status

from authz.models import TenantMembership
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory, TenantMembershipFactory

URL = "/accounts/delete-user-with-log/"
REVOKE = "accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally"
BROADCAST = "accounts.views.broadcast_to_tenant"


@patch(REVOKE, return_value={"revoked": True})
class DeleteUserRealtimeTests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.client.force_authenticate(user=self.user)
        self.rm_role = RoleFactory(tenant=self.tenant, key="rm", name="RM")
        self.subject = TenantMembershipFactory(
            tenant=self.tenant, role=self.rm_role, email="rm-leaving@example.com", user_id=uuid.uuid4()
        )

    def delete(self, email=None):
        body = {"email": email or self.subject.email, "role_id": str(self.rm_role.id)}
        return self.client.delete(URL, body, format="json", **self.auth_headers)

    def test_broadcasts_to_the_tenant_after_the_delete_is_saved(self, _revoke):
        with patch(BROADCAST) as broadcast, self.captureOnCommitCallbacks(execute=True):
            response = self.delete()

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        broadcast.assert_called_once_with(
            self.tenant.id,
            {
                "event": "record_updated",
                "entity_type": "tenant_membership",
                "record_id": f"tenant_membership:{self.subject.id}",
                "created": False,
                "data": {"deleted": True},
            },
        )

    def test_does_not_broadcast_before_the_delete_is_saved(self, _revoke):
        with patch(BROADCAST) as broadcast, self.captureOnCommitCallbacks(execute=False) as callbacks:
            self.delete()

        broadcast.assert_not_called()
        self.assertEqual(len(callbacks), 1)

    def test_does_not_broadcast_when_nobody_was_deleted(self, _revoke):
        with patch(BROADCAST) as broadcast, self.captureOnCommitCallbacks(execute=True):
            response = self.delete(email="nobody@example.com")

        self.assertEqual(response.data["logged"], 0)
        broadcast.assert_not_called()

    def test_does_not_broadcast_when_the_delete_fails(self, _revoke):
        with patch(BROADCAST) as broadcast, patch(
            "accounts.views.delete_user_everywhere", side_effect=RuntimeError("db down")
        ), self.captureOnCommitCallbacks(execute=True):
            response = self.delete()

        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        broadcast.assert_not_called()

    def test_does_not_broadcast_for_a_rejected_request(self, _revoke):
        with patch(BROADCAST) as broadcast, self.captureOnCommitCallbacks(execute=True):
            response = self.client.delete(
                URL,
                {"email": self.subject.email, "role_id": "not-a-uuid"},
                format="json",
                **self.auth_headers,
            )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        broadcast.assert_not_called()

    def test_the_real_broadcast_does_not_break_the_delete(self, _revoke):
        with self.captureOnCommitCallbacks(execute=True):
            response = self.delete()

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(TenantMembership.all_objects.get(id=self.subject.id).is_deleted)
