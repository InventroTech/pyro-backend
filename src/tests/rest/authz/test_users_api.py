from django.test import TestCase
from unittest.mock import patch

from rest_framework.test import APIRequestFactory

from core.models import Tenant
from authz.models import Role, TenantMembership
from authz.views_management import ListTenantUsersView


class ListTenantUsersApiTestCase(TestCase):
    def setUp(self):
        self.rf = APIRequestFactory()
        self.tenant = Tenant.objects.create(
            id="550e8400-e29b-41d4-a716-446655440000",
            name="Test Tenant",
            slug="test-tenant",
        )

        # Roles
        self.role_gm = Role.objects.create(tenant=self.tenant, key="GM", name="General Manager")
        self.role_agent = Role.objects.create(tenant=self.tenant, key="AGENT", name="Agent")

        # Memberships in this tenant
        TenantMembership.objects.create(
            tenant=self.tenant, email="gm@example.com", role=self.role_gm, is_active=True
        )
        TenantMembership.objects.create(
            tenant=self.tenant, email="agent1@example.com", role=self.role_agent, is_active=True
        )
        TenantMembership.objects.create(
            tenant=self.tenant, email="agent2@example.com", role=self.role_agent, is_active=False
        )

        # Another tenant data should be excluded
        other = Tenant.objects.create(
            id="660e8400-e29b-41d4-a716-446655440000", name="Other", slug="other"
        )
        other_role = Role.objects.create(tenant=other, key="AGENT", name="Agent")
        TenantMembership.objects.create(
            tenant=other, email="other@example.com", role=other_role, is_active=True
        )

        class _U:
            is_authenticated = True
        self.user = _U()

    def _attach_ctx(self, req):
        req.tenant = self.tenant
        req.user = self.user
        return req

    def test_list_users_requires_gm(self):
        req = self._attach_ctx(self.rf.get("/api/authz/users"))

        # Not GM → 403
        with patch(
            "authz.permissions._get_membership_info",
            return_value={"role_key": "AGENT", "perm_keys": []},
        ):
            resp = ListTenantUsersView.as_view()(req)
        self.assertEqual(resp.status_code, 403)

        # GM → 200 and only current-tenant memberships
        with patch(
            "authz.permissions._get_membership_info",
            return_value={"role_key": "GM", "perm_keys": []},
        ):
            resp = ListTenantUsersView.as_view()(req)

        self.assertEqual(resp.status_code, 200)
        emails = sorted([r["email"] for r in resp.data["results"]])
        self.assertEqual(emails, ["agent1@example.com", "agent2@example.com", "gm@example.com"])

        # Verify serializer shape includes role sub-object
        any_row = resp.data["results"][0]
        self.assertIn("email", any_row)
        self.assertIn("is_active", any_row)
        self.assertIn("role", any_row)
        self.assertIn("key", any_row["role"])
        self.assertIn("name", any_row["role"])
