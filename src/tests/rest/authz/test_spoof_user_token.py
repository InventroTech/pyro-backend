from django.test import TestCase, override_settings
from unittest.mock import patch
from rest_framework.test import APIRequestFactory

from core.models import Tenant
from authz.models import Role, TenantMembership
from authz.views_management import SpoofTenantUserTokenView


class SpoofTenantUserTokenApiTestCase(TestCase):
    def setUp(self):
        self.factory = APIRequestFactory()

        # Tenant and roles
        self.tenant = Tenant.objects.create(
            id="550e8400-e29b-41d4-a716-446655440000",
            name="Test Tenant",
            slug="test-tenant",
        )
        self.role_gm = Role.objects.create(tenant=self.tenant, key="GM", name="General Manager")
        self.role_agent = Role.objects.create(tenant=self.tenant, key="AGENT", name="Agent")

        # Acting GM membership (requesting user)
        self.gm_membership = TenantMembership.objects.create(
            tenant=self.tenant,
            email="gm@example.com",
            role=self.role_gm,
            is_active=True,
            user_id="11111111-1111-1111-1111-111111111111",
        )

        # Target agent membership to spoof
        self.agent_membership = TenantMembership.objects.create(
            tenant=self.tenant,
            email="agent@example.com",
            role=self.role_agent,
            is_active=True,
            user_id="22222222-2222-2222-2222-222222222222",
        )

        # Helper "user" object that mimics request.user used in permissions helper
        class _U:
            # Minimal user stub that satisfies DRF's authentication checks
            is_authenticated = True
            is_active = True

            def __init__(self, supabase_uid):
                self.supabase_uid = supabase_uid

        self.user = _U(supabase_uid=self.gm_membership.user_id)

    def _attach_ctx(self, req):
        req.tenant = self.tenant
        req.user = self.user
        return req

    @override_settings(SUPABASE_JWT_SECRET="test-secret-key")
    def test_spoof_token_happy_path_returns_jwt_and_metadata(self):
        req = self._attach_ctx(self.factory.post("/api/authz/users/agent/spoof-token/"))

        with patch(
            "authz.permissions._get_membership_info",
            return_value={"role_key": "GM", "perm_keys": ["users:spoof"]},
        ):
            response = SpoofTenantUserTokenView.as_view()(req, membership_id=self.agent_membership.id)

        self.assertEqual(response.status_code, 200)
        body = response.data

        # Basic shape checks
        self.assertIn("token", body)
        self.assertTrue(body["token"])
        self.assertEqual(body["membership_id"], self.agent_membership.id)
        self.assertEqual(body["email"], self.agent_membership.email)
        self.assertEqual(body["tenant_id"], str(self.tenant.id))

        # Audit metadata presence
        self.assertIn("audit", body)
        audit = body["audit"]
        self.assertEqual(audit["actor_membership_id"], str(self.gm_membership.id))
        self.assertEqual(audit["target_membership_id"], str(self.agent_membership.id))

    @override_settings(SUPABASE_JWT_SECRET="test-secret-key")
    def test_cannot_spoof_own_membership(self):
        req = self._attach_ctx(self.factory.post("/api/authz/users/gm/spoof-token/"))

        with patch(
            "authz.permissions._get_membership_info",
            return_value={"role_key": "GM", "perm_keys": ["users:spoof"]},
        ):
            response = SpoofTenantUserTokenView.as_view()(req, membership_id=self.gm_membership.id)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data.get("error"), "Cannot spoof your own membership")

    @override_settings(SUPABASE_JWT_SECRET="test-secret-key")
    def test_spoof_target_must_be_in_tenant_and_active(self):
        # Membership id that does not exist for this tenant
        req = self._attach_ctx(self.factory.post("/api/authz/users/999/spoof-token/"))

        with patch(
            "authz.permissions._get_membership_info",
            return_value={"role_key": "GM", "perm_keys": ["users:spoof"]},
        ):
            response = SpoofTenantUserTokenView.as_view()(req, membership_id=999999)

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.data.get("error"), "User membership not found for this tenant")

        # Inactive membership in same tenant should also not be spoofable
        inactive = TenantMembership.objects.create(
            tenant=self.tenant,
            email="inactive@example.com",
            role=self.role_agent,
            is_active=False,
            user_id="33333333-3333-3333-3333-333333333333",
        )
        req2 = self._attach_ctx(self.factory.post("/api/authz/users/inactive/spoof-token/"))
        with patch(
            "authz.permissions._get_membership_info",
            return_value={"role_key": "GM", "perm_keys": ["users:spoof"]},
        ):
            response2 = SpoofTenantUserTokenView.as_view()(req2, membership_id=inactive.id)

        self.assertEqual(response2.status_code, 404)
        self.assertEqual(response2.data.get("error"), "User membership not found for this tenant")

    @override_settings(SUPABASE_JWT_SECRET=None)
    def test_missing_supabase_jwt_secret_returns_500(self):
        req = self._attach_ctx(self.factory.post("/api/authz/users/agent/spoof-token/"))

        with patch(
            "authz.permissions._get_membership_info",
            return_value={"role_key": "GM", "perm_keys": ["users:spoof"]},
        ):
            response = SpoofTenantUserTokenView.as_view()(req, membership_id=self.agent_membership.id)

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.data.get("error"), "SUPABASE_JWT_SECRET is not configured")

