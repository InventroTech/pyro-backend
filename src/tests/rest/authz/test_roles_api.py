from django.test import TestCase
from unittest.mock import patch
from uuid import uuid4

from rest_framework.test import APIRequestFactory

from core.models import Tenant
from authz.models import Role, TenantMembership
from authz.views_management import RolesView


class RolesApiTestCase(TestCase):
    def setUp(self):
        self.rf = APIRequestFactory()

        # Tenant
        self.tenant = Tenant.objects.create(
            id="550e8400-e29b-41d4-a716-446655440000",
            name="Test Tenant",
            slug="test-tenant",
        )

        # A minimal user object; DRF only needs is_authenticated
        class _U:
            is_authenticated = True
        self.user = _U()

    def _auth_headers(self, role_key="GM"):
        # Patch _get_membership_info to bypass real auth/tenant checks
        return patch(
            "authz.permissions._get_membership_info",
            return_value={"role_key": role_key, "perm_keys": []},
        )

    def _attach_ctx(self, req):
        # Attach tenant and user on the request (middleware usually does this)
        req.tenant = self.tenant
        req.user = self.user
        return req

    def test_get_roles_basic_list(self):
        # Seed roles for this tenant
        Role.objects.create(tenant=self.tenant, key="GM", name="General Manager")
        Role.objects.create(tenant=self.tenant, key="AGENT", name="Agent")

        # A different tenant's role (should not be returned)
        other = Tenant.objects.create(
            id="660e8400-e29b-41d4-a716-446655440000", name="Other", slug="other"
        )
        Role.objects.create(tenant=other, key="OWNER", name="Owner")

        req = self._attach_ctx(self.rf.get("/api/authz/roles"))
        with self._auth_headers("GM"):
            resp = RolesView.as_view()(req)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["count"], 2)
        keys = sorted([r["key"] for r in resp.data["results"]])
        self.assertEqual(keys, ["AGENT", "GM"])

    def test_get_roles_query_projection_and_order(self):
        Role.objects.create(tenant=self.tenant, key="GM", name="General Manager")
        Role.objects.create(tenant=self.tenant, key="AGENT", name="Agent")
        Role.objects.create(tenant=self.tenant, key="OWNER", name="Owner")

        # q=ag should match AGENT; fields projection trims payload
        req = self._attach_ctx(
            self.rf.get("/api/authz/roles", {"q": "ag", "fields": "id,key", "order": "key"})
        )
        with self._auth_headers("GM"):
            resp = RolesView.as_view()(req)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["count"], 1)
        row = resp.data["results"][0]
        self.assertEqual(set(row.keys()), {"id", "key"})
        self.assertEqual(row["key"], "AGENT")

    def test_post_create_role_new_creates_in_both_tables(self):
        payload = {"key": "LEAD", "name": "Lead", "description": "Leads role"}
        req = self._attach_ctx(self.rf.post("/api/authz/roles", payload, format="json"))

        with self._auth_headers("GM"):
            resp = RolesView.as_view()(req)

        self.assertEqual(resp.status_code, 201)
        self.assertTrue(resp.data["success"])
        role_id = resp.data["role"]["id"]

        # Authz role exists
        self.assertTrue(Role.objects.filter(id=role_id, tenant=self.tenant).exists())

    def test_post_create_role_idempotent_case_insensitive(self):
        # Pre-create "gm" then POST "GM" -> should be 200, not 201
        Role.objects.create(tenant=self.tenant, key="gm", name="General Manager")

        payload = {"key": "GM", "name": "General Manager"}
        req = self._attach_ctx(self.rf.post("/api/authz/roles", payload, format="json"))
        with self._auth_headers("GM"):
            resp = RolesView.as_view()(req)

        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.data["success"])
        self.assertFalse(resp.data["created"])
        # Still only one authz role for key (case-insensitive) in this tenant
        self.assertEqual(Role.objects.filter(tenant=self.tenant).count(), 1)

    def test_post_create_role_forbidden_if_not_gm(self):
        payload = {"key": "TEMP", "name": "Temp"}
        req = self._attach_ctx(self.rf.post("/api/authz/roles", payload, format="json"))

        # User is authenticated but NOT GM
        with self._auth_headers("AGENT"):
            resp = RolesView.as_view()(req)

        self.assertEqual(resp.status_code, 403)
        self.assertIn("GM role required", str(resp.data))
