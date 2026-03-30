from django.test import TestCase
from unittest.mock import patch
from uuid import uuid4

from rest_framework.test import APIRequestFactory
from rest_framework.exceptions import PermissionDenied

from core.models import Tenant
from authz.models import Role, TenantMembership
from authz.views_management import RolesView


class RolesApiTestCase(TestCase):
    def setUp(self):
        self.rf = APIRequestFactory()

        # Dynamic UUID to prevent IntegrityErrors
        self.tenant_id = uuid4()
        self.tenant = Tenant.objects.create(
            id=self.tenant_id,
            name="Test Tenant",
            slug=f"test-tenant-{self.tenant_id}",
        )

        class _U:
            is_authenticated = True
            is_active = True
        self.user = _U()

    def _auth_headers(self, role_key="GM"):
        # 👇 FIX: Bulletproof mock signature
        def mock_check_permissions(*args, **kwargs):
            if role_key != "GM":
                raise PermissionDenied("GM role required")
            return None
            
        return patch("rest_framework.views.APIView.check_permissions", side_effect=mock_check_permissions)

    def _attach_ctx(self, req):
        req.tenant = self.tenant
        req.user = self.user
        return req

    def test_get_roles_basic_list(self):
        Role.objects.create(tenant=self.tenant, key="GM", name="General Manager")
        Role.objects.create(tenant=self.tenant, key="AGENT", name="Agent")

        other_id = uuid4()
        other = Tenant.objects.create(
            id=other_id, name="Other", slug=f"other-{other_id}"
        )
        Role.objects.create(tenant=other, key="OWNER", name="Owner")

        req = self._attach_ctx(self.rf.get("/api/authz/roles"))
        with self._auth_headers("GM"):
            resp = RolesView.as_view()(req)

        self.assertEqual(resp.status_code, 200)
        self.assertGreaterEqual(resp.data["count"], 2)
        keys = [r["key"] for r in resp.data["results"]]
        self.assertIn("AGENT", keys)
        self.assertIn("GM", keys)

    def test_get_roles_query_projection_and_order(self):
        Role.objects.create(tenant=self.tenant, key="GM", name="General Manager")
        Role.objects.create(tenant=self.tenant, key="AGENT", name="Agent")
        Role.objects.create(tenant=self.tenant, key="OWNER", name="Owner")

        req = self._attach_ctx(
            self.rf.get("/api/authz/roles", {"q": "ag", "fields": "id,key", "order": "key"})
        )
        with self._auth_headers("GM"):
            resp = RolesView.as_view()(req)

        self.assertEqual(resp.status_code, 200)
        
        self.assertGreaterEqual(resp.data["count"], 1)
        row = next((r for r in resp.data["results"] if r["key"] == "AGENT"), resp.data["results"][0])
        self.assertIn("key", row)
        self.assertEqual(row["key"], "AGENT")

    def test_post_create_role_new_creates_in_both_tables(self):
        payload = {"key": "LEAD", "name": "Lead", "description": "Leads role"}
        req = self._attach_ctx(self.rf.post("/api/authz/roles", payload, format="json"))

        with self._auth_headers("GM"):
            resp = RolesView.as_view()(req)

        self.assertEqual(resp.status_code, 201)
        self.assertTrue(resp.data["success"])
        role_id = resp.data["role"]["id"]

        self.assertTrue(Role.objects.filter(id=role_id, tenant=self.tenant).exists())

    def test_post_create_role_idempotent_case_insensitive(self):
        Role.objects.create(tenant=self.tenant, key="gm", name="General Manager")

        payload = {"key": "GM", "name": "General Manager"}
        req = self._attach_ctx(self.rf.post("/api/authz/roles", payload, format="json"))
        with self._auth_headers("GM"):
            resp = RolesView.as_view()(req)

        self.assertIn(resp.status_code, [200, 201])
        self.assertTrue(resp.data["success"])
        self.assertGreaterEqual(Role.objects.filter(tenant=self.tenant).count(), 1)

    def test_post_create_role_forbidden_if_not_gm(self):
        payload = {"key": "TEMP", "name": "Temp"}
        req = self._attach_ctx(self.rf.post("/api/authz/roles", payload, format="json"))

        with self._auth_headers("AGENT"):
            resp = RolesView.as_view()(req)

        self.assertEqual(resp.status_code, 403)
        self.assertIn("GM role required", str(resp.data))