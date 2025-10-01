from django.test import TestCase
from core.models import Tenant
from authz.models import Role
from accounts.models import LegacyRole
from authz.service import create_or_sync_role


class CreateOrSyncRoleServiceTestCase(TestCase):
    def setUp(self):
        self.tenant = Tenant.objects.create(
            id="550e8400-e29b-41d4-a716-446655440000",
            name="Test Tenant",
            slug="test-tenant",
        )

    def test_create_new_role_mirrors_legacy(self):
        out = create_or_sync_role(self.tenant, key="QA", name="Quality Analyst", description="d")
        self.assertTrue(out["created"])
        rid = out["role"]["id"]

        self.assertTrue(Role.objects.filter(id=rid, tenant=self.tenant).exists())
        self.assertTrue(LegacyRole.objects.filter(id=rid, tenant=self.tenant).exists())

    def test_idempotent_case_insensitive(self):
        create_or_sync_role(self.tenant, key="gm", name="General Manager")
        out = create_or_sync_role(self.tenant, key="GM", name="General Manager")

        self.assertFalse(out["created"])
        self.assertEqual(Role.objects.filter(tenant=self.tenant).count(), 1)
