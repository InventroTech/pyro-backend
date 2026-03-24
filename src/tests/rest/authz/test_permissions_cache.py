from django.test import TestCase

from authz import service as authz_service
from authz.models import Role, TenantMembership
from core.models import Tenant


class PermissionsCacheTestCase(TestCase):
    def setUp(self):
        authz_service._CACHE.clear()
        self.tenant = Tenant.objects.create(
            id="550e8400-e29b-41d4-a716-446655440000",
            name="Test Tenant",
            slug="test-tenant-cache",
        )
        self.role = Role.objects.create(
            tenant=self.tenant,
            key="AGENT",
            name="Agent",
        )

    def tearDown(self):
        authz_service._CACHE.clear()

    def test_drop_permissions_cache_clears_when_tenant_object_passed(self):
        """
        Cache key is built from tenant.id in get_effective_permissions.
        Invalidation should also work when caller passes a tenant object.
        """
        uid = "123e4567-e89b-12d3-a456-426614174000"
        TenantMembership.objects.create(
            tenant=self.tenant,
            email="cache-user@example.com",
            user_id=uid,
            role=self.role,
            is_active=True,
        )

        authz_service.get_effective_permissions(uid, self.tenant)
        key = authz_service._cache_key(uid, self.tenant.id)
        self.assertIn(key, authz_service._CACHE)

        # Pass tenant object (not tenant.id) to ensure normalized invalidation works.
        authz_service.drop_permissions_cache(uid, self.tenant)
        self.assertNotIn(key, authz_service._CACHE)

    def test_no_membership_result_is_not_cached(self):
        """
        No-membership should not be cached to avoid temporary 403
        after signup/link races.
        """
        uid = "123e4567-e89b-12d3-a456-426614174999"
        result = authz_service.get_effective_permissions(uid, self.tenant)

        self.assertEqual(result["role_key"], None)
        self.assertEqual(result["perm_keys"], set())
        self.assertEqual(authz_service._CACHE, {})

