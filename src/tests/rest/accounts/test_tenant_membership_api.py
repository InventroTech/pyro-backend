"""
Tests for tenant membership user create/update endpoints (formerly legacy URLs).

- URL names: tenant-membership-create, tenant-membership-update
- Legacy route names must not be registered.

Run:
  pytest src/tests/rest/accounts/test_tenant_membership_api.py -v
"""

import uuid

from django.urls import NoReverseMatch, reverse
from django.test import TestCase

from authz import service as authz_service
from authz.models import TenantMembership
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory


class TenantMembershipUrlRoutingTests(TestCase):
    """URL reversing for new routes; legacy names removed."""

    def test_reverse_create_and_update_urls(self):
        self.assertEqual(
            reverse("tenant-membership-create"),
            "/accounts/users/create/",
        )
        self.assertEqual(
            reverse("tenant-membership-update"),
            "/accounts/users/update/",
        )

    def test_legacy_url_names_not_registered(self):
        with self.assertRaises(NoReverseMatch):
            reverse("legacy-user-create")
        with self.assertRaises(NoReverseMatch):
            reverse("legacy-user-update")


class TenantMembershipCreateAPITests(BaseAPITestCase):
    """POST /accounts/users/create/"""

    def setUp(self):
        super().setUp()
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)
        self.url = "/accounts/users/create/"
        self.target_role = RoleFactory(
            tenant=self.tenant,
            key=f"rm-{uuid.uuid4().hex[:8]}",
            name="Relationship Manager",
        )

    def test_create_requires_role_id(self):
        resp = self.client.post(
            self.url,
            {
                "name": "No Role User",
                "email": f"noid-{uuid.uuid4().hex[:8]}@example.com",
            },
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(resp.status_code, 400)
        err = (resp.data or {}).get("error", "") if isinstance(resp.data, dict) else str(resp.data)
        self.assertIn("role_id", err.lower())

    def test_create_membership_success(self):
        email = f"new-{uuid.uuid4().hex[:8]}@example.com"
        resp = self.client.post(
            self.url,
            {
                "name": "New Member",
                "email": email,
                "role_id": str(self.target_role.id),
            },
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(resp.status_code, 201, resp.data)
        self.assertTrue(TenantMembership.objects.filter(tenant=self.tenant, email=email).exists())


class TenantMembershipUpdateAPITests(BaseAPITestCase):
    """POST /accounts/users/update/"""

    def setUp(self):
        super().setUp()
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)
        self.url = "/accounts/users/update/"
        self.role = RoleFactory(
            tenant=self.tenant,
            key=f"staff-{uuid.uuid4().hex[:8]}",
            name="Staff",
        )
        self.subject = TenantMembership.objects.create(
            tenant=self.tenant,
            email=f"subject-{uuid.uuid4().hex[:8]}@example.com",
            role=self.role,
            user_id=uuid.uuid4(),
            name="Before",
            is_active=True,
        )

    def test_update_membership_name(self):
        resp = self.client.post(
            self.url,
            {
                "name": "After Name",
                "email": self.subject.email,
                "role_id": str(self.role.id),
                "original_email": self.subject.email,
                "original_role_id": str(self.role.id),
            },
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(resp.status_code, 200, resp.data)
        self.subject.refresh_from_db()
        self.assertEqual(self.subject.name, "After Name")
