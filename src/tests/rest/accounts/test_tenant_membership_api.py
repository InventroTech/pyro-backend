"""
Tests for tenant membership user create/update endpoints (formerly legacy URLs).

- URL names: tenant-membership-create, tenant-membership-update
- Legacy route names must not be registered.

Run:
  pytest src/tests/rest/accounts/test_tenant_membership_api.py -v
"""

import uuid
from unittest import mock

from django.db.utils import OperationalError
from django.urls import NoReverseMatch, reverse
from django.test import TestCase

from accounts import views as accounts_views
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

    def test_update_preserves_district_state_party_when_omitted(self):
        """Partial profile updates must not wipe geo KV settings."""
        from user_settings.models import TenantMemberSetting
        from user_settings.services import (
            USER_KV_DISTRICT_KEY,
            USER_KV_PARTY_KEY,
            USER_KV_STATE_KEY,
        )

        TenantMemberSetting.objects.create(
            tenant=self.tenant,
            tenant_membership=self.subject,
            key=USER_KV_STATE_KEY,
            value=17,
        )
        TenantMemberSetting.objects.create(
            tenant=self.tenant,
            tenant_membership=self.subject,
            key=USER_KV_DISTRICT_KEY,
            value=90290,
        )
        TenantMemberSetting.objects.create(
            tenant=self.tenant,
            tenant_membership=self.subject,
            key=USER_KV_PARTY_KEY,
            value=3,
        )

        resp = self.client.post(
            self.url,
            {
                "name": "Name Only",
                "email": self.subject.email,
                "role_id": str(self.role.id),
                "original_email": self.subject.email,
                "original_role_id": str(self.role.id),
            },
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(resp.status_code, 200, resp.data)
        self.assertEqual(resp.data["state"], 17)
        self.assertEqual(resp.data["district"], 90290)
        self.assertEqual(resp.data["party"], 3)

        by_key = {
            row.key: row.value
            for row in TenantMemberSetting.objects.filter(
                tenant=self.tenant,
                tenant_membership=self.subject,
                key__in=[USER_KV_STATE_KEY, USER_KV_DISTRICT_KEY, USER_KV_PARTY_KEY],
            )
        }
        self.assertEqual(by_key[USER_KV_STATE_KEY], 17)
        self.assertEqual(by_key[USER_KV_DISTRICT_KEY], 90290)
        self.assertEqual(by_key[USER_KV_PARTY_KEY], 3)

    def test_update_can_change_district_explicitly(self):
        from user_settings.models import TenantMemberSetting
        from user_settings.services import USER_KV_DISTRICT_KEY

        TenantMemberSetting.objects.create(
            tenant=self.tenant,
            tenant_membership=self.subject,
            key=USER_KV_DISTRICT_KEY,
            value=90290,
        )

        resp = self.client.post(
            self.url,
            {
                "name": self.subject.name,
                "email": self.subject.email,
                "role_id": str(self.role.id),
                "original_email": self.subject.email,
                "original_role_id": str(self.role.id),
                "district": 90288,
            },
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(resp.status_code, 200, resp.data)
        self.assertEqual(resp.data["district"], 90288)
        row = TenantMemberSetting.objects.get(
            tenant=self.tenant,
            tenant_membership=self.subject,
            key=USER_KV_DISTRICT_KEY,
        )
        self.assertEqual(row.value, 90288)

    def test_update_sets_and_preserves_manager_email(self):
        manager = TenantMembership.objects.create(
            tenant=self.tenant,
            email=f"manager-{uuid.uuid4().hex[:8]}@example.com",
            role=self.role,
            user_id=uuid.uuid4(),
            name="Manager",
            is_active=True,
        )

        resp = self.client.post(
            self.url,
            {
                "name": self.subject.name,
                "email": self.subject.email,
                "role_id": str(self.role.id),
                "original_email": self.subject.email,
                "original_role_id": str(self.role.id),
                "manager_email": manager.email,
            },
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(resp.status_code, 200, resp.data)
        self.assertEqual(resp.data["manager_email"], manager.email)
        self.subject.refresh_from_db()
        self.assertEqual(self.subject.user_parent_id_id, manager.id)

        # Omitting manager_email must not clear the hierarchy link.
        resp2 = self.client.post(
            self.url,
            {
                "name": "Still Has Manager",
                "email": self.subject.email,
                "role_id": str(self.role.id),
                "original_email": self.subject.email,
                "original_role_id": str(self.role.id),
            },
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(resp2.status_code, 200, resp2.data)
        self.assertEqual(resp2.data["manager_email"], manager.email)
        self.subject.refresh_from_db()
        self.assertEqual(self.subject.user_parent_id_id, manager.id)

    def test_list_users_returns_geo_and_manager_email(self):
        from user_settings.models import TenantMemberSetting
        from user_settings.services import USER_KV_DISTRICT_KEY, USER_KV_STATE_KEY

        manager = TenantMembership.objects.create(
            tenant=self.tenant,
            email=f"mgr-list-{uuid.uuid4().hex[:8]}@example.com",
            role=self.role,
            user_id=uuid.uuid4(),
            name="List Manager",
            is_active=True,
        )
        self.subject.user_parent_id = manager
        self.subject.save(update_fields=["user_parent_id"])
        TenantMemberSetting.objects.create(
            tenant=self.tenant,
            tenant_membership=self.subject,
            key=USER_KV_STATE_KEY,
            value=17,
        )
        TenantMemberSetting.objects.create(
            tenant=self.tenant,
            tenant_membership=self.subject,
            key=USER_KV_DISTRICT_KEY,
            value=90290,
        )

        resp = self.client.get("/membership/users/", **self.auth_headers)
        self.assertEqual(resp.status_code, 200, resp.data)
        row = next(r for r in resp.data["results"] if r["email"] == self.subject.email)
        self.assertEqual(row["state"], 17)
        self.assertEqual(row["district"], 90290)
        self.assertEqual(row["user_parent_email"], manager.email)
        self.assertEqual(row["manager_email"], manager.email)

    def _update_payload(self, name):
        return {
            "name": name,
            "email": self.subject.email,
            "role_id": str(self.role.id),
            "original_email": self.subject.email,
            "original_role_id": str(self.role.id),
        }

    def test_update_retries_once_when_db_connection_drops(self):
        real_apply = accounts_views._apply_group_and_assignment
        calls = {"n": 0}

        def flaky_apply(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OperationalError("SSL connection has been closed unexpectedly")
            return real_apply(*args, **kwargs)

        # close_old_connections would drop the test-case transaction; the real
        # call only matters on a genuinely broken connection.
        with mock.patch.object(accounts_views, "_apply_group_and_assignment", side_effect=flaky_apply), \
                mock.patch.object(accounts_views, "close_old_connections") as close_conns:
            resp = self.client.post(
                self.url, self._update_payload("Retried Name"), format="json", **self.auth_headers
            )

        self.assertEqual(resp.status_code, 200, resp.data)
        self.assertEqual(calls["n"], 2)
        close_conns.assert_called_once()
        self.subject.refresh_from_db()
        self.assertEqual(self.subject.name, "Retried Name")

    def test_update_returns_500_when_retry_also_fails(self):
        with mock.patch.object(
            accounts_views,
            "_apply_group_and_assignment",
            side_effect=OperationalError("SSL connection has been closed unexpectedly"),
        ) as apply, mock.patch.object(accounts_views, "close_old_connections"):
            resp = self.client.post(
                self.url, self._update_payload("Never Saved"), format="json", **self.auth_headers
            )

        self.assertEqual(resp.status_code, 500, resp.data)
        self.assertIn("Failed to update user", resp.data["error"])
        self.assertEqual(apply.call_count, 2)
        self.subject.refresh_from_db()
        self.assertEqual(self.subject.name, "Before")

    def test_update_does_not_retry_other_errors(self):
        with mock.patch.object(
            accounts_views,
            "_apply_group_and_assignment",
            side_effect=ValueError("boom"),
        ) as apply, mock.patch.object(accounts_views, "close_old_connections") as close_conns:
            resp = self.client.post(
                self.url, self._update_payload("Never Saved"), format="json", **self.auth_headers
            )

        self.assertEqual(resp.status_code, 500, resp.data)
        self.assertEqual(apply.call_count, 1)
        close_conns.assert_not_called()
