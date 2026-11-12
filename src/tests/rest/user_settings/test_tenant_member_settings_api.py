"""
Tests for TenantMemberSetting-backed API: core KV settings per tenant member.

Run:
  pytest src/tests/rest/user_settings/test_tenant_member_settings_api.py -v
"""

from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from authz import service as authz_service
from authz.models import Role
from user_settings.models import TenantMemberSetting
from user_settings.services import (
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_DAILY_TARGET_KEY,
    USER_KV_GROUP_ID_KEY,
    USER_KV_LEAD_ASSIGNMENT_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
    USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
)
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory, TenantFactory, TenantMembershipFactory


class UserCoreKVSettingsAPITests(BaseAPITestCase):
    """GET/PATCH /user-settings/users/<membership_id>/core-kv-settings/"""

    def setUp(self):
        super().setUp()
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)
        self.cse_role = RoleFactory(
            tenant=self.tenant,
            key="CSE",
            name="Customer Support Executive",
        )
        self.membership.role = self.cse_role
        self.membership.save(update_fields=["role"])
        TenantMemberSetting.objects.update_or_create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key=USER_KV_GROUP_ID_KEY,
            defaults={"value": 42},
        )
        TenantMemberSetting.objects.update_or_create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key=USER_KV_DAILY_TARGET_KEY,
            defaults={"value": 10},
        )
        TenantMemberSetting.objects.update_or_create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key=USER_KV_DAILY_LIMIT_KEY,
            defaults={"value": 50},
        )

    def _url(self):
        return reverse(
            "user-core-kv-settings",
            kwargs={"user_id": str(self.membership.id)},
        )

    def test_core_kv_settings_returns_rows(self):
        response = self.client.get(self._url(), **self.auth_headers)

        self.assertEqual(response.status_code, 200, response.data)
        data = response.data
        self.assertIsInstance(data, list)
        keys = {row["key"]: row["value"] for row in data}
        self.assertEqual(keys.get(USER_KV_GROUP_ID_KEY), 42)
        self.assertEqual(keys.get(USER_KV_DAILY_TARGET_KEY), 10)
        self.assertEqual(keys.get(USER_KV_DAILY_LIMIT_KEY), 50)

    def test_patch_support_daily_limits(self):
        response = self.client.patch(
            self._url(),
            {
                "support_daily_limit_self_trial": 10,
                "support_daily_limit_other": 25,
            },
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200, response.data)
        keys = {row["key"]: row["value"] for row in response.data}
        self.assertEqual(keys[USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY], 10)
        self.assertEqual(keys[USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY], 25)

    def test_patch_support_resolve_rate_goal(self):
        response = self.client.patch(
            self._url(),
            {"support_resolve_rate_goal": 85},
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200, response.data)
        keys = {row["key"]: row["value"] for row in response.data}
        self.assertEqual(keys[USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY], 85)

    def test_patch_null_clears_support_daily_limit(self):
        TenantMemberSetting.objects.update_or_create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key=USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
            defaults={"value": 5},
        )
        response = self.client.patch(
            self._url(),
            {"support_daily_limit_self_trial": None},
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200, response.data)
        keys = {row["key"]: row["value"] for row in response.data}
        self.assertNotIn(USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY, keys)
        self.assertFalse(
            TenantMemberSetting.objects.filter(
                tenant=self.tenant,
                tenant_membership=self.membership,
                key=USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
            ).exists()
        )

    def test_patch_rejects_non_cse_membership(self):
        rm_role = RoleFactory(tenant=self.tenant, key="RM", name="RM")
        self.membership.role = rm_role
        self.membership.save(update_fields=["role"])
        response = self.client.patch(
            self._url(),
            {"support_daily_limit_self_trial": 10},
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 400, response.data)
        self.assertIn("CSE", response.data["error"])

    def test_patch_custom_fields_allowed_for_non_cse(self):
        rm_role = RoleFactory(tenant=self.tenant, key="RM", name="RM")
        self.membership.role = rm_role
        self.membership.save(update_fields=["role"])
        response = self.client.patch(
            self._url(),
            {
                "EMPLOYEE_CODE": "E-101",
                "REGION": "South",
            },
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200, response.data)
        keys = {row["key"]: row["value"] for row in response.data}
        self.assertEqual(keys["EMPLOYEE_CODE"], "E-101")
        self.assertEqual(keys["REGION"], "South")
        self.assertTrue(
            TenantMemberSetting.objects.filter(
                tenant=self.tenant,
                tenant_membership=self.membership,
                key="EMPLOYEE_CODE",
                value="E-101",
            ).exists()
        )

    def test_get_includes_custom_fields(self):
        TenantMemberSetting.objects.update_or_create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="EMPLOYEE_CODE",
            defaults={"value": "E-202"},
        )
        response = self.client.get(self._url(), **self.auth_headers)
        self.assertEqual(response.status_code, 200, response.data)
        keys = {row["key"]: row["value"] for row in response.data}
        self.assertEqual(keys.get("EMPLOYEE_CODE"), "E-202")

    def test_patch_null_clears_custom_field(self):
        TenantMemberSetting.objects.update_or_create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="EMPLOYEE_CODE",
            defaults={"value": "E-303"},
        )
        response = self.client.patch(
            self._url(),
            {"EMPLOYEE_CODE": None},
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200, response.data)
        keys = {row["key"]: row["value"] for row in response.data}
        self.assertNotIn("EMPLOYEE_CODE", keys)
        self.assertFalse(
            TenantMemberSetting.objects.filter(
                tenant=self.tenant,
                tenant_membership=self.membership,
                key="EMPLOYEE_CODE",
            ).exists()
        )

    def test_get_includes_state_district_core_keys(self):
        TenantMemberSetting.objects.update_or_create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="STATE",
            defaults={"value": 95829},
        )
        TenantMemberSetting.objects.update_or_create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="DISTRICT",
            defaults={"value": 97869},
        )
        response = self.client.get(self._url(), **self.auth_headers)
        self.assertEqual(response.status_code, 200, response.data)
        keys = {row["key"]: row["value"] for row in response.data}
        self.assertEqual(keys.get("STATE"), 95829)
        self.assertEqual(keys.get("DISTRICT"), 97869)

    def test_patch_rejects_reserved_state_key(self):
        response = self.client.patch(
            self._url(),
            {"STATE": 95829},
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 400, response.data)


class TenantCoreKVSettingsAPITests(BaseAPITestCase):
    """GET /user-settings/core-kv-settings/?membership_ids=... — a batch of members in one request."""

    def setUp(self):
        super().setUp()
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)
        self.url = reverse("tenant-core-kv-settings")
        self._set(self.membership, USER_KV_DAILY_TARGET_KEY, 10)
        self._set(self.membership, USER_KV_DAILY_LIMIT_KEY, 50)
        self.second = TenantMembershipFactory(tenant=self.tenant)
        self._set(self.second, USER_KV_GROUP_ID_KEY, 7)
        self._set(self.second, "STATE", 95829)

    def _set(self, membership, key, value):
        TenantMemberSetting.objects.update_or_create(
            tenant=membership.tenant,
            tenant_membership=membership,
            key=key,
            defaults={"value": value},
        )

    def _request(self, membership_ids):
        return self.client.get(
            self.url,
            {"membership_ids": ",".join(str(i) for i in membership_ids)},
            **self.auth_headers,
        )

    def _get(self, membership_ids):
        response = self._request(membership_ids)
        self.assertEqual(response.status_code, 200, response.data)
        return response.data["results"]

    def test_returns_requested_members_as_key_value_maps(self):
        results = self._get([self.membership.id, self.second.id])

        self.assertEqual(
            results[str(self.membership.id)],
            {USER_KV_DAILY_TARGET_KEY: 10, USER_KV_DAILY_LIMIT_KEY: 50},
        )
        self.assertEqual(results[str(self.second.id)], {USER_KV_GROUP_ID_KEY: 7, "STATE": 95829})

    def test_returns_only_the_requested_members(self):
        results = self._get([self.second.id])

        self.assertEqual(set(results), {str(self.second.id)})

    def test_values_match_the_per_member_endpoint(self):
        self._set(self.second, "EMPLOYEE_CODE", "E-101")
        results = self._get([self.second.id])

        per_member = self.client.get(
            reverse("user-core-kv-settings", kwargs={"user_id": str(self.second.id)}),
            **self.auth_headers,
        )
        self.assertEqual(per_member.status_code, 200, per_member.data)
        self.assertEqual(
            results[str(self.second.id)],
            {row["key"]: row["value"] for row in per_member.data},
        )

    def test_excludes_reserved_non_core_keys(self):
        self._set(self.membership, USER_KV_LEAD_ASSIGNMENT_KEY, {"lead_types": ["BJP"]})
        results = self._get([self.membership.id])

        self.assertNotIn(USER_KV_LEAD_ASSIGNMENT_KEY, results[str(self.membership.id)])

    def test_never_returns_other_tenants_members(self):
        outsider = TenantMembershipFactory(tenant=TenantFactory())
        self._set(outsider, USER_KV_DAILY_TARGET_KEY, 99)

        results = self._get([self.membership.id, outsider.id])

        self.assertEqual(set(results), {str(self.membership.id)})

    def test_rejects_missing_membership_ids(self):
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 400, response.data)

    def test_rejects_non_integer_membership_ids(self):
        response = self._request(["67", "abc"])
        self.assertEqual(response.status_code, 400, response.data)

    def test_accepts_up_to_the_batch_limit_and_rejects_more(self):
        from user_settings.views import CORE_KV_SETTINGS_BATCH_LIMIT

        ids = list(range(1, CORE_KV_SETTINGS_BATCH_LIMIT + 1))
        self.assertEqual(self._request(ids).status_code, 200)

        response = self._request(ids + [CORE_KV_SETTINGS_BATCH_LIMIT + 1])
        self.assertEqual(response.status_code, 400, response.data)
        self.assertIn(str(CORE_KV_SETTINGS_BATCH_LIMIT), response.data["error"])

    def test_query_count_does_not_grow_with_batch_size(self):
        self._get([self.membership.id])  # first request also loads auth/tenant lookups that are cached afterwards
        with CaptureQueriesContext(connection) as few:
            self._get([self.membership.id, self.second.id])

        members = [TenantMembershipFactory(tenant=self.tenant) for _ in range(10)]
        for index, member in enumerate(members):
            self._set(member, USER_KV_DAILY_TARGET_KEY, index)
            self._set(member, USER_KV_DAILY_LIMIT_KEY, index)

        with CaptureQueriesContext(connection) as many:
            results = self._get([self.membership.id, self.second.id] + [m.id for m in members])

        self.assertEqual(len(results), 12)
        self.assertEqual(len(many.captured_queries), len(few.captured_queries))

    def test_requires_authentication(self):
        self.client.force_authenticate(user=None)
        response = self.client.get(self.url, {"membership_ids": str(self.membership.id)})
        self.assertIn(response.status_code, (401, 403))


class GeoPartyCatalogAPITests(BaseAPITestCase):
    """GET /user-settings/geo-party-catalog/"""

    def setUp(self):
        super().setUp()
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)

    def test_catalog_returns_states_and_districts(self):
        response = self.client.get(
            reverse("geo-party-catalog"),
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200, response.data)
        self.assertIn("states", response.data)
        self.assertIn("districts", response.data)
        states = response.data["states"]
        self.assertGreaterEqual(len(states), 1)
        self.assertIsInstance(states[0]["value"], int)
        self.assertIn("label", states[0])
        self.assertRegex(states[0]["label"], r".+ \(\d+\)$")

    def test_catalog_filters_districts_by_state_id(self):
        all_resp = self.client.get(reverse("geo-party-catalog"), **self.auth_headers)
        self.assertEqual(all_resp.status_code, 200, all_resp.data)
        state_id = all_resp.data["states"][0]["value"]
        response = self.client.get(
            reverse("geo-party-catalog"),
            {"state_id": state_id},
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200, response.data)
        for opt in response.data["districts"]:
            self.assertEqual(opt["state_id"], state_id)
            self.assertIsInstance(opt["value"], int)
