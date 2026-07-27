"""
Tests for TenantMemberSetting-backed API: core KV settings per tenant member.

Run:
  pytest src/tests/rest/user_settings/test_tenant_member_settings_api.py -v
"""

from django.urls import reverse

from authz import service as authz_service
from authz.models import Role
from user_settings.models import TenantMemberSetting
from user_settings.services import (
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_DAILY_TARGET_KEY,
    USER_KV_GROUP_ID_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
    USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
)
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory


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
