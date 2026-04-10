"""
Tests for TenantMemberSetting-backed API: core KV settings per tenant member.

Run:
  pytest src/tests/rest/user_settings/test_tenant_member_settings_api.py -v
"""

from django.urls import reverse

from authz import service as authz_service
from user_settings.models import TenantMemberSetting
from user_settings.services import (
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_DAILY_TARGET_KEY,
    USER_KV_GROUP_ID_KEY,
)
from tests.base.test_setup import BaseAPITestCase


class UserCoreKVSettingsAPITests(BaseAPITestCase):
    """GET /user-settings/users/<membership_id>/core-kv-settings/"""

    def setUp(self):
        super().setUp()
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)
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

    def test_core_kv_settings_returns_rows(self):
        path = reverse(
            "user-core-kv-settings",
            kwargs={"user_id": str(self.membership.id)},
        )
        response = self.client.get(path, **self.auth_headers)

        self.assertEqual(response.status_code, 200, response.data)
        data = response.data
        self.assertIsInstance(data, list)
        keys = {row["key"]: row["value"] for row in data}
        self.assertEqual(keys.get(USER_KV_GROUP_ID_KEY), 42)
        self.assertEqual(keys.get(USER_KV_DAILY_TARGET_KEY), 10)
        self.assertEqual(keys.get(USER_KV_DAILY_LIMIT_KEY), 50)
