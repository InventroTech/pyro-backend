"""
Tests for batched upsert_user_kv_settings.

Run:
  pytest src/tests/rest/user_settings/test_upsert_user_kv_settings.py -v
"""

from user_settings.models import TenantMemberSetting
from user_settings.services import (
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_DAILY_TARGET_KEY,
    USER_KV_GROUP_ID_KEY,
    upsert_user_kv_settings,
)
from tests.base.test_setup import BaseAPITestCase


class UpsertUserKvSettingsTests(BaseAPITestCase):
    def test_creates_all_three_keys(self):
        upsert_user_kv_settings(
            tenant=self.tenant,
            tenant_membership=self.membership,
            group_id=7,
            daily_target=10,
            daily_limit=20,
        )
        rows = {
            r.key: r.value
            for r in TenantMemberSetting.objects.filter(
                tenant=self.tenant,
                tenant_membership=self.membership,
                key__in=[USER_KV_GROUP_ID_KEY, USER_KV_DAILY_TARGET_KEY, USER_KV_DAILY_LIMIT_KEY],
            )
        }
        self.assertEqual(rows[USER_KV_GROUP_ID_KEY], 7)
        self.assertEqual(rows[USER_KV_DAILY_TARGET_KEY], 10)
        self.assertEqual(rows[USER_KV_DAILY_LIMIT_KEY], 20)

    def test_updates_existing_rows(self):
        upsert_user_kv_settings(
            tenant=self.tenant,
            tenant_membership=self.membership,
            group_id=1,
            daily_target=5,
            daily_limit=15,
        )
        upsert_user_kv_settings(
            tenant=self.tenant,
            tenant_membership=self.membership,
            group_id=2,
            daily_target=6,
            daily_limit=16,
        )
        rows = {
            r.key: r.value
            for r in TenantMemberSetting.objects.filter(
                tenant=self.tenant,
                tenant_membership=self.membership,
                key__in=[USER_KV_GROUP_ID_KEY, USER_KV_DAILY_TARGET_KEY, USER_KV_DAILY_LIMIT_KEY],
            )
        }
        self.assertEqual(rows[USER_KV_GROUP_ID_KEY], 2)
        self.assertEqual(rows[USER_KV_DAILY_TARGET_KEY], 6)
        self.assertEqual(rows[USER_KV_DAILY_LIMIT_KEY], 16)
        self.assertEqual(
            TenantMemberSetting.objects.filter(
                tenant=self.tenant,
                tenant_membership=self.membership,
            ).count(),
            3,
        )
