"""
Tests for per-day RM trial targets:
- get_rm_daily_targets_sum() sums each RM's frozen daily snapshots across a
  range, falling back to their standing DAILY_TARGET for days with no
  snapshot yet (see RmDailyTargetSnapshotJobHandler, which writes these
  rows automatically).
- analytics.RmDailyTargetsView (the RM PRD dashboard's endpoint) reflects
  those snapshots when summing a date range.
"""
from datetime import date

from django.urls import reverse
from rest_framework import status

from tests.base.test_setup import BaseAPITestCase
from user_settings.models import TenantMemberSetting
from user_settings.services import (
    USER_KV_DAILY_TARGET_KEY,
    get_rm_daily_targets_sum,
    set_rm_daily_target,
)


class GetRmDailyTargetsSumTests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        # self.membership is the RM created by BaseAPITestCase
        self.day1 = date(2026, 9, 1)
        self.day2 = date(2026, 9, 2)
        self.day3 = date(2026, 9, 3)

    def test_falls_back_to_daily_target_times_days_when_no_overrides_exist(self):
        TenantMemberSetting.objects.create(
            tenant=self.tenant, tenant_membership=self.membership, key=USER_KV_DAILY_TARGET_KEY, value=9,
        )
        totals = get_rm_daily_targets_sum(self.tenant, [self.membership.id], self.day1, self.day3)
        self.assertEqual(totals[self.membership.id], 27)  # 9 * 3 days

    def test_sums_real_varying_per_day_targets(self):
        # the exact scenario this feature exists for: 10 today, 12 tomorrow, 34 the day after
        set_rm_daily_target(tenant=self.tenant, tenant_membership=self.membership, target_date=self.day1, target=10)
        set_rm_daily_target(tenant=self.tenant, tenant_membership=self.membership, target_date=self.day2, target=12)
        set_rm_daily_target(tenant=self.tenant, tenant_membership=self.membership, target_date=self.day3, target=34)

        totals = get_rm_daily_targets_sum(self.tenant, [self.membership.id], self.day1, self.day3)
        self.assertEqual(totals[self.membership.id], 56)  # 10 + 12 + 34

    def test_mixes_overrides_and_fallback_within_the_same_range(self):
        TenantMemberSetting.objects.create(
            tenant=self.tenant, tenant_membership=self.membership, key=USER_KV_DAILY_TARGET_KEY, value=5,
        )
        # only day2 has an explicit override; day1 and day3 fall back to 5 each
        set_rm_daily_target(tenant=self.tenant, tenant_membership=self.membership, target_date=self.day2, target=20)

        totals = get_rm_daily_targets_sum(self.tenant, [self.membership.id], self.day1, self.day3)
        self.assertEqual(totals[self.membership.id], 5 + 20 + 5)

    def test_zero_when_no_override_and_no_daily_target_set(self):
        totals = get_rm_daily_targets_sum(self.tenant, [self.membership.id], self.day1, self.day3)
        self.assertEqual(totals[self.membership.id], 0)

    def test_single_day_range_uses_just_that_day(self):
        set_rm_daily_target(tenant=self.tenant, tenant_membership=self.membership, target_date=self.day1, target=7)
        totals = get_rm_daily_targets_sum(self.tenant, [self.membership.id], self.day1, self.day1)
        self.assertEqual(totals[self.membership.id], 7)


class RmDailyTargetsAnalyticsViewReflectsSnapshotsTest(BaseAPITestCase):
    """
    The RM PRD dashboard's own endpoint (analytics app) must reflect
    frozen RmDailyTarget snapshots when summing a multi-day range, not just
    the flat DAILY_TARGET.
    """

    def setUp(self):
        super().setUp()
        self.url = reverse("analytics:rm-daily-targets")

    def test_sums_real_per_day_targets_across_a_range(self):
        set_rm_daily_target(
            tenant=self.tenant, tenant_membership=self.membership,
            target_date=date(2026, 9, 1), target=10,
        )
        set_rm_daily_target(
            tenant=self.tenant, tenant_membership=self.membership,
            target_date=date(2026, 9, 2), target=12,
        )
        set_rm_daily_target(
            tenant=self.tenant, tenant_membership=self.membership,
            target_date=date(2026, 9, 3), target=34,
        )

        response = self.client.get(
            self.url, {"from": "2026-09-01", "to": "2026-09-03"}, **self.auth_headers,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data.get(str(self.membership.user_id)), 56)
