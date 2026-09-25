"""
Tests for per-day RM trial targets:
- get_rm_daily_targets_sum() sums each RM's real day-by-day targets across a
  range, falling back to their standing DAILY_TARGET for days with no
  explicit override.
- RmDailyTargetOverridesView lets a manager list/set/clear a specific RM's
  target for a specific date.
- analytics.RmDailyTargetsView (the RM PRD dashboard's endpoint) reflects
  those overrides when summing a date range.
"""
from datetime import date, timedelta

from django.urls import reverse
from rest_framework import status

from tests.base.test_setup import BaseAPITestCase
from user_settings.models import RmDailyTarget, TenantMemberSetting
from user_settings.services import (
    USER_KV_DAILY_TARGET_KEY,
    get_rm_daily_targets_sum,
    list_rm_daily_targets,
    set_rm_daily_target,
    delete_rm_daily_target,
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


class RmDailyTargetOverridesViewTests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.url = reverse("rm-daily-target-overrides")
        self.day = "2026-09-10"

    def test_requires_authentication(self):
        response = self.client.post(
            self.url,
            {"tenant_membership_id": self.membership.id, "date": self.day, "target": 12},
            format="json",
        )
        self.assertIn(response.status_code, (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN))

    def test_post_creates_an_override_and_get_lists_it(self):
        response = self.client.post(
            self.url,
            {"tenant_membership_id": self.membership.id, "date": self.day, "target": 12},
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data, {"date": self.day, "target": 12})

        list_response = self.client.get(
            self.url,
            {"tenant_membership_id": self.membership.id, "from": "2026-09-09", "to": "2026-09-11"},
            **self.auth_headers,
        )
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(list_response.data, [{"date": self.day, "target": 12}])

    def test_post_upserts_rather_than_duplicating(self):
        self.client.post(
            self.url, {"tenant_membership_id": self.membership.id, "date": self.day, "target": 12},
            format="json", **self.auth_headers,
        )
        self.client.post(
            self.url, {"tenant_membership_id": self.membership.id, "date": self.day, "target": 20},
            format="json", **self.auth_headers,
        )
        self.assertEqual(
            RmDailyTarget.objects.filter(tenant=self.tenant, tenant_membership=self.membership).count(), 1
        )
        row = RmDailyTarget.objects.get(tenant=self.tenant, tenant_membership=self.membership)
        self.assertEqual(row.target, 20)

    def test_negative_target_rejected(self):
        response = self.client.post(
            self.url, {"tenant_membership_id": self.membership.id, "date": self.day, "target": -5},
            format="json", **self.auth_headers,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_delete_clears_the_override(self):
        set_rm_daily_target(
            tenant=self.tenant, tenant_membership=self.membership,
            target_date=date(2026, 9, 10), target=12,
        )
        response = self.client.delete(
            f"{self.url}?tenant_membership_id={self.membership.id}&date={self.day}", **self.auth_headers,
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertEqual(
            RmDailyTarget.objects.filter(tenant=self.tenant, tenant_membership=self.membership).count(), 0
        )


class RmDailyTargetsAnalyticsViewReflectsOverridesTest(BaseAPITestCase):
    """
    The RM PRD dashboard's own endpoint (analytics app) must reflect
    RmDailyTarget overrides when summing a multi-day range, not just the
    flat DAILY_TARGET.
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
