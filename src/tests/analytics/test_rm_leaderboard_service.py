"""
Tests for compute_score and RmLeaderboardService.compute_and_store_period
— the ranking layer behind refresh_rm_leaderboard_snapshot. Builds on
RmPerformanceDailySummary rows directly (not raw rm_activity_events) since
that's exactly the boundary this service operates at.
"""
from datetime import date, timedelta

from django.test import TestCase

from analytics.models import RmLeaderboardSnapshot, RmPerformanceDailySummary
from analytics.services import (
    RmLeaderboardService,
    compute_score,
    ist_month_end,
    ist_month_start,
    ist_week_end,
    ist_week_start,
)
from tests.factories import TenantFactory, TenantMembershipFactory
from user_settings.models import TenantMemberSetting
from user_settings.services import USER_KV_DAILY_TARGET_KEY


class ComputeScoreTest(TestCase):
    def test_returns_a_percentage_not_a_ratio(self):
        # 7 achieved / 5 target -> 140%, not 1.4
        self.assertEqual(compute_score(7, 5), 140.0)

    def test_returns_none_for_zero_target_not_negative_infinity(self):
        # -inf isn't valid JSON; None round-trips as `null` cleanly.
        self.assertIsNone(compute_score(5, 0))

    def test_zero_achieved_with_a_real_target_is_a_real_zero_percent(self):
        self.assertEqual(compute_score(0, 10), 0.0)


class IstPeriodBoundaryTest(TestCase):
    def test_week_starts_monday(self):
        # 2026-06-17 is a Wednesday
        wednesday = date(2026, 6, 17)
        monday = date(2026, 6, 15)
        self.assertEqual(ist_week_start(wednesday), monday)
        self.assertEqual(ist_week_start(monday), monday)  # Monday itself

    def test_week_end_is_sunday(self):
        monday = date(2026, 6, 15)
        self.assertEqual(ist_week_end(monday), date(2026, 6, 21))

    def test_month_start_and_end(self):
        mid_june = date(2026, 6, 17)
        self.assertEqual(ist_month_start(mid_june), date(2026, 6, 1))
        self.assertEqual(ist_month_end(ist_month_start(mid_june)), date(2026, 6, 30))

    def test_month_end_handles_december_year_rollover(self):
        december = date(2026, 12, 10)
        self.assertEqual(ist_month_end(ist_month_start(december)), date(2026, 12, 31))

    def test_month_end_handles_leap_february(self):
        # 2028 is a leap year
        feb = date(2028, 2, 10)
        self.assertEqual(ist_month_end(ist_month_start(feb)), date(2028, 2, 29))


class RmLeaderboardServiceTest(TestCase):
    """
    TenantMembership.user_id is a real UUIDField — rm_user_id elsewhere
    (RmPerformanceDailySummary, RmLeaderboardSnapshot) is a plain
    CharField with no such constraint, but must still match the
    membership's real UUID string for the service's active-RM lookup to
    find it. make_rm lets the factory generate a real UUID and returns
    (membership, rm_user_id) so tests never have to invent a fake one.
    """

    def setUp(self):
        self.tenant = TenantFactory()
        self.today = date(2026, 6, 15)

    def make_rm(self, target=None, is_active=True, tenant=None):
        membership = TenantMembershipFactory(tenant=tenant or self.tenant, is_active=is_active)
        rm_user_id = str(membership.user_id)
        if target is not None:
            TenantMemberSetting.objects.create(
                tenant=tenant or self.tenant, tenant_membership=membership, key=USER_KV_DAILY_TARGET_KEY, value=target,
            )
        return membership, rm_user_id

    def make_summary_row(self, rm_user_id, touches=10, unique_leads=8, achieved=0, d=None, tenant=None):
        RmPerformanceDailySummary.objects.create(
            tenant=tenant or self.tenant,
            rm_user_id=rm_user_id,
            date=d or self.today,
            touches=touches,
            unique_leads=unique_leads,
            disposition_counts={"TRIAL_ACTIVATED": achieved} if achieved else {},
        )

    def test_ranks_by_score_desc_then_achieved_desc_then_rm_id_asc(self):
        _, rm1 = self.make_rm(target=10)
        _, rm2 = self.make_rm(target=2)
        self.make_summary_row(rm1, achieved=2)  # 2/10 = 20%
        self.make_summary_row(rm2, achieved=1)  # 1/2 = 50%

        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)

        rows = list(
            RmLeaderboardSnapshot.objects.filter(
                tenant=self.tenant, period_type="day", period_start=self.today
            ).order_by("rank")
        )
        self.assertEqual([r.rm_user_id for r in rows], [rm2, rm1])
        self.assertEqual(rows[0].rank, 1)
        self.assertEqual(rows[0].score, 50.0)
        self.assertEqual(rows[1].rank, 2)
        self.assertEqual(rows[1].score, 20.0)

    def test_tied_score_breaks_by_achieved_desc(self):
        _, rm1 = self.make_rm(target=10)
        _, rm2 = self.make_rm(target=5)
        self.make_summary_row(rm1, achieved=2)  # 2/10 = 20%
        self.make_summary_row(rm2, achieved=1)  # 1/5 = 20% — same %, fewer trials

        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)

        rows = list(
            RmLeaderboardSnapshot.objects.filter(
                tenant=self.tenant, period_type="day", period_start=self.today
            ).order_by("rank")
        )
        self.assertEqual(rows[0].rm_user_id, rm1)  # more raw trials wins the tie

    def test_no_target_configured_sorts_last_not_as_a_false_zero(self):
        _, rm1 = self.make_rm(target=None)  # no target at all
        _, rm2 = self.make_rm(target=10)
        self.make_summary_row(rm1, achieved=0)
        self.make_summary_row(rm2, achieved=0)  # a REAL 0% — has a target, just missed it

        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)

        rows = list(
            RmLeaderboardSnapshot.objects.filter(
                tenant=self.tenant, period_type="day", period_start=self.today
            ).order_by("rank")
        )
        self.assertEqual(rows[0].rm_user_id, rm2)  # real 0% ranks above "no target"
        self.assertIsNone(rows[1].score)
        self.assertEqual(rows[1].rm_user_id, rm1)

    def test_rank_is_always_a_definite_integer_even_with_no_score(self):
        _, rm1 = self.make_rm(target=None)
        self.make_summary_row(rm1)

        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)

        row = RmLeaderboardSnapshot.objects.get(tenant=self.tenant, rm_user_id=rm1)
        self.assertIsNone(row.score)
        self.assertEqual(row.rank, 1)  # ranked, not null/skipped

    def test_excludes_inactive_rms_from_the_board(self):
        _, rm1 = self.make_rm(target=10, is_active=True)
        _, rm2 = self.make_rm(target=10, is_active=False)
        self.make_summary_row(rm1, achieved=1)
        self.make_summary_row(rm2, achieved=5)  # would win on numbers alone

        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)

        rm_ids = set(
            RmLeaderboardSnapshot.objects.filter(
                tenant=self.tenant, period_type="day", period_start=self.today
            ).values_list("rm_user_id", flat=True)
        )
        self.assertEqual(rm_ids, {rm1})

    def test_week_period_sums_across_the_whole_week(self):
        _, rm1 = self.make_rm(target=10)
        monday = ist_week_start(self.today)
        self.make_summary_row(rm1, touches=3, achieved=1, d=monday)
        self.make_summary_row(rm1, touches=4, achieved=1, d=monday + timedelta(days=2))

        RmLeaderboardService.compute_and_store_period(
            self.tenant, "week", monday, ist_week_end(monday)
        )

        row = RmLeaderboardSnapshot.objects.get(tenant=self.tenant, rm_user_id=rm1, period_type="week")
        self.assertEqual(row.touches, 7)
        self.assertEqual(row.achieved, 2)

    def test_rerunning_is_idempotent_not_additive(self):
        _, rm1 = self.make_rm(target=10)
        self.make_summary_row(rm1, touches=5, achieved=1)

        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)
        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)

        row = RmLeaderboardSnapshot.objects.get(
            tenant=self.tenant, rm_user_id=rm1, period_type="day", period_start=self.today
        )
        self.assertEqual(row.touches, 5)  # not 10 — replaced, not appended

    def test_a_tenant_with_zero_active_rms_clears_any_stale_rows(self):
        membership, rm1 = self.make_rm(target=10)
        self.make_summary_row(rm1, achieved=1)
        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)
        self.assertTrue(RmLeaderboardSnapshot.objects.filter(tenant=self.tenant).exists())

        membership.is_active = False
        membership.save(update_fields=["is_active"])
        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)

        self.assertFalse(
            RmLeaderboardSnapshot.objects.filter(tenant=self.tenant, period_type="day", period_start=self.today).exists()
        )

    def test_tenant_isolation(self):
        other_tenant = TenantFactory()
        _, rm1 = self.make_rm(target=10)
        _, rm_other = self.make_rm(target=10, tenant=other_tenant)
        self.make_summary_row(rm1, achieved=1)
        self.make_summary_row(rm_other, achieved=1, tenant=other_tenant)

        RmLeaderboardService.compute_and_store_period(self.tenant, "day", self.today, self.today)

        self.assertEqual(
            RmLeaderboardSnapshot.objects.filter(tenant=self.tenant).count(), 1
        )
        self.assertFalse(RmLeaderboardSnapshot.objects.filter(tenant=other_tenant).exists())
