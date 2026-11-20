"""
Tests for the RM Leaderboard read API — /rm-leaderboard/ and
/rm-leaderboard/me/. These only ever SELECT RmLeaderboardSnapshot rows
(written ahead of time by refresh_rm_leaderboard_snapshot, or directly by
these tests) — no live aggregation happens on the read path, so every
test here sets up snapshot rows directly rather than going through the
refresh job.
"""
from datetime import timedelta

from django.core.cache import cache
from django.urls import reverse
from django.utils import timezone
from rest_framework import status

from analytics.models import RmLeaderboardSnapshot
from analytics.services import ist_today
from core.models import TenantSettings
from tests.base.test_setup import BaseAPITestCase, MultiTenantAPITestCase
from tests.factories import TenantMembershipFactory


def enable_leaderboard(tenant):
    TenantSettings.objects.update_or_create(tenant=tenant, defaults={"rm_leaderboard_enabled": True})


def make_snapshot_row(tenant, rm_user_id, rank, period_type="day", period_start=None, **extra):
    defaults = {"touches": 0, "unique_leads": 0, "achieved": 0, "target": 0, "score": None}
    defaults.update(extra)
    return RmLeaderboardSnapshot.objects.create(
        tenant=tenant,
        rm_user_id=rm_user_id,
        period_type=period_type,
        period_start=period_start or ist_today(),
        rank=rank,
        **defaults,
    )


class RmLeaderboardFeatureFlagTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        cache.clear()
        self.url = reverse("analytics:rm-leaderboard")
        self.me_url = reverse("analytics:rm-leaderboard-me")

    def test_board_404s_when_flag_is_off(self):
        # TenantSettings.rm_leaderboard_enabled defaults to False — no
        # TenantSettings row exists yet for self.tenant at all.
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_me_404s_when_flag_is_off(self):
        response = self.client.get(self.me_url, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_board_available_once_flag_is_on(self):
        enable_leaderboard(self.tenant)
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)


class RmLeaderboardBoardTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        cache.clear()
        enable_leaderboard(self.tenant)
        self.url = reverse("analytics:rm-leaderboard")

    def test_invalid_period_type_is_rejected(self):
        response = self.client.get(self.url, {"period_type": "year"}, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_returns_rows_ordered_by_stored_rank(self):
        make_snapshot_row(self.tenant, "rm-a", rank=2, score=40.0)
        make_snapshot_row(self.tenant, "rm-b", rank=1, score=90.0)
        make_snapshot_row(self.tenant, "rm-c", rank=3, score=10.0)

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        rows = response.data["rows"]
        self.assertEqual([r["rm_user_id"] for r in rows], ["rm-b", "rm-a", "rm-c"])
        self.assertEqual([r["rank"] for r in rows], [1, 2, 3])

    def test_limit_caps_the_number_of_rows(self):
        for i in range(1, 6):
            make_snapshot_row(self.tenant, f"rm-{i}", rank=i)

        response = self.client.get(self.url, {"limit": 2}, **self.auth_headers)

        self.assertEqual(len(response.data["rows"]), 2)
        self.assertEqual([r["rank"] for r in response.data["rows"]], [1, 2])

    def test_no_snapshot_yet_is_stale_not_an_error(self):
        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["rows"], [])
        self.assertTrue(response.data["stale"])

    def test_fresh_snapshot_is_not_stale(self):
        make_snapshot_row(self.tenant, "rm-a", rank=1)

        response = self.client.get(self.url, **self.auth_headers)

        self.assertFalse(response.data["stale"])

    def test_snapshot_older_than_threshold_is_stale(self):
        row = make_snapshot_row(self.tenant, "rm-a", rank=1)
        RmLeaderboardSnapshot.objects.filter(pk=row.pk).update(
            computed_at=timezone.now() - timedelta(minutes=20)
        )

        response = self.client.get(self.url, **self.auth_headers)

        self.assertTrue(response.data["stale"])


class RmLeaderboardTeamScopeTest(BaseAPITestCase):
    """
    scope=team is the ASM/manager view: re-ranked 1..N within the
    requesting user's own TenantMembership hierarchy (self.membership +
    direct_reports, via TeamResolver — the same hierarchy TeamOverviewView
    already uses), with no gaps left over from the tenant-wide rank.
    """

    def setUp(self):
        super().setUp()
        cache.clear()
        enable_leaderboard(self.tenant)
        self.url = reverse("analytics:rm-leaderboard")

        # self.membership (the authenticated viewer) is the ASM here.
        self.report_a = TenantMembershipFactory(
            tenant=self.tenant, user_parent_id=self.membership, is_active=True,
        )
        self.report_b = TenantMembershipFactory(
            tenant=self.tenant, user_parent_id=self.membership, is_active=True,
        )
        # Outsiders — not in this ASM's hierarchy, must never appear.
        self.outsider_top = TenantMembershipFactory(tenant=self.tenant, is_active=True)
        self.outsider_mid = TenantMembershipFactory(tenant=self.tenant, is_active=True)

        # Tenant-wide ranks: outsider_top=1, report_a=2, outsider_mid=3,
        # report_b=4, asm(self)=5 — team members have gaps (2, 4, 5).
        make_snapshot_row(self.tenant, str(self.outsider_top.user_id), rank=1)
        make_snapshot_row(self.tenant, str(self.report_a.user_id), rank=2)
        make_snapshot_row(self.tenant, str(self.outsider_mid.user_id), rank=3)
        make_snapshot_row(self.tenant, str(self.report_b.user_id), rank=4)
        make_snapshot_row(self.tenant, str(self.membership.user_id), rank=5)

    def test_team_scope_only_includes_self_and_reports(self):
        response = self.client.get(self.url, {"scope": "team"}, **self.auth_headers)

        rm_ids = {r["rm_user_id"] for r in response.data["rows"]}
        self.assertEqual(
            rm_ids, {str(self.report_a.user_id), str(self.report_b.user_id), str(self.membership.user_id)}
        )

    def test_team_scope_reranks_with_no_gaps(self):
        response = self.client.get(self.url, {"scope": "team"}, **self.auth_headers)

        rows = response.data["rows"]
        # original tenant-wide order (2, 4, 5) preserved, renumbered 1..3
        self.assertEqual(
            [r["rm_user_id"] for r in rows],
            [str(self.report_a.user_id), str(self.report_b.user_id), str(self.membership.user_id)],
        )
        self.assertEqual([r["rank"] for r in rows], [1, 2, 3])

    def test_tenant_scope_is_unaffected_and_still_has_gaps_for_the_filtered_view(self):
        # sanity check: the default (tenant) scope is untouched by team logic
        response = self.client.get(self.url, **self.auth_headers)
        rows = response.data["rows"]
        self.assertEqual(len(rows), 5)
        self.assertEqual([r["rank"] for r in rows], [1, 2, 3, 4, 5])


class RmLeaderboardMeTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        cache.clear()
        enable_leaderboard(self.tenant)
        self.url = reverse("analytics:rm-leaderboard")
        self.me_url = reverse("analytics:rm-leaderboard-me")

    def test_me_returns_own_row_even_when_outside_the_top_n(self):
        make_snapshot_row(self.tenant, "rm-top", rank=1, score=99.0)
        make_snapshot_row(self.tenant, str(self.membership.user_id), rank=2, score=10.0)

        board_response = self.client.get(self.url, {"limit": 1}, **self.auth_headers)
        me_response = self.client.get(self.me_url, **self.auth_headers)

        board_rm_ids = {r["rm_user_id"] for r in board_response.data["rows"]}
        self.assertNotIn(str(self.membership.user_id), board_rm_ids)  # outside the top 1

        self.assertEqual(me_response.status_code, status.HTTP_200_OK)
        self.assertEqual(me_response.data["row"]["rm_user_id"], str(self.membership.user_id))
        self.assertEqual(me_response.data["row"]["rank"], 2)

    def test_me_is_null_when_viewer_has_no_snapshot_row(self):
        response = self.client.get(self.me_url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsNone(response.data["row"])
        self.assertTrue(response.data["stale"])


class RmLeaderboardCrossTenantCacheTest(MultiTenantAPITestCase):
    """
    Fix #2: the cache key must include tenant_id (plus period_type,
    period_start, scope, limit) so one tenant's cached board can never be
    served to another tenant making the identically-shaped request.
    """

    def setUp(self):
        super().setUp()
        cache.clear()
        enable_leaderboard(self.tenant)
        enable_leaderboard(self.tenant_b)
        self.url = reverse("analytics:rm-leaderboard")

        make_snapshot_row(self.tenant, "tenant-a-rm", rank=1, score=11.0)
        make_snapshot_row(self.tenant_b, "tenant-b-rm", rank=1, score=22.0)

    def test_no_cross_tenant_cache_hit(self):
        # Same period_type/period_start/scope/limit on both requests —
        # only the tenant differs. Warm tenant A's cache entry first.
        response_a = self.client.get(self.url, **self.auth_headers)
        self.assertEqual([r["rm_user_id"] for r in response_a.data["rows"]], ["tenant-a-rm"])

        response_b = self.client.get(self.url, **self.auth_headers_b)
        self.assertEqual([r["rm_user_id"] for r in response_b.data["rows"]], ["tenant-b-rm"])
