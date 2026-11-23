"""Reusable cache wired into CSE / Team / RM PRD / ticket filter endpoints."""
from unittest.mock import MagicMock, patch

from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES, api_request
from utils.cache_utils import invalidate_team_analytics_cache, reset_page_cache_stats


@override_settings(CACHES=LOC_MEM_CACHES)
class ReusableCacheViewTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    def _call(self, view_cls, path, query=None):
        with patch.object(view_cls, "authentication_classes", []), patch.object(
            view_cls, "permission_classes", []
        ):
            return view_cls.as_view()(api_request(path, query=query))

    @patch("analytics.views.TeamOverviewSerializer")
    @patch("analytics.views.TeamMetricsService")
    @patch("analytics.views.TeamResolver.get_team_user_ids", return_value={"u1"})
    def test_team_overview_second_call_hits_cache(self, _team_ids, metrics_cls, serializer_cls):
        from analytics.views import TeamOverviewView

        metrics = metrics_cls.return_value
        metrics.get_overview.return_value = {"attendance": 1}
        serializer_cls.return_value.data = {"attendance": 1}

        path = "/analytics/team/overview/"
        query = {"date": "2026-01-15"}
        self.assertEqual(self._call(TeamOverviewView, path, query).status_code, 200)
        self.assertEqual(self._call(TeamOverviewView, path, query).status_code, 200)
        self.assertEqual(metrics.get_overview.call_count, 1)

    @patch("analytics.views.MemberBreakdownSerializer")
    @patch("analytics.views.TeamMetricsService")
    @patch("analytics.views.TeamResolver.get_team_user_ids", return_value={"u1"})
    def test_team_members_second_call_hits_cache(self, _team_ids, metrics_cls, serializer_cls):
        from analytics.views import TeamMembersView

        metrics_cls.return_value.get_member_breakdown.return_value = [{"user_id": "u1"}]
        serializer_cls.return_value.data = [{"user_id": "u1"}]

        path = "/analytics/team/members/"
        query = {"date": "2026-01-15"}
        self.assertEqual(self._call(TeamMembersView, path, query).status_code, 200)
        self.assertEqual(self._call(TeamMembersView, path, query).status_code, 200)
        self.assertEqual(metrics_cls.return_value.get_member_breakdown.call_count, 1)

    @patch("analytics.views.EventBreakdownSerializer")
    @patch("analytics.views.TeamMetricsService")
    @patch("analytics.views.TeamResolver.get_team_user_ids", return_value={"u1"})
    def test_team_events_second_call_hits_cache(self, _team_ids, metrics_cls, serializer_cls):
        from analytics.views import TeamEventsView

        metrics_cls.return_value.get_event_breakdown.return_value = {"call": 2}
        serializer_cls.return_value.data = [{"event_type": "call", "count": 2}]

        path = "/analytics/team/events/"
        query = {"date": "2026-01-15"}
        self.assertEqual(self._call(TeamEventsView, path, query).status_code, 200)
        self.assertEqual(self._call(TeamEventsView, path, query).status_code, 200)
        self.assertEqual(metrics_cls.return_value.get_event_breakdown.call_count, 1)

    @patch("analytics.views.TeamMetricsService")
    def test_team_unassigned_second_call_hits_cache(self, metrics_cls):
        from analytics.views import UnassignedLeadsBreakdownView

        metrics_cls.return_value.get_unassigned_leads_breakdown.return_value = {
            "total": 5,
            "by_source": [],
        }

        path = "/analytics/team/unassigned-leads/"
        self.assertEqual(self._call(UnassignedLeadsBreakdownView, path).status_code, 200)
        self.assertEqual(self._call(UnassignedLeadsBreakdownView, path).status_code, 200)
        self.assertEqual(
            metrics_cls.return_value.get_unassigned_leads_breakdown.call_count, 1
        )

    @patch("analytics.cse_metrics.CseVisibilityResolver.clamp_cse_name_filter", side_effect=lambda n, e: n)
    @patch("analytics.cse_metrics.CseMetricsService")
    @patch("analytics.views._resolve_cse_analytics_context")
    @patch("analytics.views.CseOverviewSerializer")
    def test_cse_overview_second_call_hits_cache(
        self, serializer_cls, resolve_ctx, service_cls, _clamp
    ):
        from analytics.views import CseOverviewView

        resolve_ctx.return_value = (
            {"allowed_cse_emails": {"a@example.com"}, "visibility_scope": "team"},
            None,
        )
        serializer_cls.return_value.data = {"opened": 2}
        service_cls.return_value.get_overview.return_value = {"opened": 2}

        path = "/analytics/cse/overview/"
        query = {"from": "2026-01-01", "to": "2026-01-07"}
        self.assertEqual(self._call(CseOverviewView, path, query).status_code, 200)
        self.assertEqual(self._call(CseOverviewView, path, query).status_code, 200)
        self.assertEqual(service_cls.return_value.get_overview.call_count, 1)

    @patch("analytics.cse_metrics.CseVisibilityResolver.clamp_cse_name_filter", side_effect=lambda n, e: n)
    @patch("analytics.cse_metrics.CseMetricsService")
    @patch("analytics.views._resolve_cse_analytics_context")
    @patch("analytics.views.CseMemberBreakdownSerializer")
    def test_cse_members_second_call_hits_cache(
        self, serializer_cls, resolve_ctx, service_cls, _clamp
    ):
        from analytics.views import CseMembersView

        resolve_ctx.return_value = (
            {"allowed_cse_emails": {"a@example.com"}, "visibility_scope": "team"},
            None,
        )
        serializer_cls.return_value.data = [{"cse_name": "A"}]
        service_cls.return_value.get_member_breakdown.return_value = [{"cse_name": "A"}]

        path = "/analytics/cse/members/"
        query = {"from": "2026-01-01", "to": "2026-01-07"}
        self.assertEqual(self._call(CseMembersView, path, query).status_code, 200)
        self.assertEqual(self._call(CseMembersView, path, query).status_code, 200)
        self.assertEqual(service_cls.return_value.get_member_breakdown.call_count, 1)

    @patch("analytics.cse_metrics.CseVisibilityResolver.clamp_cse_name_filter", side_effect=lambda n, e: n)
    @patch("analytics.cse_metrics.CseMetricsService")
    @patch("analytics.views._resolve_cse_analytics_context")
    @patch("analytics.views.CseTimeSeriesSerializer")
    def test_cse_time_series_second_call_hits_cache(
        self, serializer_cls, resolve_ctx, service_cls, _clamp
    ):
        from analytics.views import CseTimeSeriesView

        resolve_ctx.return_value = (
            {"allowed_cse_emails": {"a@example.com"}, "visibility_scope": "team"},
            None,
        )
        serializer_cls.return_value.data = [{"date": "2026-01-01", "opened": 1}]
        service_cls.return_value.get_time_series.return_value = [
            {"date": "2026-01-01", "opened": 1}
        ]

        path = "/analytics/cse/time-series/"
        query = {"from": "2026-01-01", "to": "2026-01-07"}
        self.assertEqual(self._call(CseTimeSeriesView, path, query).status_code, 200)
        self.assertEqual(self._call(CseTimeSeriesView, path, query).status_code, 200)
        self.assertEqual(service_cls.return_value.get_time_series.call_count, 1)

    @patch("analytics.cse_metrics.CseMetricsService")
    @patch("analytics.views._resolve_cse_analytics_context")
    @patch("analytics.views.CseFilterOptionsSerializer")
    def test_cse_filter_options_second_call_hits_cache(
        self, serializer_cls, resolve_ctx, service_cls
    ):
        from analytics.views import CseFilterOptionsView

        resolve_ctx.return_value = (
            {"allowed_cse_emails": {"a@example.com"}, "visibility_scope": "team"},
            None,
        )
        serializer_cls.return_value.data = {"states": ["KA"]}
        service_cls.return_value.get_filter_options.return_value = {"states": ["KA"]}

        path = "/analytics/cse/filter-options/"
        self.assertEqual(self._call(CseFilterOptionsView, path).status_code, 200)
        self.assertEqual(self._call(CseFilterOptionsView, path).status_code, 200)
        self.assertEqual(service_cls.return_value.get_filter_options.call_count, 1)

    @patch("user_settings.services.get_rm_daily_targets_sum", return_value={11: 5})
    @patch("authz.models.TenantMembership.objects")
    def test_rm_daily_targets_second_call_hits_cache(self, membership_objects, targets_sum):
        from analytics.views import RmDailyTargetsView

        membership_objects.filter.return_value.values.return_value = [
            {"id": 11, "user_id": "rm-1"},
        ]

        path = "/analytics/rm-daily-targets/"
        query = {"from": "2026-01-01", "to": "2026-01-01"}
        response1 = self._call(RmDailyTargetsView, path, query)
        response2 = self._call(RmDailyTargetsView, path, query)
        self.assertEqual(response1.status_code, 200)
        self.assertEqual(response2.status_code, 200)
        self.assertEqual(response1.data, {"rm-1": 5})
        self.assertEqual(targets_sum.call_count, 1)

    @patch("user_settings.services.get_lead_filter_options", return_value={"lead_types": ["A"]})
    @patch("user_settings.services.kv_int_by_membership", return_value={})
    @patch("user_settings.models.Group.objects")
    @patch("authz.models.TenantMembership.objects")
    def test_rm_filter_options_second_call_hits_cache(
        self, membership_objects, group_objects, _kv, _lead_opts
    ):
        from analytics.views import RmPrdFilterOptionsView

        group_qs = MagicMock()
        group_objects.filter.return_value = group_qs
        group_qs.exclude.return_value = group_qs
        group_qs.values_list.return_value = group_qs
        group_qs.distinct.return_value = []

        path = "/analytics/rm-filter-options/"
        with patch(
            "user_settings.geo_party_catalog.resolve_catalog_name",
            return_value="State",
        ):
            def _membership_filter(**kwargs):
                qs = MagicMock()
                qs.filter.return_value = qs
                qs.exclude.return_value = qs
                qs.values_list.return_value = qs
                qs.distinct.return_value = qs
                qs.order_by.return_value = ["Mgr"]
                qs.__iter__ = lambda self: iter([])
                return qs

            membership_objects.filter.side_effect = _membership_filter

            response1 = self._call(RmPrdFilterOptionsView, path)
            response2 = self._call(RmPrdFilterOptionsView, path)

        self.assertEqual(response1.status_code, 200)
        self.assertEqual(response2.status_code, 200)
        self.assertEqual(_lead_opts.call_count, 1)

    @patch("analytics.views.distinct_data_values", side_effect=[["Open"], ["PosterA"]])
    @patch("analytics.views.support_ticket_records_qs")
    def test_ticket_filter_options_second_call_hits_cache(self, qs_mock, distinct_mock):
        from analytics.views import SupportTicketFilterOptionsView

        qs_mock.return_value = MagicMock()

        response1 = self._call(SupportTicketFilterOptionsView, "/analytics/support-ticket-filter/")
        distinct_mock.side_effect = [["Open"], ["PosterA"], ["Open"], ["PosterA"]]
        response2 = self._call(SupportTicketFilterOptionsView, "/analytics/support-ticket-filter/")

        self.assertEqual(response1.status_code, 200)
        self.assertEqual(response2.status_code, 200)
        self.assertEqual(response1.data["resolution_statuses"], ["Open"])
        self.assertEqual(distinct_mock.call_count, 2)

    def test_refresh_forces_team_overview_recompute(self):
        from analytics.views import TeamOverviewView

        with patch("analytics.views.TeamResolver.get_team_user_ids", return_value={"u1"}), patch(
            "analytics.views.TeamMetricsService"
        ) as metrics_cls, patch(
            "analytics.views.TeamOverviewSerializer"
        ) as serializer_cls:
            metrics = metrics_cls.return_value
            metrics.get_overview.return_value = {"attendance": 1}
            serializer_cls.return_value.data = {"attendance": 1}

            path = "/analytics/team/overview/"
            self._call(TeamOverviewView, path, {"date": "2026-01-15"})
            self._call(TeamOverviewView, path, {"date": "2026-01-15", "refresh": "1"})
            self.assertEqual(metrics.get_overview.call_count, 2)

    @patch("analytics.views.TeamOverviewSerializer")
    @patch("analytics.views.TeamMetricsService")
    @patch("analytics.views.TeamResolver.get_team_user_ids", return_value={"u1"})
    def test_team_overview_invalidation_forces_recompute(
        self, _team_ids, metrics_cls, serializer_cls
    ):
        from analytics.views import TeamOverviewView

        metrics = metrics_cls.return_value
        metrics.get_overview.return_value = {"attendance": 1}
        serializer_cls.return_value.data = {"attendance": 1}

        path = "/analytics/team/overview/"
        query = {"date": "2026-01-15"}
        self._call(TeamOverviewView, path, query)
        invalidate_team_analytics_cache(42)
        self._call(TeamOverviewView, path, query)
        self.assertEqual(metrics.get_overview.call_count, 2)
