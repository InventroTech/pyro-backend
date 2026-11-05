"""
Tests for RmPerformanceSummaryView — the Performance tab's per-RM numbers
(touches, unique leads, latest-touch-per-lead disposition counts) computed
server-side via DISTINCT ON/GROUP BY instead of shipping every raw
CALL_TOUCH row to the browser to sum. See the view's own docstring for why:
a busy tenant can log 15-20k+ touches in a single day, and the old
client-side aggregation of a multi-day range was taking 10+ minutes.
"""
from datetime import timedelta

from django.urls import reverse
from django.utils import timezone
from rest_framework import status

from analytics.models import RmActivityEvent
from tests.base.test_setup import BaseAPITestCase, MultiTenantAPITestCase
from tests.factories import TenantMembershipFactory


class RmPerformanceSummaryViewTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.url = reverse("analytics:rm-performance-summary")
        self.now = timezone.now()
        self.wide_window = {
            "from": (self.now - timedelta(days=1)).strftime("%Y-%m-%d"),
            "to": (self.now + timedelta(days=1)).strftime("%Y-%m-%d"),
        }

    def make_touch(
        self,
        rm_user_id="rm-1",
        rm_name="Asha",
        manager_name="Manager One",
        state="KA",
        lead_record_id=1,
        updated_status="TRIAL_ACTIVATED",
        started_at=None,
        ended_at=None,
    ):
        started_at = started_at or self.now
        ended_at = ended_at or started_at
        RmActivityEvent.objects.create(
            tenant=self.tenant,
            event_type="CALL_TOUCH",
            event_data={
                "rm_user_id": rm_user_id,
                "rm_name": rm_name,
                "manager_name": manager_name,
                "team": "",
                "state": state,
                "lead_record_id": lead_record_id,
                "updated_status": updated_status,
                "lead_group": None,
                "party": None,
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "duration_seconds": 60,
            },
        )

    def test_requires_authentication(self):
        response = self.client.get(self.url)
        self.assertIn(response.status_code, (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN))

    def test_response_shape_is_by_rm_plus_team(self):
        self.make_touch(lead_record_id=1, updated_status="TRIAL_ACTIVATED")
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(set(response.data.keys()), {"by_rm", "team"})

    def test_counts_touches_and_unique_leads(self):
        self.make_touch(lead_record_id=1, updated_status="NOT_CONNECTED")
        self.make_touch(lead_record_id=1, updated_status="CALL_BACK")  # same lead, second touch
        self.make_touch(lead_record_id=2, updated_status="TRIAL_ACTIVATED")

        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        row = response.data["by_rm"][0]
        self.assertEqual(row["touches"], 3)
        self.assertEqual(row["unique_leads"], 2)  # lead 1 counted once despite 2 touches

    def test_disposition_counts_use_only_each_leads_latest_touch(self):
        # lead 1: NOT_CONNECTED then later CALL_BACK — only CALL_BACK (the
        # latest) should count, matching the dashboard's "classified by most
        # recent touch" rule
        self.make_touch(
            lead_record_id=1, updated_status="NOT_CONNECTED",
            started_at=self.now - timedelta(minutes=10), ended_at=self.now - timedelta(minutes=9),
        )
        self.make_touch(
            lead_record_id=1, updated_status="CALL_BACK",
            started_at=self.now - timedelta(minutes=5), ended_at=self.now - timedelta(minutes=4),
        )

        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        row = response.data["by_rm"][0]
        self.assertEqual(row["disposition_counts"], {"CALL_BACK": 1})
        self.assertEqual(row["touches"], 2)  # touches still counts BOTH raw touches

    def test_exact_started_at_tie_keeps_the_earlier_inserted_touch(self):
        # mirrors the old client-side latestTouchPerLead: on an exact tie it
        # keeps whichever was encountered (inserted) first, not an arbitrary one
        tie_time = self.now
        self.make_touch(lead_record_id=1, updated_status="NOT_CONNECTED", started_at=tie_time, ended_at=tie_time)
        self.make_touch(lead_record_id=1, updated_status="TRIAL_ACTIVATED", started_at=tie_time, ended_at=tie_time)

        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        row = response.data["by_rm"][0]
        self.assertEqual(row["disposition_counts"], {"NOT_CONNECTED": 1})

    def test_separates_rows_by_rm(self):
        self.make_touch(rm_user_id="rm-1", rm_name="Asha", lead_record_id=1, updated_status="TRIAL_ACTIVATED")
        self.make_touch(rm_user_id="rm-2", rm_name="Bala", lead_record_id=2, updated_status="NOT_CONNECTED")

        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        by_rm = {row["rm_user_id"]: row for row in response.data["by_rm"]}
        self.assertEqual(set(by_rm.keys()), {"rm-1", "rm-2"})
        self.assertEqual(by_rm["rm-1"]["rm_name"], "Asha")
        self.assertEqual(by_rm["rm-2"]["rm_name"], "Bala")

    def test_rm_user_id_scopes_to_one_rm(self):
        self.make_touch(rm_user_id="rm-1", updated_status="TRIAL_ACTIVATED")
        self.make_touch(rm_user_id="rm-2", updated_status="NOT_CONNECTED")

        response = self.client.get(
            self.url, data={**self.wide_window, "rm_user_id": "rm-1"}, **self.auth_headers
        )
        self.assertEqual(len(response.data["by_rm"]), 1)
        self.assertEqual(response.data["by_rm"][0]["rm_user_id"], "rm-1")

    def test_excludes_touches_outside_the_date_range(self):
        self.make_touch(
            lead_record_id=1, updated_status="TRIAL_ACTIVATED",
            started_at=self.now - timedelta(days=10), ended_at=self.now - timedelta(days=10),
        )

        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        self.assertEqual(response.data["by_rm"], [])
        self.assertEqual(response.data["team"], {"touches": 0, "unique_leads": 0, "disposition_counts": {}})

    def test_ignores_non_call_touch_events(self):
        RmActivityEvent.objects.create(
            tenant=self.tenant,
            event_type="LOGIN",
            event_data={
                "rm_user_id": "rm-1", "rm_name": "Asha", "manager_name": "", "team": "", "state": "",
                "started_at": self.now.isoformat(), "ended_at": None, "duration_seconds": None,
            },
        )
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        self.assertEqual(response.data["by_rm"], [])

    def test_a_null_duration_call_with_no_disposition_still_counts_as_a_touch_but_not_a_disposition(self):
        # record_lead_touch_event writes updated_status: None only in
        # pathological cases; a lead with no disposition can't be classified
        self.make_touch(lead_record_id=1, updated_status=None)
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        row = response.data["by_rm"][0]
        self.assertEqual(row["touches"], 1)
        self.assertEqual(row["disposition_counts"], {})


class RmPerformanceSummaryViewTeamTotalsTest(BaseAPITestCase):
    """`team` is NOT just a sum of the by_rm rows — a lead reassigned
    between two RMs within the range must count once at the team level,
    even though it appears in both RMs' own rows (matching the old
    client-side computeTeamTotals' global, not per-RM, dedup)."""

    def setUp(self):
        super().setUp()
        self.url = reverse("analytics:rm-performance-summary")
        self.now = timezone.now()
        self.wide_window = {
            "from": (self.now - timedelta(days=1)).strftime("%Y-%m-%d"),
            "to": (self.now + timedelta(days=1)).strftime("%Y-%m-%d"),
        }

    def make_touch(self, rm_user_id, lead_record_id, updated_status, started_at, ended_at=None):
        ended_at = ended_at or started_at
        RmActivityEvent.objects.create(
            tenant=self.tenant,
            event_type="CALL_TOUCH",
            event_data={
                "rm_user_id": rm_user_id, "rm_name": rm_user_id, "manager_name": "", "team": "", "state": "",
                "lead_record_id": lead_record_id, "updated_status": updated_status,
                "lead_group": None, "party": None,
                "started_at": started_at.isoformat(), "ended_at": ended_at.isoformat(), "duration_seconds": 60,
            },
        )

    def test_a_lead_reassigned_between_two_rms_counts_once_at_team_level(self):
        # lead 1: rm-1 touches it first (NOT_CONNECTED), then it's
        # reassigned to rm-2 who touches it later (TRIAL_ACTIVATED) — each
        # RM's own by_rm row sees it as 1 of their own unique leads, but the
        # team total must count it once, classified by the true latest touch
        self.make_touch("rm-1", lead_record_id=1, updated_status="NOT_CONNECTED", started_at=self.now - timedelta(minutes=10))
        self.make_touch("rm-2", lead_record_id=1, updated_status="TRIAL_ACTIVATED", started_at=self.now - timedelta(minutes=5))
        self.make_touch("rm-2", lead_record_id=2, updated_status="NOT_CONNECTED", started_at=self.now)

        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        by_rm = {row["rm_user_id"]: row for row in response.data["by_rm"]}

        # each RM's own row still honestly reports lead 1 as their own unique lead
        self.assertEqual(by_rm["rm-1"]["unique_leads"], 1)
        self.assertEqual(by_rm["rm-2"]["unique_leads"], 2)

        # naive sum would be 1 + 2 = 3 distinct leads; the true team total is 2
        # (lead 1 counted once, lead 2 once) — this is exactly why `team`
        # can't be derived by summing the by_rm rows on the frontend
        team = response.data["team"]
        self.assertEqual(team["unique_leads"], 2)
        self.assertEqual(team["touches"], 3)
        # classified by the TRUE latest touch (rm-2's TRIAL_ACTIVATED), not
        # rm-1's earlier NOT_CONNECTED
        self.assertEqual(team["disposition_counts"], {"TRIAL_ACTIVATED": 1, "NOT_CONNECTED": 1})


class RmPerformanceSummaryViewFilterBarParityTest(BaseAPITestCase):
    """manager_name/lead_group/state/party must narrow this endpoint
    exactly the way the filter bar narrows the raw events feed client-side
    (visibleEvents) — otherwise picking one of these filters would show
    different numbers on Performance than on Adherence for the exact same
    selection, which is the one thing this endpoint must never do."""

    def setUp(self):
        super().setUp()
        self.url = reverse("analytics:rm-performance-summary")
        self.now = timezone.now()
        self.wide_window = {
            "from": (self.now - timedelta(days=1)).strftime("%Y-%m-%d"),
            "to": (self.now + timedelta(days=1)).strftime("%Y-%m-%d"),
        }

    def make_touch(self, lead_record_id, manager_name="Mgr A", lead_group="Group A", state="KA", party="BJP"):
        RmActivityEvent.objects.create(
            tenant=self.tenant,
            event_type="CALL_TOUCH",
            event_data={
                "rm_user_id": "rm-1", "rm_name": "Asha", "manager_name": manager_name, "team": "",
                "state": state, "lead_record_id": lead_record_id, "updated_status": "TRIAL_ACTIVATED",
                "lead_group": lead_group, "party": party,
                "started_at": self.now.isoformat(), "ended_at": self.now.isoformat(), "duration_seconds": 60,
            },
        )

    def test_manager_name_filters_to_exact_match(self):
        self.make_touch(lead_record_id=1, manager_name="Mgr A")
        self.make_touch(lead_record_id=2, manager_name="Mgr B")
        response = self.client.get(
            self.url, data={**self.wide_window, "manager_name": "Mgr A"}, **self.auth_headers
        )
        self.assertEqual(response.data["by_rm"][0]["touches"], 1)
        self.assertEqual(response.data["team"]["touches"], 1)

    def test_lead_group_filters_to_exact_match(self):
        self.make_touch(lead_record_id=1, lead_group="Group A")
        self.make_touch(lead_record_id=2, lead_group="Group B")
        response = self.client.get(
            self.url, data={**self.wide_window, "lead_group": "Group B"}, **self.auth_headers
        )
        self.assertEqual(response.data["by_rm"][0]["touches"], 1)
        self.assertEqual(response.data["team"]["touches"], 1)

    def test_state_filters_to_exact_match(self):
        self.make_touch(lead_record_id=1, state="KA")
        self.make_touch(lead_record_id=2, state="TN")
        response = self.client.get(self.url, data={**self.wide_window, "state": "TN"}, **self.auth_headers)
        self.assertEqual(response.data["by_rm"][0]["touches"], 1)
        self.assertEqual(response.data["team"]["touches"], 1)

    def test_party_filters_to_exact_match(self):
        self.make_touch(lead_record_id=1, party="BJP")
        self.make_touch(lead_record_id=2, party="DMK")
        response = self.client.get(self.url, data={**self.wide_window, "party": "DMK"}, **self.auth_headers)
        self.assertEqual(response.data["by_rm"][0]["touches"], 1)
        self.assertEqual(response.data["team"]["touches"], 1)

    def test_no_filters_returns_everything(self):
        self.make_touch(lead_record_id=1, manager_name="Mgr A")
        self.make_touch(lead_record_id=2, manager_name="Mgr B")
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        self.assertEqual(response.data["by_rm"][0]["touches"], 2)
        self.assertEqual(response.data["team"]["touches"], 2)


class RmPerformanceSummaryViewTenantIsolationTest(MultiTenantAPITestCase):
    def setUp(self):
        super().setUp()
        self.url = reverse("analytics:rm-performance-summary")
        now = timezone.now()

        def make_event(tenant, rm_name):
            RmActivityEvent.objects.create(
                tenant=tenant,
                event_type="CALL_TOUCH",
                event_data={
                    "rm_user_id": "rm-1",
                    "rm_name": rm_name,
                    "manager_name": "",
                    "team": "",
                    "state": "",
                    "lead_record_id": 1,
                    "updated_status": "TRIAL_ACTIVATED",
                    "lead_group": None,
                    "party": None,
                    "started_at": now.isoformat(),
                    "ended_at": now.isoformat(),
                    "duration_seconds": 60,
                },
            )

        make_event(self.tenant, "Mine")
        make_event(self.tenant_b, "Other")

        self.wide_window = {
            "from": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
            "to": (now + timedelta(days=1)).strftime("%Y-%m-%d"),
        }

    def test_only_returns_request_tenants_rows(self):
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        names = {row["rm_name"] for row in response.data["by_rm"]}
        self.assertIn("Mine", names)
        self.assertNotIn("Other", names)

    def test_other_tenant_only_sees_its_own_row(self):
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers_b)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        names = {row["rm_name"] for row in response.data["by_rm"]}
        self.assertIn("Other", names)
        self.assertNotIn("Mine", names)


class RmPerformanceSummaryViewManagerScopingTest(BaseAPITestCase):
    """manager_user_id scopes to the signed-in manager's own hierarchy via
    TeamResolver — same precedent as RmActivityEventListView."""

    def setUp(self):
        super().setUp()
        self.url = reverse("analytics:rm-performance-summary")
        now = timezone.now()

        self.direct_report = TenantMembershipFactory(tenant=self.tenant, user_parent_id=self.membership)
        self.outside_rm = TenantMembershipFactory(tenant=self.tenant)

        def make_event(rm_user_id, rm_name):
            RmActivityEvent.objects.create(
                tenant=self.tenant,
                event_type="CALL_TOUCH",
                event_data={
                    "rm_user_id": rm_user_id,
                    "rm_name": rm_name,
                    "manager_name": "",
                    "team": "",
                    "state": "",
                    "lead_record_id": 1,
                    "updated_status": "TRIAL_ACTIVATED",
                    "lead_group": None,
                    "party": None,
                    "started_at": now.isoformat(),
                    "ended_at": now.isoformat(),
                    "duration_seconds": 60,
                },
            )

        make_event(self.direct_report.user_id, "In Team")
        make_event(self.outside_rm.user_id, "Outside Team")

        self.wide_window = {
            "from": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
            "to": (now + timedelta(days=1)).strftime("%Y-%m-%d"),
        }

    def test_manager_user_id_scopes_to_own_hierarchy_only(self):
        response = self.client.get(
            self.url, data={**self.wide_window, "manager_user_id": self.supabase_uid}, **self.auth_headers
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        names = {row["rm_name"] for row in response.data["by_rm"]}
        self.assertIn("In Team", names)
        self.assertNotIn("Outside Team", names)

    def test_without_manager_user_id_returns_whole_tenant(self):
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        names = {row["rm_name"] for row in response.data["by_rm"]}
        self.assertIn("In Team", names)
        self.assertIn("Outside Team", names)
