"""
Tests for the RM PRD analytics activity log:
- record_lead_touch_event() only writes a CALL_TOUCH row for the 4 lead
  disposition events, never for anything else, and attributes it to whoever
  is actually authenticated (not a user id read out of the payload).
- RmActivityEventListView never leaks another tenant's rows.
- RmActivityEventListView's manager_user_id param (RM PRD's ASM "my team"
  view) scopes to the signed-in manager's own hierarchy, not the whole tenant.
"""
from datetime import timedelta

from django.urls import reverse
from django.utils import timezone
from rest_framework import status

from analytics.models import RmActivityEvent
from analytics.rm_activity import record_lead_touch_event
from crm_records.models import Record
from tests.base.test_setup import BaseAPITestCase, MultiTenantAPITestCase
from tests.factories import TenantMembershipFactory
from user_settings.models import Group, TenantMemberSetting
from user_settings.services import USER_KV_GROUP_ID_KEY


class RecordLeadTouchEventTests(BaseAPITestCase):
    """Unit-level: exercises record_lead_touch_event() directly, no HTTP."""

    def setUp(self):
        super().setUp()
        self.record = Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data={"lead_bucket": "hot", "affiliated_party": "BJP", "praja_id": "1793876"},
        )

    def test_disposition_events_each_write_one_call_touch_row(self):
        cases = [
            ("lead.call_not_connected", "NOT_CONNECTED"),
            ("lead.call_back_later", "CALL_BACK"),
            ("lead.not_interested", "NOT_INTERESTED"),
            ("lead.trial_activated", "TRIAL_ACTIVATED"),
        ]
        for event_name, expected_status in cases:
            with self.subTest(event_name=event_name):
                before = RmActivityEvent.objects.count()
                record_lead_touch_event(
                    event_name,
                    self.record,
                    {"duration_seconds": 42},
                    self.tenant,
                    self.user,
                )
                self.assertEqual(RmActivityEvent.objects.count(), before + 1)

                row = RmActivityEvent.objects.latest("id")
                self.assertEqual(row.event_type, "CALL_TOUCH")
                self.assertEqual(row.event_data["updated_status"], expected_status)
                self.assertEqual(row.event_data["rm_user_id"], str(self.supabase_uid))
                self.assertEqual(row.event_data["praja_id"], "1793876")

    def test_non_disposition_event_writes_nothing(self):
        before = RmActivityEvent.objects.count()
        record_lead_touch_event(
            "button_click",
            self.record,
            {"duration_seconds": 42},
            self.tenant,
            self.user,
        )
        self.assertEqual(RmActivityEvent.objects.count(), before)

    def test_rm_id_comes_from_request_user_not_payload(self):
        """A spoofed user id in the payload must be ignored."""
        record_lead_touch_event(
            "lead.trial_activated",
            self.record,
            {"duration_seconds": 10, "user_supabase_uid": "someone-elses-uid"},
            self.tenant,
            self.user,
        )
        row = RmActivityEvent.objects.latest("id")
        self.assertEqual(row.event_data["rm_user_id"], str(self.supabase_uid))
        self.assertNotEqual(row.event_data["rm_user_id"], "someone-elses-uid")

    def test_no_tenant_writes_nothing(self):
        before = RmActivityEvent.objects.count()
        record_lead_touch_event(
            "lead.trial_activated",
            self.record,
            {"duration_seconds": 10},
            None,
            self.user,
        )
        self.assertEqual(RmActivityEvent.objects.count(), before)


class LeadGroupResolutionTests(BaseAPITestCase):
    """
    lead_group isn't a field on the lead record — it's the touching RM's own
    GROUP assignment (user_settings.Group), resolved at write time from the
    same GROUP user setting the Add/Edit User screen and Lead Groups page
    use — not a crm_records.lead_pipeline Bucket (a lead's Bucket match was
    never a reliable proxy for "which group is this RM working in").
    """

    def setUp(self):
        super().setUp()
        self.record = Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data={"assigned_to": self.supabase_uid},
        )

    def test_resolves_the_touching_rms_own_group_name(self):
        group = Group.objects.create(tenant=self.tenant, name="Tamil Nadu Group")
        TenantMemberSetting.objects.create(
            tenant=self.tenant, tenant_membership=self.membership, key=USER_KV_GROUP_ID_KEY, value=group.id,
        )
        record_lead_touch_event(
            "lead.trial_activated", self.record, {"duration_seconds": 10}, self.tenant, self.user
        )
        row = RmActivityEvent.objects.latest("id")
        self.assertEqual(row.event_data["lead_group"], "Tamil Nadu Group")

    def test_no_group_assigned_resolves_to_none(self):
        record_lead_touch_event(
            "lead.trial_activated", self.record, {"duration_seconds": 10}, self.tenant, self.user
        )
        row = RmActivityEvent.objects.latest("id")
        self.assertIsNone(row.event_data["lead_group"])


class RmActivityEventListViewTenantIsolationTest(MultiTenantAPITestCase):
    def setUp(self):
        super().setUp()
        self.url = reverse("analytics:rm-activity-events")
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

    def test_requires_authentication(self):
        response = self.client.get(self.url)
        self.assertIn(response.status_code, (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN))

    def test_list_only_returns_request_tenants_events(self):
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        rows = response.data["data"]
        rm_names = {row["event_data"]["rm_name"] for row in rows}
        self.assertIn("Mine", rm_names)
        self.assertNotIn("Other", rm_names)

    def test_other_tenant_only_sees_its_own_event(self):
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers_b)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        rows = response.data["data"]
        rm_names = {row["event_data"]["rm_name"] for row in rows}
        self.assertIn("Other", rm_names)
        self.assertNotIn("Mine", rm_names)


class RmActivityEventListViewManagerScopingTest(BaseAPITestCase):
    """manager_user_id (RM PRD's ASM 'my team' view) scopes to the signed-in
    manager's own hierarchy via TenantMembership, not a manager_name text
    match — two managers sharing a display name must not leak into each
    other's view."""

    def setUp(self):
        super().setUp()
        self.url = reverse("analytics:rm-activity-events")
        now = timezone.now()

        # self.membership (BaseAPITestCase) is the signed-in "ASM" for this test.
        self.direct_report = TenantMembershipFactory(
            tenant=self.tenant, user_parent_id=self.membership,
        )
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
            self.url,
            data={**self.wide_window, "manager_user_id": self.supabase_uid},
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        rm_names = {row["event_data"]["rm_name"] for row in response.data["data"]}
        self.assertIn("In Team", rm_names)
        self.assertNotIn("Outside Team", rm_names)

    def test_without_manager_user_id_returns_whole_tenant(self):
        response = self.client.get(self.url, data=self.wide_window, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        rm_names = {row["event_data"]["rm_name"] for row in response.data["data"]}
        self.assertIn("In Team", rm_names)
        self.assertIn("Outside Team", rm_names)
