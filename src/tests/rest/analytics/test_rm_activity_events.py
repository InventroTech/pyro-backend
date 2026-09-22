"""
Tests for the RM PRD analytics activity log:
- record_lead_touch_event() only writes a CALL_TOUCH row for the 4 lead
  disposition events, never for anything else, and attributes it to whoever
  is actually authenticated (not a user id read out of the payload).
- RmActivityEventListView never leaks another tenant's rows.
"""
from datetime import timedelta

from django.urls import reverse
from django.utils import timezone
from rest_framework import status

from analytics.models import RmActivityEvent
from analytics.rm_activity import record_lead_touch_event
from crm_records.models import Record
from tests.base.test_setup import BaseAPITestCase, MultiTenantAPITestCase


class RecordLeadTouchEventTests(BaseAPITestCase):
    """Unit-level: exercises record_lead_touch_event() directly, no HTTP."""

    def setUp(self):
        super().setUp()
        self.record = Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data={"lead_bucket": "hot", "affiliated_party": "BJP"},
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
                    "lead_bucket": None,
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
