from unittest.mock import patch

from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.test.utils import CaptureQueriesContext
from rest_framework import status

from crm_records.models import EventLog, Record
from object_history.models import ObjectHistory
from tests.base.test_setup import BaseAPITestCase
from tests.factories import TenantFactory


def _worked_lead(**extra):
    data = {
        "name": "Worked Lead",
        "praja_id": "9001",
        "lead_source": "referral",
        "lead_stage": "NOT_CONNECTED",
        "call_attempts": 3,
        "assigned_to": "rm-123",
        "last_call_outcome": "not_connected",
        "next_call_at": "2026-10-28T10:00:00+00:00",
        "snooze_unassign_at": "2026-10-28T12:00:00+00:00",
        "not_connected_unassign_at": "2026-10-28T12:00:00+00:00",
        "first_assigned_at": "2026-10-26T04:00:00+00:00",
        "first_assigned_to": "rm-123",
        "first_assigned_today_at": "2026-10-28T04:00:00+00:00",
        "first_assignment_today_date": "2026-10-28",
    }
    data.update(extra)
    return data


class MakeLeadsFreshApiTests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.url = "/crm-records/leads/make-fresh/"
        self.client.force_authenticate(user=self.user)
        self.headers = {
            **self.auth_headers,
            "HTTP_X_TENANT_SLUG": self.tenant.slug,
        }

    def _post(self, payload):
        return self.client.post(self.url, payload, format="json", **self.headers)

    def test_resets_selected_leads_to_fresh_pool(self):
        lead = Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data=_worked_lead(),
        )

        response = self._post({"praja_ids": ["9001"]})

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["updated"], ["9001"])
        self.assertEqual(response.data["skipped"], [])
        self.assertEqual(response.data["not_found"], [])

        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "FRESH")
        self.assertEqual(lead.data["call_attempts"], 0)
        self.assertEqual(lead.data["name"], "Worked Lead")
        self.assertEqual(lead.data["praja_id"], "9001")
        self.assertEqual(lead.data["lead_source"], "referral")
        for key in (
            "assigned_to",
            "last_call_outcome",
            "next_call_at",
            "snooze_unassign_at",
            "not_connected_unassign_at",
            "first_assigned_at",
            "first_assigned_to",
            "first_assigned_today_at",
            "first_assignment_today_date",
        ):
            self.assertNotIn(key, lead.data)

        event = EventLog.objects.get(record=lead, event="lead.made_fresh")
        self.assertEqual(event.payload["praja_id"], "9001")
        self.assertEqual(event.payload["record_id"], lead.id)
        self.assertEqual(event.payload["previous_lead_stage"], "NOT_CONNECTED")
        self.assertEqual(event.payload["previous_call_attempts"], 3)
        self.assertEqual(event.payload["previous_assigned_to"], "rm-123")
        self.assertEqual(event.payload["actor"], self.email)

    def test_matches_praja_ids_stored_as_numbers_or_text(self):
        numeric = Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data=_worked_lead(praja_id=445566),
        )
        text = Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data=_worked_lead(praja_id="PRAJA-1"),
        )

        response = self._post({"praja_ids": [445566, " PRAJA-1 "]})

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["updated"], ["445566", "PRAJA-1"])
        numeric.refresh_from_db()
        text.refresh_from_db()
        self.assertEqual(numeric.data["lead_stage"], "FRESH")
        self.assertEqual(text.data["lead_stage"], "FRESH")

    def test_resets_every_stage_and_reports_missing_ids(self):
        stages = {"1": "CLOSED", "2": "trial_activated", "3": "NOT_INTERESTED", "4": "SNOOZED"}
        leads = {
            praja_id: Record.objects.create(
                tenant=self.tenant,
                entity_type="lead",
                data=_worked_lead(name=stage, lead_stage=stage, praja_id=praja_id),
            )
            for praja_id, stage in stages.items()
        }
        Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data={"name": "Already", "lead_stage": "FRESH", "call_attempts": 0, "praja_id": "6"},
        )
        Record.objects.create(
            tenant=self.tenant,
            entity_type="support_ticket",
            data={"name": "Ticket", "lead_stage": "ASSIGNED", "praja_id": "5"},
        )

        response = self._post({"praja_ids": ["1", "2", "3", "4", "5", "6", "999999"]})

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["updated"], ["1", "2", "3", "4"])
        self.assertEqual(response.data["skipped"], [{"praja_id": "6", "reason": "already_fresh"}])
        self.assertEqual(response.data["not_found"], ["5", "999999"])

        for lead in leads.values():
            lead.refresh_from_db()
            self.assertEqual(lead.data["lead_stage"], "FRESH")
            self.assertEqual(lead.data["call_attempts"], 0)
            self.assertNotIn("assigned_to", lead.data)
        closed_event = EventLog.objects.get(record=leads["1"], event="lead.made_fresh")
        self.assertEqual(closed_event.payload["previous_lead_stage"], "CLOSED")

    def test_does_not_reset_another_tenants_lead(self):
        other = TenantFactory()
        lead = Record.objects.create(
            tenant=other,
            entity_type="lead",
            data=_worked_lead(name="Other tenant"),
        )

        response = self._post({"praja_ids": ["9001"]})

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["updated"], [])
        self.assertEqual(response.data["not_found"], ["9001"])
        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "NOT_CONNECTED")
        self.assertEqual(lead.data["assigned_to"], "rm-123")

    def test_rejects_empty_or_invalid_ids(self):
        empty = self._post({"praja_ids": []})
        self.assertEqual(empty.status_code, status.HTTP_400_BAD_REQUEST)

        missing = self._post({})
        self.assertEqual(missing.status_code, status.HTTP_400_BAD_REQUEST)

        bad = self._post({"praja_ids": ["has space"]})
        self.assertEqual(bad.status_code, status.HTTP_400_BAD_REQUEST)

        record_ids_only = self._post({"record_ids": [1]})
        self.assertEqual(record_ids_only.status_code, status.HTTP_400_BAD_REQUEST)

    def test_accepts_more_than_500_ids_across_batches(self):
        leads = [
            Record.objects.create(
                tenant=self.tenant,
                entity_type="lead",
                data=_worked_lead(praja_id=str(praja_id)),
            )
            for praja_id in (1, 250, 700)
        ]
        praja_ids = [str(n) for n in range(1, 701)]

        with patch("crm_records.make_fresh.BATCH_SIZE", 100):
            response = self._post({"praja_ids": praja_ids})

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["updated"], ["1", "250", "700"])
        self.assertEqual(len(response.data["not_found"]), 697)
        for lead in leads:
            lead.refresh_from_db()
            self.assertEqual(lead.data["lead_stage"], "FRESH")
            self.assertNotIn("assigned_to", lead.data)

    def test_writes_record_history_for_each_reset_lead(self):
        lead = Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data=_worked_lead(),
        )
        content_type = ContentType.objects.get_for_model(Record)
        versions_before = ObjectHistory.objects.filter(
            content_type=content_type, object_id=str(lead.id)
        ).count()

        response = self._post({"praja_ids": ["9001"]})

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        history = ObjectHistory.objects.filter(
            content_type=content_type, object_id=str(lead.id)
        ).order_by("-version")
        self.assertEqual(history.count(), versions_before + 1)
        latest = history.first()
        self.assertEqual(latest.version, versions_before + 1)
        self.assertEqual(latest.metadata["action"], "make_fresh")
        self.assertEqual(latest.changes["lead_stage"], {"from": "NOT_CONNECTED", "to": "FRESH"})
        self.assertEqual(latest.changes["assigned_to"], {"from": "rm-123", "to": None})

    def test_query_count_does_not_grow_with_lead_count(self):
        def run(praja_ids):
            for praja_id in praja_ids:
                Record.objects.create(
                    tenant=self.tenant,
                    entity_type="lead",
                    data=_worked_lead(praja_id=praja_id),
                )
            with CaptureQueriesContext(connection) as queries:
                response = self._post({"praja_ids": praja_ids})
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
            self.assertEqual(len(response.data["updated"]), len(praja_ids))
            return len(queries)

        run(["warm-up"])
        small = run([f"s{n}" for n in range(2)])
        large = run([f"l{n}" for n in range(60)])

        self.assertEqual(large, small)

    def test_second_call_is_already_fresh(self):
        lead = Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data=_worked_lead(),
        )

        first = self._post({"praja_ids": ["9001"]})
        second = self._post({"praja_ids": ["9001"]})

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data["updated"], ["9001"])
        self.assertEqual(second.data["skipped"], [{"praja_id": "9001", "reason": "already_fresh"}])
        self.assertEqual(EventLog.objects.filter(record=lead, event="lead.made_fresh").count(), 1)
