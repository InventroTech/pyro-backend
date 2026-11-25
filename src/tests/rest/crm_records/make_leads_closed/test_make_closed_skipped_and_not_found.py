"""Leads make-closed must skip or report as missing, and how results are reported."""

from crm_records.models import EventLog, Record
from .helpers import MakeClosedAPITestCase


class MakeClosedSkippedTests(MakeClosedAPITestCase):
    def test_skips_a_lead_that_is_already_closed_and_clean(self):
        self.raw_record({"praja_id": "9001", "lead_stage": "CLOSED", "call_attempts": 4})

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["updated"], [])
        self.assertEqual(response.data["skipped"], [{"praja_id": "9001", "reason": "already_closed"}])

    def test_treats_lowercase_or_padded_closed_as_already_closed(self):
        self.raw_record({"praja_id": "1", "lead_stage": "closed"})
        self.raw_record({"praja_id": "2", "lead_stage": "  Closed "})

        response = self.post_ids(["1", "2"])

        self.assertEqual(
            response.data["skipped"],
            [{"praja_id": "1", "reason": "already_closed"}, {"praja_id": "2", "reason": "already_closed"}],
        )

    def test_does_not_touch_a_skipped_lead(self):
        lead = self.raw_record({"praja_id": "9001", "lead_stage": "CLOSED", "call_attempts": 4})
        before = (lead.data, lead.updated_at)

        self.post_ids(["9001"])

        lead.refresh_from_db()
        self.assertEqual((lead.data, lead.updated_at), before)

    def test_second_call_on_the_same_leads_skips_them(self):
        lead = self.lead()

        first = self.post_ids(["9001"])
        second = self.post_ids(["9001"])

        self.assertEqual(first.data["updated"], ["9001"])
        self.assertEqual(second.data["updated"], [])
        self.assertEqual(second.data["skipped"], [{"praja_id": "9001", "reason": "already_closed"}])
        self.assertEqual(EventLog.objects.filter(record=lead, event="lead.made_closed").count(), 1)


class MakeClosedNotFoundTests(MakeClosedAPITestCase):
    def test_reports_an_unknown_praja_id_as_not_found(self):
        response = self.post_ids(["999999"])

        self.assertEqual(response.data, {"updated": [], "skipped": [], "not_found": ["999999"]})

    def test_does_not_close_a_non_lead_record_with_the_same_praja_id(self):
        ticket = self.raw_record({"praja_id": "9001", "lead_stage": "ASSIGNED"}, entity_type="support_ticket")

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["not_found"], ["9001"])
        ticket.refresh_from_db()
        self.assertEqual(ticket.data["lead_stage"], "ASSIGNED")

    def test_does_not_close_a_soft_deleted_lead(self):
        lead = self.lead()
        lead.delete()

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["not_found"], ["9001"])
        deleted = Record.all_objects.get(pk=lead.pk)
        self.assertEqual(deleted.data["lead_stage"], "NOT_CONNECTED")

    def test_does_not_match_a_lead_without_a_praja_id(self):
        lead = self.raw_record({"name": "No praja id", "lead_stage": "FRESH"})

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["not_found"], ["9001"])
        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "FRESH")

    def test_does_not_match_on_part_of_a_praja_id(self):
        self.lead(praja_id="90011")

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["not_found"], ["9001"])


class MakeClosedResultReportingTests(MakeClosedAPITestCase):
    def test_reports_updated_skipped_and_not_found_together(self):
        self.lead(praja_id="1")
        self.raw_record({"praja_id": "2", "lead_stage": "CLOSED"})

        response = self.post_ids(["1", "2", "3"])

        self.assertEqual(
            response.data,
            {
                "updated": ["1"],
                "skipped": [{"praja_id": "2", "reason": "already_closed"}],
                "not_found": ["3"],
            },
        )

    def test_keeps_the_order_the_ids_were_sent_in(self):
        for praja_id in ("30", "10", "20"):
            self.lead(praja_id=praja_id)

        response = self.post_ids(["30", "10", "20"])

        self.assertEqual(response.data["updated"], ["30", "10", "20"])

    def test_reports_a_duplicated_id_once(self):
        lead = self.lead()

        response = self.post_ids(["9001", "9001", " 9001 ", 9001])

        self.assertEqual(response.data["updated"], ["9001"])
        self.assertEqual(EventLog.objects.filter(record=lead, event="lead.made_closed").count(), 1)

    def test_reports_duplicated_unknown_ids_once(self):
        response = self.post_ids(["404", "404"])

        self.assertEqual(response.data["not_found"], ["404"])
