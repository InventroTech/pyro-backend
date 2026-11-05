"""Audit trail: the lead.made_closed event and object history (HistoryEngine)."""

from django.contrib.contenttypes.models import ContentType

from crm_records.models import EventLog, Record
from object_history.models import ObjectHistory
from .helpers import MakeClosedAPITestCase


class MakeClosedEventTests(MakeClosedAPITestCase):
    def test_writes_one_made_closed_event_per_closed_lead(self):
        leads = [self.lead(praja_id=str(n)) for n in (1, 2)]

        self.post_ids(["1", "2"])

        for lead in leads:
            self.assertEqual(EventLog.objects.filter(record=lead, event="lead.made_closed").count(), 1)

    def test_event_payload_keeps_the_previous_state(self):
        lead = self.lead()

        self.post_ids(["9001"])

        event = EventLog.objects.get(record=lead, event="lead.made_closed")
        self.assertEqual(
            event.payload,
            {
                "record_id": lead.id,
                "praja_id": "9001",
                "actor": self.email,
                "previous_lead_stage": "NOT_CONNECTED",
                "previous_call_attempts": 3,
                "previous_assigned_to": "rm-123",
            },
        )

    def test_writes_no_event_for_skipped_or_missing_leads(self):
        skipped = self.raw_record({"praja_id": "1", "lead_stage": "CLOSED"})

        self.post_ids(["1", "404"])

        self.assertFalse(EventLog.objects.filter(event="lead.made_closed").exists())
        self.assertFalse(EventLog.objects.filter(record=skipped).exists())

    def test_does_not_write_a_made_fresh_event(self):
        self.lead()

        self.post_ids(["9001"])

        self.assertFalse(EventLog.objects.filter(event="lead.made_fresh").exists())


class MakeClosedObjectHistoryTests(MakeClosedAPITestCase):
    def history_for(self, record):
        content_type = ContentType.objects.get_for_model(Record)
        return ObjectHistory.objects.filter(content_type=content_type, object_id=str(record.id)).order_by("-version")

    def test_adds_one_history_version_per_closed_lead(self):
        leads = [self.lead(praja_id=str(n)) for n in (1, 2)]
        before = {lead.id: self.history_for(lead).count() for lead in leads}

        self.post_ids(["1", "2"])

        for lead in leads:
            self.assertEqual(self.history_for(lead).count(), before[lead.id] + 1)

    def test_new_history_version_number_follows_the_previous_one(self):
        lead = self.lead()
        before = self.history_for(lead).count()

        self.post_ids(["9001"])

        self.assertEqual(self.history_for(lead).first().version, before + 1)

    def test_history_marks_the_action_as_make_closed(self):
        lead = self.lead()

        self.post_ids(["9001"])

        self.assertEqual(self.history_for(lead).first().metadata["action"], "make_closed")

    def test_history_shows_stage_and_assignee_changes(self):
        lead = self.lead()

        self.post_ids(["9001"])

        changes = self.history_for(lead).first().changes
        self.assertEqual(changes["lead_stage"], {"from": "NOT_CONNECTED", "to": "CLOSED"})
        self.assertEqual(changes["assigned_to"], {"from": "rm-123", "to": None})

    def test_history_does_not_show_call_attempts_as_changed(self):
        lead = self.lead()

        self.post_ids(["9001"])

        self.assertNotIn("call_attempts", self.history_for(lead).first().changes)

    def test_adds_no_history_for_a_skipped_lead(self):
        lead = self.raw_record({"praja_id": "9001", "lead_stage": "CLOSED"})
        before = self.history_for(lead).count()

        self.post_ids(["9001"])

        self.assertEqual(self.history_for(lead).count(), before)

    def test_adds_no_history_when_the_request_is_rejected(self):
        lead = self.lead()
        before = self.history_for(lead).count()

        self.post_ids(["9001", "bad id"])

        self.assertEqual(self.history_for(lead).count(), before)
