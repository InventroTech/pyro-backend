from django.urls import reverse
from rest_framework import status

from crm_records.models import EventLog, Record
from support_ticket.constants import (
    SUPPORT_EVENT_NOT_CONNECTED,
    SUPPORT_EVENT_RESOLVED,
    SUPPORT_EVENT_TAKE_BREAK,
    SUPPORT_TICKET_ENTITY_TYPE,
)
from support_ticket.events import dispatch_support_ticket_event
from tests.rest.support_ticket.support_rules import seed_support_ticket_rules
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_dump_factory import dump_data


class SupportTicketEventHandlerTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        seed_support_ticket_rules(self.tenant)
        self.record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="cust_1",
                support_ticket_type="Self_Trial",
                call_attempts=0,
                assigned_to=self.supabase_uid,
                cse_name=self.email,
            ),
        )

    def test_not_connected_snoozes_then_closes_per_rules(self):
        for expected_attempts in range(1, 6):
            dispatch_support_ticket_event(
                SUPPORT_EVENT_NOT_CONNECTED,
                self.record,
                {"cse_remarks": "no answer"} if expected_attempts == 1 else {},
            )
            self.record.refresh_from_db()
            self.assertEqual(self.record.data["call_attempts"], expected_attempts)
            self.assertEqual(self.record.data["resolution_status"], "Snoozed")
            self.assertIsNone(self.record.data.get("assigned_to"))
            self.assertIsNotNone(self.record.data.get("snooze_until"))

        dispatch_support_ticket_event(
            SUPPORT_EVENT_NOT_CONNECTED,
            self.record,
            {},
        )
        self.record.refresh_from_db()
        self.assertEqual(self.record.data["call_attempts"], 6)
        self.assertEqual(self.record.data["resolution_status"], "Closed")

    def test_take_break_unassigns_per_rules(self):
        self.record.data = {
            **self.record.data,
            "resolution_status": "Resolved",
            "assigned_to": self.supabase_uid,
            "cse_name": self.email,
        }
        self.record.save(update_fields=["data"])

        dispatch_support_ticket_event(
            SUPPORT_EVENT_TAKE_BREAK,
            self.record,
            {"resolutionStatus": "Resolved"},
        )
        self.record.refresh_from_db()
        self.assertIsNone(self.record.data.get("assigned_to"))
        self.assertIsNone(self.record.data.get("cse_name"))
        self.assertEqual(self.record.data["resolution_status"], "Resolved")

    def test_record_event_api_dispatches_take_break(self):
        url = reverse("crm_records:record-events")
        payload = {
            "record_id": self.record.id,
            "event": SUPPORT_EVENT_TAKE_BREAK,
            "payload": {"resolutionStatus": "Resolved"},
        }
        response = self.client.post(url, payload, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.record.refresh_from_db()
        self.assertIsNone(self.record.data.get("assigned_to"))
        self.assertIsNone(self.record.data.get("cse_name"))
        event_log = EventLog.objects.filter(record=self.record).order_by("-id").first()
        self.assertIsNotNone(event_log)
        self.assertEqual(event_log.event, SUPPORT_EVENT_TAKE_BREAK)

    def test_record_event_api_dispatches_support_rules(self):
        url = reverse("crm_records:record-events")
        payload = {
            "record_id": self.record.id,
            "event": SUPPORT_EVENT_RESOLVED,
            "payload": {
                "reason": "Self Trial completion",
                "resolutionTime": "1:00",
                "callStatus": "Answered",
            },
        }
        response = self.client.post(url, payload, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.record.refresh_from_db()
        self.assertEqual(self.record.data["resolution_status"], "Resolved")
        self.assertEqual(self.record.data["resolution_time"], "1:00")
        self.assertEqual(EventLog.objects.filter(record=self.record).count(), 1)
