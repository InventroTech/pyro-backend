from unittest.mock import patch

from django.urls import reverse
from django.utils import timezone
from rest_framework import status

from background_jobs.models import JobType
from crm_records.models import EventLog, Record
from support_ticket.constants import (
    SUPPORT_EVENT_CALL_LATER,
    SUPPORT_EVENT_CANNOT_RESOLVE,
    SUPPORT_EVENT_NOT_CONNECTED,
    SUPPORT_EVENT_RESOLVED,
    SUPPORT_EVENT_TAKE_BREAK,
    SUPPORT_TICKET_ENTITY_TYPE,
)
from support_ticket.events import dispatch_support_ticket_event
from support_ticket.services import SaveResolvedTicketPrajaService
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
                support_ticket_id=456,
                support_ticket_type="Self_Trial",
                call_attempts=0,
                assigned_to=self.supabase_uid,
                cse_name=self.email,
            ),
        )

    def test_not_connected_self_trial_always_snoozes_keeps_assignee(self):
        for expected_attempts in range(1, 7):
            dispatch_support_ticket_event(
                SUPPORT_EVENT_NOT_CONNECTED,
                self.record,
                {"cse_remarks": "no answer"} if expected_attempts == 1 else {},
            )
            self.record.refresh_from_db()
            self.assertEqual(self.record.data["call_attempts"], expected_attempts)
            self.assertEqual(self.record.data["resolution_status"], "Snoozed")
            self.assertEqual(self.record.data.get("assigned_to"), self.supabase_uid)
            self.assertIsNotNone(self.record.data.get("snooze_until"))

    def test_take_break_unassigns_per_rules(self):
        self.record.data = {
            **self.record.data,
            "resolution_status": "Open",
            "call_status": "Call Waiting",
            "assigned_to": self.supabase_uid,
            "cse_name": self.email,
            "first_assigned_to": self.supabase_uid,
            "first_assigned_at": timezone.now().isoformat(),
        }
        self.record.save(update_fields=["data"])

        dispatch_support_ticket_event(
            SUPPORT_EVENT_TAKE_BREAK,
            self.record,
            {"resolutionStatus": "Open"},
        )
        self.record.refresh_from_db()
        self.assertIsNone(self.record.data.get("assigned_to"))
        self.assertIsNone(self.record.data.get("cse_name"))
        self.assertIsNone(self.record.data.get("first_assigned_to"))
        self.assertIsNone(self.record.data.get("first_assigned_at"))

    def test_take_break_keeps_not_connected_assigned(self):
        self.record.data = {
            **self.record.data,
            "resolution_status": "Snoozed",
            "call_status": "Not Connected",
            "assigned_to": self.supabase_uid,
            "cse_name": self.email,
            "first_assigned_to": self.supabase_uid,
            "first_assigned_at": timezone.now().isoformat(),
        }
        self.record.save(update_fields=["data"])

        dispatch_support_ticket_event(
            SUPPORT_EVENT_TAKE_BREAK,
            self.record,
            {"resolutionStatus": "Snoozed"},
        )
        self.record.refresh_from_db()
        self.assertEqual(self.record.data.get("assigned_to"), self.supabase_uid)
        self.assertEqual(self.record.data.get("cse_name"), self.email)
        self.assertEqual(self.record.data.get("first_assigned_to"), self.supabase_uid)
        self.assertIsNotNone(self.record.data.get("first_assigned_at"))

    def test_record_event_api_dispatches_take_break(self):
        url = reverse("crm_records:record-events")
        payload = {
            "record_id": self.record.id,
            "event": SUPPORT_EVENT_TAKE_BREAK,
            "payload": {"resolutionStatus": "Open"},
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

    @patch("support_ticket.events.get_queue_service")
    def test_resolved_enqueues_praja_save_resolved_ticket(self, mock_get_queue):
        dispatch_support_ticket_event(
            SUPPORT_EVENT_RESOLVED,
            self.record,
            {
                "reason": "Self Trial completion",
                "resolutionTime": "1:00",
                "callStatus": "Connected",
            },
        )
        praja_calls = [
            c
            for c in mock_get_queue.return_value.enqueue_job.call_args_list
            if c.kwargs.get("job_type") == JobType.SEND_TO_PRAJA
        ]
        self.assertEqual(len(praja_calls), 1)
        self.assertEqual(
            praja_calls[0].kwargs["payload"]["object_type"],
            "save_resolved_ticket",
        )

    @patch("support_ticket.events.get_queue_service")
    def test_not_connected_does_not_enqueue_praja(self, mock_get_queue):
        dispatch_support_ticket_event(
            SUPPORT_EVENT_NOT_CONNECTED,
            self.record,
            {"cse_remarks": "no answer"},
        )
        praja_calls = [
            c
            for c in mock_get_queue.return_value.enqueue_job.call_args_list
            if c.kwargs.get("job_type") == JobType.SEND_TO_PRAJA
        ]
        self.assertEqual(praja_calls, [])

    @patch("support_ticket.events.get_queue_service")
    def test_not_connected_close_enqueues_praja(self, mock_get_queue):
        # Non–Self Trial closes on 5th NC attempt.
        self.record.data = {
            **self.record.data,
            "support_ticket_type": "in_trial",
            "call_attempts": 0,
            "assigned_to": self.supabase_uid,
            "cse_name": self.email,
        }
        self.record.save(update_fields=["data"])

        for _ in range(4):
            dispatch_support_ticket_event(
                SUPPORT_EVENT_NOT_CONNECTED,
                self.record,
                {"cse_remarks": "no answer"},
            )
            self.record.refresh_from_db()

        dispatch_support_ticket_event(
            SUPPORT_EVENT_NOT_CONNECTED,
            self.record,
            {},
        )
        self.record.refresh_from_db()
        self.assertEqual(self.record.data["resolution_status"], "Closed")
        self.assertEqual(self.record.data.get("assigned_to"), self.supabase_uid)

        praja_calls = [
            c
            for c in mock_get_queue.return_value.enqueue_job.call_args_list
            if c.kwargs.get("job_type") == JobType.SEND_TO_PRAJA
        ]
        self.assertEqual(len(praja_calls), 1)
        self.assertEqual(praja_calls[0].kwargs["payload"]["ticket_status"], "CLOSED")

    @patch("support_ticket.events.get_queue_service")
    def test_call_later_does_not_enqueue_praja(self, mock_get_queue):
        dispatch_support_ticket_event(
            SUPPORT_EVENT_CALL_LATER,
            self.record,
            {"callStatus": "Connected", "cseRemarks": "call back later"},
        )
        praja_calls = [
            c
            for c in mock_get_queue.return_value.enqueue_job.call_args_list
            if c.kwargs.get("job_type") == JobType.SEND_TO_PRAJA
        ]
        self.assertEqual(praja_calls, [])

    def test_save_resolved_ticket_payload_from_record(self):
        self.record.data = {
            **self.record.data,
            "user_id": 123,
            "support_ticket_id": 456,
            "resolution_status": "Resolved",
            "tasks": [
                {"task": "Verify ID", "status": "Yes"},
                {"task": "Close case", "status": "Yes"},
            ],
        }
        self.record.save(update_fields=["data"])
        service = SaveResolvedTicketPrajaService()
        payload = service.build_payload(self.record)
        self.assertEqual(
            payload,
            {
                "user_id": 123,
                "ticket_id": self.record.id,
                "ticket_type": "self_trial",
                "ticket_status": "RESOLVED",
                "all_tasks_completed": True,
            },
        )
