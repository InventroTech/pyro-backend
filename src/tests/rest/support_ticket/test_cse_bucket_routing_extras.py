"""Tests for NOT Connected list API."""

from django.urls import reverse
from django.utils import timezone
from rest_framework import status

from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RecordFactory
from tests.factories.support_ticket_dump_factory import dump_data


class GetNotConnectedTicketsAPITest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.url = reverse("support_ticket:not-connected-tickets")
        Record.objects.filter(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
        ).delete()

    def test_lists_only_my_non_terminal_not_connected(self):
        mine = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(user_id="u1", name="Mine NC", support_ticket_type="in_trial"),
                "assigned_to": self.supabase_uid,
                "cse_name": self.email,
                "call_status": "Not Connected",
                "resolution_status": "Snoozed",
                "call_attempts": 1,
                "first_assigned_at": timezone.now().isoformat(),
            },
        )
        # Other CSE
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(user_id="u2", name="Other CSE", support_ticket_type="in_trial"),
                "assigned_to": "00000000-0000-0000-0000-000000000099",
                "call_status": "Not Connected",
                "resolution_status": "Snoozed",
            },
        )
        # Terminal Closed — excluded
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(user_id="u3", name="Closed", support_ticket_type="in_trial"),
                "assigned_to": self.supabase_uid,
                "call_status": "Not Connected",
                "resolution_status": "Closed",
            },
        )
        # Open Call Waiting — not NC
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(user_id="u4", name="Waiting", support_ticket_type="in_trial"),
                "assigned_to": self.supabase_uid,
                "call_status": "Call Waiting",
                "resolution_status": "Open",
            },
        )

        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        ids = [t["id"] for t in response.data]
        self.assertEqual(ids, [mine.id])
