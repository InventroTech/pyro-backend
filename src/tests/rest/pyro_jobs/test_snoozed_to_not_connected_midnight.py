"""Day-end snooze job: Election Contestants change stage and stay assigned."""

import uuid

from django.test import TestCase
from django.utils import timezone

from crm_records.models import Record
from pyro_jobs.jobs.snoozed_to_not_connected_midnight import (
    TENANT_ID,
    run_snoozed_to_not_connected_midnight,
)
from tests.factories import RecordFactory, TenantFactory
from user_settings.models import Group


class SnoozedToNotConnectedMidnightTests(TestCase):
    def setUp(self):
        super().setUp()
        self.tenant = TenantFactory(id=uuid.UUID(TENANT_ID), slug="midnight-election-tenant")
        Group.objects.create(
            tenant=self.tenant,
            name="Election Contestants",
            group_data={
                "party": ["BJP"],
                "lead_sources": ["CONTESTANT_FORM"],
            },
        )

    def _lead(self, **data):
        payload = {
            "lead_stage": "SNOOZED",
            "lead_status": "SALES LEAD",
            "next_call_at": timezone.now().isoformat(),
            "snooze_unassign_at": timezone.now().isoformat(),
            "assigned_to": "rm-1",
        }
        payload.update(data)
        return RecordFactory(tenant=self.tenant, entity_type="lead", data=payload)

    def test_election_contestants_move_to_not_connected_and_stay_assigned(self):
        sticky = self._lead(
            affiliated_party="BJP",
            lead_source="CONTESTANT_FORM",
            assigned_to="rm-election",
        )
        other = self._lead(
            affiliated_party="BJP",
            lead_source="OTHER",
            assigned_to="rm-other",
        )

        result = run_snoozed_to_not_connected_midnight({})

        sticky.refresh_from_db()
        other.refresh_from_db()
        self.assertEqual(sticky.data.get("lead_stage"), "NOT_CONNECTED")
        self.assertEqual(sticky.data.get("assigned_to"), "rm-election")
        self.assertNotIn("snooze_unassign_at", sticky.data)
        self.assertEqual(other.data.get("lead_stage"), "NOT_CONNECTED")
        self.assertIsNone(other.data.get("assigned_to"))
        self.assertNotIn("snooze_unassign_at", other.data)
        self.assertEqual(result["updated"], 2)
        self.assertEqual(result["assignment_kept"], 1)
        self.assertEqual(
            Record.objects.filter(tenant=self.tenant, entity_type="lead").count(),
            2,
        )
