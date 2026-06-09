"""
Unit tests for support-ticket rule actions (same style as lead rule-engine tests).

Leads test ``action_update_fields`` / ``compute_next_call_from_attempts`` directly
without seeding full ``RuleSet`` rows — see ``test_rule_engine_call_back_later.py``.
"""

from datetime import datetime

from django.test import TestCase
from django.utils import timezone

from crm_records.rule_engine import (
    action_compute_next_call_from_attempts,
    action_update_fields,
)
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from support_ticket.events import prepare_support_ticket_event_payload
from tests.factories import RecordFactory, TenantFactory
from tests.factories.support_ticket_dump_factory import dump_data


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


class SupportTicketRuleEngineTests(TestCase):
    def setUp(self):
        super().setUp()
        self.tenant = TenantFactory()
        self.record = RecordFactory(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(user_id="cust_1", call_attempts=0, resolution_time="3:15"),
        )

    def test_prepare_payload_accumulates_resolution_time_and_maps_camel_case(self):
        prepared = prepare_support_ticket_event_payload(
            self.record,
            {
                "cseRemarks": "note",
                "resolutionTime": "2:30",
                "callStatus": "Answered",
            },
            actor_email="cse@example.com",
            actor_user_id="00000000-0000-0000-0000-000000000001",
        )
        self.assertEqual(prepared["cse_remarks"], "note")
        self.assertEqual(prepared["call_status"], "Answered")
        self.assertEqual(prepared["resolution_time"], "5:45")
        self.assertEqual(prepared["cse_name"], "cse@example.com")
        for camel in ("cseRemarks", "callStatus", "resolutionTime"):
            self.assertNotIn(camel, prepared)

    def test_not_connected_first_attempt_rule_actions(self):
        """Mirrors production rule: snooze + 60m next_call_at / snooze_until."""
        ctx = {
            "record": self.record,
            "payload": {"cse_remarks": "no answer"},
            "event": "support.not_connected",
        }
        action_update_fields(
            ctx,
            updates={
                "cse_name": None,
                "assigned_to": None,
                "call_status": "Not Connected",
                "cse_remarks": "{{payload.cse_remarks}}",
                "completed_at": "{{now}}",
                "resolution_status": "Snoozed",
            },
            increments={"call_attempts": 1},
        )
        action_compute_next_call_from_attempts(
            ctx, fixed_minutes=60, attempts_field="call_attempts", target_field="next_call_at"
        )
        action_compute_next_call_from_attempts(
            ctx, fixed_minutes=60, attempts_field="call_attempts", target_field="snooze_until"
        )

        self.record.refresh_from_db()
        self.assertEqual(self.record.data["call_attempts"], 1)
        self.assertEqual(self.record.data["resolution_status"], "Snoozed")
        self.assertIsNone(self.record.data.get("assigned_to"))

        before = timezone.now()
        snooze_until = _parse_iso(self.record.data["snooze_until"])
        delta = (snooze_until - before).total_seconds()
        self.assertTrue(3600 - 120 <= delta <= 3600 + 120)

    def test_compute_next_call_from_attempts_fixed_minutes(self):
        """Same pattern as ``test_lead_pipeline_sales_lead.test_compute_next_call...``."""
        record = RecordFactory(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(call_attempts=1),
        )
        ctx = {"record": record}
        before = timezone.now()
        action_compute_next_call_from_attempts(
            ctx,
            fixed_minutes=60,
            attempts_field="call_attempts",
            target_field="snooze_until",
        )
        record.refresh_from_db()
        parsed = _parse_iso(record.data["snooze_until"])
        delta = (parsed - before).total_seconds()
        self.assertTrue(3600 - 120 <= delta <= 3600 + 120)
