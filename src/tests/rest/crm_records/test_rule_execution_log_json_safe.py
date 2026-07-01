"""Rule execution logging must not break API callers on non-JSON-native values."""

from datetime import datetime

from django.utils import timezone

from crm_records.models import Record, RuleExecutionLog, RuleSet
from crm_records.rule_engine import _json_safe_for_log, action_update_fields, execute_rules
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_dump_factory import dump_data
from tests.rest.support_ticket.support_rules import seed_support_ticket_rules


class RuleExecutionLogJsonSafeTest(BaseAPITestCase):
    def test_json_safe_for_log_coerces_datetime(self):
        payload = [
            {
                "action": "update_fields",
                "result": {"updated_fields": {"completed_at": datetime(2026, 1, 1, 12, 0, 0)}},
            }
        ]
        safe = _json_safe_for_log(payload)
        self.assertIsInstance(safe[0]["result"]["updated_fields"]["completed_at"], str)

    def test_execute_rules_does_not_raise_when_updates_include_datetime(self):
        seed_support_ticket_rules(self.tenant)
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(call_attempts=0, resolution_time="0:00"),
        )
        execute_rules(
            "support.resolved",
            record,
            {
                "cse_name": self.email,
                "assigned_to": str(self.supabase_uid),
                "call_status": "Answered",
                "cse_remarks": "done",
                "resolution_time": "1:00",
                "other_reasons": [],
            },
            str(self.tenant.id),
        )
        self.assertTrue(RuleExecutionLog.objects.filter(record=record).exists())
        record.refresh_from_db()
        self.assertEqual(record.data["resolution_status"], "Resolved")

    def test_action_update_fields_initializes_null_record_data(self):
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(call_attempts=0),
        )
        record.data = None
        ctx = {
            "record": record,
            "payload": {},
            "event": "support.resolved",
            "record_data": {},
        }
        action_update_fields(
            ctx,
            updates={"resolution_status": "Resolved", "completed_at": timezone.now()},
        )
        record.refresh_from_db()
        self.assertIsInstance(record.data, dict)
        self.assertEqual(record.data["resolution_status"], "Resolved")
        self.assertIsInstance(record.data["completed_at"], str)
