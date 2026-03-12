"""
Tests for rule engine action_update_fields: call_back_later with no time selected
sets next_call_at to now + 1 hour.

Run:
  pytest src/tests/rest/crm_records/test_rule_engine_call_back_later.py -v
  python manage.py test tests.rest.crm_records.test_rule_engine_call_back_later -v 2
"""

from datetime import datetime, timedelta

from django.test import TestCase
from django.utils import timezone

from crm_records.models import Record
from crm_records.rule_engine import action_update_fields

from tests.factories import TenantFactory, RecordFactory


def _parse_next_call_at(value):
    """Parse next_call_at string to datetime for comparison."""
    if not value:
        return None
    s = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(s)

def _assert_approximately_one_hour_from_now(actual_iso: str, tolerance_seconds: int = 120):
    """Assert actual_iso is approximately now + 1 hour (within tolerance_seconds)."""
    now = timezone.now()
    one_hour_later = now + timedelta(hours=1)
    actual = _parse_next_call_at(actual_iso)
    assert actual is not None, "next_call_at should be set"
    if actual.tzinfo is None and now.tzinfo is not None:
        from datetime import timezone as tz
        actual = actual.replace(tzinfo=tz.utc)
    elif actual.tzinfo is not None and now.tzinfo is None:
        actual = actual.replace(tzinfo=None)
    diff = abs((actual - one_hour_later).total_seconds())
    assert diff <= tolerance_seconds, (
        f"next_call_at {actual_iso} should be ~1h from now; got diff {diff}s (tolerance {tolerance_seconds}s)"
    )


class CallBackLaterNextCallAtDefaultTests(TestCase):
    """Tests for defaulting next_call_at to now + 1h when call back later has no time selected."""

    def setUp(self):
        super().setUp()
        self.tenant = TenantFactory()
        self.record = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "IN_QUEUE",
                "call_attempts": 0,
            },
        )

    def _ctx(self, event="lead.call_back_later", payload=None):
        payload = payload or {}
        return {
            "record": self.record,
            "payload": payload,
            "event": event,
        }

    def test_call_back_later_with_empty_next_call_at_sets_next_call_at_to_one_hour_later(self):
        """When call_back_later and next_call_at is empty string, it is set to now + 1h."""
        updates = {
            "lead_stage": "SNOOZED",
            "assigned_to": "rm-uuid-123",
            "next_call_at": "",
        }
        payload = {"assigned_to": "rm-uuid-123", "next_call_at": ""}
        ctx = self._ctx(payload=payload)

        action_update_fields(ctx, updates)

        self.assertIn("next_call_at", self.record.data)
        _assert_approximately_one_hour_from_now(self.record.data["next_call_at"])
        self.assertEqual(self.record.data["lead_stage"], "SNOOZED")

    def test_call_back_later_with_next_call_at_missing_from_updates_sets_to_one_hour_later(self):
        """When call_back_later and next_call_at is not in updates (user did not select time), set to now + 1h."""
        updates = {
            "lead_stage": "SNOOZED",
            "assigned_to": "rm-uuid-456",
        }
        payload = {"assigned_to": "rm-uuid-456"}
        ctx = self._ctx(payload=payload)

        action_update_fields(ctx, updates)

        self.assertIn("next_call_at", self.record.data)
        _assert_approximately_one_hour_from_now(self.record.data["next_call_at"])

    def test_call_back_later_with_next_call_at_null_string_sets_to_one_hour_later(self):
        """When next_call_at is the string 'null', treat as no time selected and set to now + 1h."""
        updates = {
            "lead_stage": "SNOOZED",
            "assigned_to": "rm-uuid",
            "next_call_at": "null",
        }
        payload = {"assigned_to": "rm-uuid", "next_call_at": "null"}
        ctx = self._ctx(payload=payload)

        action_update_fields(ctx, updates)

        self.assertIn("next_call_at", self.record.data)
        _assert_approximately_one_hour_from_now(self.record.data["next_call_at"])

    def test_call_back_later_with_time_selected_preserves_next_call_at(self):
        """When user selects a time, next_call_at is not overwritten."""
        chosen_time = (timezone.now() + timedelta(days=1)).isoformat()
        updates = {
            "lead_stage": "SNOOZED",
            "assigned_to": "rm-uuid",
            "next_call_at": chosen_time,
        }
        payload = {"assigned_to": "rm-uuid", "next_call_at": chosen_time}
        ctx = self._ctx(payload=payload)

        action_update_fields(ctx, updates)

        self.assertEqual(self.record.data["next_call_at"], chosen_time)

    def test_call_back_later_sets_snooze_unassign_at_when_no_time_selected(self):
        """When no time selected, snooze_unassign_at is still set (12h); next_call_at is defaulted."""
        updates = {
            "lead_stage": "SNOOZED",
            "assigned_to": "rm-uuid",
            "next_call_at": "",
        }
        payload = {"assigned_to": "rm-uuid"}
        ctx = self._ctx(payload=payload)

        action_update_fields(ctx, updates)

        self.assertIn("snooze_unassign_at", self.record.data)
        self.assertIn("next_call_at", self.record.data)
        _assert_approximately_one_hour_from_now(self.record.data["next_call_at"])

    def test_non_call_back_later_event_does_not_set_next_call_at(self):
        """A non-call_back_later event with empty next_call_at in updates does not get default next_call_at."""
        updates = {
            "lead_stage": "NOT_CONNECTED",
            "next_call_at": "",
        }
        ctx = self._ctx(event="lead.not_connected", payload={"lead_stage": "NOT_CONNECTED"})

        action_update_fields(ctx, updates)

        # Empty string would remain as-is (we don't default for non-call_back_later)
        self.assertEqual(self.record.data.get("next_call_at"), "")
