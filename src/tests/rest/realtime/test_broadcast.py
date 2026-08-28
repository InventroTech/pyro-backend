from unittest.mock import patch

from django.test import TestCase

from realtime.broadcast import (
    broadcast_record_updated,
    skip_realtime_broadcast,
)
from tests.factories import RecordFactory


class BroadcastRecordUpdatedTests(TestCase):
    def test_payload_omits_full_jsonb_data(self):
        with patch("realtime.broadcast.broadcast_to_tenant") as send:
            RecordFactory(
                data={
                    "name": "Heavy lead",
                    "notes": "x" * 5000,
                    "lead_stage": "FRESH",
                    "assigned_to": "user-1",
                    "tasks": [{"id": 1}],
                    "reject_reason": "none",
                }
            )

        send.assert_called_once()
        payload = send.call_args.args[1]
        self.assertEqual(payload["event"], "record_updated")
        self.assertEqual(payload["lead_stage"], "FRESH")
        self.assertEqual(payload["assigned_to"], "user-1")
        self.assertNotIn("name", payload.get("data") or {})
        self.assertNotIn("notes", payload.get("data") or {})
        self.assertEqual(payload["data"]["tasks"], [{"id": 1}])
        self.assertEqual(payload["data"]["reject_reason"], "none")

    def test_duplicate_broadcast_is_suppressed(self):
        with patch("realtime.broadcast.broadcast_to_tenant") as send:
            record = RecordFactory()
            broadcast_record_updated(record)
        self.assertEqual(send.call_count, 1)

    def test_skip_context_blocks_broadcast(self):
        with patch("realtime.broadcast.broadcast_to_tenant") as send:
            with skip_realtime_broadcast():
                RecordFactory()
        send.assert_not_called()
