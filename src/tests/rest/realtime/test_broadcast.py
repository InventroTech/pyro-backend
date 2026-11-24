from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from realtime import broadcast as broadcast_mod
from realtime.broadcast import (
    broadcast_record_updated,
    flush_pending_record_broadcasts,
    is_realtime_oversight_role,
    skip_realtime_broadcast,
    tenant_oversight_group_name,
)


class OversightRoleTests(SimpleTestCase):
    @override_settings(
        REALTIME_OVERSIGHT_ROLE_KEYS=["admin", "manager", "tl", "team_lead"]
    )
    def test_oversight_role_matching(self):
        self.assertTrue(is_realtime_oversight_role("admin"))
        self.assertTrue(is_realtime_oversight_role("Manager"))
        self.assertTrue(is_realtime_oversight_role("sales_manager"))
        self.assertTrue(is_realtime_oversight_role("team_lead"))
        self.assertTrue(is_realtime_oversight_role("TL"))
        self.assertFalse(is_realtime_oversight_role("RM"))
        self.assertFalse(is_realtime_oversight_role("CSE"))
        self.assertFalse(is_realtime_oversight_role(None))


class BroadcastRecordUpdatedTests(SimpleTestCase):
    def tearDown(self):
        flush_pending_record_broadcasts()
        with broadcast_mod._recent_lock:
            broadcast_mod._recent_broadcasts.clear()
        super().tearDown()

    def _record(
        self,
        *,
        assigned_to="user-1",
        stage="FRESH",
        record_id="rec-1",
        updated_at=None,
        extra_data=None,
    ):
        record = MagicMock()
        record.id = record_id
        record.tenant_id = "tenant-1"
        record.tenant = "tenant-1"
        record.entity_type = "lead"
        record.updated_at = updated_at or timezone.now()
        data = {
            "name": "Heavy lead",
            "notes": "x" * 100,
            "lead_stage": stage,
            "assigned_to": assigned_to,
            "tasks": [{"id": 1}],
            "reject_reason": "none",
        }
        if extra_data:
            data.update(extra_data)
        record.data = data
        return record

    def test_assigned_lead_goes_to_user_and_oversight(self):
        record = self._record()
        with patch(
            "realtime.broadcast._resolve_assignee_user_pk", return_value=42
        ), patch("realtime.broadcast.broadcast_to_user") as send_user, patch(
            "realtime.broadcast.broadcast_to_tenant_oversight"
        ) as send_oversight, patch(
            "realtime.broadcast.broadcast_to_tenant"
        ) as send_tenant:
            with patch.object(broadcast_mod, "_COALESCE_SECONDS", 0):
                broadcast_record_updated(record, created=True)

        send_user.assert_called_once()
        send_oversight.assert_called_once()
        send_tenant.assert_not_called()
        self.assertEqual(send_user.call_args.args[0], 42)
        payload = send_user.call_args.args[1]
        self.assertEqual(payload["event"], "record_updated")
        self.assertEqual(payload["lead_stage"], "FRESH")
        self.assertEqual(payload["assigned_to"], "user-1")
        self.assertNotIn("name", payload.get("data") or {})
        self.assertEqual(payload["data"]["tasks"], [{"id": 1}])
        self.assertEqual(
            send_oversight.call_args.args[0],
            "tenant-1",
        )

    def test_unassigned_goes_to_oversight_only(self):
        record = self._record(assigned_to=None)
        with patch("realtime.broadcast.broadcast_to_user") as send_user, patch(
            "realtime.broadcast.broadcast_to_tenant_oversight"
        ) as send_oversight, patch(
            "realtime.broadcast.broadcast_to_tenant"
        ) as send_tenant:
            with patch.object(broadcast_mod, "_COALESCE_SECONDS", 0):
                broadcast_record_updated(record, created=True)

        send_user.assert_not_called()
        send_oversight.assert_called_once()
        send_tenant.assert_not_called()

    def test_duplicate_broadcast_is_suppressed(self):
        record = self._record(assigned_to=None)
        with patch("realtime.broadcast.broadcast_to_tenant_oversight") as send:
            with patch.object(broadcast_mod, "_COALESCE_SECONDS", 0):
                broadcast_record_updated(record, created=True)
                broadcast_record_updated(record, created=True)
        self.assertEqual(send.call_count, 1)

    def test_skip_context_blocks_broadcast(self):
        record = self._record()
        with patch("realtime.broadcast.broadcast_to_tenant_oversight") as send_oversight, patch(
            "realtime.broadcast.broadcast_to_user"
        ) as send_user:
            with skip_realtime_broadcast():
                broadcast_record_updated(record, created=True)
        send_oversight.assert_not_called()
        send_user.assert_not_called()

    def test_oversight_group_name(self):
        self.assertEqual(
            tenant_oversight_group_name("abc"),
            "tenant_abc_oversight",
        )


class CoalesceBroadcastTests(SimpleTestCase):
    """No DB — exercises last-write-wins coalesce before group_send."""

    def tearDown(self):
        flush_pending_record_broadcasts()
        with broadcast_mod._recent_lock:
            broadcast_mod._recent_broadcasts.clear()
        super().tearDown()

    def _record(self, *, stage: str, updated_at, tasks=None, reject_reason=None):
        record = MagicMock()
        record.id = "rec-1"
        record.tenant_id = "tenant-1"
        record.tenant = "tenant-1"
        record.entity_type = "lead"
        record.updated_at = updated_at
        data = {"lead_stage": stage, "assigned_to": "u1", "tasks": tasks or []}
        if reject_reason is not None:
            data["reject_reason"] = reject_reason
        record.data = data
        return record

    def test_rapid_updates_coalesce_to_one_fanout(self):
        t1 = timezone.now()
        t2 = t1 + timedelta(milliseconds=5)
        first = self._record(stage="A", updated_at=t1, tasks=[{"id": 1}])
        second = self._record(
            stage="C",
            updated_at=t2,
            tasks=[{"id": 3}],
            reject_reason="x",
        )

        with patch(
            "realtime.broadcast._resolve_assignee_user_pk", return_value=7
        ), patch("realtime.broadcast.broadcast_to_user") as send_user, patch(
            "realtime.broadcast.broadcast_to_tenant_oversight"
        ) as send_oversight, patch(
            "realtime.broadcast.broadcast_to_tenant"
        ) as send_tenant:
            with patch.object(broadcast_mod, "_COALESCE_SECONDS", 30.0):
                broadcast_record_updated(first, created=False)
                broadcast_record_updated(second, created=False)
                self.assertEqual(send_user.call_count, 0)
                flush_pending_record_broadcasts()

        self.assertEqual(send_user.call_count, 1)
        self.assertEqual(send_oversight.call_count, 1)
        send_tenant.assert_not_called()
        self.assertEqual(send_user.call_args.args[0], 7)
        payload = send_user.call_args.args[1]
        self.assertEqual(payload["lead_stage"], "C")
        self.assertEqual(payload["data"]["tasks"], [{"id": 3}])
        self.assertEqual(payload["data"]["reject_reason"], "x")

    def test_created_bypasses_coalesce(self):
        record = self._record(stage="NEW", updated_at=timezone.now())
        with patch(
            "realtime.broadcast._resolve_assignee_user_pk", return_value=7
        ), patch("realtime.broadcast.broadcast_to_user") as send_user, patch(
            "realtime.broadcast.broadcast_to_tenant_oversight"
        ) as send_oversight:
            with patch.object(broadcast_mod, "_COALESCE_SECONDS", 30.0):
                broadcast_record_updated(record, created=True)
                self.assertEqual(send_user.call_count, 1)
                self.assertEqual(send_oversight.call_count, 1)
