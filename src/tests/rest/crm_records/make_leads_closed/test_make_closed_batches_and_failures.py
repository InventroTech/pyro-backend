"""Batching, partial failures, query count and the fresh-leads cache."""

from unittest.mock import patch

from django.core.cache import cache
from django.db import connection
from django.test.utils import CaptureQueriesContext

from crm_records.make_closed import make_leads_closed
from crm_records.models import EventLog
from object_history.engine import HistoryEngine
from .helpers import MakeClosedAPITestCase


def fail_on_call(number, real):
    """Wrap ``real`` so its ``number``-th call raises, as if the database dropped mid-run."""
    calls = {"count": 0}

    def wrapper(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == number:
            raise RuntimeError("database went away")
        return real(*args, **kwargs)

    return wrapper


class MakeClosedBatchTests(MakeClosedAPITestCase):
    def test_handles_lists_longer_than_one_batch(self):
        leads = [self.lead(praja_id=str(n)) for n in (1, 250, 700)]

        with patch("crm_records.make_closed.BATCH_SIZE", 100):
            response = self.post_ids([str(n) for n in range(1, 701)])

        self.assertEqual(response.data["updated"], ["1", "250", "700"])
        self.assertEqual(len(response.data["not_found"]), 697)
        for lead in leads:
            lead.refresh_from_db()
            self.assertEqual(lead.data["lead_stage"], "CLOSED")

    def test_closes_leads_on_both_sides_of_a_batch_boundary(self):
        leads = [self.lead(praja_id=str(n)) for n in range(1, 6)]

        with patch("crm_records.make_closed.BATCH_SIZE", 2):
            response = self.post_ids([str(n) for n in range(1, 6)])

        self.assertEqual(response.data["updated"], ["1", "2", "3", "4", "5"])
        for lead in leads:
            lead.refresh_from_db()
            self.assertEqual(lead.data["lead_stage"], "CLOSED")

    def test_list_exactly_one_batch_long(self):
        for n in range(1, 4):
            self.lead(praja_id=str(n))

        with patch("crm_records.make_closed.BATCH_SIZE", 3):
            response = self.post_ids(["1", "2", "3"])

        self.assertEqual(response.data["updated"], ["1", "2", "3"])

    def test_a_large_list_of_unknown_ids_is_all_not_found(self):
        ids = [str(n) for n in range(100000, 105000)]

        response = self.post_ids(ids)

        self.assertEqual(response.data["updated"], [])
        self.assertEqual(len(response.data["not_found"]), 5000)

    def test_query_count_does_not_grow_with_lead_count(self):
        def run(praja_ids):
            for praja_id in praja_ids:
                self.lead(praja_id=praja_id)
            with CaptureQueriesContext(connection) as queries:
                response = self.post_ids(praja_ids)
            self.assertEqual(len(response.data["updated"]), len(praja_ids))
            return len(queries)

        run(["warm-up"])
        self.assertEqual(run([f"l{n}" for n in range(60)]), run([f"s{n}" for n in range(2)]))


class MakeClosedPartialFailureTests(MakeClosedAPITestCase):
    def test_a_failing_event_write_rolls_back_only_its_batch(self):
        first = self.lead(praja_id="1")
        second = self.lead(praja_id="2")

        with patch("crm_records.make_closed.BATCH_SIZE", 1), patch.object(
            EventLog.objects, "bulk_create", side_effect=fail_on_call(2, EventLog.objects.bulk_create)
        ):
            with self.assertRaises(RuntimeError):
                make_leads_closed(tenant=self.tenant, praja_ids=["1", "2"], actor="ops@example.com")

        first.refresh_from_db()
        second.refresh_from_db()
        self.assertEqual(first.data["lead_stage"], "CLOSED")
        self.assertEqual(second.data["lead_stage"], "NOT_CONNECTED")
        self.assertEqual(second.data["assigned_to"], "rm-123")
        self.assertFalse(EventLog.objects.filter(record=second, event="lead.made_closed").exists())

    def test_a_failing_history_write_rolls_back_the_stage_change_too(self):
        lead = self.lead()

        with patch.object(HistoryEngine, "bulk_capture_updates", side_effect=RuntimeError("history down")):
            with self.assertRaises(RuntimeError):
                make_leads_closed(tenant=self.tenant, praja_ids=["9001"], actor="ops@example.com")

        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "NOT_CONNECTED")
        self.assertEqual(lead.data["assigned_to"], "rm-123")
        self.assertFalse(EventLog.objects.filter(record=lead, event="lead.made_closed").exists())

    def test_retrying_after_a_failure_closes_the_rest(self):
        self.lead(praja_id="1")
        second = self.lead(praja_id="2")

        with patch("crm_records.make_closed.BATCH_SIZE", 1), patch.object(
            EventLog.objects, "bulk_create", side_effect=fail_on_call(2, EventLog.objects.bulk_create)
        ):
            with self.assertRaises(RuntimeError):
                make_leads_closed(tenant=self.tenant, praja_ids=["1", "2"], actor="ops@example.com")

        response = self.post_ids(["1", "2"])

        self.assertEqual(response.data["updated"], ["2"])
        self.assertEqual(response.data["skipped"], [{"praja_id": "1", "reason": "already_closed"}])
        second.refresh_from_db()
        self.assertEqual(second.data["lead_stage"], "CLOSED")


class MakeClosedFreshCacheTests(MakeClosedAPITestCase):
    def cache_key(self):
        return f"fresh_leads_inventory:{self.tenant.id}"

    def test_clears_the_fresh_leads_count_when_a_lead_is_closed(self):
        self.lead(lead_stage="FRESH", call_attempts=0)
        cache.set(self.cache_key(), 42)

        self.post_ids(["9001"])

        self.assertIsNone(cache.get(self.cache_key()))

    def test_keeps_the_fresh_leads_count_when_nothing_changed(self):
        self.raw_record({"praja_id": "9001", "lead_stage": "CLOSED"})
        cache.set(self.cache_key(), 42)

        self.post_ids(["9001", "404"])

        self.assertEqual(cache.get(self.cache_key()), 42)
