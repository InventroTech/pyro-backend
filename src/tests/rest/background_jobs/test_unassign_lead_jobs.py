"""
Tests for lead unassignment background jobs:

- UnassignSnoozedLeadsJobHandler: unassigns SNOOZED leads when snooze_unassign_at has passed
- ReleaseLeadsAfter12hJobHandler: clears assigned_to on NOT_CONNECTED leads when
  first_assigned_today_at + 12h has passed (or legacy not_connected_unassign_at)

Run:
  pytest src/tests/rest/background_jobs/test_unassign_lead_jobs.py -v
  python manage.py test tests.rest.background_jobs.test_unassign_lead_jobs -v 2
"""

from datetime import datetime, timedelta

from django.test import TestCase
from django.utils import timezone


def _parse_iso(s):
    """Parse ISO timestamp; support Z suffix."""
    if not s:
        return None
    s = str(s).replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def _assert_next_call_at_about_one_hour_later(test_case, next_call_at_str, tolerance_seconds=5):
    """Assert next_call_at is set and approximately 1 hour in the future (handler uses its own now)."""
    test_case.assertTrue(next_call_at_str, msg="next_call_at should be set")
    parsed = _parse_iso(next_call_at_str)
    now = timezone.now()
    if parsed.tzinfo is None and now.tzinfo is not None:
        from datetime import timezone as tz

        parsed = parsed.replace(tzinfo=tz.utc)
    delta = (parsed - now).total_seconds()
    test_case.assertGreater(delta, 3600 - tolerance_seconds, msg="next_call_at should be ~1h in the future")
    test_case.assertLess(delta, 3600 + tolerance_seconds, msg="next_call_at should be ~1h in the future")


from crm_records.models import Record
from background_jobs.models import BackgroundJob, JobType
from background_jobs.job_handlers import (
    ReleaseLeadsAfter12hJobHandler,
    SnoozedToNotConnectedMidnightJobHandler,
    UnassignSnoozedLeadsJobHandler,
)

from tests.factories import TenantFactory, RecordFactory, BackgroundJobFactory


class UnassignSnoozedLeadsJobHandlerTests(TestCase):
    """Tests for UnassignSnoozedLeadsJobHandler: SNOOZED leads with snooze_unassign_at passed get unassigned."""

    def setUp(self):
        super().setUp()
        self.handler = UnassignSnoozedLeadsJobHandler()
        self.tenant = TenantFactory()

    def _make_job(self):
        return BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.UNASSIGN_SNOOZED_LEADS,
            payload={},
        )

    def test_unassigns_snoozed_lead_when_snooze_unassign_at_passed(self):
        """Lead with SNOOZED, assigned_to set, snooze_unassign_at in the past, call_attempts < 6 is unassigned."""
        now = timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "SNOOZED",
                "assigned_to": "rm-uuid-123",
                "snooze_unassign_at": past,
                "call_attempts": 2,
            },
        )
        job = self._make_job()
        result = self.handler.process(job)

        self.assertTrue(result)
        lead.refresh_from_db()
        self.assertIsNone(lead.data.get("assigned_to"))
        self.assertEqual(lead.data.get("lead_stage"), "SNOOZED")
        self.assertNotIn("snooze_unassign_at", lead.data)
        _assert_next_call_at_about_one_hour_later(self, lead.data.get("next_call_at"))
        self.assertEqual(lead.data.get("call_attempts"), 2)
        self.assertEqual(job.result["unassigned_count"], 1)

    def test_does_not_unassign_when_snooze_unassign_at_in_future(self):
        """Lead with snooze_unassign_at in the future is not unassigned."""
        now = timezone.now()
        future = (now + timedelta(hours=2)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "SNOOZED",
                "assigned_to": "rm-uuid",
                "snooze_unassign_at": future,
                "call_attempts": 1,
            },
        )
        job = self._make_job()
        self.handler.process(job)

        lead.refresh_from_db()
        self.assertEqual(lead.data.get("assigned_to"), "rm-uuid")
        self.assertEqual(lead.data.get("snooze_unassign_at"), future)
        self.assertEqual(job.result["unassigned_count"], 0)

    def test_does_not_unassign_when_call_attempts_6_or_more(self):
        """Lead with call_attempts >= 6 is not selected."""
        now = timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "SNOOZED",
                "assigned_to": "rm-uuid",
                "snooze_unassign_at": past,
                "call_attempts": 6,
            },
        )
        job = self._make_job()
        self.handler.process(job)

        lead.refresh_from_db()
        self.assertEqual(lead.data.get("assigned_to"), "rm-uuid")
        self.assertEqual(job.result["unassigned_count"], 0)

    def test_unassigns_multiple_eligible_snoozed_leads(self):
        """Multiple SNOOZED leads with snooze_unassign_at passed are all unassigned."""
        now = timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        for i in range(2):
            RecordFactory(
                tenant=self.tenant,
                entity_type="lead",
                data={
                    "lead_stage": "SNOOZED",
                    "assigned_to": f"rm-{i}",
                    "snooze_unassign_at": past,
                    "call_attempts": i,
                },
            )
        job = self._make_job()
        self.handler.process(job)

        self.assertEqual(job.result["unassigned_count"], 2)
        for lead in Record.objects.filter(tenant=self.tenant, entity_type="lead", data__lead_stage="SNOOZED"):
            self.assertFalse(lead.data.get("assigned_to"), msg=f"Lead {lead.id} should have assigned_to cleared")

    def test_get_retry_delay_returns_expected_delays(self):
        """get_retry_delay returns 60, 300, 900 for attempts 1, 2, 3+."""
        self.assertEqual(self.handler.get_retry_delay(1), 60)
        self.assertEqual(self.handler.get_retry_delay(2), 300)
        self.assertEqual(self.handler.get_retry_delay(3), 900)
        self.assertEqual(self.handler.get_retry_delay(5), 900)


class SnoozedToNotConnectedMidnightJobHandlerTests(TestCase):
    """SnoozedToNotConnectedMidnight: only SNOOZED + SALES LEAD where next_call_at is today's date (reset TZ)."""

    def setUp(self):
        super().setUp()
        self.handler = SnoozedToNotConnectedMidnightJobHandler()
        self.tenant = TenantFactory()

    def _make_job(self):
        return BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.SNOOZED_TO_NOT_CONNECTED_MIDNIGHT,
            payload={},
        )

    def test_transitions_when_next_call_at_is_today(self):
        """Lead with next_call_at on today's calendar date (UTC) becomes NOT_CONNECTED."""
        now = timezone.now()
        today_ts = now.replace(hour=15, minute=0, second=0, microsecond=0)
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "SNOOZED",
                "lead_status": "SALES LEAD",
                "assigned_to": "rm-1",
                "snooze_unassign_at": (now - timedelta(hours=1)).isoformat(),
                "next_call_at": today_ts.isoformat(),
                "call_attempts": 1,
            },
        )
        job = self._make_job()
        self.assertTrue(self.handler.process(job))
        lead.refresh_from_db()
        self.assertEqual(lead.data.get("lead_stage"), "NOT_CONNECTED")
        self.assertIsNone(lead.data.get("assigned_to"))
        self.assertNotIn("snooze_unassign_at", lead.data)
        self.assertEqual(lead.data.get("next_call_at"), today_ts.isoformat())
        self.assertEqual(job.result["updated"], 1)

    def test_skips_when_next_call_at_is_tomorrow(self):
        """Lead snoozed until tomorrow (next_call_at next calendar day) stays SNOOZED."""
        now = timezone.now()
        tomorrow_ts = (now + timedelta(days=1)).replace(hour=10, minute=0, second=0, microsecond=0)
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "SNOOZED",
                "lead_status": "SALES LEAD",
                "assigned_to": "rm-2",
                "next_call_at": tomorrow_ts.isoformat(),
                "call_attempts": 1,
            },
        )
        job = self._make_job()
        self.assertTrue(self.handler.process(job))
        lead.refresh_from_db()
        self.assertEqual(lead.data.get("lead_stage"), "SNOOZED")
        self.assertEqual(lead.data.get("assigned_to"), "rm-2")
        self.assertEqual(lead.data.get("next_call_at"), tomorrow_ts.isoformat())
        self.assertEqual(job.result["updated"], 0)


class ReleaseLeadsAfter12hJobHandlerTests(TestCase):
    """Tests for ReleaseLeadsAfter12hJobHandler: NOT_CONNECTED + 12h since first_assigned_today_at."""

    def setUp(self):
        super().setUp()
        self.handler = ReleaseLeadsAfter12hJobHandler()
        self.tenant = TenantFactory()

    def _make_job(self):
        return BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.RELEASE_LEADS_AFTER_12H,
            payload={},
        )

    def test_releases_not_connected_when_first_assigned_today_at_plus_12h_passed(self):
        """Lead with NOT_CONNECTED, assigned_to set, anchor 13h ago is released."""
        now = timezone.now()
        anchor = (now - timedelta(hours=13)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "NOT_CONNECTED",
                "assigned_to": "rm-uuid-456",
                "first_assigned_today_at": anchor,
                "first_assignment_today_date": timezone.localtime(now).date().isoformat(),
                "call_attempts": 2,
                "first_assigned_to": "rm-uuid-456",
                "first_assigned_at": (now - timedelta(hours=24)).isoformat(),
            },
        )
        job = self._make_job()
        result = self.handler.process(job)

        self.assertTrue(result)
        lead.refresh_from_db()
        self.assertIsNone(lead.data.get("assigned_to"))
        self.assertEqual(lead.data.get("lead_stage"), "NOT_CONNECTED")
        self.assertNotIn("first_assigned_today_at", lead.data)
        self.assertNotIn("first_assignment_today_date", lead.data)
        _assert_next_call_at_about_one_hour_later(self, lead.data.get("next_call_at"))
        self.assertEqual(lead.data.get("call_attempts"), 2)
        self.assertEqual(lead.data.get("first_assigned_to"), "rm-uuid-456")
        self.assertIn("first_assigned_at", lead.data)
        self.assertEqual(job.result["released_count"], 1)

    def test_legacy_releases_when_not_connected_unassign_at_passed_and_no_anchor(self):
        """Pre-migration rows: not_connected_unassign_at in the past still release."""
        now = timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "NOT_CONNECTED",
                "assigned_to": "rm-legacy",
                "not_connected_unassign_at": past,
                "call_attempts": 1,
            },
        )
        job = self._make_job()
        self.handler.process(job)
        lead.refresh_from_db()
        self.assertIsNone(lead.data.get("assigned_to"))
        self.assertNotIn("not_connected_unassign_at", lead.data)
        self.assertEqual(job.result["released_count"], 1)

    def test_does_not_change_call_attempts_on_release(self):
        """Release does not change call_attempts; it stays as-is (e.g. 3 remains 3)."""
        now = timezone.now()
        anchor = (now - timedelta(hours=13)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "NOT_CONNECTED",
                "assigned_to": "rm-uuid",
                "first_assigned_today_at": anchor,
                "call_attempts": 3,
            },
        )
        job = self._make_job()
        self.handler.process(job)

        lead.refresh_from_db()
        self.assertEqual(lead.data.get("call_attempts"), 3)

    def test_does_not_release_when_first_assigned_today_at_within_12h(self):
        """Lead with anchor less than 12h ago is not released."""
        now = timezone.now()
        recent = (now - timedelta(hours=2)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "NOT_CONNECTED",
                "assigned_to": "rm-uuid",
                "first_assigned_today_at": recent,
                "call_attempts": 1,
            },
        )
        job = self._make_job()
        self.handler.process(job)

        lead.refresh_from_db()
        self.assertEqual(lead.data.get("assigned_to"), "rm-uuid")
        self.assertEqual(lead.data.get("first_assigned_today_at"), recent)
        self.assertEqual(job.result["released_count"], 0)

    def test_does_not_release_when_call_attempts_6_or_more(self):
        """Lead with call_attempts >= 6 is not selected."""
        now = timezone.now()
        anchor = (now - timedelta(hours=13)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "NOT_CONNECTED",
                "assigned_to": "rm-uuid",
                "first_assigned_today_at": anchor,
                "call_attempts": 6,
            },
        )
        job = self._make_job()
        self.handler.process(job)

        lead.refresh_from_db()
        self.assertEqual(lead.data.get("assigned_to"), "rm-uuid")
        self.assertEqual(job.result["released_count"], 0)

    def test_does_not_release_when_lead_stage_not_not_connected(self):
        """Lead with lead_stage SNOOZED is not processed by ReleaseLeadsAfter12h (different job)."""
        now = timezone.now()
        anchor = (now - timedelta(hours=13)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "SNOOZED",
                "assigned_to": "rm-uuid",
                "first_assigned_today_at": anchor,
                "call_attempts": 1,
            },
        )
        job = self._make_job()
        self.handler.process(job)

        lead.refresh_from_db()
        self.assertEqual(lead.data.get("assigned_to"), "rm-uuid")
        self.assertEqual(job.result["released_count"], 0)

    def test_preserves_first_assigned_at_and_first_assigned_to(self):
        """first_assigned_at and first_assigned_to are not removed (for daily limit tracking)."""
        now = timezone.now()
        anchor = (now - timedelta(hours=13)).isoformat()
        first_at = (now - timedelta(hours=24)).isoformat()
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "NOT_CONNECTED",
                "assigned_to": "rm-uuid",
                "first_assigned_today_at": anchor,
                "call_attempts": 1,
                "first_assigned_to": "original-rm",
                "first_assigned_at": first_at,
            },
        )
        job = self._make_job()
        self.handler.process(job)

        lead.refresh_from_db()
        self.assertEqual(lead.data.get("first_assigned_to"), "original-rm")
        self.assertEqual(lead.data.get("first_assigned_at"), first_at)

    def test_releases_multiple_eligible_not_connected_leads(self):
        """Multiple NOT_CONNECTED leads with anchor passed are all released."""
        now = timezone.now()
        anchor = (now - timedelta(hours=13)).isoformat()
        for i in range(2):
            RecordFactory(
                tenant=self.tenant,
                entity_type="lead",
                data={
                    "lead_stage": "NOT_CONNECTED",
                    "assigned_to": f"rm-{i}",
                    "first_assigned_today_at": anchor,
                    "call_attempts": i,
                },
            )
        job = self._make_job()
        self.handler.process(job)

        self.assertEqual(job.result["released_count"], 2)
        for lead in Record.objects.filter(tenant=self.tenant, entity_type="lead", data__lead_stage="NOT_CONNECTED"):
            self.assertFalse(lead.data.get("assigned_to"), msg=f"Lead {lead.id} should have assigned_to cleared")
            self.assertNotIn("first_assigned_today_at", lead.data)

    def test_get_retry_delay_returns_expected_delays(self):
        """get_retry_delay returns 60, 300, 900 for attempts 1, 2, 3+."""
        self.assertEqual(self.handler.get_retry_delay(1), 60)
        self.assertEqual(self.handler.get_retry_delay(2), 300)
        self.assertEqual(self.handler.get_retry_delay(3), 900)
