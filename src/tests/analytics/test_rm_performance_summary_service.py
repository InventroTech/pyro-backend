"""
Tests for RmPerformanceSummaryService.compute_and_store_day — the base
daily-aggregation layer the RM Leaderboard's own refresh job sums. These
are service-level (no HTTP): create raw RmActivityEvent rows, call
compute_and_store_day, assert on RmPerformanceDailySummary /
RmPerformanceDailyLeadLatest rows directly.
"""
from datetime import date, timedelta

from django.test import TestCase

from analytics.models import RmActivityEvent, RmPerformanceDailyLeadLatest, RmPerformanceDailySummary
from analytics.services import RmPerformanceSummaryService
from analytics.utils import get_utc_datetime_range_for_ist_date
from tests.factories import TenantFactory


def ist_instant(target_date, hours_into_day=2):
    """A UTC datetime that safely falls inside target_date's IST calendar day."""
    utc_start, _ = get_utc_datetime_range_for_ist_date(target_date)
    return utc_start + timedelta(hours=hours_into_day)


class RmPerformanceSummaryServiceTest(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()
        self.today = date(2026, 6, 15)

    def make_touch(
        self,
        rm_user_id="rm-1",
        rm_name="Asha",
        manager_name="Manager One",
        state="KA",
        party="",
        lead_group="",
        lead_record_id=1,
        updated_status="TRIAL_ACTIVATED",
        started_at=None,
        ended_at=None,
        tenant=None,
    ):
        started_at = started_at or ist_instant(self.today)
        ended_at = ended_at or started_at
        RmActivityEvent.objects.create(
            tenant=tenant or self.tenant,
            event_type="CALL_TOUCH",
            event_data={
                "rm_user_id": rm_user_id,
                "rm_name": rm_name,
                "manager_name": manager_name,
                "team": "",
                "state": state,
                "lead_record_id": lead_record_id,
                "updated_status": updated_status,
                "lead_group": lead_group,
                "party": party,
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "duration_seconds": 60,
            },
        )

    def test_counts_touches_and_dedups_unique_leads(self):
        self.make_touch(lead_record_id=1, updated_status="NOT_CONNECTED")
        self.make_touch(lead_record_id=1, updated_status="CALL_BACK")  # same lead, second touch
        self.make_touch(lead_record_id=2, updated_status="TRIAL_ACTIVATED")

        RmPerformanceSummaryService.compute_and_store_day(self.tenant, self.today)

        row = RmPerformanceDailySummary.objects.get(tenant=self.tenant, rm_user_id="rm-1", date=self.today)
        self.assertEqual(row.touches, 3)
        self.assertEqual(row.unique_leads, 2)

    def test_disposition_counts_reflect_only_each_leads_latest_touch(self):
        base = ist_instant(self.today, hours_into_day=2)
        self.make_touch(lead_record_id=1, updated_status="NOT_CONNECTED", started_at=base)
        self.make_touch(lead_record_id=1, updated_status="TRIAL_ACTIVATED", started_at=base + timedelta(minutes=5))

        RmPerformanceSummaryService.compute_and_store_day(self.tenant, self.today)

        row = RmPerformanceDailySummary.objects.get(tenant=self.tenant, rm_user_id="rm-1", date=self.today)
        self.assertEqual(row.disposition_counts, {"TRIAL_ACTIVATED": 1})

    def test_separate_buckets_per_state_party_lead_group(self):
        self.make_touch(lead_record_id=1, state="KA", party="A", lead_group="G1")
        self.make_touch(lead_record_id=2, state="TG", party="B", lead_group="G2")

        RmPerformanceSummaryService.compute_and_store_day(self.tenant, self.today)

        rows = RmPerformanceDailySummary.objects.filter(tenant=self.tenant, rm_user_id="rm-1", date=self.today)
        self.assertEqual(rows.count(), 2)
        buckets = {(r.state, r.party, r.lead_group): r.touches for r in rows}
        self.assertEqual(buckets[("KA", "A", "G1")], 1)
        self.assertEqual(buckets[("TG", "B", "G2")], 1)

    def test_open_call_with_no_ended_at_is_excluded(self):
        touch_time = ist_instant(self.today)
        RmActivityEvent.objects.create(
            tenant=self.tenant,
            event_type="CALL_TOUCH",
            event_data={
                "rm_user_id": "rm-1", "rm_name": "Asha", "manager_name": "Manager One",
                "team": "", "state": "KA", "lead_record_id": 1, "updated_status": None,
                "lead_group": "", "party": "", "started_at": touch_time.isoformat(),
                "ended_at": None, "duration_seconds": None,
            },
        )

        RmPerformanceSummaryService.compute_and_store_day(self.tenant, self.today)

        self.assertFalse(RmPerformanceDailySummary.objects.filter(tenant=self.tenant, date=self.today).exists())

    def test_touch_just_before_ist_midnight_excluded_from_next_day(self):
        utc_start, utc_end = get_utc_datetime_range_for_ist_date(self.today)
        just_before_midnight = utc_end - timedelta(seconds=1)
        just_after_midnight = utc_end + timedelta(seconds=2)
        self.make_touch(lead_record_id=1, started_at=just_before_midnight)
        self.make_touch(lead_record_id=2, started_at=just_after_midnight)

        RmPerformanceSummaryService.compute_and_store_day(self.tenant, self.today)
        tomorrow = self.today + timedelta(days=1)
        RmPerformanceSummaryService.compute_and_store_day(self.tenant, tomorrow)

        today_row = RmPerformanceDailySummary.objects.get(tenant=self.tenant, date=self.today)
        tomorrow_row = RmPerformanceDailySummary.objects.get(tenant=self.tenant, date=tomorrow)
        self.assertEqual(today_row.touches, 1)
        self.assertEqual(tomorrow_row.touches, 1)

    def test_rerunning_is_idempotent_not_additive(self):
        self.make_touch(lead_record_id=1)
        self.make_touch(lead_record_id=2)

        RmPerformanceSummaryService.compute_and_store_day(self.tenant, self.today)
        RmPerformanceSummaryService.compute_and_store_day(self.tenant, self.today)

        row = RmPerformanceDailySummary.objects.get(tenant=self.tenant, rm_user_id="rm-1", date=self.today)
        self.assertEqual(row.touches, 2)

    def test_tenant_isolation(self):
        other_tenant = TenantFactory()
        self.make_touch(lead_record_id=1, tenant=self.tenant)
        self.make_touch(lead_record_id=99, tenant=other_tenant)

        RmPerformanceSummaryService.compute_and_store_day(self.tenant, self.today)

        self.assertEqual(
            RmPerformanceDailySummary.objects.filter(tenant=self.tenant, date=self.today).count(), 1
        )
        self.assertFalse(
            RmPerformanceDailySummary.objects.filter(tenant=other_tenant, date=self.today).exists()
        )

    def test_lead_latest_has_exactly_one_row_per_lead_when_reassigned_between_rms(self):
        base = ist_instant(self.today, hours_into_day=2)
        self.make_touch(rm_user_id="rm-1", lead_record_id=1, updated_status="CALL_BACK", started_at=base)
        self.make_touch(
            rm_user_id="rm-2", lead_record_id=1, updated_status="TRIAL_ACTIVATED",
            started_at=base + timedelta(hours=1),
        )
        self.make_touch(rm_user_id="rm-1", lead_record_id=2, updated_status="NOT_CONNECTED", started_at=base)

        RmPerformanceSummaryService.compute_and_store_day(self.tenant, self.today)

        rm1_row = RmPerformanceDailySummary.objects.get(tenant=self.tenant, rm_user_id="rm-1", date=self.today)
        rm2_row = RmPerformanceDailySummary.objects.get(tenant=self.tenant, rm_user_id="rm-2", date=self.today)
        self.assertEqual(rm1_row.unique_leads, 2)
        self.assertEqual(rm2_row.unique_leads, 1)

        lead_1_rows = RmPerformanceDailyLeadLatest.objects.filter(
            tenant=self.tenant, date=self.today, lead_record_id=1
        )
        self.assertEqual(lead_1_rows.count(), 1)
        self.assertEqual(lead_1_rows.get().rm_user_id, "rm-2")
