"""
Tests for the purge_old_log_tables job handler's follow-up scheduling behavior.

Run (from repo root):

  pytest src/tests/rest/pyro_jobs/test_purge_old_log_tables_job.py -v
"""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone

from core.log_retention import (
    get_log_retention_chunk_size,
    get_log_retention_days,
    get_log_retention_max_runtime_seconds,
)
from pyro_jobs.jobs.purge_old_log_tables import FOLLOW_UP_DELAY, run_purge_old_log_tables
from pyro_jobs.models import PyroJob
from tests.factories import EventLogFactory, TenantFactory


@pytest.mark.django_db(transaction=True)
class TestPurgeOldLogTablesFollowUp:
    def test_schedules_follow_up_when_has_more(self):
        fake_stats = {
            "cutoff": timezone.now().isoformat(),
            "days": 30,
            "has_more": True,
            "object_history": 1000,
            "event_logs": 0,
            "rule_exec_logs": 0,
            "background_jobs": 0,
            "pyro_jobs": 0,
        }
        with patch(
            "pyro_jobs.jobs.purge_old_log_tables.purge_old_log_rows",
            return_value=fake_stats,
        ):
            run_purge_old_log_tables({"days": 30, "chunk_size": 1000, "max_runtime_seconds": 300})

        follow_up = PyroJob.objects.get(
            job_name="purge_old_log_tables", status=PyroJob.STATUS_PENDING
        )
        assert follow_up.payload == {
            "days": 30,
            "chunk_size": 1000,
            "max_runtime_seconds": 300,
        }
        expected_run_at = timezone.now() + FOLLOW_UP_DELAY
        assert abs((follow_up.run_at - expected_run_at).total_seconds()) < 5

    def test_no_follow_up_when_fully_drained(self):
        fake_stats = {
            "cutoff": timezone.now().isoformat(),
            "days": 30,
            "has_more": False,
            "object_history": 0,
            "event_logs": 0,
            "rule_exec_logs": 0,
            "background_jobs": 0,
            "pyro_jobs": 0,
        }
        with patch(
            "pyro_jobs.jobs.purge_old_log_tables.purge_old_log_rows",
            return_value=fake_stats,
        ):
            run_purge_old_log_tables({"days": 30})

        assert not PyroJob.objects.filter(job_name="purge_old_log_tables").exists()

    def test_default_payload_values_forwarded_and_used_in_follow_up(self):
        fake_stats = {
            "cutoff": timezone.now().isoformat(),
            "days": get_log_retention_days(),
            "has_more": True,
            "object_history": 1,
            "event_logs": 0,
            "rule_exec_logs": 0,
            "background_jobs": 0,
            "pyro_jobs": 0,
        }
        with patch(
            "pyro_jobs.jobs.purge_old_log_tables.purge_old_log_rows",
            return_value=fake_stats,
        ) as mock_purge:
            run_purge_old_log_tables({})  # empty payload -> settings defaults

        mock_purge.assert_called_once_with(
            days=get_log_retention_days(),
            chunk_size=get_log_retention_chunk_size(),
            max_runtime_seconds=get_log_retention_max_runtime_seconds(),
        )
        follow_up = PyroJob.objects.get(
            job_name="purge_old_log_tables", status=PyroJob.STATUS_PENDING
        )
        assert follow_up.payload == {
            "days": get_log_retention_days(),
            "chunk_size": get_log_retention_chunk_size(),
            "max_runtime_seconds": get_log_retention_max_runtime_seconds(),
        }

    def test_invalid_days_raises_without_scheduling_follow_up(self):
        with patch(
            "pyro_jobs.jobs.purge_old_log_tables.purge_old_log_rows"
        ) as mock_purge:
            with pytest.raises(ValueError, match=">= 1"):
                run_purge_old_log_tables({"days": 0})

        mock_purge.assert_not_called()
        assert not PyroJob.objects.filter(job_name="purge_old_log_tables").exists()

    def test_invalid_chunk_size_raises_without_scheduling_follow_up(self):
        with patch(
            "pyro_jobs.jobs.purge_old_log_tables.purge_old_log_rows"
        ) as mock_purge:
            with pytest.raises(ValueError, match=">= 1"):
                run_purge_old_log_tables({"days": 30, "chunk_size": 0})

        mock_purge.assert_not_called()
        assert not PyroJob.objects.filter(job_name="purge_old_log_tables").exists()

    def test_only_one_follow_up_scheduled_per_run(self):
        """has_more is a single aggregate flag across all 5 tables — even when
        several tables individually hit their time budget, exactly one
        follow-up job should be scheduled, not one per table."""
        fake_stats = {
            "cutoff": timezone.now().isoformat(),
            "days": 30,
            "has_more": True,
            "object_history": 500,
            "event_logs": 500,
            "rule_exec_logs": 0,
            "background_jobs": 500,
            "pyro_jobs": 0,
        }
        with patch(
            "pyro_jobs.jobs.purge_old_log_tables.purge_old_log_rows",
            return_value=fake_stats,
        ):
            run_purge_old_log_tables({"days": 30})

        assert PyroJob.objects.filter(job_name="purge_old_log_tables").count() == 1

    def test_follow_up_blocks_creator_dedup_like_a_normal_pending_job(self):
        """The pending follow-up row must satisfy pyro_job_creator's own
        already_scheduled query, so the periodic creator loop skips creating
        a second daily-scheduled row while catch-up is in flight."""
        fake_stats = {
            "cutoff": timezone.now().isoformat(),
            "days": 30,
            "has_more": True,
            "object_history": 1,
            "event_logs": 0,
            "rule_exec_logs": 0,
            "background_jobs": 0,
            "pyro_jobs": 0,
        }
        with patch(
            "pyro_jobs.jobs.purge_old_log_tables.purge_old_log_rows",
            return_value=fake_stats,
        ):
            run_purge_old_log_tables({"days": 30})

        already_scheduled = PyroJob.objects.filter(
            job_name="purge_old_log_tables",
            is_deleted=False,
            status__in=[PyroJob.STATUS_PENDING, PyroJob.STATUS_RUNNING],
        ).exists()
        assert already_scheduled is True

    def test_end_to_end_real_purge_schedules_follow_up(self):
        """Uses the real purge_old_log_rows (not mocked) with a deterministic
        fake clock forcing the time budget to expire after one chunk, proving
        the whole has_more -> schedule_once chain works end to end."""
        from crm_records.models import EventLog

        tenant = TenantFactory()
        old_ts = timezone.now() - timedelta(days=90)
        for _ in range(5):
            ev = EventLogFactory(tenant=tenant)
            EventLog.all_objects.filter(pk=ev.pk).update(created_at=old_ts)

        counter = iter(range(10_000))
        with patch(
            "core.log_retention.time.monotonic", side_effect=lambda: next(counter)
        ):
            run_purge_old_log_tables(
                {"days": 30, "chunk_size": 2, "max_runtime_seconds": 1}
            )

        follow_up = PyroJob.objects.get(
            job_name="purge_old_log_tables", status=PyroJob.STATUS_PENDING
        )
        assert follow_up.payload["chunk_size"] == 2
        assert follow_up.run_at > timezone.now() + timedelta(minutes=55)
