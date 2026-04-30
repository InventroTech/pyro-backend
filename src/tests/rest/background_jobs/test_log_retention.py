"""
Tests for log retention and :class:`~background_jobs.job_handlers.PurgeOldLogTablesJobHandler`.

Covers :func:`core.log_retention.purge_old_log_rows`, tenant persistent object history
(TenantSettings), and the purge job handler payload/result behavior.

Run (from repo root):

  pytest src/tests/rest/background_jobs/test_log_retention.py -v
"""
from __future__ import annotations

import uuid
from datetime import timedelta
from unittest.mock import patch

import pytest
from django.contrib.contenttypes.models import ContentType
from django.utils import timezone

from background_jobs.job_handlers import PurgeOldLogTablesJobHandler
from background_jobs.models import BackgroundJob, JobStatus, JobType
from core.log_retention import get_log_retention_days, purge_old_log_rows
from core.models import TenantSettings
from crm_records.models import EventLog, Record, RuleExecutionLog, RuleSet
from object_history.models import ObjectHistory
from tests.factories import (
    BackgroundJobFactory,
    EventLogFactory,
    RecordFactory,
    TenantFactory,
)


def _set_created_at(model, pk, ts):
    model.all_objects.filter(pk=pk).update(created_at=ts)


def _make_object_history_row(*, tenant, old=True, persistent_history=False):
    ct = ContentType.objects.get_for_model(Record)
    oh = ObjectHistory.objects.create(
        tenant=tenant,
        content_type=ct,
        object_id=str(uuid.uuid4()),
        object_repr="r",
        action="updated",
        actor_user=None,
        actor_label=None,
        version=1,
        changes={},
        before_state={},
        after_state={},
        metadata={},
        persistent_history=persistent_history,
    )
    if old:
        _set_created_at(
            ObjectHistory,
            oh.pk,
            timezone.now() - timedelta(days=90),
        )
    return oh


@pytest.mark.django_db(transaction=True)
class TestPurgeOldLogRows:
    def test_deletes_old_non_persistent_object_history(self):
        tenant = TenantFactory()
        oh = _make_object_history_row(tenant=tenant, old=True, persistent_history=False)
        assert ObjectHistory.all_objects.filter(pk=oh.pk).exists()

        stats = purge_old_log_rows(days=30, chunk_size=100)

        assert stats["object_history"] >= 1
        assert not ObjectHistory.all_objects.filter(pk=oh.pk).exists()

    def test_keeps_persistent_object_history_older_than_cutoff(self):
        tenant = TenantFactory()
        oh = _make_object_history_row(tenant=tenant, old=True, persistent_history=True)

        purge_old_log_rows(days=30, chunk_size=100)

        assert ObjectHistory.all_objects.filter(pk=oh.pk).exists()

    def test_keeps_recent_non_persistent_object_history(self):
        tenant = TenantFactory()
        oh = _make_object_history_row(tenant=tenant, old=False, persistent_history=False)

        purge_old_log_rows(days=30, chunk_size=100)

        assert ObjectHistory.all_objects.filter(pk=oh.pk).exists()

    def test_deletes_old_event_logs(self):
        tenant = TenantFactory()
        ev = EventLogFactory(tenant=tenant)
        _set_created_at(EventLog, ev.pk, timezone.now() - timedelta(days=90))

        stats = purge_old_log_rows(days=30, chunk_size=100)

        assert stats["event_logs"] >= 1
        assert not EventLog.all_objects.filter(pk=ev.pk).exists()

    def test_deletes_old_rule_exec_logs(self):
        tenant = TenantFactory()
        record = RecordFactory(tenant=tenant)
        rule = RuleSet.objects.create(
            tenant=tenant,
            event_name="evt",
            condition={},
            actions=[],
        )
        log = RuleExecutionLog.objects.create(
            tenant=tenant,
            record=record,
            rule=rule,
            event_name="evt",
            matched=False,
        )
        _set_created_at(RuleExecutionLog, log.pk, timezone.now() - timedelta(days=90))

        stats = purge_old_log_rows(days=30, chunk_size=100)

        assert stats["rule_exec_logs"] >= 1
        assert not RuleExecutionLog.all_objects.filter(pk=log.pk).exists()

    def test_deletes_old_completed_background_jobs_only(self):
        tenant = TenantFactory()
        done = BackgroundJobFactory(
            tenant=tenant,
            job_type=JobType.PARTNER_LEAD_ASSIGN,
            status=JobStatus.COMPLETED,
        )
        pending = BackgroundJobFactory(
            tenant=tenant,
            job_type=JobType.PARTNER_LEAD_ASSIGN,
            status=JobStatus.PENDING,
        )
        old_ts = timezone.now() - timedelta(days=90)
        _set_created_at(BackgroundJob, done.pk, old_ts)
        _set_created_at(BackgroundJob, pending.pk, old_ts)

        stats = purge_old_log_rows(days=30, chunk_size=100)

        assert stats["background_jobs"] >= 1
        assert not BackgroundJob.all_objects.filter(pk=done.pk).exists()
        assert BackgroundJob.all_objects.filter(pk=pending.pk).exists()

    def test_invalid_days_raises(self):
        with pytest.raises(ValueError, match=">= 1"):
            purge_old_log_rows(days=0)

    def test_chunked_delete_removes_all_rows(self):
        tenant = TenantFactory()
        old_ts = timezone.now() - timedelta(days=90)
        for _ in range(5):
            ev = EventLogFactory(tenant=tenant)
            _set_created_at(EventLog, ev.pk, old_ts)

        stats = purge_old_log_rows(days=30, chunk_size=2)

        assert stats["event_logs"] == 5
        assert (
            EventLog.all_objects.filter(
                tenant=tenant,
                created_at__lt=timezone.now() - timedelta(days=30),
            ).count()
            == 0
        )


@pytest.mark.django_db(transaction=True)
class TestTenantSettingsPersistentHistory:
    def test_save_true_sets_persistent_history_on_existing_rows(self):
        tenant = TenantFactory()
        oh = _make_object_history_row(tenant=tenant, old=False, persistent_history=False)
        assert oh.persistent_history is False

        TenantSettings.objects.create(
            tenant=tenant,
            persistent_object_history=True,
        )

        oh.refresh_from_db()
        assert oh.persistent_history is True

    def test_save_false_after_true_clears_flag(self):
        tenant = TenantFactory()
        oh = _make_object_history_row(tenant=tenant, old=False, persistent_history=False)
        ts = TenantSettings.objects.create(
            tenant=tenant,
            persistent_object_history=True,
        )
        oh.refresh_from_db()
        assert oh.persistent_history is True

        ts.persistent_object_history = False
        ts.save()

        oh.refresh_from_db()
        assert oh.persistent_history is False

    def test_object_history_should_persist(self):
        tenant = TenantFactory()
        assert TenantSettings.object_history_should_persist(tenant) is False
        TenantSettings.objects.create(
            tenant=tenant,
            persistent_object_history=True,
        )
        assert TenantSettings.object_history_should_persist(tenant) is True

    def test_delete_settings_clears_persistent_history_on_rows(self):
        tenant = TenantFactory()
        oh = _make_object_history_row(tenant=tenant, old=False, persistent_history=False)
        ts = TenantSettings.objects.create(
            tenant=tenant,
            persistent_object_history=True,
        )
        oh.refresh_from_db()
        assert oh.persistent_history is True

        ts.delete()

        assert not TenantSettings.objects.filter(pk=tenant.pk).exists()
        oh.refresh_from_db()
        assert oh.persistent_history is False


@pytest.mark.django_db(transaction=True)
class TestPurgeOldLogTablesJobHandler:
    def test_process_calls_purge_and_sets_result(self):
        handler = PurgeOldLogTablesJobHandler()
        job = BackgroundJobFactory(
            job_type=JobType.PURGE_OLD_LOG_TABLES,
            payload={"days": 30},
            status=JobStatus.PENDING,
        )
        with patch(
            "core.log_retention.purge_old_log_rows",
            return_value={
                "cutoff": "2020-01-01T00:00:00+00:00",
                "days": 30,
                "object_history": 1,
                "event_logs": 2,
                "rule_exec_logs": 0,
                "background_jobs": 0,
            },
        ) as mock_purge:
            ok = handler.process(job)

        assert ok is True
        mock_purge.assert_called_once_with(days=30)
        assert job.result["success"] is True
        assert job.result["object_history"] == 1
        assert job.result["event_logs"] == 2

    def test_validate_payload_accepts_missing_days(self):
        handler = PurgeOldLogTablesJobHandler()
        with patch("core.log_retention.get_log_retention_days", return_value=30):
            assert handler.validate_payload({}) is True

    def test_validate_payload_rejects_bad_days(self):
        handler = PurgeOldLogTablesJobHandler()
        assert handler.validate_payload({"days": 0}) is False
        assert handler.validate_payload({"days": "nope"}) is False


def test_get_log_retention_days_uses_settings(settings):
    settings.LOG_RETENTION_DAYS = 45
    assert get_log_retention_days() == 45
