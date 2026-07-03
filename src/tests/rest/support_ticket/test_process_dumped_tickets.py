import threading
from datetime import timedelta
from unittest.mock import Mock, patch

from django.db import IntegrityError, close_old_connections
from django.test import TransactionTestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status

from background_jobs.job_handlers import ProcessDumpedTicketsJobHandler
from background_jobs.job_processor import (
    PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME,
    SUPPORT_TICKET_DUMP_ENQUEUE_INTERVAL,
    JobProcessor,
)
from background_jobs.models import BackgroundJob, JobStatus, JobType
from crm_records.models import Record
from support_ticket.views import (
    SUPPORT_TICKET_ENTITY_TYPE,
    _dedupe_dumps_latest_wins,
    enqueue_process_dumped_tickets_for_pending_dumps,
    enqueue_process_dumped_tickets_job,
    enqueue_ticket_created_mixpanel,
    process_dumped_tickets,
)
from support_ticket.models import SupportTicketDump
from tests.base.test_setup import BaseAPITestCase
from tests.factories import TenantFactory
from tests.factories.support_ticket_dump_factory import (
    SupportTicketDumpFactory,
    dump_data,
)


class ProcessDumpedTicketsIngestTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        SupportTicketDump.objects.all().delete()
        Record.objects.filter(entity_type=SUPPORT_TICKET_ENTITY_TYPE).delete()

    def test_dedupe_latest_wins(self):
        first = SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="user_a", name="First"),
        )
        second = SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="user_a", name="Second"),
        )
        result = _dedupe_dumps_latest_wins([first, second])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].data.get("name"), "Second")

    def test_dedupe_collapses_int_and_str_user_id(self):
        first = SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id=2974459, name="Numeric id"),
        )
        second = SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="2974459", name="String id"),
        )
        result = _dedupe_dumps_latest_wins([first, second])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].data.get("name"), "String id")

    def test_dedupe_prefers_self_trial_over_later_non_self_trial(self):
        in_trial = SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(
                user_id="user_self_trial_priority",
                name="Later in trial",
                poster="in_trial",
            ),
        )
        self_trial = SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(
                user_id="user_self_trial_priority",
                name="Earlier self trial",
                support_ticket_type="SELF TRIAL",
            ),
        )
        result = _dedupe_dumps_latest_wins([self_trial, in_trial])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].data.get("name"), "Earlier self trial")

    def test_process_uses_open_self_trial_record_not_support_ticket_table(self):
        """Open-state dedupe must read from records only."""
        open_self_trial_record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="cust_records_only",
                name="Open self trial record",
                support_ticket_type="SELF TRIAL",
            ),
        )
        other_open_record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="cust_records_only",
                name="Other open type",
                support_ticket_type="in_trial",
            ),
        )
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(
                user_id="cust_records_only",
                name="Incoming in trial",
                poster="in_trial",
            ),
        )

        process_dumped_tickets(tenant_id=self.tenant_id)

        open_self_trial_record.refresh_from_db()
        self.assertIsNone(open_self_trial_record.data.get("resolution_status"))
        self.assertFalse(
            Record.objects.filter(id=other_open_record.id).exists()
        )
        self.assertEqual(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="cust_records_only",
            ).count(),
            1,
        )

    def test_process_skips_self_trial_dump_when_open_self_trial_record_exists(self):
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="cust_skip_self_trial",
                name="Keep this record",
                support_ticket_type="SELF TRIAL",
            ),
        )
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(
                user_id="cust_skip_self_trial",
                name="Should not insert",
                support_ticket_type="SELF TRIAL",
            ),
        )

        result = process_dumped_tickets(tenant_id=self.tenant_id)

        self.assertEqual(result.inserted_tickets, 0)
        self.assertEqual(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="cust_skip_self_trial",
            ).count(),
            1,
        )
        self.assertEqual(
            Record.objects.get(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="cust_skip_self_trial",
            ).data.get("name"),
            "Keep this record",
        )

    def test_process_inserts_self_trial_when_no_open_self_trial_record(self):
        other_open_record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="cust_new_self_trial",
                name="Paid record",
                support_ticket_type="paid",
            ),
        )
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(
                user_id="cust_new_self_trial",
                name="New self trial",
                support_ticket_type="SELF TRIAL",
            ),
        )

        process_dumped_tickets(tenant_id=self.tenant_id)

        self.assertFalse(Record.objects.filter(id=other_open_record.id).exists())

        new_record = Record.objects.get(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data__name="New self trial",
        )
        self.assertIsNone(new_record.data.get("resolution_status"))

    @patch("support_ticket.views.get_queue_service")
    def test_ticket_created_mixpanel_includes_support_ticket_type(self, mock_get_queue):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(
                user_id="mixpanel_user",
                name="Mixpanel ticket",
                support_ticket_type="SELF TRIAL",
                poster="legacy_poster",
                release_build_number="1.2.3",
            ),
        )

        process_dumped_tickets(
            tenant_id=self.tenant_id,
            on_ticket_created=enqueue_ticket_created_mixpanel,
        )

        mock_queue = mock_get_queue.return_value
        mixpanel_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type") == JobType.SEND_MIXPANEL_EVENT
            and call.kwargs.get("payload", {}).get("event_name") == "pyro_st_ticket_created"
        ]
        self.assertEqual(len(mixpanel_calls), 1)
        properties = mixpanel_calls[0].kwargs["payload"]["properties"]
        record = Record.objects.get(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data__user_id="mixpanel_user",
        )
        self.assertEqual(record.data.get("release_build_number"), "1.2.3")
        self.assertEqual(properties["support_ticket_type"], "SELF TRIAL")
        self.assertEqual(properties["poster"], "legacy_poster")
        self.assertEqual(properties["release_build_number"], "1.2.3")

    @patch("support_ticket.views.get_queue_service")
    def test_ticket_created_mixpanel_support_ticket_type_not_poster_fallback(
        self, mock_get_queue
    ):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(
                user_id="mixpanel_poster_only",
                name="Poster only ticket",
                poster="in_trial",
            ),
        )

        process_dumped_tickets(
            tenant_id=self.tenant_id,
            on_ticket_created=enqueue_ticket_created_mixpanel,
        )

        mock_queue = mock_get_queue.return_value
        mixpanel_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type") == JobType.SEND_MIXPANEL_EVENT
            and call.kwargs.get("payload", {}).get("event_name") == "pyro_st_ticket_created"
        ]
        self.assertEqual(len(mixpanel_calls), 1)
        properties = mixpanel_calls[0].kwargs["payload"]["properties"]
        self.assertIsNone(properties["support_ticket_type"])
        self.assertEqual(properties["poster"], "in_trial")

    def test_process_maps_dump_fields_to_record(self):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(
                user_id="cust_extra",
                name="Extra Fields",
                rm_name="RM One",
                custom_segment="vip_trial",
            ),
        )

        process_dumped_tickets(tenant_id=self.tenant_id)

        record = Record.objects.get(
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            tenant=self.tenant,
            data__user_id="cust_extra",
        )
        self.assertEqual(record.data["rm_name"], "RM One")
        self.assertEqual(record.data["custom_segment"], "vip_trial")

    def test_process_inserts_records_from_dump(self):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="cust_1", name="Mirror Me", poster="in_trial"),
        )

        result = process_dumped_tickets(tenant_id=self.tenant_id)

        self.assertEqual(result.inserted_tickets, 1)
        self.assertEqual(result.mirrored_records, 1)

        record = Record.objects.get(
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            tenant=self.tenant,
            data__user_id="cust_1",
        )
        self.assertEqual(record.data["name"], "Mirror Me")
        self.assertEqual(record.data["poster"], "in_trial")
        self.assertIsNotNone(record.data.get("dumped_at"))
        self.assertEqual(record.data["call_status"], "Call Waiting")

    def test_process_does_not_write_support_ticket_table(self):
        from support_ticket.models import SupportTicket

        SupportTicket.objects.all().delete()
        count_before = SupportTicket.objects.count()
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="cust_no_legacy", name="Records Only"),
        )

        process_dumped_tickets(tenant_id=self.tenant_id)

        self.assertEqual(SupportTicket.objects.count(), count_before)
        self.assertTrue(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="cust_no_legacy",
            ).exists()
        )

    def test_process_counts_dumps_without_user_id_as_skipped(self):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data={"name": "No user"},
        )
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="valid_user", name="Valid"),
        )

        result = process_dumped_tickets(tenant_id=self.tenant_id)

        self.assertEqual(result.skipped_tickets, 1)
        self.assertEqual(result.inserted_tickets, 1)
        self.assertTrue(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="valid_user",
            ).exists()
        )

    def test_process_scoped_to_tenant(self):
        other_tenant = TenantFactory()
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="tenant_a", name="Mine"),
        )
        SupportTicketDumpFactory.create(
            tenant_id=other_tenant.id,
            data=dump_data(user_id="tenant_b", name="Other"),
        )

        result = process_dumped_tickets(tenant_id=self.tenant_id)

        self.assertEqual(result.inserted_tickets, 1)
        self.assertTrue(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="tenant_a",
            ).exists()
        )
        self.assertFalse(
            Record.objects.filter(
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="tenant_b",
            ).exists()
        )
        self.assertTrue(
            SupportTicketDump.objects.filter(
                tenant_id=other_tenant.id, is_processed=False
            ).exists()
        )

    def test_record_create_failure_rollbacks_open_record_deletions(self):
        open_record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="cust_rollback",
                name="Open record",
                resolution_status=None,
            ),
        )
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="cust_rollback", name="Replacement"),
        )

        with patch.object(
            Record.objects,
            "create",
            side_effect=IntegrityError("simulated create failure"),
        ):
            with self.assertRaises(IntegrityError):
                process_dumped_tickets(tenant_id=self.tenant_id)

        open_record.refresh_from_db()
        self.assertEqual(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="cust_rollback",
            ).count(),
            1,
        )
        self.assertEqual(open_record.id, Record.objects.get(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data__user_id="cust_rollback",
        ).id)
        self.assertEqual(open_record.data.get("name"), "Open record")
        self.assertTrue(
            SupportTicketDump.objects.filter(
                tenant_id=self.tenant_id,
                data__user_id="cust_rollback",
                is_processed=False,
            ).exists()
        )

    def test_process_replaces_open_record_not_snoozed(self):
        open_record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="cust_2",
                name="Open record",
                resolution_status=None,
            ),
        )
        snoozed = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="cust_3",
                name="Snoozed record",
                resolution_status="Snoozed",
            ),
        )
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="cust_2", name="Replacement"),
        )
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="cust_3", name="Should not replace snoozed"),
        )

        process_dumped_tickets(tenant_id=self.tenant_id)

        self.assertFalse(Record.objects.filter(id=open_record.id).exists())
        self.assertTrue(Record.objects.filter(id=snoozed.id).exists())
        self.assertEqual(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="cust_2",
            ).count(),
            1,
        )
        self.assertEqual(
            Record.objects.get(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="cust_2",
            ).data.get("name"),
            "Replacement",
        )
        self.assertEqual(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="cust_3",
            ).count(),
            2,
        )

    def test_enqueue_for_pending_dumps_only_when_unprocessed_exist(self):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="pending_user"),
        )
        result = enqueue_process_dumped_tickets_for_pending_dumps()
        self.assertEqual(len(result["enqueued"]), 1)
        self.assertEqual(result["enqueued"][0]["tenant_id"], str(self.tenant_id))

        SupportTicketDump.objects.all().update(is_processed=True)
        result2 = enqueue_process_dumped_tickets_for_pending_dumps()
        self.assertEqual(result2["enqueued"], [])

    def test_enqueue_dedupes_active_jobs_per_tenant(self):
        job1 = enqueue_process_dumped_tickets_job(self.tenant_id)
        job2 = enqueue_process_dumped_tickets_job(self.tenant_id)
        self.assertIsNotNone(job1)
        self.assertIsNone(job2)
        self.assertEqual(
            BackgroundJob.objects.filter(
                job_type=JobType.PROCESS_DUMPED_TICKETS,
                tenant_id=self.tenant_id,
            ).count(),
            1,
        )

    def test_enqueue_skips_when_job_is_processing(self):
        enqueue_process_dumped_tickets_job(self.tenant_id)
        job = BackgroundJob.objects.get(
            job_type=JobType.PROCESS_DUMPED_TICKETS,
            tenant_id=self.tenant_id,
        )
        job.status = JobStatus.PROCESSING
        job.save(update_fields=["status"])

        self.assertIsNone(enqueue_process_dumped_tickets_job(self.tenant_id))
        self.assertEqual(
            BackgroundJob.objects.filter(
                job_type=JobType.PROCESS_DUMPED_TICKETS,
                tenant_id=self.tenant_id,
            ).count(),
            1,
        )

    def test_enqueue_skips_when_job_is_retrying(self):
        enqueue_process_dumped_tickets_job(self.tenant_id)
        job = BackgroundJob.objects.get(
            job_type=JobType.PROCESS_DUMPED_TICKETS,
            tenant_id=self.tenant_id,
        )
        job.status = JobStatus.RETRYING
        job.save(update_fields=["status"])

        self.assertIsNone(enqueue_process_dumped_tickets_job(self.tenant_id))


class ProcessDumpedTicketsSchedulerTest(BaseAPITestCase):
    """Scheduler throttle for process_dumped_tickets enqueue ticks."""

    def _make_processor(self):
        processor = JobProcessor(worker_id="test-scheduler")
        processor._last_support_ticket_dump_enqueue_at = None
        return processor

    @staticmethod
    def _mock_atomic(mock_atomic):
        mock_atomic.return_value.__enter__ = Mock(return_value=None)
        mock_atomic.return_value.__exit__ = Mock(return_value=False)

    @patch("background_jobs.job_processor.transaction.atomic")
    @patch("background_jobs.job_processor.EntityTypeDiscoverySyncState.objects")
    @patch("support_ticket.views.enqueue_process_dumped_tickets_for_pending_dumps")
    def test_scheduler_skips_tick_within_five_minutes(
        self, mock_enqueue, mock_state_objects, mock_atomic,
    ):
        self._mock_atomic(mock_atomic)
        mock_enqueue.return_value = {"enqueued": [], "skipped_active_job": []}

        scheduler_state = Mock()
        scheduler_state.last_success_at = timezone.now() - timedelta(seconds=60)
        mock_state_objects.select_for_update.return_value.get_or_create.return_value = (
            scheduler_state,
            False,
        )

        self._make_processor()._maybe_enqueue_process_dumped_tickets()
        mock_enqueue.assert_not_called()

    @patch("background_jobs.job_processor.transaction.atomic")
    @patch("background_jobs.job_processor.EntityTypeDiscoverySyncState.objects")
    @patch("support_ticket.views.enqueue_process_dumped_tickets_for_pending_dumps")
    def test_scheduler_runs_tick_after_five_minutes(
        self, mock_enqueue, mock_state_objects, mock_atomic,
    ):
        self._mock_atomic(mock_atomic)
        mock_enqueue.return_value = {"enqueued": [], "skipped_active_job": []}

        scheduler_state = Mock()
        scheduler_state.last_success_at = timezone.now() - timedelta(
            seconds=SUPPORT_TICKET_DUMP_ENQUEUE_INTERVAL + 1
        )
        scheduler_state.last_error = "old error"
        mock_state_objects.select_for_update.return_value.get_or_create.return_value = (
            scheduler_state,
            False,
        )

        self._make_processor()._maybe_enqueue_process_dumped_tickets()

        mock_enqueue.assert_called_once()
        self.assertIsNotNone(scheduler_state.last_success_at)
        self.assertIsNone(scheduler_state.last_error)
        scheduler_state.save.assert_called_once_with(
            update_fields=["last_success_at", "last_error", "updated_at"]
        )

    @patch("background_jobs.job_processor.EntityTypeDiscoverySyncState.objects")
    @patch("background_jobs.job_processor.transaction.atomic")
    @patch(
        "support_ticket.views.enqueue_process_dumped_tickets_for_pending_dumps",
        side_effect=RuntimeError("enqueue failed"),
    )
    def test_scheduler_records_error_without_advancing_success(
        self, mock_enqueue, mock_atomic, mock_state_objects,
    ):
        self._mock_atomic(mock_atomic)

        scheduler_state = Mock()
        scheduler_state.last_success_at = None
        mock_state_objects.select_for_update.return_value.get_or_create.return_value = (
            scheduler_state,
            True,
        )

        self._make_processor()._maybe_enqueue_process_dumped_tickets()

        mock_enqueue.assert_called_once()
        mock_state_objects.update_or_create.assert_called_once()
        call_kwargs = mock_state_objects.update_or_create.call_args.kwargs
        self.assertEqual(
            call_kwargs["job_name"],
            PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME,
        )
        self.assertIn("enqueue failed", call_kwargs["defaults"]["last_error"])


class ProcessDumpedTicketsEnqueueRaceTest(TransactionTestCase):
    """
    TransactionTestCase is required so concurrent threads each commit
    independently (advisory locks are transaction-scoped).
    """

    def setUp(self):
        self.tenant = TenantFactory()
        self.tenant_id = str(self.tenant.id)
        BackgroundJob.objects.all().delete()

    def test_concurrent_enqueue_creates_only_one_job(self):
        worker_count = 8
        results: list = []
        errors: list = []
        barrier = threading.Barrier(worker_count)

        def worker():
            close_old_connections()
            try:
                barrier.wait(timeout=5)
                job = enqueue_process_dumped_tickets_job(self.tenant_id)
                results.append(job.id if job else None)
            except Exception as exc:
                errors.append(exc)
            finally:
                close_old_connections()

        threads = [threading.Thread(target=worker) for _ in range(worker_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        created_ids = [job_id for job_id in results if job_id is not None]
        self.assertEqual(len(created_ids), 1)
        self.assertEqual(sum(1 for job_id in results if job_id is None), worker_count - 1)
        self.assertEqual(
            BackgroundJob.objects.filter(
                job_type=JobType.PROCESS_DUMPED_TICKETS,
                tenant_id=self.tenant_id,
            ).count(),
            1,
        )

    def test_concurrent_scheduler_tick_creates_only_one_job(self):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="race_user"),
        )
        worker_count = 4
        results: list = []
        errors: list = []
        barrier = threading.Barrier(worker_count)

        def worker():
            close_old_connections()
            try:
                barrier.wait(timeout=5)
                result = enqueue_process_dumped_tickets_for_pending_dumps()
                results.append(result)
            except Exception as exc:
                errors.append(exc)
            finally:
                close_old_connections()

        threads = [threading.Thread(target=worker) for _ in range(worker_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(
            BackgroundJob.objects.filter(
                job_type=JobType.PROCESS_DUMPED_TICKETS,
                tenant_id=self.tenant_id,
            ).count(),
            1,
        )
        enqueued_counts = [len(r.get("enqueued") or []) for r in results]
        self.assertEqual(sum(enqueued_counts), 1)
        skipped_counts = [len(r.get("skipped_active_job") or []) for r in results]
        self.assertEqual(sum(skipped_counts), worker_count - 1)


class ProcessDumpedTicketsJobHandlerTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        SupportTicketDump.objects.all().delete()
        Record.objects.filter(entity_type=SUPPORT_TICKET_ENTITY_TYPE).delete()

    @patch("support_ticket.views.enqueue_ticket_created_mixpanel")
    def test_job_handler_processes_tenant_dumps(self, mock_mixpanel):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="job_user", name="From job"),
        )
        job = BackgroundJob.objects.create(
            job_type=JobType.PROCESS_DUMPED_TICKETS,
            status=JobStatus.PENDING,
            tenant_id=self.tenant_id,
            payload={},
        )

        ok = ProcessDumpedTicketsJobHandler().process(job)

        self.assertTrue(ok)
        self.assertEqual(job.result["inserted_tickets"], 1)
        self.assertTrue(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                data__user_id="job_user",
            ).exists()
        )
        mock_mixpanel.assert_called_once()


class ProcessDumpedTicketsAPITest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        BackgroundJob.objects.all().delete()
        self.url = reverse("support_ticket:process-dumped-tickets")

    def test_api_enqueues_job_for_tenant(self):
        response = self.client.post(
            self.url,
            data={"tenant_id": str(self.tenant_id)},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.assertEqual(response.data["tenant_id"], str(self.tenant_id))
        self.assertTrue(
            BackgroundJob.objects.filter(
                job_type=JobType.PROCESS_DUMPED_TICKETS,
                tenant_id=self.tenant_id,
                status=JobStatus.PENDING,
            ).exists()
        )
