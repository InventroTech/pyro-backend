import threading
from datetime import timedelta
from unittest.mock import patch

from django.db import IntegrityError, close_old_connections
from django.db.models import Max
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
from core.models import EntityTypeDiscoverySyncState
from crm_records.models import Record
from support_ticket.views import (
    SUPPORT_TICKET_ENTITY_TYPE,
    _dedupe_dumps_latest_wins,
    enqueue_process_dumped_tickets_for_pending_dumps,
    enqueue_process_dumped_tickets_job,
    enqueue_ticket_created_mixpanel,
    on_ticket_created_after_dump,
    process_dumped_tickets,
)
from support_ticket.models import SupportTicket, SupportTicketDump
from tests.base.test_setup import BaseAPITestCase
from tests.factories import TenantFactory
from tests.factories.support_ticket_dump_factory import (
    SupportTicketDumpFactory,
    dump_data,
)
from tests.factories.support_ticket_factory import (
    SnoozedSupportTicketFactory,
    UnassignedSupportTicketFactory,
)


class ProcessDumpedTicketsIngestTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        SupportTicketDump.objects.all().delete()
        SupportTicket.objects.all().delete()
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
        """Open-state dedupe must read from records, not support_ticket."""
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
        self.assertFalse(
            SupportTicket.objects.filter(user_id="cust_records_only").exists()
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

    @patch("support_ticket.views.get_queue_service")
    def test_open_ticket_enqueues_praja_on_dump_process(self, mock_get_queue):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(
                user_id="12345",
                name="Open ticket",
                resolution_status="Open",
            ),
        )

        process_dumped_tickets(
            tenant_id=self.tenant_id,
            on_ticket_created=on_ticket_created_after_dump,
        )

        mock_queue = mock_get_queue.return_value
        praja_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type") == JobType.SEND_TO_PRAJA
        ]
        self.assertEqual(len(praja_calls), 1)
        payload = praja_calls[0].kwargs["payload"]
        self.assertEqual(payload["object_type"], "save_resolved_ticket")
        self.assertEqual(payload["user_id"], 12345)
        self.assertEqual(payload["ticket_status"], "OPEN")
        record = Record.objects.get(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data__user_id="12345",
        )
        self.assertEqual(payload["ticket_id"], record.id)

    @patch("support_ticket.views.get_queue_service")
    def test_non_open_ticket_does_not_enqueue_praja_on_dump_process(self, mock_get_queue):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="67890", name="Pending ticket"),
        )

        process_dumped_tickets(
            tenant_id=self.tenant_id,
            on_ticket_created=on_ticket_created_after_dump,
        )

        mock_queue = mock_get_queue.return_value
        praja_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type") == JobType.SEND_TO_PRAJA
        ]
        self.assertEqual(praja_calls, [])

    @patch.object(SupportTicket.objects, "bulk_create", wraps=SupportTicket.objects.bulk_create)
    def test_bulk_create_ignores_conflicts(self, mock_bulk_create):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="cust_conflict", name="Conflict Test"),
        )

        process_dumped_tickets(tenant_id=self.tenant_id)

        mock_bulk_create.assert_called_once()
        self.assertTrue(mock_bulk_create.call_args.kwargs.get("ignore_conflicts"))
        inserted = mock_bulk_create.call_args.args[0]
        self.assertEqual(len(inserted), 1)
        self.assertIsNotNone(inserted[0].id)

    def test_process_assigns_monotonic_support_ticket_ids(self):
        max_before = SupportTicket.all_objects.aggregate(m=Max("id"))["m"] or 0
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="cust_id_alloc", name="Id Alloc"),
        )

        process_dumped_tickets(tenant_id=self.tenant_id)

        ticket = SupportTicket.objects.get(user_id="cust_id_alloc")
        self.assertEqual(ticket.id, max_before + 1)

    def test_process_maps_dump_fields_to_ticket_and_record(self):
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

        ticket = SupportTicket.objects.get(user_id="cust_extra")
        self.assertEqual(ticket.rm_name, "RM One")

        record = Record.objects.get(entity_type=SUPPORT_TICKET_ENTITY_TYPE, tenant=self.tenant)
        self.assertEqual(record.data["rm_name"], "RM One")
        self.assertEqual(record.data["custom_segment"], "vip_trial")

    def test_process_inserts_support_ticket_and_mirrors_records(self):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="cust_1", name="Mirror Me", poster="in_trial"),
        )

        result = process_dumped_tickets(tenant_id=self.tenant_id)

        self.assertEqual(result.inserted_tickets, 1)
        self.assertEqual(result.mirrored_records, 1)

        ticket = SupportTicket.objects.get(user_id="cust_1")
        self.assertEqual(ticket.name, "Mirror Me")
        self.assertEqual(ticket.poster, "in_trial")
        self.assertIsNotNone(ticket.dumped_at)

        record = Record.objects.get(entity_type=SUPPORT_TICKET_ENTITY_TYPE, tenant=self.tenant)
        self.assertEqual(record.data["user_id"], "cust_1")
        self.assertEqual(record.data["support_ticket_id"], ticket.id)
        self.assertEqual(record.data["name"], "Mirror Me")
        self.assertEqual(record.data["call_status"], "Call Waiting")

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
        self.assertTrue(SupportTicket.objects.filter(user_id="valid_user").exists())

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
        self.assertTrue(SupportTicket.objects.filter(user_id="tenant_a").exists())
        self.assertFalse(SupportTicket.objects.filter(user_id="tenant_b").exists())
        self.assertTrue(
            SupportTicketDump.objects.filter(
                tenant_id=other_tenant.id, is_processed=False
            ).exists()
        )

    def test_bulk_create_failure_rollbacks_open_ticket_deletions(self):
        open_ticket = UnassignedSupportTicketFactory.create(
            tenant=self.tenant,
            user_id="cust_rollback",
            resolution_status=None,
        )
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            data=dump_data(user_id="cust_rollback", name="Replacement"),
        )

        with patch.object(
            SupportTicket.objects,
            "bulk_create",
            side_effect=IntegrityError("simulated bulk_create failure"),
        ):
            with self.assertRaises(IntegrityError):
                process_dumped_tickets(tenant_id=self.tenant_id)

        open_ticket.refresh_from_db()
        self.assertEqual(
            SupportTicket.objects.filter(user_id="cust_rollback").count(),
            1,
        )
        self.assertEqual(open_ticket.id, SupportTicket.objects.get(user_id="cust_rollback").id)
        self.assertNotEqual(open_ticket.name, "Replacement")
        self.assertTrue(
            SupportTicketDump.objects.filter(
                tenant_id=self.tenant_id,
                data__user_id="cust_rollback",
                is_processed=False,
            ).exists()
        )

    def test_process_replaces_open_support_ticket_not_snoozed(self):
        open_ticket = UnassignedSupportTicketFactory.create(
            tenant=self.tenant,
            user_id="cust_2",
            resolution_status=None,
        )
        snoozed = SnoozedSupportTicketFactory.create(
            tenant=self.tenant,
            user_id="cust_3",
            resolution_status="Snoozed",
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

        self.assertFalse(SupportTicket.objects.filter(id=open_ticket.id).exists())
        self.assertTrue(SupportTicket.objects.filter(id=snoozed.id).exists())
        self.assertEqual(SupportTicket.objects.filter(user_id="cust_2").count(), 1)
        self.assertEqual(SupportTicket.objects.get(user_id="cust_2").name, "Replacement")
        self.assertEqual(SupportTicket.objects.filter(user_id="cust_3").count(), 2)

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
    """DB-backed scheduler throttle for process_dumped_tickets enqueue ticks."""

    def setUp(self):
        super().setUp()
        EntityTypeDiscoverySyncState.objects.filter(
            job_name=PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME,
        ).delete()

    @patch("support_ticket.views.enqueue_process_dumped_tickets_for_pending_dumps")
    def test_scheduler_skips_tick_within_five_minutes(self, mock_enqueue):
        mock_enqueue.return_value = {"enqueued": [], "skipped_active_job": []}
        EntityTypeDiscoverySyncState.objects.create(
            job_name=PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME,
            last_success_at=timezone.now() - timedelta(seconds=60),
        )
        processor = JobProcessor(worker_id="test-scheduler")
        processor._last_support_ticket_dump_enqueue_at = None
        processor._maybe_enqueue_process_dumped_tickets()
        mock_enqueue.assert_not_called()

    @patch("support_ticket.views.enqueue_process_dumped_tickets_for_pending_dumps")
    def test_scheduler_runs_tick_after_five_minutes(self, mock_enqueue):
        mock_enqueue.return_value = {"enqueued": [], "skipped_active_job": []}
        EntityTypeDiscoverySyncState.objects.create(
            job_name=PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME,
            last_success_at=timezone.now()
            - timedelta(seconds=SUPPORT_TICKET_DUMP_ENQUEUE_INTERVAL + 1),
        )
        processor = JobProcessor(worker_id="test-scheduler")
        processor._last_support_ticket_dump_enqueue_at = None
        processor._maybe_enqueue_process_dumped_tickets()
        mock_enqueue.assert_called_once()
        state = EntityTypeDiscoverySyncState.objects.get(
            job_name=PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME,
        )
        self.assertIsNotNone(state.last_success_at)
        self.assertIsNone(state.last_error)

    @patch(
        "support_ticket.views.enqueue_process_dumped_tickets_for_pending_dumps",
        side_effect=RuntimeError("enqueue failed"),
    )
    def test_scheduler_records_error_without_advancing_success(self, mock_enqueue):
        processor = JobProcessor(worker_id="test-scheduler")
        processor._maybe_enqueue_process_dumped_tickets()
        mock_enqueue.assert_called_once()
        state = EntityTypeDiscoverySyncState.objects.get(
            job_name=PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME,
        )
        self.assertIsNone(state.last_success_at)
        self.assertIn("enqueue failed", state.last_error or "")


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
        SupportTicket.objects.all().delete()

    @patch("support_ticket.views.on_ticket_created_after_dump")
    def test_job_handler_processes_tenant_dumps(self, mock_on_ticket_created):
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
        self.assertTrue(SupportTicket.objects.filter(user_id="job_user").exists())
        mock_on_ticket_created.assert_called_once()


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
