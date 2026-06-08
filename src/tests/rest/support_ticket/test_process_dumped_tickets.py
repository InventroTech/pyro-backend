from unittest.mock import patch

from django.db import IntegrityError
from django.db.models import Max
from django.urls import reverse
from rest_framework import status

from background_jobs.job_handlers import ProcessDumpedTicketsJobHandler
from background_jobs.models import BackgroundJob, JobStatus, JobType
from crm_records.models import Record
from support_ticket.views import (
    SUPPORT_TICKET_ENTITY_TYPE,
    _dedupe_dumps_latest_wins,
    enqueue_process_dumped_tickets_for_pending_dumps,
    enqueue_process_dumped_tickets_job,
    process_dumped_tickets,
)
from support_ticket.models import SupportTicket, SupportTicketDump
from tests.base.test_setup import BaseAPITestCase
from tests.factories import TenantFactory
from tests.factories.support_ticket_dump_factory import SupportTicketDumpFactory
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
            user_id="user_a",
            name="First",
        )
        second = SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            user_id="user_a",
            name="Second",
        )
        result = _dedupe_dumps_latest_wins([first, second])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "Second")

    @patch.object(SupportTicket.objects, "bulk_create", wraps=SupportTicket.objects.bulk_create)
    def test_bulk_create_ignores_conflicts(self, mock_bulk_create):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            user_id="cust_conflict",
            name="Conflict Test",
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
            user_id="cust_id_alloc",
            name="Id Alloc",
        )

        process_dumped_tickets(tenant_id=self.tenant_id)

        ticket = SupportTicket.objects.get(user_id="cust_id_alloc")
        self.assertEqual(ticket.id, max_before + 1)

    def test_process_inserts_support_ticket_and_mirrors_records(self):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            user_id="cust_1",
            name="Mirror Me",
            poster="in_trial",
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

    def test_process_scoped_to_tenant(self):
        other_tenant = TenantFactory()
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            user_id="tenant_a",
            name="Mine",
        )
        SupportTicketDumpFactory.create(
            tenant_id=other_tenant.id,
            user_id="tenant_b",
            name="Other",
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
            user_id="cust_rollback",
            name="Replacement",
        )

        with patch.object(
            SupportTicket.objects,
            "bulk_create",
            side_effect=IntegrityError("simulated bulk_create failure"),
        ):
            with self.assertRaises(IntegrityError):
                process_dumped_tickets(tenant_id=self.tenant_id)

        self.assertTrue(SupportTicket.objects.filter(id=open_ticket.id).exists())
        self.assertFalse(SupportTicket.objects.filter(user_id="cust_rollback").exists())
        self.assertTrue(
            SupportTicketDump.objects.filter(
                tenant_id=self.tenant_id,
                user_id="cust_rollback",
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
            user_id="cust_2",
            name="Replacement",
        )
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            user_id="cust_3",
            name="Should not replace snoozed",
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
            user_id="pending_user",
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


class ProcessDumpedTicketsJobHandlerTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        SupportTicketDump.objects.all().delete()
        SupportTicket.objects.all().delete()

    @patch("support_ticket.views.enqueue_ticket_created_mixpanel")
    def test_job_handler_processes_tenant_dumps(self, mock_mixpanel):
        SupportTicketDumpFactory.create(
            tenant_id=self.tenant_id,
            user_id="job_user",
            name="From job",
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
