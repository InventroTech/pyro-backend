"""Tests for per-tenant background job enqueue helpers."""
from unittest.mock import patch

from django.test import TestCase

from background_jobs.models import BackgroundJob, JobType
from background_jobs.queue_service import get_queue_service
from background_jobs.tenant_jobs import enqueue_for_all_tenants
from tests.factories import TenantFactory


class EnqueueForAllTenantsTests(TestCase):
    def test_enqueue_sets_tenant_id_on_each_job(self):
        t1 = TenantFactory()
        t2 = TenantFactory()
        queue = get_queue_service()
        jobs = enqueue_for_all_tenants(
            queue,
            job_type=JobType.UNASSIGN_SNOOZED_LEADS,
            payload={},
            priority=0,
        )
        self.assertEqual(len(jobs), 2)
        tenant_ids = {str(j.tenant_id) for j in jobs}
        self.assertEqual(tenant_ids, {str(t1.id), str(t2.id)})
        for job in jobs:
            row = BackgroundJob.objects.get(pk=job.pk)
            self.assertIsNotNone(row.tenant_id)
