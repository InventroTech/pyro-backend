"""API tests for manual background job enqueue / re-run."""
from django.urls import reverse
from rest_framework import status

from background_jobs.models import BackgroundJob, JobStatus, JobType
from tests.base.test_setup import BaseAPITestCase, MultiTenantAPITestCase


class ManualJobControlAPITest(BaseAPITestCase):
    def test_list_job_types_excludes_execute_function(self):
        url = reverse("job-types")
        response = self.client.get(url, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        job_types = response.data["job_types"]
        self.assertIn(JobType.SEND_CSE_ASSIGNED_EVENT, job_types)
        self.assertIn(JobType.SEND_TO_PRAJA, job_types)
        self.assertNotIn(JobType.EXECUTE_FUNCTION, job_types)

    def test_enqueue_cse_assigned_job(self):
        url = reverse("job-enqueue")
        payload = {
            "job_type": JobType.SEND_CSE_ASSIGNED_EVENT,
            "payload": {"user_id": 3181247, "cse_email": "cse@example.com"},
            "priority": 5,
        }
        response = self.client.post(url, payload, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["job_type"], JobType.SEND_CSE_ASSIGNED_EVENT)
        self.assertEqual(response.data["status"], JobStatus.PENDING)
        self.assertEqual(response.data["payload"]["user_id"], 3181247)

        job = BackgroundJob.objects.get(pk=response.data["id"])
        self.assertEqual(job.tenant_id, self.tenant.id)
        self.assertEqual(job.priority, 5)

    def test_enqueue_save_resolved_ticket_job(self):
        url = reverse("job-enqueue")
        payload = {
            "job_type": JobType.SEND_TO_PRAJA,
            "payload": {
                "object_type": "save_resolved_ticket",
                "user_id": 3181247,
                "ticket_id": 1419024,
                "ticket_type": "self_trial",
                "ticket_status": "RESOLVED",
                "all_tasks_completed": False,
            },
        }
        response = self.client.post(url, payload, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["job_type"], JobType.SEND_TO_PRAJA)
        self.assertEqual(response.data["payload"]["ticket_id"], 1419024)

    def test_enqueue_rejects_invalid_payload(self):
        url = reverse("job-enqueue")
        payload = {
            "job_type": JobType.SEND_CSE_ASSIGNED_EVENT,
            "payload": {"user_id": 3181247},
        }
        response = self.client.post(url, payload, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("error", response.data)

    def test_enqueue_rejects_execute_function(self):
        url = reverse("job-enqueue")
        payload = {
            "job_type": JobType.EXECUTE_FUNCTION,
            "payload": {"function_module": "os", "function_name": "system", "args": [], "kwargs": {}},
        }
        response = self.client.post(url, payload, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_rerun_completed_job_clones_pending(self):
        source = BackgroundJob.objects.create(
            job_type=JobType.SEND_CSE_ASSIGNED_EVENT,
            status=JobStatus.COMPLETED,
            priority=0,
            payload={"user_id": 3181247, "cse_email": "cse@example.com"},
            tenant_id=self.tenant.id,
            attempts=1,
            max_attempts=3,
            result={"success": True},
        )
        url = reverse("job-rerun", kwargs={"job_id": source.id})
        response = self.client.post(url, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertNotEqual(response.data["id"], source.id)
        self.assertEqual(response.data["source_job_id"], source.id)
        self.assertEqual(response.data["status"], JobStatus.PENDING)

        source.refresh_from_db()
        self.assertEqual(source.status, JobStatus.COMPLETED)

        cloned = BackgroundJob.objects.get(pk=response.data["id"])
        self.assertEqual(cloned.payload, source.payload)
        self.assertEqual(cloned.job_type, source.job_type)


class ManualJobControlCrossTenantAPITest(MultiTenantAPITestCase):
    def test_enqueue_allows_pyro_admin_tenant_override(self):
        url = reverse("job-enqueue")
        payload = {
            "job_type": JobType.SEND_CSE_ASSIGNED_EVENT,
            "payload": {"user_id": 3181247, "cse_email": "cse@example.com"},
            "tenant_id": str(self.tenant_b.id),
        }
        response = self.client.post(url, payload, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["tenant_id"], str(self.tenant_b.id))

        job = BackgroundJob.objects.get(pk=response.data["id"])
        self.assertEqual(str(job.tenant_id), str(self.tenant_b.id))

    def test_job_detail_can_resolve_cross_tenant_job_for_pyro_admin(self):
        source = BackgroundJob.objects.create(
            job_type=JobType.SEND_CSE_ASSIGNED_EVENT,
            status=JobStatus.COMPLETED,
            priority=0,
            payload={"user_id": 3181247, "cse_email": "cse@example.com"},
            tenant_id=self.tenant_b.id,
            attempts=1,
            max_attempts=3,
            result={"success": True},
        )
        url = reverse("job-detail", kwargs={"job_id": source.id})
        response = self.client.get(url, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], source.id)
        self.assertEqual(response.data["tenant_id"], str(self.tenant_b.id))

    def test_rerun_allows_pyro_admin_tenant_override(self):
        source = BackgroundJob.objects.create(
            job_type=JobType.SEND_CSE_ASSIGNED_EVENT,
            status=JobStatus.COMPLETED,
            priority=0,
            payload={"user_id": 3181247, "cse_email": "cse@example.com"},
            tenant_id=self.tenant_b.id,
            attempts=1,
            max_attempts=3,
            result={"success": True},
        )
        url = reverse("job-rerun", kwargs={"job_id": source.id})
        response = self.client.post(
            url,
            {"tenant_id": str(self.tenant_b.id)},
            format="json",
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["tenant_id"], str(self.tenant_b.id))
        self.assertEqual(response.data["source_job_id"], source.id)
