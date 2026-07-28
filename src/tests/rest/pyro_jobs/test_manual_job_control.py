"""API tests for manual pyro job enqueue / re-run."""
import uuid
from unittest import mock

import jwt
from django.conf import settings
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from pyro_jobs.handlers import JOB_HANDLERS
from pyro_jobs.models import PyroJob
from tests.factories import (
    RoleFactory,
    SupabaseAuthUserFactory,
    TenantFactory,
    TenantMembershipFactory,
    UserFactory,
)


def _make_auth(tenant):
    uid = str(uuid.uuid4())
    email = f"{uid[:8]}@example.com"
    UserFactory(
        supabase_uid=uid,
        email=email,
        tenant_id=str(tenant.id),
        role="authenticated",
    )
    SupabaseAuthUserFactory(id=uuid.UUID(uid), email=email)
    role = RoleFactory(tenant=tenant, key="pyro_admin", name="Pyro Admin")
    TenantMembershipFactory(tenant=tenant, user_id=uid, email=email, role=role)
    token = jwt.encode(
        {
            "sub": uid,
            "email": email,
            "tenant_id": str(tenant.id),
            "role": "authenticated",
            "aud": "authenticated",
            "user_data": {"tenant_id": str(tenant.id)},
        },
        settings.SUPABASE_JWT_SECRET,
        algorithm="HS256",
    )
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return {
        "HTTP_AUTHORIZATION": f"Bearer {token}",
        "HTTP_X_TENANT_ID": str(tenant.id),
    }


class ManualPyroJobControlAPITest(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.tenant = TenantFactory()
        self.auth_headers = _make_auth(self.tenant)

    def test_list_job_types(self):
        url = reverse("pyro-job-types")
        response = self.client.get(url, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        job_types = response.data["job_types"]
        for job_name in JOB_HANDLERS.keys():
            self.assertIn(job_name, job_types)

    def test_enqueue_purge_job(self):
        url = reverse("pyro-job-enqueue")
        payload = {
            "job_name": "purge_old_log_tables",
            "payload": {
                "days": 30,
                "chunk_size": 1000,
                "max_chunks_per_table": 20,
            },
        }
        response = self.client.post(url, payload, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["job_name"], "purge_old_log_tables")
        self.assertEqual(response.data["status"], PyroJob.STATUS_PENDING)
        self.assertEqual(response.data["payload"]["days"], 30)
        self.assertEqual(response.data["ran_pending_jobs"], [])

        job = PyroJob.objects.get(pk=response.data["id"])
        self.assertEqual(job.job_name, "purge_old_log_tables")
        self.assertEqual(job.status, PyroJob.STATUS_PENDING)
        self.assertFalse(job.is_deleted)

    def test_enqueue_runs_existing_pending_then_creates_new(self):
        existing = PyroJob.objects.create(
            job_name="purge_old_log_tables",
            payload={},
            run_at=timezone.now(),
            status=PyroJob.STATUS_PENDING,
            is_deleted=False,
        )
        url = reverse("pyro-job-enqueue")
        payload = {
            "job_name": "purge_old_log_tables",
            "payload": {"days": 7},
        }
        with mock.patch.dict(
            "pyro_jobs.queue_service.JOB_HANDLERS",
            {"purge_old_log_tables": lambda _payload: {"ok": True}},
            clear=False,
        ):
            response = self.client.post(url, payload, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertNotEqual(response.data["id"], existing.id)
        self.assertEqual(len(response.data["ran_pending_jobs"]), 1)
        self.assertEqual(response.data["ran_pending_jobs"][0]["id"], existing.id)
        self.assertEqual(response.data["ran_pending_jobs"][0]["status"], PyroJob.STATUS_COMPLETED)
        self.assertEqual(response.data["payload"]["days"], 7)

        existing.refresh_from_db()
        self.assertEqual(existing.status, PyroJob.STATUS_COMPLETED)
        self.assertTrue(existing.is_deleted)
        self.assertEqual(existing.result, {"ok": True})

        self.assertEqual(
            PyroJob.objects.filter(
                job_name="purge_old_log_tables",
                status=PyroJob.STATUS_PENDING,
                is_deleted=False,
            ).count(),
            1,
        )

    def test_enqueue_rejects_unknown_job_name(self):
        url = reverse("pyro-job-enqueue")
        payload = {
            "job_name": "not_a_real_job",
            "payload": {},
        }
        response = self.client.post(url, payload, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("error", response.data)

    def test_job_detail_returns_fields(self):
        source = PyroJob.objects.create(
            job_name="dispatch_data_sync",
            payload={},
            run_at=timezone.now(),
            status=PyroJob.STATUS_COMPLETED,
            attempts=1,
            max_attempts=3,
            result={"success": True},
            is_deleted=True,
        )
        url = reverse("pyro-job-detail", kwargs={"job_id": source.id})
        response = self.client.get(url, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], source.id)
        self.assertEqual(response.data["job_name"], "dispatch_data_sync")
        self.assertEqual(response.data["status"], PyroJob.STATUS_COMPLETED)
        self.assertEqual(response.data["result"], {"success": True})

    def test_rerun_completed_job_clones_pending(self):
        source = PyroJob.objects.create(
            job_name="snoozed_to_not_connected_midnight",
            payload={},
            run_at=timezone.now(),
            status=PyroJob.STATUS_COMPLETED,
            attempts=1,
            max_attempts=3,
            result={"updated": 2},
            is_deleted=True,
        )
        url = reverse("pyro-job-rerun", kwargs={"job_id": source.id})
        response = self.client.post(url, format="json", **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertNotEqual(response.data["id"], source.id)
        self.assertEqual(response.data["source_job_id"], source.id)
        self.assertEqual(response.data["status"], PyroJob.STATUS_PENDING)
        self.assertEqual(response.data["job_name"], source.job_name)
        self.assertEqual(response.data["ran_pending_jobs"], [])

        source.refresh_from_db()
        self.assertEqual(source.status, PyroJob.STATUS_COMPLETED)

        cloned = PyroJob.objects.get(pk=response.data["id"])
        self.assertEqual(cloned.payload, source.payload)
        self.assertEqual(cloned.job_name, source.job_name)
        self.assertFalse(cloned.is_deleted)
