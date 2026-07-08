import uuid
from unittest.mock import MagicMock, patch

from rest_framework import status

from accounts.models import SupabaseAuthUser
from background_jobs.models import JobType
from crm_records.models import Record
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory, TenantMembershipFactory


class ManualLeadAssignmentEventsTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.rm_uuid = str(uuid.uuid4())
        self.rm_email = "rm.assignee@example.com"
        self.rm_role = RoleFactory(tenant=self.tenant, key="rm", name="RM")
        SupabaseAuthUser.objects.create(id=uuid.UUID(self.rm_uuid), email=self.rm_email)
        TenantMembershipFactory(
            tenant=self.tenant,
            user_id=self.rm_uuid,
            email=self.rm_email,
            role=self.rm_role,
        )

        self.lead = Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Manual Assign Lead",
                "praja_id": "445566",
                "lead_stage": "FRESH",
            },
        )
        self.url = f"/crm-records/records/{self.lead.id}/"
        self.client.force_authenticate(user=self.user)
        self.patch_headers = {
            **self.auth_headers,
            "HTTP_X_TENANT_SLUG": self.tenant.slug,
        }

    @patch("crm_records.lead_pipeline.post_assignment.get_queue_service")
    def test_gm_assign_lead_enqueues_rm_assigned_events(self, mock_get_queue):
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue

        payload = {
            "data": {
                **self.lead.data,
                "assigned_to": self.rm_uuid,
                "lead_stage": "ASSIGNED",
            }
        }
        response = self.client.patch(self.url, payload, format="json", **self.patch_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        rm_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type") == JobType.SEND_RM_ASSIGNED_EVENT
        ]
        mixpanel_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type") == JobType.SEND_MIXPANEL_EVENT
            and call.kwargs.get("payload", {}).get("event_name") == "pyro_crm_rm_assigned_backend"
        ]
        self.assertEqual(len(rm_calls), 1)
        self.assertEqual(
            rm_calls[0].kwargs["payload"],
            {"praja_id": 445566, "rm_email": self.rm_email},
        )
        self.assertEqual(len(mixpanel_calls), 1)

    @patch("crm_records.lead_pipeline.post_assignment.get_queue_service")
    def test_update_without_assignment_change_does_not_enqueue_events(self, mock_get_queue):
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        self.lead.data = {
            **self.lead.data,
            "assigned_to": self.rm_uuid,
            "lead_stage": "ASSIGNED",
        }
        self.lead.save(update_fields=["data"])

        payload = {
            "data": {
                **self.lead.data,
                "latest_remarks": "Called customer",
            }
        }
        response = self.client.patch(self.url, payload, format="json", **self.patch_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        assignment_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type")
            in (JobType.SEND_RM_ASSIGNED_EVENT, JobType.SEND_MIXPANEL_EVENT)
        ]
        self.assertEqual(len(assignment_calls), 0)

    @patch("crm_records.lead_pipeline.post_assignment.get_queue_service")
    def test_reassign_to_different_rm_enqueues_events(self, mock_get_queue):
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        other_rm_uuid = str(uuid.uuid4())
        other_rm_email = "other.rm@example.com"
        SupabaseAuthUser.objects.create(id=uuid.UUID(other_rm_uuid), email=other_rm_email)
        TenantMembershipFactory(
            tenant=self.tenant,
            user_id=other_rm_uuid,
            email=other_rm_email,
            role=self.rm_role,
        )
        self.lead.data = {
            **self.lead.data,
            "assigned_to": self.rm_uuid,
            "lead_stage": "ASSIGNED",
        }
        self.lead.save(update_fields=["data"])

        payload = {
            "data": {
                **self.lead.data,
                "assigned_to": other_rm_uuid,
            }
        }
        response = self.client.patch(self.url, payload, format="json", **self.patch_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        rm_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type") == JobType.SEND_RM_ASSIGNED_EVENT
        ]
        self.assertEqual(len(rm_calls), 1)
        self.assertEqual(
            rm_calls[0].kwargs["payload"],
            {"praja_id": 445566, "rm_email": other_rm_email},
        )
