from datetime import timedelta

from django.utils import timezone as django_timezone

from crm_records.models import Record
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RecordFactory
from authz import service as authz_service


class LeadFollowupNotificationsAPITests(BaseAPITestCase):
    """
    API tests for RM follow-up notifications endpoint
    (/crm-records/leads/followups/).
    """

    def setUp(self):
        super().setUp()
        self.url = "/crm-records/leads/followups/"
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)

    def test_unauthenticated_returns_403(self):
        self.client.force_authenticate(user=None)
        response = self.client.get(self.url, **self.auth_headers)
        assert response.status_code == 403
        self.client.force_authenticate(user=self.user)

    def test_returns_only_assigned_due_followups_for_current_rm(self):
        now = django_timezone.now()
        past = (now - timedelta(minutes=5)).isoformat()
        future = (now + timedelta(hours=2)).isoformat()

        # Due follow-up assigned to current RM
        lead_due = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "My Due Lead",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": self.supabase_uid,
                "next_call_at": past,
                "call_attempts": 1,
                "latest_remarks": "Call back soon",
            },
        )

        # Lead assigned to other RM - should not appear
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Other RM Lead",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": "some-other-user",
                "next_call_at": past,
                "call_attempts": 1,
            },
        )

        # Future follow-up - should not appear
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Future Lead",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": self.supabase_uid,
                "next_call_at": future,
                "call_attempts": 1,
            },
        )

        response = self.client.get(self.url, **self.auth_headers)
        assert response.status_code == 200
        body = response.json()
        # Body is a simple list of notifications
        assert isinstance(body, list)
        ids = {item["id"] for item in body}
        assert lead_due.id in ids
        # Ensure only our due lead is present
        assert ids == {lead_due.id}

