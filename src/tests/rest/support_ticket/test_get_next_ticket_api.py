from datetime import timedelta
from unittest.mock import MagicMock, patch

import jwt
from django.conf import settings
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from rest_framework import status

from background_jobs.models import JobType
from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from support_ticket.views import _record_ticket_type_key
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_dump_factory import dump_data


def _open_record(
    *,
    tenant,
    user_id: str,
    name: str,
    support_ticket_type: str = "in_trial",
):
    return Record.objects.create(
        tenant=tenant,
        entity_type=SUPPORT_TICKET_ENTITY_TYPE,
        data=dump_data(
            user_id=user_id,
            name=name,
            support_ticket_type=support_ticket_type,
            call_status="Call Waiting",
            call_attempts=0,
        ),
    )


class GetNextTicketAPITest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.url = reverse("support_ticket:get-next-ticket")
        Record.objects.filter(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
        ).delete()

    def test_get_next_ticket_empty_when_no_records(self):
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, {})

    @override_settings(DEFAULT_TENANT_SLUG="missing-tenant-slug-for-get-next-test")
    def test_get_next_ticket_forbidden_when_tenant_unresolved(self):
        """Reject unresolved tenant instead of returning an empty ticket payload."""
        _open_record(tenant=self.tenant, user_id="queued", name="Queued")
        self.membership.delete()
        token = jwt.encode(
            {
                "sub": self.supabase_uid,
                "email": self.email,
                "aud": "authenticated",
            },
            settings.SUPABASE_JWT_SECRET,
            algorithm="HS256",
        )
        if isinstance(token, bytes):
            token = token.decode("utf-8")

        response = self.client.get(
            self.url,
            HTTP_AUTHORIZATION=f"Bearer {token}",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @patch("support_ticket.views.get_queue_service")
    def test_get_next_ticket_enqueues_cse_assigned_event(self, mock_get_queue):
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        customer_user_id = "123456"
        _open_record(tenant=self.tenant, user_id=customer_user_id, name="Customer")

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        cse_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type") == JobType.SEND_CSE_ASSIGNED_EVENT
        ]
        self.assertEqual(len(cse_calls), 1)
        self.assertEqual(
            cse_calls[0].kwargs["payload"],
            {"user_id": 123456, "cse_email": self.email},
        )

    @patch("support_ticket.views.get_queue_service")
    def test_get_next_ticket_assigned_mixpanel_includes_release_build_number(
        self, mock_get_queue
    ):
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="123456",
                name="Customer",
                release_build_number="9.8.7",
            ),
        )

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        mixpanel_calls = [
            call
            for call in mock_queue.enqueue_job.call_args_list
            if call.kwargs.get("job_type") == JobType.SEND_MIXPANEL_EVENT
            and call.kwargs.get("payload", {}).get("event_name") == "pyro_st_assigned"
        ]
        self.assertEqual(len(mixpanel_calls), 1)
        self.assertEqual(
            mixpanel_calls[0].kwargs["payload"]["properties"]["release_build_number"],
            "9.8.7",
        )

    def test_get_next_ticket_assigns_newest_unassigned_record(self):
        older = _open_record(tenant=self.tenant, user_id="old_user", name="Older")
        newer = _open_record(tenant=self.tenant, user_id="new_user", name="Newer")
        older.created_at = timezone.now() - timedelta(hours=2)
        older.save(update_fields=["created_at"])
        newer.created_at = timezone.now() - timedelta(hours=1)
        newer.save(update_fields=["created_at"])

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], newer.id)
        self.assertEqual(response.data["ticket"]["record_id"], newer.id)
        self.assertEqual(response.data["ticket"]["name"], "Newer")
        self.assertEqual(response.data["ticket"]["assigned_to"], self.supabase_uid)

        newer.refresh_from_db()
        self.assertEqual(newer.data["assigned_to"], self.supabase_uid)
        self.assertEqual(newer.data["cse_name"], self.email)

    def test_get_next_ticket_returns_existing_assignment_first(self):
        assigned = _open_record(tenant=self.tenant, user_id="mine", name="Mine")
        assigned.data = {
            **assigned.data,
            "assigned_to": self.supabase_uid,
            "cse_name": self.email,
        }
        assigned.save(update_fields=["data"])
        _open_record(tenant=self.tenant, user_id="other", name="Other")

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], assigned.id)
        self.assertEqual(response.data["ticket"]["user_id"], "mine")

    def test_get_next_ticket_lifo_across_priority_one_types(self):
        """All five priority-1 support_ticket_type values share one LIFO pool."""
        self_trail = _open_record(
            tenant=self.tenant,
            user_id="self_user",
            name="Self Trail",
            support_ticket_type="Self_Trial",
        )
        in_trial = _open_record(
            tenant=self.tenant,
            user_id="trial_user",
            name="In Trial",
            support_ticket_type="in_trial",
        )
        paid = _open_record(
            tenant=self.tenant,
            user_id="paid_user",
            name="Paid",
            support_ticket_type="paid",
        )
        self_trail.created_at = timezone.now() - timedelta(hours=3)
        self_trail.save(update_fields=["created_at"])
        in_trial.created_at = timezone.now() - timedelta(hours=2)
        in_trial.save(update_fields=["created_at"])
        paid.created_at = timezone.now() - timedelta(hours=1)
        paid.save(update_fields=["created_at"])

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], paid.id)
        self.assertEqual(response.data["ticket"]["support_ticket_type"], "paid")

    def test_get_next_ticket_defers_rest_until_priority_one_exhausted(self):
        rest = _open_record(
            tenant=self.tenant,
            user_id="rest_user",
            name="Rest",
            support_ticket_type="free",
        )
        in_trial = _open_record(
            tenant=self.tenant,
            user_id="trial_user",
            name="In Trial",
            support_ticket_type="in_trial",
        )
        rest.created_at = timezone.now() - timedelta(hours=1)
        rest.save(update_fields=["created_at"])
        in_trial.created_at = timezone.now() - timedelta(hours=2)
        in_trial.save(update_fields=["created_at"])

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], in_trial.id)

    def test_get_next_ticket_self_trail_lifo_within_bucket(self):
        older = _open_record(
            tenant=self.tenant,
            user_id="old_self",
            name="Older Self",
            support_ticket_type="Self_Trial",
        )
        newer = _open_record(
            tenant=self.tenant,
            user_id="new_self",
            name="Newer Self",
            support_ticket_type="Self_Trial",
        )
        older.created_at = timezone.now() - timedelta(hours=2)
        older.save(update_fields=["created_at"])
        newer.created_at = timezone.now() - timedelta(hours=1)
        newer.save(update_fields=["created_at"])

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], newer.id)

    def test_get_next_ticket_assigns_record_with_json_null_assignment_fields(self):
        """Mirrored records store unassigned/open as JSON null, not missing keys."""
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(
                    user_id="json_null_user",
                    name="JSON Null User",
                    support_ticket_type="in_trial",
                ),
                "assigned_to": None,
                "resolution_status": None,
            },
        )

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], record.id)
        self.assertEqual(response.data["ticket"]["assigned_to"], self.supabase_uid)

    def test_get_next_ticket_skips_record_at_max_attempts(self):
        exhausted = _open_record(
            tenant=self.tenant,
            user_id="exhausted",
            name="Exhausted",
            support_ticket_type="in_trial",
        )
        exhausted.data = {**exhausted.data, "call_attempts": 3}
        exhausted.save(update_fields=["data"])
        available = _open_record(
            tenant=self.tenant,
            user_id="available",
            name="Available",
            support_ticket_type="in_trial",
        )

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], available.id)

    def test_record_ticket_type_key_falls_back_to_poster(self):
        """Legacy mirrored rows may still have ``poster`` without ``support_ticket_type``."""
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(user_id="legacy_user", name="Legacy", poster="paid"),
        )
        self.assertEqual(_record_ticket_type_key(record), "paid")

    def test_get_next_ticket_returns_due_snoozed_before_fresh_open(self):
        """Due not-connected retries must surface before new open tickets (lead parity)."""
        past = (timezone.now() - timedelta(minutes=10)).isoformat()
        snoozed = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(
                    user_id="snoozed_user",
                    name="Snoozed Retry",
                    support_ticket_type="in_trial",
                ),
                "resolution_status": "Snoozed",
                "call_status": "Not Connected",
                "call_attempts": 1,
                "snooze_until": past,
                "next_call_at": past,
                "assigned_to": None,
                "cse_name": None,
            },
        )
        _open_record(
            tenant=self.tenant,
            user_id="fresh_user",
            name="Fresh Open",
            support_ticket_type="in_trial",
        )

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], snoozed.id)
        self.assertEqual(response.data["ticket"]["resolution_status"], "Snoozed")

    def test_get_next_ticket_skips_snoozed_not_yet_due(self):
        future = (timezone.now() + timedelta(hours=1)).isoformat()
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(
                    user_id="future_snooze",
                    name="Not Due Yet",
                    support_ticket_type="in_trial",
                ),
                "resolution_status": "Snoozed",
                "snooze_until": future,
                "next_call_at": future,
                "assigned_to": None,
            },
        )
        fresh = _open_record(
            tenant=self.tenant,
            user_id="fresh_user",
            name="Fresh Open",
            support_ticket_type="in_trial",
        )

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], fresh.id)

    def test_get_next_ticket_includes_jatra_link(self):
        jatra_link = "https://www.thecircleapp.in/jatra/98obia11ve"
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="jatra_user",
                name="Arjun Patel",
                support_ticket_type="in_trial",
                Jatra_link=jatra_link,
            ),
        )

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], record.id)
        self.assertEqual(response.data["ticket"]["Jatra_link"], jatra_link)
