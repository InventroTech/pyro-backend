from datetime import timedelta

from django.urls import reverse
from django.utils import timezone
from rest_framework import status

from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_dump_factory import dump_data


def _open_record(*, tenant, user_id: str, name: str, poster: str = "in_trial"):
    return Record.objects.create(
        tenant=tenant,
        entity_type=SUPPORT_TICKET_ENTITY_TYPE,
        data=dump_data(
            user_id=user_id,
            name=name,
            poster=poster,
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

    def test_get_next_ticket_lifo_across_priority_one_posters(self):
        """All five priority-1 posters share one LIFO pool (newest wins)."""
        self_trail = _open_record(
            tenant=self.tenant,
            user_id="self_user",
            name="Self Trail",
            poster="Self Trail",
        )
        in_trial = _open_record(
            tenant=self.tenant,
            user_id="trial_user",
            name="In Trial",
            poster="In Trial",
        )
        paid = _open_record(
            tenant=self.tenant,
            user_id="paid_user",
            name="Paid",
            poster="paid",
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
        self.assertEqual(response.data["ticket"]["poster"], "paid")

    def test_get_next_ticket_defers_rest_until_priority_one_exhausted(self):
        rest = _open_record(
            tenant=self.tenant,
            user_id="rest_user",
            name="Rest",
            poster="Rest",
        )
        in_trial = _open_record(
            tenant=self.tenant,
            user_id="trial_user",
            name="In Trial",
            poster="in_trial",
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
            poster="Self Trail",
        )
        newer = _open_record(
            tenant=self.tenant,
            user_id="new_self",
            name="Newer Self",
            poster="Self Trail",
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
                **dump_data(user_id="json_null_user", name="JSON Null User", poster="in_trial"),
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
            poster="in_trial",
        )
        exhausted.data = {**exhausted.data, "call_attempts": 3}
        exhausted.save(update_fields=["data"])
        available = _open_record(
            tenant=self.tenant,
            user_id="available",
            name="Available",
            poster="in_trial",
        )

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ticket"]["id"], available.id)
