import uuid
from datetime import timedelta

from django.urls import reverse
from rest_framework import status
from django.utils import timezone

from accounts.models import SupabaseAuthUser
from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_dump_factory import dump_data


class SupportTicketUpdateViewTest(BaseAPITestCase):
    """Test the support ticket update API endpoint (records-backed)."""

    def setUp(self):
        super().setUp()

        self.record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="test_user_123",
                name="Test User",
                phone="1234567890",
                layout_status="pending",
            ),
        )

        self.url = reverse("support_ticket:update-ticket")

        self.cse_user = SupabaseAuthUser.objects.create(
            id=uuid.uuid4(),
            email="cse@example.com",
        )
        self.cse_uuid = str(self.cse_user.id)

    def _refresh_record(self):
        self.record.refresh_from_db()
        return self.record

    def test_update_assigned_to_success(self):
        data = {
            "ticket_id": self.record.id,
            "assigned_to": self.cse_uuid,
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(response.data["success"])
        self.assertEqual(response.data["message"], "Ticket updated successfully")
        self.assertEqual(response.data["updated_by"], self.email)
        self.assertIn("assigned_to", response.data["updated_fields"])

        record = self._refresh_record()
        self.assertEqual(record.data.get("assigned_to"), self.cse_uuid)

    def test_update_multiple_fields_success(self):
        data = {
            "ticket_id": self.record.id,
            "assigned_to": self.cse_uuid,
            "cse_name": "john.doe@company.com",
            "resolution_status": "In Progress",
            "cse_remarks": "Working on this issue",
            "call_status": "In Progress",
            "layout_status": "assigned",
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["success"])
        self.assertEqual(len(response.data["updated_fields"]), 6)

        record = self._refresh_record()
        self.assertEqual(record.data.get("assigned_to"), self.cse_uuid)
        self.assertEqual(record.data.get("cse_name"), "john.doe@company.com")
        self.assertEqual(record.data.get("resolution_status"), "In Progress")
        self.assertEqual(record.data.get("cse_remarks"), "Working on this issue")
        self.assertEqual(record.data.get("call_status"), "In Progress")
        self.assertEqual(record.data.get("layout_status"), "assigned")

    def test_update_snooze_until_success(self):
        future_time = timezone.now() + timedelta(hours=2)

        data = {
            "ticket_id": self.record.id,
            "snooze_until": future_time.isoformat(),
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        record = self._refresh_record()
        self.assertIsNotNone(record.data.get("snooze_until"))

    def test_update_assigned_to_null(self):
        self.record.data["assigned_to"] = self.cse_uuid
        self.record.save(update_fields=["data", "updated_at"])

        data = {
            "ticket_id": self.record.id,
            "assigned_to": None,
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        record = self._refresh_record()
        self.assertIsNone(record.data.get("assigned_to"))

    def test_ticket_not_found(self):
        data = {
            "ticket_id": 99999,
            "assigned_to": self.cse_uuid,
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Ticket not found", str(response.data))

    def test_missing_ticket_id(self):
        data = {
            "assigned_to": self.cse_uuid,
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Invalid request data", str(response.data))
        self.assertIn("ticket_id", str(response.data))

    def test_no_fields_provided(self):
        data = {
            "ticket_id": self.record.id,
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("At least one field must be provided for update", str(response.data))

    def test_invalid_uuid_format(self):
        data = {
            "ticket_id": self.record.id,
            "assigned_to": "invalid-uuid-format",
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Invalid request data", str(response.data))

    def test_unauthorized_request(self):
        data = {
            "ticket_id": self.record.id,
            "assigned_to": self.cse_uuid,
        }

        response = self.client.patch(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_invalid_auth_header(self):
        data = {
            "ticket_id": self.record.id,
            "assigned_to": self.cse_uuid,
        }

        invalid_headers = {
            "HTTP_AUTHORIZATION": "Bearer invalid_token",
            "HTTP_X_TENANT_ID": self.tenant_id,
        }

        response = self.client.patch(self.url, data, format="json", **invalid_headers)

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_empty_string_fields(self):
        data = {
            "ticket_id": self.record.id,
            "cse_name": "",
            "cse_remarks": "",
            "call_status": "",
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        record = self._refresh_record()
        self.assertEqual(record.data.get("cse_name"), "")
        self.assertEqual(record.data.get("cse_remarks"), "")
        self.assertEqual(record.data.get("call_status"), "")

    def test_long_field_values(self):
        long_string = "x" * 1000

        data = {
            "ticket_id": self.record.id,
            "cse_remarks": long_string,
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        record = self._refresh_record()
        self.assertEqual(record.data.get("cse_remarks"), long_string)

    def test_update_response_structure(self):
        data = {
            "ticket_id": self.record.id,
            "assigned_to": self.cse_uuid,
            "cse_name": "test@example.com",
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertIn("success", response_data)
        self.assertIn("message", response_data)
        self.assertIn("updated_ticket", response_data)
        self.assertIn("updated_by", response_data)
        self.assertIn("updated_fields", response_data)

        updated_ticket = response_data["updated_ticket"]
        self.assertIn("id", updated_ticket)
        self.assertIn("assigned_to", updated_ticket)
        self.assertIn("cse_name", updated_ticket)

        self.assertIn("assigned_to", response_data["updated_fields"])
        self.assertIn("cse_name", response_data["updated_fields"])

    def test_concurrent_updates(self):
        user1 = SupabaseAuthUser.objects.create(id=uuid.uuid4(), email="user1@example.com")
        user2 = SupabaseAuthUser.objects.create(id=uuid.uuid4(), email="user2@example.com")

        data1 = {
            "ticket_id": self.record.id,
            "assigned_to": str(user1.id),
            "cse_name": "user1@example.com",
        }

        data2 = {
            "ticket_id": self.record.id,
            "assigned_to": str(user2.id),
            "cse_remarks": "Updated by user 2",
        }

        response1 = self.client.patch(self.url, data1, format="json", **self.auth_headers)
        response2 = self.client.patch(self.url, data2, format="json", **self.auth_headers)

        self.assertEqual(response1.status_code, status.HTTP_200_OK)
        self.assertEqual(response2.status_code, status.HTTP_200_OK)

        record = self._refresh_record()
        self.assertEqual(record.data.get("assigned_to"), data2["assigned_to"])
        self.assertEqual(record.data.get("cse_name"), data1["cse_name"])
        self.assertEqual(record.data.get("cse_remarks"), data2["cse_remarks"])

    def test_update_with_existing_assigned_ticket(self):
        original_cse_user = SupabaseAuthUser.objects.create(
            id=uuid.uuid4(), email="original@example.com"
        )
        self.record.data["assigned_to"] = str(original_cse_user.id)
        self.record.data["cse_name"] = "original@example.com"
        self.record.save(update_fields=["data", "updated_at"])

        new_cse_user = SupabaseAuthUser.objects.create(
            id=uuid.uuid4(), email="new@example.com"
        )

        data = {
            "ticket_id": self.record.id,
            "assigned_to": str(new_cse_user.id),
            "cse_name": "new@example.com",
        }

        response = self.client.patch(self.url, data, format="json", **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        record = self._refresh_record()
        self.assertEqual(record.data.get("assigned_to"), str(new_cse_user.id))
        self.assertEqual(record.data.get("cse_name"), "new@example.com")


class SupportTicketUpdateSerializerTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(user_id="u1"),
        )

    def _request(self):
        return type("Req", (), {"tenant": self.tenant})()

    def test_valid_data(self):
        from support_ticket.serializers import SupportTicketUpdateSerializer

        data = {
            "ticket_id": self.record.id,
            "assigned_to": str(uuid.uuid4()),
            "cse_name": "test@example.com",
        }

        serializer = SupportTicketUpdateSerializer(
            data=data,
            context={"request": self._request()},
        )
        self.assertTrue(serializer.is_valid())

    def test_invalid_ticket_id(self):
        from support_ticket.serializers import SupportTicketUpdateSerializer

        data = {
            "ticket_id": 99999,
            "assigned_to": str(uuid.uuid4()),
        }

        serializer = SupportTicketUpdateSerializer(
            data=data,
            context={"request": self._request()},
        )
        self.assertFalse(serializer.is_valid())
        self.assertIn("ticket_id", serializer.errors)

    def test_no_update_fields(self):
        from support_ticket.serializers import SupportTicketUpdateSerializer

        data = {
            "ticket_id": self.record.id,
        }

        serializer = SupportTicketUpdateSerializer(
            data=data,
            context={"request": self._request()},
        )
        self.assertFalse(serializer.is_valid())
        self.assertIn("non_field_errors", serializer.errors)

    def test_invalid_uuid(self):
        from support_ticket.serializers import SupportTicketUpdateSerializer

        data = {
            "ticket_id": self.record.id,
            "assigned_to": "invalid-uuid",
        }

        serializer = SupportTicketUpdateSerializer(
            data=data,
            context={"request": self._request()},
        )
        self.assertFalse(serializer.is_valid())
        self.assertIn("assigned_to", serializer.errors)
