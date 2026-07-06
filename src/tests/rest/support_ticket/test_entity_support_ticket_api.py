import pytest
from django.test import TestCase, override_settings
from rest_framework import status
from rest_framework.test import APIClient

from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from tests.factories import TenantFactory
from tests.factories.support_ticket_dump_factory import dump_data


@pytest.mark.django_db
@override_settings(
    PYRO_SECRET="test-pyro-secret-entity-st",
    DEFAULT_TENANT_SLUG="entity-support-ticket-tenant",
)
class SupportTicketEntityAPITest(TestCase):
    def setUp(self):
        self.tenant = TenantFactory(slug="entity-support-ticket-tenant")
        self.client = APIClient()
        self.url = "/entity/support_ticket/"
        self.headers = {"HTTP_X_SECRET_PYRO": "test-pyro-secret-entity-st"}

        self.ticket = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="user_abc",
                layout_status="pending",
            ),
        )

        self.existing_tasks = [
            {"task": "Sending a Demo", "status": "Yes"},
            {"task": "App Installation", "status": "No"},
            {"task": "Create/Update Layout", "status": "Null"},
        ]

    def _set_tasks(self):
        self.ticket.data["tasks"] = list(self.existing_tasks)
        self.ticket.save(update_fields=["data", "updated_at"])

    def _payload(self, data, ticket_id=None):
        return {
            "ticket_id": ticket_id if ticket_id is not None else self.ticket.id,
            "data": data,
        }

    def test_patch_by_ticket_id(self):
        response = self.client.patch(
            self.url,
            self._payload(
                {
                    "resolution_status": "Resolved",
                    "cse_remarks": "Fixed via entity API",
                }
            ),
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["data"]["resolution_status"], "Resolved")

    def test_patch_ticket_id_at_root_with_data_updates(self):
        response = self.client.patch(
            self.url,
            self._payload({"layout_status": "assigned"}),
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["data"]["layout_status"], "assigned")

    def test_patch_requires_ticket_id(self):
        response = self.client.patch(
            self.url,
            {"data": {"resolution_status": "Resolved"}},
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_patch_requires_data_object(self):
        response = self.client.patch(
            self.url,
            {
                "ticket_id": self.ticket.id,
                "resolution_status": "Resolved",
            },
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_patch_not_found(self):
        response = self.client.patch(
            self.url,
            self._payload({"resolution_status": "Resolved"}, ticket_id=999999),
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_patch_requires_update_fields(self):
        response = self.client.patch(
            self.url,
            {"ticket_id": self.ticket.id, "data": {}},
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_patch_merge_one_task_preserves_others(self):
        self._set_tasks()

        response = self.client.patch(
            self.url,
            self._payload({"tasks": [{"task": "App Installation", "status": "Yes"}]}),
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        tasks = response.data["data"]["tasks"]
        self.assertEqual(len(tasks), 3)
        self.assertEqual(
            next(t for t in tasks if t["task"] == "App Installation")["status"],
            "Yes",
        )
        self.assertEqual(
            next(t for t in tasks if t["task"] == "Sending a Demo")["status"],
            "Yes",
        )
        self.assertEqual(
            next(t for t in tasks if t["task"] == "Create/Update Layout")["status"],
            "Null",
        )

    def test_patch_merge_two_tasks(self):
        self._set_tasks()

        response = self.client.patch(
            self.url,
            self._payload(
                {
                    "tasks": [
                        {"task": "App Installation", "status": "Yes"},
                        {"task": "Create/Update Layout", "status": "No"},
                    ],
                }
            ),
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        tasks = response.data["data"]["tasks"]
        self.assertEqual(len(tasks), 3)
        self.assertEqual(
            next(t for t in tasks if t["task"] == "App Installation")["status"],
            "Yes",
        )
        self.assertEqual(
            next(t for t in tasks if t["task"] == "Create/Update Layout")["status"],
            "No",
        )
        self.assertEqual(
            next(t for t in tasks if t["task"] == "Sending a Demo")["status"],
            "Yes",
        )

    def test_patch_merge_all_tasks(self):
        self._set_tasks()

        response = self.client.patch(
            self.url,
            self._payload(
                {
                    "tasks": [
                        {"task": "Sending a Demo", "status": "Yes"},
                        {"task": "App Installation", "status": "Yes"},
                        {"task": "Create/Update Layout", "status": "Yes"},
                    ],
                }
            ),
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        tasks = response.data["data"]["tasks"]
        self.assertEqual(len(tasks), 3)
        self.assertTrue(all(t["status"] == "Yes" for t in tasks))

    def test_patch_tasks_on_empty_ticket_creates_list(self):
        response = self.client.patch(
            self.url,
            self._payload(
                {
                    "tasks": [
                        {"task": "Sending a Demo", "status": "Yes"},
                        {"task": "App Installation", "status": "No"},
                    ],
                }
            ),
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["data"]["tasks"]), 2)

    def test_patch_tasks_with_other_ticket_attributes(self):
        self._set_tasks()

        response = self.client.patch(
            self.url,
            self._payload(
                {
                    "tasks": [{"task": "App Installation", "status": "Yes"}],
                    "resolution_status": "In Progress",
                    "cse_remarks": "Demo done",
                    "layout_status": "assigned",
                }
            ),
            format="json",
            **self.headers,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["data"]["resolution_status"], "In Progress")
        self.assertEqual(response.data["data"]["cse_remarks"], "Demo done")
        self.assertEqual(len(response.data["data"]["tasks"]), 3)
