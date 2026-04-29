"""
POST /entity/ (upsert) vs POST /entity/backfill/ (idempotent create).

POST /entity/ now performs an upsert: if a record with the same
(tenant, entity_type, data.praja_id) exists, the incoming data is merged
into it and 200 is returned. Otherwise a new record is created (201).

Run with: pytest src/tests/rest/crm_records/test_entity_backfill.py -v
"""

import pytest
from django.test import TestCase, override_settings
from rest_framework.test import APIClient

from crm_records.models import Record
from tests.factories import TenantFactory


@pytest.mark.django_db
@override_settings(
    PYRO_SECRET="test-pyro-secret-backfill",
    DEFAULT_TENANT_SLUG="entity-backfill-tenant",
)
class EntityBackfillApiTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory(slug="entity-backfill-tenant")
        self.client = APIClient()
        self.headers = {"HTTP_X_SECRET_PYRO": "test-pyro-secret-backfill"}
        self.entity_url = "/entity/"
        self.backfill_url = "/entity/backfill/"

    def _create_payload(self, praja_id: str):
        return {
            "data": {
                "praja_id": praja_id,
                "name": "Backfill Test Lead",
                "lead_stage": "FRESH",
            },
        }

    def test_post_entity_duplicate_praja_id_upserts_and_returns_200(self):
        pid = "ENT_DUP_UPSERT"
        body = self._create_payload(pid)
        r1 = self.client.post(self.entity_url, body, format="json", **self.headers)
        self.assertEqual(r1.status_code, 201)
        first_id = r1.data["id"]

        updated_body = {
            "data": {
                "praja_id": pid,
                "name": "Updated Lead Name",
                "lead_stage": "ASSIGNED",
                "phone_number": "+9999999999",
            },
        }
        r2 = self.client.post(self.entity_url, updated_body, format="json", **self.headers)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.data["id"], first_id)
        self.assertEqual(r2.data["data"]["name"], "Updated Lead Name")
        self.assertEqual(r2.data["data"]["lead_stage"], "ASSIGNED")
        self.assertEqual(r2.data["data"]["phone_number"], "+9999999999")
        self.assertEqual(Record.objects.filter(data__praja_id=pid).count(), 1)

    def test_post_entity_upsert_preserves_existing_fields(self):
        """Upsert merges: fields not in the update payload are preserved."""
        pid = "ENT_MERGE_PRESERVE"
        body = {
            "data": {
                "praja_id": pid,
                "name": "Original Name",
                "lead_stage": "FRESH",
                "phone_number": "+1111111111",
                "notes": "important note",
            },
        }
        r1 = self.client.post(self.entity_url, body, format="json", **self.headers)
        self.assertEqual(r1.status_code, 201)

        r2 = self.client.post(
            self.entity_url,
            {"data": {"praja_id": pid, "name": "New Name"}},
            format="json",
            **self.headers,
        )
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.data["data"]["name"], "New Name")
        self.assertEqual(r2.data["data"]["phone_number"], "+1111111111")
        self.assertEqual(r2.data["data"]["notes"], "important note")

    def test_post_entity_without_praja_id_creates_new_record(self):
        """POST without praja_id always creates (no lookup key for upsert)."""
        body = {"data": {"name": "No Praja Lead", "lead_stage": "FRESH"}}
        r1 = self.client.post(self.entity_url, body, format="json", **self.headers)
        self.assertEqual(r1.status_code, 201)
        r2 = self.client.post(self.entity_url, body, format="json", **self.headers)
        self.assertEqual(r2.status_code, 201)
        self.assertNotEqual(r1.data["id"], r2.data["id"])

    def test_post_backfill_creates_when_absent_returns_201(self):
        pid = "BF_CREATE_201"
        r = self.client.post(
            self.backfill_url,
            self._create_payload(pid),
            format="json",
            **self.headers,
        )
        self.assertEqual(r.status_code, 201)
        self.assertNotIn("backfill_skipped", r.data)
        self.assertEqual(Record.objects.filter(data__praja_id=pid).count(), 1)

    def test_post_backfill_existing_praja_id_returns_200_skipped(self):
        pid = "BF_SKIP_200"
        body = self._create_payload(pid)
        r1 = self.client.post(self.backfill_url, body, format="json", **self.headers)
        self.assertEqual(r1.status_code, 201)
        first_id = r1.data["id"]
        r2 = self.client.post(self.backfill_url, body, format="json", **self.headers)
        self.assertEqual(r2.status_code, 200)
        self.assertTrue(r2.data.get("backfill_skipped"))
        self.assertEqual(r2.data["id"], first_id)
        self.assertEqual(Record.objects.filter(data__praja_id=pid).count(), 1)

    def test_post_backfill_missing_praja_id_returns_400(self):
        r = self.client.post(
            self.backfill_url,
            {"data": {"name": "no praja", "lead_stage": "FRESH"}},
            format="json",
            **self.headers,
        )
        self.assertEqual(r.status_code, 400)
        self.assertIn("error", r.data)
        self.assertIn("praja_id", r.data["error"].lower())

    def test_get_entity_backfill_returns_405(self):
        r = self.client.get(self.backfill_url, **self.headers)
        self.assertEqual(r.status_code, 405)

    def test_get_entity_list_uses_has_next_pagination_meta(self):
        for i in range(11):
            Record.objects.create(
                tenant=self.tenant,
                entity_type="lead",
                data={"praja_id": f"ENT_PAGE_{i}", "name": f"Lead {i}"},
            )

        r = self.client.get(f"{self.entity_url}?page=1&page_size=10", **self.headers)
        self.assertEqual(r.status_code, 200)
        self.assertIn("data", r.data)
        self.assertIn("page_meta", r.data)
        self.assertNotIn("total_count", r.data["page_meta"])
        self.assertNotIn("number_of_pages", r.data["page_meta"])
        self.assertTrue(r.data["page_meta"]["has_next"])
        self.assertFalse(r.data["page_meta"]["has_previous"])
