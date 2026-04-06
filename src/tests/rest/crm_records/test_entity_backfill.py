"""
POST /entity/ vs POST /entity/backfill/: duplicate handling and idempotent create.

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

    def test_post_entity_duplicate_praja_id_returns_409(self):
        pid = "ENT_DUP_409"
        body = self._create_payload(pid)
        r1 = self.client.post(self.entity_url, body, format="json", **self.headers)
        self.assertEqual(r1.status_code, 201)
        r2 = self.client.post(self.entity_url, body, format="json", **self.headers)
        self.assertEqual(r2.status_code, 409)
        self.assertIn("existing_record_id", r2.data)
        self.assertEqual(Record.objects.filter(data__praja_id=pid).count(), 1)

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
