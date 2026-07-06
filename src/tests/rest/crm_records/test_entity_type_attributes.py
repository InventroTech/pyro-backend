from django.test import TestCase

from core.models import TenantEntityType
from crm_records.models import EntityTypeSchema
from crm_records.tenant_entity_type_attributes import attributes_from_schema_json
from tests.base.test_setup import BaseAPITestCase


class TenantEntityTypeAttributesTest(TestCase):
    def test_attributes_from_schema_json(self):
        attrs = attributes_from_schema_json({
            "fields": {
                "lead_score": {"type": "number"},
                "name": {"type": "string"},
            }
        })
        self.assertIn("data.lead_score", attrs)
        self.assertIn("data.name", attrs)
        self.assertIn("id", attrs)


class EntityTypeAttributesApiTest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.client.force_authenticate(user=self.user)

        TenantEntityType.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            schema_json={
                "fields": {
                    "lead_score": {"type": "number"},
                    "affiliated_party": {"type": "string"},
                }
            },
            fields_count=2,
        )

        EntityTypeSchema.objects.create(
            tenant=self.tenant,
            entity_type="ticket",
            attributes=["id", "data.subject"],
        )

    def test_prefers_tenant_entity_types_for_lead(self):
        response = self.client.get(
            "/crm-records/entity-attributes/",
            {"entity_type": "lead"},
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["source"], "tenant_entity_types")
        self.assertIn("data.lead_score", response.data["attributes"])
        self.assertIn("data.affiliated_party", response.data["attributes"])

    def test_falls_back_to_entity_type_schemas(self):
        response = self.client.get(
            "/crm-records/entity-attributes/",
            {"entity_type": "ticket"},
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["source"], "entity_type_schemas")
        self.assertEqual(response.data["attributes"], ["id", "data.subject"])

    def test_404_when_neither_source_has_entity_type(self):
        response = self.client.get(
            "/crm-records/entity-attributes/",
            {"entity_type": "missing_type"},
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, 404)
