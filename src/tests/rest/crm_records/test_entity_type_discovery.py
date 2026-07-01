from datetime import timedelta

import pytest
from django.test import TestCase
from django.utils import timezone

from core.models import EntityTypeDiscoverySyncState, TenantEntityType
from crm_records.entity_type_discovery import (
    discover_entity_types_from_records,
    merge_schema_fields,
)
from crm_records.models import Record
from tests.factories import TenantFactory


@pytest.mark.django_db
class EntityTypeDiscoveryTests(TestCase):
    def setUp(self):
        self.tenant_a = TenantFactory(slug="entity-discovery-a")
        self.tenant_b = TenantFactory(slug="entity-discovery-b")

    def _set_record_updated_at(self, record, updated_at):
        Record.objects.filter(pk=record.pk).update(updated_at=updated_at)
        record.refresh_from_db()
        return record

    def test_discovers_unique_fields_per_tenant_and_entity_type(self):
        base_time = timezone.now() - timedelta(minutes=5)
        r1 = Record.objects.create(
            tenant=self.tenant_a,
            entity_type="lead",
            data={
                "name": "Lead A",
                "phone_number": "+111",
                "lead_score": 10,
                "is_active": True,
            },
        )
        r2 = Record.objects.create(
            tenant=self.tenant_a,
            entity_type="lead",
            data={"lead_stage": "FRESH", "metadata": {"source": "api"}},
        )
        r3 = Record.objects.create(
            tenant=self.tenant_b,
            entity_type="ticket",
            data={"title": "Ticket B", "tags": ["vip"]},
        )
        self._set_record_updated_at(r1, base_time)
        self._set_record_updated_at(r2, base_time + timedelta(seconds=1))
        self._set_record_updated_at(r3, base_time + timedelta(seconds=2))

        result = discover_entity_types_from_records(batch_size=100)

        self.assertEqual(result.processed, 3)
        lead = TenantEntityType.objects.get(tenant=self.tenant_a, entity_type="lead")
        self.assertEqual(
            lead.schema_json,
            {
                "fields": {
                    "is_active": {"type": "boolean"},
                    "lead_score": {"type": "number"},
                    "lead_stage": {"type": "string"},
                    "metadata": {"type": "object"},
                    "name": {"type": "string"},
                    "phone_number": {"type": "string"},
                }
            },
        )
        self.assertEqual(lead.fields_count, 6)

        ticket = TenantEntityType.objects.get(tenant=self.tenant_b, entity_type="ticket")
        self.assertEqual(
            ticket.schema_json,
            {
                "fields": {
                    "tags": {"type": "array"},
                    "title": {"type": "string"},
                }
            },
        )

    def test_updated_record_with_new_field_is_processed_again(self):
        record = Record.objects.create(
            tenant=self.tenant_a,
            entity_type="lead",
            data={"name": "Original"},
        )
        first_time = timezone.now() - timedelta(minutes=2)
        self._set_record_updated_at(record, first_time)

        discover_entity_types_from_records(batch_size=100)

        record.data = {"name": "Original", "lead_stage": "FRESH"}
        record.save(update_fields=["data", "updated_at"])

        result = discover_entity_types_from_records(batch_size=100)

        self.assertEqual(result.processed, 1)
        entity = TenantEntityType.objects.get(tenant=self.tenant_a, entity_type="lead")
        self.assertEqual(
            entity.schema_json,
            {
                "fields": {
                    "lead_stage": {"type": "string"},
                    "name": {"type": "string"},
                }
            },
        )

    def test_bookmark_uses_id_tie_breaker_for_same_updated_at(self):
        same_time = timezone.now() - timedelta(minutes=1)
        r1 = Record.objects.create(
            tenant=self.tenant_a,
            entity_type="lead",
            data={"name": "Lead A"},
        )
        r2 = Record.objects.create(
            tenant=self.tenant_a,
            entity_type="lead",
            data={"lead_score": 5},
        )
        self._set_record_updated_at(r1, same_time)
        self._set_record_updated_at(r2, same_time)

        first = discover_entity_types_from_records(batch_size=1)
        state = EntityTypeDiscoverySyncState.objects.get(job_name="entity_type_discovery")
        self.assertEqual(first.processed, 1)
        self.assertEqual(state.last_processed_updated_at, same_time)
        self.assertEqual(state.last_processed_record_id, r1.id)

        second = discover_entity_types_from_records(batch_size=1)

        self.assertEqual(second.processed, 1)
        entity = TenantEntityType.objects.get(tenant=self.tenant_a, entity_type="lead")
        self.assertEqual(
            entity.schema_json,
            {
                "fields": {
                    "lead_score": {"type": "number"},
                    "name": {"type": "string"},
                }
            },
        )

    def test_conflicting_non_null_field_types_become_mixed(self):
        schema, changed = merge_schema_fields(
            {"fields": {"lead_score": {"type": "number"}}},
            {"lead_score": {"type": "string"}},
        )

        self.assertTrue(changed)
        self.assertEqual(schema["fields"]["lead_score"], {"type": "mixed"})
