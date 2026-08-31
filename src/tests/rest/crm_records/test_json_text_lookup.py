"""data->>'field' identity lookups (btree-friendly) vs Django data__field (jsonb ->)."""

import pytest
from django.test import TestCase

from crm_records.helper import (
    filter_json_text_equals,
    filter_json_text_equals_any,
    json_text_equals_value,
)
from crm_records.models import Record
from tests.factories import RecordFactory, TenantFactory


@pytest.mark.django_db
class JsonTextEqualsLookupTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()

    def test_compiled_sql_uses_text_operator_not_jsonb_arrow(self):
        qs = filter_json_text_equals(Record.objects.all(), "praja_id", "ABC")
        sql = str(qs.query)
        self.assertIn("->>", sql)
        self.assertIn("praja_id", sql)
        # Django data__praja_id uses `->` equality to jsonb, which skips the btree.
        orm_sql = str(Record.objects.filter(data__praja_id="ABC").query)
        self.assertIn("->", orm_sql)
        self.assertNotIn("->>", orm_sql)

    def test_finds_string_and_numeric_json_values(self):
        string_lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"praja_id": "PRAJA-1", "name": "string"},
        )
        numeric_lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"praja_id": 99001, "name": "numeric"},
        )
        found_string = filter_json_text_equals(
            Record.objects.filter(tenant=self.tenant, entity_type="lead"),
            "praja_id",
            "PRAJA-1",
        ).get()
        found_numeric = filter_json_text_equals(
            Record.objects.filter(tenant=self.tenant, entity_type="lead"),
            "praja_id",
            99001,
        ).get()
        self.assertEqual(found_string.id, string_lead.id)
        self.assertEqual(found_numeric.id, numeric_lead.id)

    def test_strips_whitespace_and_rejects_blank(self):
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"praja_id": "KEEP", "name": "x"},
        )
        found = filter_json_text_equals(
            Record.objects.filter(tenant=self.tenant),
            "praja_id",
            "  KEEP  ",
        ).first()
        self.assertIsNotNone(found)
        self.assertIsNone(json_text_equals_value(""))
        self.assertIsNone(json_text_equals_value(None))
        self.assertEqual(
            filter_json_text_equals(Record.objects.all(), "praja_id", "").count(),
            0,
        )

    def test_equals_any_matches_either_json_key(self):
        record = RecordFactory(
            tenant=self.tenant,
            entity_type="support_ticket",
            data={"ticket_id": "T-9", "name": "ticket"},
        )
        found = filter_json_text_equals_any(
            Record.objects.filter(tenant=self.tenant, entity_type="support_ticket"),
            ("support_ticket_id", "ticket_id"),
            "T-9",
        ).first()
        self.assertEqual(found.id, record.id)

    def test_rejects_unsafe_json_key(self):
        with self.assertRaises(ValueError):
            filter_json_text_equals(Record.objects.all(), "praja_id;drop", "x")
