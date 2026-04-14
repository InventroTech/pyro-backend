from django.test import TestCase
from core.models import Tenant, Entity
from crm_records.models import Record
from crm_records.services import sync_entity_schema

class EntitySchemaSyncTests(TestCase):
    def setUp(self):
        """
        This runs before every test. We use it to set up our fake database.
        """
        # 1. Create a fake tenant
        self.tenant = Tenant.objects.create(name="Test Company")

        Entity.objects.create(tenant=self.tenant, name="lead", schema=[])

        # 2. Create fake Records with different JSON data
        Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data={"first_name": "John", "user_age": 30}
        )
        Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data={"first_name": "Jane", "wants_newsletter": True}
        )
        # 3. Create a record with NO data to ensure the code doesn't crash
        Record.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            data={}
        )

    def test_sync_entity_schema_combines_json_keys(self):
        """
        Tests that the service extracts unique keys from the JSON data
        and saves them into the Entity array field.
        """
        # ACT: Run your background service function
        processed_count = sync_entity_schema(self.tenant, "lead")

        # ASSERT 1: It should have processed all 3 records
        self.assertEqual(processed_count, 3)

        # ASSERT 2: The Entity should have been created in the database
        entity = Entity.objects.get(tenant=self.tenant, name="lead")

        # ASSERT 3: The attributes array should perfectly match our unique keys
        # We use assertCountEqual because the order of the array doesn't matter, just the contents.
        expected_keys = ["first_name", "user_age", "wants_newsletter"]
        self.assertCountEqual(entity.schema, expected_keys)

    def test_sync_ignores_other_entity_types(self):
        """
        Tests that running the sync for 'lead' doesn't accidentally pull JSON keys from 'ticket' records.
        """
        # Add a ticket record
        Record.objects.create(
            tenant=self.tenant,
            entity_type="ticket",
            data={"ticket_priority": "High"}
        )

        # Run the sync ONLY for leads
        sync_entity_schema(self.tenant, "lead")

        # Fetch the lead entity
        entity = Entity.objects.get(tenant=self.tenant, name="lead")

        # The 'ticket_priority' key should NOT be in the lead schema
        self.assertNotIn("ticket_priority", entity.schema)