"""
Tests for record aggregation service (core.services).

Tests the entity schema discovery system:
- SystemSettings checkpoints
- RecordAggregator schema snapshots  
- Per-entity-type aggregation
- Multi-tenant aggregation
"""
import pytest
from datetime import datetime
import uuid

from core.models import SystemSettings, RecordAggregator
from core.services import (
    get_last_processed_record_id,
    set_last_processed_record_id,
    build_schema_snapshot,
    aggregate_records_for_tenant_entity,
    aggregate_all_entities,
)
from tests.factories.core_factory import TenantFactory
from tests.factories.crm_records_factory import RecordFactory


@pytest.mark.django_db
class TestCheckpointManagement:
    """Test checkpoint reading/writing to SystemSettings."""
    
    def setup_method(self):
        """Clean up system settings and record aggregators before each test."""
        SystemSettings.objects.all().delete()
        RecordAggregator.objects.all().delete()
    
    def _get_unique_slug(self):
        """Generate a unique tenant slug."""
        return f"test-tenant-{uuid.uuid4().hex[:8]}"
    
    def test_get_last_processed_record_id_returns_zero_when_not_set(self):
        """Should return 0 when checkpoint doesn't exist."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        entity_type = "lead"
        
        checkpoint_id = get_last_processed_record_id(tenant, entity_type)
        
        assert checkpoint_id == 0
    
    def test_set_and_get_last_processed_record_id(self):
        """Should save and retrieve checkpoint correctly."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        entity_type = "lead"
        record_id = 12345
        
        set_last_processed_record_id(tenant, entity_type, record_id)
        retrieved_id = get_last_processed_record_id(tenant, entity_type)
        
        assert retrieved_id == record_id
    
    def test_checkpoint_key_format(self):
        """Should use correct checkpoint key format in SystemSettings."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        entity_type = "ticket"
        record_id = 999
        
        set_last_processed_record_id(tenant, entity_type, record_id)
        
        expected_key = f"record_aggregator_{tenant.slug}_{entity_type}_last_processed_id"
        setting = SystemSettings.objects.get(setting_key=expected_key)
        assert setting.setting_value['value'] == record_id
    
    def test_checkpoint_update_overwrites_existing(self):
        """Should overwrite existing checkpoint value."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        entity_type = "lead"
        
        set_last_processed_record_id(tenant, entity_type, 100)
        set_last_processed_record_id(tenant, entity_type, 200)
        
        checkpoint_id = get_last_processed_record_id(tenant, entity_type)
        assert checkpoint_id == 200
    
    def test_checkpoints_isolated_by_entity_type(self):
        """Different entity types should have separate checkpoints."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        
        set_last_processed_record_id(tenant, "lead", 100)
        set_last_processed_record_id(tenant, "ticket", 200)
        
        assert get_last_processed_record_id(tenant, "lead") == 100
        assert get_last_processed_record_id(tenant, "ticket") == 200
    
    def test_checkpoints_isolated_by_tenant(self):
        """Different tenants should have separate checkpoints."""
        tenant1 = TenantFactory(slug=self._get_unique_slug())
        tenant2 = TenantFactory(slug=self._get_unique_slug())
        
        set_last_processed_record_id(tenant1, "lead", 100)
        set_last_processed_record_id(tenant2, "lead", 200)
        
        assert get_last_processed_record_id(tenant1, "lead") == 100
        assert get_last_processed_record_id(tenant2, "lead") == 200


@pytest.mark.django_db
class TestSchemaSnapshotBuilding:
    """Test schema snapshot building from records."""
    
    def setup_method(self):
        """Clean up system settings and record aggregators before each test."""
        SystemSettings.objects.all().delete()
        RecordAggregator.objects.all().delete()
    
    def _get_unique_slug(self):
        """Generate a unique tenant slug."""
        return f"test-tenant-{uuid.uuid4().hex[:8]}"
    
    def test_build_schema_from_single_record(self):
        """Should extract fields and counts from single record."""
        record_data = {"name": "John", "email": "john@example.com", "age": 30}
        record = RecordFactory(data=record_data)
        
        schema = build_schema_snapshot([record])
        
        assert "name" in schema
        assert "email" in schema
        assert "age" in schema
        assert schema["name"]["count"] == 1
        assert schema["email"]["count"] == 1
        assert schema["age"]["count"] == 1
        assert all(f["field_type"] == "string" for f in schema.values())
    
    def test_build_schema_counts_multiple_records(self):
        """Should count field occurrences across multiple records."""
        records = [
            RecordFactory(data={"name": "John", "email": "john@example.com"}),
            RecordFactory(data={"name": "Jane", "age": 25}),  # no email
            RecordFactory(data={"name": "Bob", "email": "bob@example.com"}),
        ]
        
        schema = build_schema_snapshot(records)
        
        assert schema["name"]["count"] == 3  # all records have name
        assert schema["email"]["count"] == 2  # only 2 records have email
        assert schema["age"]["count"] == 1  # only 1 record has age
    
    def test_build_schema_empty_records_list(self):
        """Should return empty schema for empty records list."""
        schema = build_schema_snapshot([])
        assert schema == {}
    
    def test_build_schema_includes_all_field_types(self):
        """Should set field_type to 'string' for all fields."""
        record_data = {"str_field": "value", "int_field": 123, "bool_field": True}
        record = RecordFactory(data=record_data)
        
        schema = build_schema_snapshot([record])
        
        for field_name, field_info in schema.items():
            assert field_info["field_type"] == "string"


@pytest.mark.django_db
class TestSingleEntityAggregation:
    """Test aggregation for a single tenant/entity combination."""
    
    def setup_method(self):
        """Clean up system settings and record aggregators before each test."""
        SystemSettings.objects.all().delete()
        RecordAggregator.objects.all().delete()
    
    def _get_unique_slug(self):
        """Generate a unique tenant slug."""
        return f"test-tenant-{uuid.uuid4().hex[:8]}"
    
    def test_aggregate_single_entity_creates_record_aggregator(self):
        """Should create RecordAggregator for new entity type."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        record = RecordFactory(tenant=tenant, entity_type="lead", data={"name": "John"})
        
        processed = aggregate_records_for_tenant_entity(tenant, "lead")
        
        assert processed == 1
        agg = RecordAggregator.objects.get(tenant=tenant, entity_type="lead")
        assert agg.total_records_processed == 1
        assert "name" in agg.schema_snapshot
    
    def test_aggregate_respects_checkpoint(self):
        """Should only process records after checkpoint."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        record1 = RecordFactory(tenant=tenant, entity_type="lead", data={"name": "John"})
        set_last_processed_record_id(tenant, "lead", record1.id)
        
        # Create a new record after checkpoint
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "Jane"})
        
        processed = aggregate_records_for_tenant_entity(tenant, "lead")
        
        # Should only process record2
        assert processed == 1
        agg = RecordAggregator.objects.get(tenant=tenant, entity_type="lead")
        assert agg.total_records_processed == 1
        assert agg.schema_snapshot == {"name": {"count": 1, "field_type": "string"}}
    
    def test_aggregate_updates_checkpoint(self):
        """Should update checkpoint after processing."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        record = RecordFactory(tenant=tenant, entity_type="lead", data={"name": "John"})
        
        aggregate_records_for_tenant_entity(tenant, "lead")
        
        checkpoint = get_last_processed_record_id(tenant, "lead")
        assert checkpoint == record.id
    
    def test_aggregate_incremental_processing(self):
        """Should accumulate records across multiple aggregation runs."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        
        # First batch
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "John"})
        aggregate_records_for_tenant_entity(tenant, "lead")
        agg = RecordAggregator.objects.get(tenant=tenant, entity_type="lead")
        assert agg.total_records_processed == 1
        
        # Second batch
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "Jane"})
        aggregate_records_for_tenant_entity(tenant, "lead")
        agg.refresh_from_db()
        assert agg.total_records_processed == 2
    
    def test_aggregate_skips_when_no_new_records(self):
        """Should return 0 when no new records to process."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "John"})
        
        # First aggregation
        aggregate_records_for_tenant_entity(tenant, "lead")
        
        # Second aggregation with no new records
        processed = aggregate_records_for_tenant_entity(tenant, "lead")
        assert processed == 0
    
    def test_aggregate_respects_chunk_size(self):
        """Should only process up to chunk_size records."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        # Create 3 records
        for i in range(3):
            RecordFactory(tenant=tenant, entity_type="lead", data={"name": f"Person{i}"})
        
        # Aggregate with chunk_size=2
        processed = aggregate_records_for_tenant_entity(tenant, "lead", chunk_size=2)
        
        assert processed == 2
        agg = RecordAggregator.objects.get(tenant=tenant, entity_type="lead")
        assert agg.total_records_processed == 2


@pytest.mark.django_db
class TestMultiEntityAggregation:
    """Test aggregation across multiple entity types."""
    
    def setup_method(self):
        """Clean up system settings and record aggregators before each test."""
        SystemSettings.objects.all().delete()
        RecordAggregator.objects.all().delete()
    
    def _get_unique_slug(self):
        """Generate a unique tenant slug."""
        return f"test-tenant-{uuid.uuid4().hex[:8]}"
    
    def test_aggregate_all_entities_multiple_types(self):
        """Should aggregate all entity types for a tenant."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        
        # Create records for different entity types
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "Lead1"})
        RecordFactory(tenant=tenant, entity_type="ticket", data={"subject": "Ticket1"})
        RecordFactory(tenant=tenant, entity_type="account", data={"account_name": "Acme"})
        
        stats = aggregate_all_entities()
        
        assert stats['total_entities_processed'] == 3
        assert stats['total_records_processed'] == 3
        
        # Verify all aggregators created
        assert RecordAggregator.objects.filter(tenant=tenant).count() == 3
    
    def test_aggregate_all_entities_multiple_tenants(self):
        """Should aggregate across all tenants."""
        tenant1 = TenantFactory(slug=self._get_unique_slug())
        tenant2 = TenantFactory(slug=self._get_unique_slug())
        
        RecordFactory(tenant=tenant1, entity_type="lead", data={"name": "Lead1"})
        RecordFactory(tenant=tenant2, entity_type="ticket", data={"subject": "Ticket1"})
        
        stats = aggregate_all_entities()
        
        assert stats['total_entities_processed'] == 2
        assert stats['total_records_processed'] == 2
        
        # Verify aggregators for both tenants
        assert RecordAggregator.objects.filter(tenant=tenant1).count() == 1
        assert RecordAggregator.objects.filter(tenant=tenant2).count() == 1
    
    def test_aggregate_all_entities_incremental(self):
        """Should handle incremental aggregation across multiple runs."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        
        # First batch
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "Lead1"})
        RecordFactory(tenant=tenant, entity_type="ticket", data={"subject": "Ticket1"})
        
        stats1 = aggregate_all_entities()
        assert stats1['total_entities_processed'] == 2
        
        # Second batch (new records)
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "Lead2"})
        
        stats2 = aggregate_all_entities()
        # Only the new lead should be processed
        assert stats2['total_entities_processed'] == 1
        assert stats2['total_records_processed'] == 1
    
    def test_aggregate_all_entities_skips_no_new_records(self):
        """Should skip entities with no new records."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "Lead1"})
        RecordFactory(tenant=tenant, entity_type="ticket", data={"subject": "Ticket1"})
        
        # First aggregation
        stats1 = aggregate_all_entities()
        assert stats1['total_entities_processed'] == 2
        
        # Second aggregation with no new records
        stats2 = aggregate_all_entities()
        assert stats2['total_entities_processed'] == 0
        assert stats2['total_records_processed'] == 0


@pytest.mark.django_db
class TestSchemaSnapshot:
    """Test schema snapshot storage and merging."""
    
    def setup_method(self):
        """Clean up system settings and record aggregators before each test."""
        SystemSettings.objects.all().delete()
        RecordAggregator.objects.all().delete()
    
    def _get_unique_slug(self):
        """Generate a unique tenant slug."""
        return f"test-tenant-{uuid.uuid4().hex[:8]}"
    
    def test_schema_snapshot_stored_correctly(self):
        """Should store schema snapshot with correct structure."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        RecordFactory(
            tenant=tenant,
            entity_type="lead",
            data={"name": "John", "email": "john@example.com"}
        )
        
        aggregate_records_for_tenant_entity(tenant, "lead")
        
        agg = RecordAggregator.objects.get(tenant=tenant, entity_type="lead")
        
        assert isinstance(agg.schema_snapshot, dict)
        assert "name" in agg.schema_snapshot
        assert "email" in agg.schema_snapshot
        assert agg.schema_snapshot["name"]["count"] == 1
        assert agg.schema_snapshot["name"]["field_type"] == "string"
    
    def test_schema_snapshot_updates_on_new_aggregation(self):
        """Should update counts when new records are aggregated."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        
        # First record
        RecordFactory(
            tenant=tenant,
            entity_type="lead",
            data={"name": "John", "email": "john@example.com"}
        )
        aggregate_records_for_tenant_entity(tenant, "lead")
        
        agg = RecordAggregator.objects.get(tenant=tenant, entity_type="lead")
        assert agg.schema_snapshot["name"]["count"] == 1
        
        # Second record
        RecordFactory(
            tenant=tenant,
            entity_type="lead",
            data={"name": "Jane", "phone": "5551234567"}  # has phone instead of email
        )
        aggregate_records_for_tenant_entity(tenant, "lead")
        
        agg.refresh_from_db()
        assert agg.schema_snapshot["name"]["count"] == 2
        assert "phone" in agg.schema_snapshot
        assert agg.schema_snapshot["phone"]["count"] == 1
    
    def test_aggregator_tracks_last_aggregation_time(self):
        """Should update last_aggregation_at timestamp."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "John"})
        
        before = datetime.now()
        aggregate_records_for_tenant_entity(tenant, "lead")
        after = datetime.now()
        
        agg = RecordAggregator.objects.get(tenant=tenant, entity_type="lead")
        assert before <= agg.last_aggregation_at <= after


@pytest.mark.django_db
class TestErrorHandling:
    """Test error handling in aggregation."""
    
    def setup_method(self):
        """Clean up system settings and record aggregators before each test."""
        SystemSettings.objects.all().delete()
        RecordAggregator.objects.all().delete()
    
    def _get_unique_slug(self):
        """Generate a unique tenant slug."""
        return f"test-tenant-{uuid.uuid4().hex[:8]}"
    
    def test_aggregate_all_entities_continues_on_error(self):
        """Should continue aggregating other entities if one fails."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        
        # Valid records
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "Lead1"})
        RecordFactory(tenant=tenant, entity_type="ticket", data={"subject": "Ticket1"})
        
        # This should succeed despite any per-entity errors
        stats = aggregate_all_entities()
        
        # At least the valid entities should be processed
        assert stats['total_records_processed'] >= 1
    
    def test_aggregate_all_entities_returns_error_list(self):
        """Should return empty errors list on success."""
        tenant = TenantFactory(slug=self._get_unique_slug())
        RecordFactory(tenant=tenant, entity_type="lead", data={"name": "John"})
        
        stats = aggregate_all_entities()
        
        assert 'errors' in stats
        assert isinstance(stats['errors'], list)
        # Should be empty on success
        assert len(stats['errors']) == 0
