"""
Record Aggregation Service

Handles periodic schema discovery from the records table.
Scans new records, captures field names and their occurrence counts,
and updates the RecordAggregator with schema snapshots.
"""
import logging
from typing import Dict, Any
from collections import defaultdict

from django.utils import timezone

from crm_records.models import Record
from .models import SystemSettings, RecordAggregator

logger = logging.getLogger(__name__)


def get_last_processed_record_id(tenant, entity_type: str) -> int:
    """
    Get the last processed record ID for a specific tenant and entity type.
    
    Args:
        tenant: The tenant object
        entity_type: The entity type string
        
    Returns:
        The last processed record ID for this tenant/entity combo, or 0 if not set
    """
    checkpoint_key = f"record_aggregator_{tenant.slug}_{entity_type}_last_processed_id"
    try:
        setting = SystemSettings.objects.get(setting_key=checkpoint_key)
        return setting.setting_value.get('value', 0) if isinstance(setting.setting_value, dict) else 0
    except SystemSettings.DoesNotExist:
        return 0


def set_last_processed_record_id(tenant, entity_type: str, record_id: int) -> None:
    """
    Update the last processed record ID for a specific tenant and entity type.
    
    Args:
        tenant: The tenant object
        entity_type: The entity type string
        record_id: The record ID to set
    """
    checkpoint_key = f"record_aggregator_{tenant.slug}_{entity_type}_last_processed_id"
    setting, created = SystemSettings.objects.get_or_create(
        setting_key=checkpoint_key,
        defaults={
            'description': f'Checkpoint for record aggregator job - {entity_type} in {tenant.slug}'
        }
    )
    setting.setting_value = {'value': record_id}
    setting.save()


def build_schema_snapshot(records_queryset) -> Dict[str, Dict[str, Any]]:
    """
    Build a schema snapshot from records.
    Extracts all field names with their count and type information.
    
    Args:
        records_queryset: QuerySet or list of Record objects to analyze
        
    Returns:
        Dictionary mapping field names to {count: int, field_type: str}
    """
    schema = defaultdict(lambda: {'count': 0})
    
    for record in records_queryset:
        record_data = record.data or {}
        
        # Process each field in the record data
        for field_name, field_value in record_data.items():
            if field_name not in schema:
                schema[field_name] = {'count': 0}
            
            schema[field_name]['count'] += 1
    
    # Build snapshot with count and field_type
    snapshot = {}
    for field_name, field_data in schema.items():
        snapshot[field_name] = {
            'count': field_data['count'],
            'field_type': 'string'  # Default type; can be enhanced with type detection
        }
    
    return snapshot


def aggregate_records_for_tenant_entity(tenant, entity_type, chunk_size: int = 1000) -> int:
    """
    Aggregate records for a specific tenant and entity type.
    
    Process flow:
    1. Get last_processed_record_id from SystemSettings
    2. Fetch new records from records table (from last_processed_record_id onwards)
    3. Build schema snapshot from those records
    4. Update or create RecordAggregator with schema snapshot
    5. Update SystemSettings with new last_processed_record_id
    
    Args:
        tenant: The Tenant object
        entity_type: The entity type string (e.g., 'lead', 'ticket')
        chunk_size: Number of records to process per call (default: 1000)
        
    Returns:
        Number of records processed
    """
    try:
        # 1. Get the last processed record ID for this tenant/entity combo
        last_processed_id = get_last_processed_record_id(tenant, entity_type)
        
        # 2. Fetch new records for this tenant and entity type
        records_query = Record.objects.filter(
            tenant=tenant,
            entity_type=entity_type,
            id__gt=last_processed_id  # Only get records we haven't processed
        ).order_by('id')[:chunk_size]
        
        # Use iterator() for memory-efficient streaming from database
        # Streams records in chunks of 100 rather than loading all at once
        new_records = list(records_query.iterator(chunk_size=100))
        
        if not new_records:
            return 0
        
        # 3. Build schema snapshot
        schema_snapshot = build_schema_snapshot(new_records)
        
        # 4. Update or create RecordAggregator
        aggregator, created = RecordAggregator.objects.get_or_create(
            tenant=tenant,
            entity_type=entity_type,
            defaults={'schema_snapshot': {}}
        )
        
        # Merge new schema with existing schema
        current_snapshot = aggregator.schema_snapshot or {}
        
        # Merge schema with existing snapshot
        for field_name, field_info in schema_snapshot.items():
            if field_name in current_snapshot:
                # Update count and field_type for existing fields
                current_snapshot[field_name] = {
                    'count': field_info['count'],
                    'field_type': field_info['field_type']
                }
            else:
                current_snapshot[field_name] = field_info
        
        # Update the aggregator
        aggregator.schema_snapshot = current_snapshot
        aggregator.total_records_processed += len(new_records)
        aggregator.last_aggregation_at = timezone.now()
        aggregator.save()
        
        # 5. Update the checkpoint for this tenant/entity combo
        last_new_record_id = new_records[-1].id  # Get last record from list
        set_last_processed_record_id(tenant, entity_type, last_new_record_id)
        
        logger.info(
            f"Aggregated {len(new_records)} {entity_type} records for tenant {tenant.slug} "
            f"(ID range: {last_processed_id + 1} to {last_new_record_id})"
        )
        
        return len(new_records)
        
    except Exception as e:
        logger.error(
            f"Error aggregating {entity_type} records for tenant {tenant.slug}: {str(e)}"
        )
        raise


def aggregate_all_entities(chunk_size: int = 1000) -> Dict[str, Any]:
    """
    Aggregate all entities across all tenants.
    
    Process:
    1. Get all distinct tenants with records
    2. For each tenant, get all distinct entity types
    3. Call aggregate_records_for_tenant_entity for each (tenant, entity_type) pair
    
    Args:
        chunk_size: Number of records to process per entity (default: 1000)
        
    Returns:
        Dictionary with aggregation statistics
    """
    from core.models import Tenant
    from django.db import connection
    
    stats = {
        'total_entities_processed': 0,
        'total_records_processed': 0,
        'errors': []
    }
    
    try:
        # Get all distinct tenant IDs from records table using raw SQL to avoid ORM filtering issues
        with connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT tenant_id FROM records WHERE tenant_id IS NOT NULL")
            tenant_ids = [row[0] for row in cursor.fetchall()]
        
        # Fetch the actual tenant objects
        tenants_with_records = Tenant.objects.filter(id__in=tenant_ids)
        
        for tenant in tenants_with_records:
            # Get all entity types for this tenant
            entity_types = Record.objects.filter(
                tenant=tenant
            ).values_list('entity_type', flat=True).distinct()
            
            for entity_type in entity_types:
                try:
                    processed = aggregate_records_for_tenant_entity(tenant, entity_type, chunk_size)
                    stats['total_records_processed'] += processed
                    if processed > 0:
                        stats['total_entities_processed'] += 1
                        
                except Exception as e:
                    error_msg = f"Failed to aggregate {entity_type} for {tenant.slug}: {str(e)}"
                    logger.error(error_msg)
                    stats['errors'].append(error_msg)
        
        return stats
        
    except Exception as e:
        error_msg = f"Error in aggregate_all_entities: {str(e)}"
        logger.error(error_msg)
        stats['errors'].append(error_msg)
        return stats
