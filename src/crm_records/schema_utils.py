"""
Schema Utilities for EntityTypeSchema

This module provides utilities for automatically detecting and updating
attributes in EntityTypeSchema based on record data.
"""
import logging
from typing import Dict, Any, List, Set
from django.db import transaction
from .models import EntityTypeSchema, Record

logger = logging.getLogger(__name__)


def extract_attribute_paths(data: Dict[str, Any], prefix: str = '') -> Set[str]:
    """
    Recursively extract all attribute paths from a nested dictionary.
    
    Args:
        data: Dictionary to extract paths from
        prefix: Prefix to prepend to paths (for nested structures)
        
    Returns:
        Set of attribute paths (e.g., {'id', 'name', 'data.email', 'data.phone', 'data.user.name'})
    
    Example:
        >>> data = {'name': 'John', 'data': {'email': 'john@example.com', 'phone': '123'}}
        >>> extract_attribute_paths(data)
        {'name', 'data.email', 'data.phone'}
    """
    paths = set()
    
    if not isinstance(data, dict):
        return paths
    
    for key, value in data.items():
        # Skip None values and empty dicts
        if value is None:
            continue
        
        # Build the full path
        if prefix:
            full_path = f"{prefix}.{key}"
        else:
            full_path = key
        
        paths.add(full_path)
        
        # Recursively process nested dictionaries
        if isinstance(value, dict) and value:
            nested_paths = extract_attribute_paths(value, prefix=full_path)
            paths.update(nested_paths)
        # Handle lists of dictionaries
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    nested_paths = extract_attribute_paths(item, prefix=full_path)
                    paths.update(nested_paths)
    
    return paths


def get_record_attribute_paths(record: Record) -> Set[str]:
    """
    Extract all attribute paths from a Record instance.
    
    Includes:
    - Model fields: 'id', 'entity_type', 'created_at', 'updated_at', 'tenant_id'
    - Data fields: 'data.email', 'data.phone', etc.
    - Nested data fields: 'data.user.profile.name', etc.
    
    Args:
        record: Record instance
        
    Returns:
        Set of attribute paths
    """
    paths = set()
    
    # Add model-level fields
    model_fields = ['id', 'entity_type', 'created_at', 'updated_at', 'tenant_id']
    paths.update(model_fields)
    
    # Extract paths from data field
    if record.data and isinstance(record.data, dict):
        data_paths = extract_attribute_paths(record.data, prefix='data')
        paths.update(data_paths)
    
    return paths


def update_entity_type_schema_attributes(
    record: Record,
    tenant_id: str = None
) -> EntityTypeSchema:
    """
    Update EntityTypeSchema with new attributes found in the record.
    
    This function:
    1. Extracts all attribute paths from the record
    2. Gets or creates EntityTypeSchema for the record's entity_type
    3. Merges new attributes with existing ones
    4. Updates the schema
    
    Args:
        record: Record instance to extract attributes from
        tenant_id: Optional tenant ID. If not provided, uses record.tenant_id
        
    Returns:
        Updated EntityTypeSchema instance
        
    Example:
        >>> record = Record.objects.get(id=123)
        >>> schema = update_entity_type_schema_attributes(record)
        >>> print(schema.attributes)
        ['id', 'entity_type', 'data.email', 'data.phone', 'data.name']
    """
    if not record:
        logger.warning("update_entity_type_schema_attributes: record is None")
        return None
    
    # Get tenant_id
    tenant = record.tenant if hasattr(record, 'tenant') else None
    tenant_id = tenant_id or (tenant.id if tenant else None)
    
    if not tenant_id:
        logger.warning(f"update_entity_type_schema_attributes: No tenant_id for record {record.id}")
        return None
    
    # Get entity_type
    entity_type = record.entity_type if hasattr(record, 'entity_type') else None
    if not entity_type:
        logger.warning(f"update_entity_type_schema_attributes: No entity_type for record {record.id}")
        return None
    
    # Extract all attribute paths from the record
    new_attributes = get_record_attribute_paths(record)
    
    # Get or create EntityTypeSchema
    schema, created = EntityTypeSchema.objects.get_or_create(
        tenant_id=tenant_id,
        entity_type=entity_type,
        defaults={
            'attributes': [],
            'rules': []
        }
    )
    
    # Get existing attributes as a set for easy comparison
    existing_attributes = set(schema.attributes or [])
    
    # Find new attributes
    new_attributes_to_add = new_attributes - existing_attributes
    
    if new_attributes_to_add:
        # Merge and sort attributes
        all_attributes = sorted(list(existing_attributes | new_attributes))
        
        # Update the schema
        schema.attributes = all_attributes
        schema.save(update_fields=['attributes', 'updated_at'])
        
        logger.info(
            f"update_entity_type_schema_attributes: Updated schema for entity_type '{entity_type}' "
            f"(tenant: {tenant_id}). Added {len(new_attributes_to_add)} new attributes: {sorted(new_attributes_to_add)}"
        )
    else:
        logger.debug(
            f"update_entity_type_schema_attributes: No new attributes found for entity_type '{entity_type}' "
            f"(tenant: {tenant_id})"
        )
    
    return schema

