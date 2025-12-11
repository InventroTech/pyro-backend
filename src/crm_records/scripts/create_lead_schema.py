#!/usr/bin/env python
"""
Script to create EntityTypeSchema for 'lead' entity type based on sample lead data.
Extracts all attributes including nested ones from the data structure.
"""
import os
import sys
import django

# Setup Django
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from crm_records.models import EntityTypeSchema
from core.models import Tenant


def extract_attributes_from_data(data, prefix="data", attributes=None):
    """
    Recursively extract all attribute paths from a JSON object.
    
    Args:
        data: The JSON object/dict/list
        prefix: Current path prefix (default: "data")
        attributes: Set to collect attributes (default: None, creates new set)
    
    Returns:
        Set of attribute paths
    """
    if attributes is None:
        attributes = set()
    
    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{prefix}.{key}" if prefix else key
            attributes.add(current_path)
            
            # Recursively process nested structures
            if isinstance(value, (dict, list)):
                extract_attributes_from_data(value, current_path, attributes)
    elif isinstance(data, list):
        # For arrays, we'll add the array path and process first item if it's a dict
        attributes.add(prefix)
        if data and isinstance(data[0], dict):
            # Process the structure of array items
            extract_attributes_from_data(data[0], prefix, attributes)
    
    return attributes


def create_lead_schema(tenant_id=None):
    """
    Create EntityTypeSchema for 'lead' entity type.
    
    Args:
        tenant_id: Optional tenant UUID. If None, will use first tenant.
    """
    # Sample lead data structure
    sample_lead_data = {
        "tasks": [
            {
                "task": "Sending a Demo",
                "status": "Yes"
            },
            {
                "task": "App Installation",
                "status": "Yes"
            },
            {
                "task": "Create/Update Layout",
                "status": "Null"
            },
            {
                "task": "Layout Feedback",
                "status": "Null"
            },
            {
                "task": "Trial Subscription",
                "status": "Null"
            },
            {
                "task": "Premium Poster/ Video Poster Share",
                "status": "Null"
            }
        ],
        "praja_id": "PRAJA_277BDA3C",
        "lead_score": 74.42,
        "lead_stage": "scheduled",
        "assigned_to": "USER483681",
        "lead_source": "WhatsApp Campaign",
        "closure_time": None,
        "next_call_at": "2025-11-30T06:13:30.087368",
        "phone_number": "+918882820919",
        "rm_dashboard": "Amit Kumar",
        "call_attempts": 2,
        "whatsapp_link": "https://wa.me/918882820919",
        "latest_remarks": "Technical team evaluation in progress",
        "affiliated_party": "Channel Partner",
        "package_to_pitch": "Annual Premium - ₹9,999/year",
        "user_profile_link": "https://app.thepyro.ai/users/USR758210",
        "premium_poster_count": 3,
        "last_active_date_time": "2025-11-22T15:13:30.087399"
    }
    
    # Base Record model attributes
    base_attributes = [
        "id",
        "tenant_id",
        "entity_type",
        "name",
        "data",
        "created_at",
        "updated_at"
    ]
    
    # Extract attributes from data field
    data_attributes = extract_attributes_from_data(sample_lead_data, prefix="data")
    
    # Combine all attributes
    all_attributes = sorted(list(set(base_attributes + list(data_attributes))))
    
    # Get or create tenant
    if tenant_id:
        try:
            tenant = Tenant.objects.get(id=tenant_id)
        except Tenant.DoesNotExist:
            print(f"Error: Tenant with ID {tenant_id} not found.")
            return
    else:
        tenant = Tenant.objects.first()
        if not tenant:
            print("Error: No tenants found in database.")
            return
    
    print(f"Using tenant: {tenant.name} ({tenant.id})")
    print(f"Extracted {len(all_attributes)} attributes for 'lead' entity type")
    
    # Create or update schema
    schema, created = EntityTypeSchema.objects.update_or_create(
        tenant=tenant,
        entity_type="lead",
        defaults={
            "attributes": all_attributes,
            "description": "Lead entity schema extracted from sample lead data structure"
        }
    )
    
    action = "Created" if created else "Updated"
    print(f"\n{action} EntityTypeSchema for 'lead':")
    print(f"  ID: {schema.id}")
    print(f"  Entity Type: {schema.entity_type}")
    print(f"  Total Attributes: {len(schema.attributes)}")
    print(f"\nAttributes ({len(schema.attributes)}):")
    for attr in schema.attributes:
        print(f"  - {attr}")
    
    return schema


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create EntityTypeSchema for 'lead' entity type")
    parser.add_argument("--tenant-id", type=str, help="Tenant UUID (optional, uses first tenant if not provided)")
    args = parser.parse_args()
    
    create_lead_schema(tenant_id=args.tenant_id)

