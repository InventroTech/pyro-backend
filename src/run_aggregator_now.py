#!/usr/bin/env python
import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from core.services import aggregate_all_entities

print("🚀 Running record aggregator immediately...")
stats = aggregate_all_entities()

print(f"\n✅ Aggregation Complete!")
print(f"  📊 Total entities processed: {stats['total_entities_processed']}")
print(f"  📝 Total records processed: {stats['total_records_processed']}")
if stats['errors']:
    print(f"  ❌ Errors: {len(stats['errors'])}")
    for error in stats['errors']:
        print(f"     - {error}")
else:
    print(f"  ✨ No errors!")
