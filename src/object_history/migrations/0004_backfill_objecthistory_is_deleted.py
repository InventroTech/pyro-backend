"""
Backfill rows where is_deleted was persisted as NULL (bypass SoftDelete managers).
"""

from django.db import connection, migrations


def backfill_alive(apps, schema_editor):
    """
    Rows with NULL is_deleted are invisible to SoftDeleteManager; use UPDATE directly.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE object_history SET is_deleted = false WHERE is_deleted IS NULL"
        )


class Migration(migrations.Migration):

    dependencies = [
        ("object_history", "0003_objecthistory_soft_delete"),
    ]

    operations = [
        migrations.RunPython(backfill_alive, migrations.RunPython.noop),
    ]
