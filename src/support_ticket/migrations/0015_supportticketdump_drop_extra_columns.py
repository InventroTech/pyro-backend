"""
Copy prod-only drift columns into ``data``, then drop them.
Requires 0014 (JSON ``data`` + legacy ingest column removal) to already be applied.
"""

import json
import uuid

from django.db import migrations


_EXTRA_DUMP_COLUMNS = (
    "assigned_to",
    "resolution_status",
    "resolution_time",
    "cse_name",
    "cse_remarks",
    "call_status",
    "call_attempts",
    "rm_name",
    "completed_at",
    "snooze_until",
    "dumped_at",
)

_DROP_SQL = "\n".join(
    f'ALTER TABLE support_ticket_dump DROP COLUMN IF EXISTS "{column}";'
    for column in _EXTRA_DUMP_COLUMNS
)


def _column_names(schema_editor, table: str) -> set[str]:
    with schema_editor.connection.cursor() as cursor:
        return {
            col.name
            for col in schema_editor.connection.introspection.get_table_description(
                cursor, table
            )
        }


def _normalize_data(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _serialize_extra_value(value):
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def forwards_copy_extra_columns_into_data(apps, schema_editor):
    table = apps.get_model("support_ticket", "SupportTicketDump")._meta.db_table
    existing = _column_names(schema_editor, table)
    to_merge = [c for c in _EXTRA_DUMP_COLUMNS if c in existing]
    if not to_merge:
        return

    select_cols = ", ".join(f'"{c}"' for c in to_merge)
    SupportTicketDump = apps.get_model("support_ticket", "SupportTicketDump")

    with schema_editor.connection.cursor() as cursor:
        cursor.execute(f'SELECT "id", "data", {select_cols} FROM "{table}"')
        for row_id, data, *extras in cursor.fetchall():
            payload = _normalize_data(data)
            changed = False
            for field, value in zip(to_merge, extras):
                if field in payload:
                    continue
                serialized = _serialize_extra_value(value)
                if serialized is None:
                    continue
                payload[field] = serialized
                changed = True
            if changed:
                SupportTicketDump._default_manager.filter(pk=row_id).update(data=payload)


class Migration(migrations.Migration):

    dependencies = [
        ("support_ticket", "0014_supportticketdump_data_json"),
    ]

    operations = [
        migrations.RunPython(
            forwards_copy_extra_columns_into_data,
            migrations.RunPython.noop,
        ),
        migrations.RunSQL(sql=_DROP_SQL, reverse_sql=migrations.RunSQL.noop),
    ]
