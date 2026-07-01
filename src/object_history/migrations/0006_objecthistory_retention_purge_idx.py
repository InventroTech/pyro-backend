from django.db import migrations, models

_RETENTION_INDEX = models.Index(
    fields=["tenant", "created_at"],
    name="object_hist_retention_idx",
    condition=models.Q(
        persistent_history=False,
        is_deleted=False,
        deleted_at__isnull=True,
    ),
)

_CREATE_INDEX_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS object_hist_retention_idx
ON public.object_history (tenant_id, created_at)
WHERE persistent_history = false
  AND is_deleted = false
  AND deleted_at IS NULL
"""

_DROP_INDEX_SQL = "DROP INDEX CONCURRENTLY IF EXISTS public.object_hist_retention_idx"


def _run_outside_transaction(connection, statements):
    """Run DDL with autocommit so CREATE INDEX CONCURRENTLY is allowed."""
    old_autocommit = connection.get_autocommit()
    try:
        connection.set_autocommit(True)
        with connection.cursor() as cursor:
            for sql in statements:
                cursor.execute(sql)
    finally:
        connection.set_autocommit(old_autocommit)


def create_retention_index(apps, schema_editor):
    if schema_editor.connection.vendor != "postgresql":
        return
    _run_outside_transaction(
        schema_editor.connection,
        [
            "SET statement_timeout TO 0",
            _CREATE_INDEX_SQL,
            "RESET statement_timeout",
        ],
    )


def drop_retention_index(apps, schema_editor):
    if schema_editor.connection.vendor != "postgresql":
        return
    _run_outside_transaction(
        schema_editor.connection,
        [
            "SET statement_timeout TO 0",
            _DROP_INDEX_SQL,
            "RESET statement_timeout",
        ],
    )


class Migration(migrations.Migration):
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    atomic = False

    dependencies = [
        ("object_history", "0005_alter_objecthistory_managers_and_more"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AddIndex(
                    model_name="objecthistory",
                    index=_RETENTION_INDEX,
                ),
            ],
            database_operations=[
                migrations.RunPython(
                    create_retention_index,
                    drop_retention_index,
                    atomic=False,
                ),
            ],
        ),
    ]
