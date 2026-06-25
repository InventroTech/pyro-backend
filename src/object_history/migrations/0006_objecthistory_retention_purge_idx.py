from django.db import migrations, models


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
                    index=models.Index(
                        fields=["tenant", "created_at"],
                        name="object_hist_retention_idx",
                        condition=models.Q(
                            persistent_history=False,
                            is_deleted=False,
                            deleted_at__isnull=True,
                        ),
                    ),
                ),
            ],
            database_operations=[
                migrations.RunSQL(
                    sql="""
                    SET statement_timeout TO 0;
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS object_hist_retention_idx
                    ON public.object_history (tenant_id, created_at)
                    WHERE persistent_history = false
                      AND is_deleted = false
                      AND deleted_at IS NULL;
                    RESET statement_timeout;
                    """,
                    reverse_sql="""
                    DROP INDEX CONCURRENTLY IF EXISTS public.object_hist_retention_idx;
                    """,
                    # RunSQL defaults to atomic=True, which breaks CONCURRENTLY.
                    atomic=False,
                ),
            ],
        ),
    ]
