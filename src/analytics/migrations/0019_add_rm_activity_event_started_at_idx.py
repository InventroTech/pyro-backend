from django.db import migrations


class Migration(migrations.Migration):
    # Required for CREATE INDEX CONCURRENTLY.
    atomic = False

    dependencies = [
        ("analytics", "0018_rename_disposition_to_updated_status"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS rm_events_tenant_started_idx
            ON public.rm_activity_events (tenant_id, (event_data->>'started_at'))
            WHERE is_deleted = false;
            """,
            reverse_sql="""
            DROP INDEX CONCURRENTLY IF EXISTS public.rm_events_tenant_started_idx;
            """,
        ),
    ]
