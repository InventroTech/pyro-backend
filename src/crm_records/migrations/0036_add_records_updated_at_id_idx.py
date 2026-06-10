from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("crm_records", "0035_add_records_alive_tenant_entity_id_idx"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_updated_at_id_idx
            ON public.records (updated_at, id)
            WHERE is_deleted = false AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_updated_at_id_idx;
            """,
        ),
    ]
