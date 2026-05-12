from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("crm_records", "0034_add_records_lead_queue_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_alive_tenant_entity_id_idx
            ON public.records (tenant_id, entity_type, id)
            WHERE is_deleted = false AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_alive_tenant_entity_id_idx;
            """,
        ),
    ]
