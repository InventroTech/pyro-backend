from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("crm_records", "0033_add_records_tenant_entity_type_id_idx"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_tenant_lead_stage_upper_idx
            ON public.records (tenant_id, (UPPER(COALESCE(data->>'lead_stage', ''))))
            WHERE entity_type = 'lead';
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_tenant_lead_stage_upper_idx;
            """,
        ),
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_lead_queue_sort_idx
            ON public.records (
                tenant_id,
                (COALESCE((data->>'call_attempts')::int, 0)) ASC,
                (COALESCE((data->>'lead_score')::float, 0)) DESC,
                created_at DESC,
                id ASC
            )
            WHERE entity_type = 'lead';
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_lead_queue_sort_idx;
            """,
        ),
    ]
