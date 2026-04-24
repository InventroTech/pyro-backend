from django.db import migrations


class Migration(migrations.Migration):
    # Required for CREATE INDEX CONCURRENTLY.
    atomic = False

    dependencies = [
        ("crm_records", "0028_record_lead_field_expression_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX IF NOT EXISTS records_lead_queueable_main_idx
            ON public.records (tenant_id)
            WHERE entity_type = 'lead'
              AND (
                (data->>'assigned_to') IS NULL
                OR TRIM(COALESCE(data->>'assigned_to', '')) = ''
                OR LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) IN ('null', 'none')
              )
              AND UPPER(COALESCE(data->>'lead_stage', '')) IN ('FRESH', 'IN_QUEUE')
              AND COALESCE((data->>'call_attempts')::int, 0) = 0;

            CREATE INDEX IF NOT EXISTS records_tenant_entity_idx
            ON public.records (tenant_id, entity_type);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_lead_queueable_main_idx;
            DROP INDEX IF EXISTS public.records_tenant_entity_idx;
            """,
        ),
    ]
