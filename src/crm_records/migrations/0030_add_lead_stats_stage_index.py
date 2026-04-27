from django.db import migrations


class Migration(migrations.Migration):
    # Required for CREATE INDEX CONCURRENTLY.
    atomic = False

    dependencies = [
        ("crm_records", "0029_add_queueable_lead_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX IF NOT EXISTS records_lead_stats_tenant_stage_idx
            ON public.records (tenant_id, ((data->>'lead_stage')))
            WHERE entity_type = 'lead';
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_lead_stats_tenant_stage_idx;
            """,
        ),
    ]
