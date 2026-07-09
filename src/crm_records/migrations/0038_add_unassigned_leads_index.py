from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("crm_records", "0037_record_change_pg_notify"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_unassigned_leads_idx
            ON public.records (tenant_id)
            WHERE entity_type = 'lead'
              AND (
                (data->>'assigned_to') IS NULL
                OR TRIM(COALESCE(data->>'assigned_to', '')) = ''
                OR LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) IN ('null', 'none')
              );
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_unassigned_leads_idx;
            """,
        ),
    ]
