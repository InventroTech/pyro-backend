from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("crm_records", "0032_alter_apisecretkey_managers_alter_bucket_managers_and_more"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_tenant_entity_type_id_idx
            ON public.records (tenant_id, entity_type, id);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_tenant_entity_type_id_idx;
            """,
        ),
    ]
