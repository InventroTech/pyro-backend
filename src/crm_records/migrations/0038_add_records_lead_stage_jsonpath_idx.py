from django.db import migrations


class Migration(migrations.Migration):
    """
    Btree on ``(tenant_id, (data->'lead_stage'))`` for alive leads.

    Django ``Record.objects.filter(data__lead_stage=…)`` compiles to jsonb path ``->``,
    not text ``->>``. Migration ``0031`` indexes ``(data->>'lead_stage')`` for stats-style
    scans; this index matches ORM equality filters on ``data__lead_stage``.

    Plain ``CREATE INDEX`` / ``DROP INDEX`` (not ``CONCURRENTLY``) so ``migrate`` succeeds
    inside a transaction.
    """

    dependencies = [
        ("crm_records", "0037_realign_lead_queue_expression_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX IF NOT EXISTS records_lead_tnt_lead_stage_jsonpath_idx
            ON public.records (tenant_id, ((data->'lead_stage')))
            WHERE entity_type = 'lead'
              AND is_deleted = false
              AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_lead_tnt_lead_stage_jsonpath_idx;
            """,
        ),
    ]
