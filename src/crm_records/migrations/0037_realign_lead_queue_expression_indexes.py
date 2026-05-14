from django.db import migrations


class Migration(migrations.Migration):
    """
    Replace ``records_lead_queue_sort_idx`` / ``records_tenant_lead_stage_upper_idx`` so they
    match ``PullStrategyApplier`` and default ``Record.objects`` (alive leads):

    - Sort key uses ``COALESCE(lead_score::float, -1) DESC NULLS LAST`` (not ``0``), plus
      ``updated_at`` between tiebreaker timestamps and ``id`` — see pull_strategy.py.
    - Partial WHERE includes ``is_deleted = false`` and ``deleted_at IS NULL``.

    Safe for DBs created from migration 0034 / user's production DDL: drops then recreates.
    """

    atomic = False

    dependencies = [
        ("crm_records", "0036_add_records_jsonb_text_expression_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX CONCURRENTLY IF EXISTS public.records_tenant_lead_stage_upper_idx;
            DROP INDEX CONCURRENTLY IF EXISTS public.records_lead_queue_sort_idx;
            """,
            reverse_sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_tenant_lead_stage_upper_idx
            ON public.records (tenant_id, (UPPER(COALESCE(data->>'lead_stage', ''))))
            WHERE entity_type = 'lead';

            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_lead_queue_sort_idx
            ON public.records (
                tenant_id,
                (COALESCE((data->>'call_attempts')::int, 0)) ASC,
                (COALESCE((data->>'lead_score')::double precision, (0)::double precision)) DESC,
                created_at DESC,
                id ASC
            )
            WHERE entity_type = 'lead';
            """,
        ),
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_tenant_lead_stage_upper_idx
            ON public.records (tenant_id, (UPPER(COALESCE(data->>'lead_stage', ''))))
            WHERE entity_type = 'lead'
              AND is_deleted = false
              AND deleted_at IS NULL;

            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_lead_queue_sort_idx
            ON public.records (
                tenant_id,
                (COALESCE((data->>'call_attempts')::int, 0)) ASC,
                (COALESCE((data->>'lead_score')::double precision, (-1)::double precision))
                    DESC NULLS LAST,
                created_at DESC,
                updated_at DESC,
                id ASC
            )
            WHERE entity_type = 'lead'
              AND is_deleted = false
              AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX CONCURRENTLY IF EXISTS public.records_tenant_lead_stage_upper_idx;
            DROP INDEX CONCURRENTLY IF EXISTS public.records_lead_queue_sort_idx;
            """,
        ),
    ]
