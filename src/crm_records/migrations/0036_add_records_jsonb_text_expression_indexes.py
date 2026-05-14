from django.db import migrations


class Migration(migrations.Migration):
    """
    Composite btree indexes aligned with Django JSONField lookups that use the jsonb
    ``->`` operator (``data__key`` / ``data__key__in`` → ``(data->'key')`` … ``jsonb``),
    not ``->>`` text extraction.

    Drops legacy ``*_txt_idx`` indexes from an earlier draft (``KeyTextTransform`` / ``->>``)
    if present, then creates ``*_jsonpath_idx`` on ``(tenant_id, (data->'…'))`` for alive leads.

    Uses plain ``CREATE INDEX`` (not ``CONCURRENTLY``) so ``migrate`` works inside Django’s
    transaction; for large tables run equivalent ``CREATE INDEX CONCURRENTLY`` manually
    during a maintenance window if needed.
    """

    dependencies = [
        ("crm_records", "0035_add_records_alive_tenant_entity_id_idx"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP INDEX IF EXISTS public.records_lead_tnt_assigned_to_txt_idx;
            DROP INDEX IF EXISTS public.records_lead_tnt_lead_source_txt_idx;
            DROP INDEX IF EXISTS public.records_lead_tnt_lead_status_txt_idx;
            DROP INDEX IF EXISTS public.records_lead_tnt_first_assigned_to_txt_idx;

            CREATE INDEX IF NOT EXISTS records_lead_tnt_assigned_to_jsonpath_idx
            ON public.records (tenant_id, ((data->'assigned_to')))
            WHERE entity_type = 'lead'
              AND is_deleted = false
              AND deleted_at IS NULL;

            CREATE INDEX IF NOT EXISTS records_lead_tnt_lead_source_jsonpath_idx
            ON public.records (tenant_id, ((data->'lead_source')))
            WHERE entity_type = 'lead'
              AND is_deleted = false
              AND deleted_at IS NULL;

            CREATE INDEX IF NOT EXISTS records_lead_tnt_lead_status_jsonpath_idx
            ON public.records (tenant_id, ((data->'lead_status')))
            WHERE entity_type = 'lead'
              AND is_deleted = false
              AND deleted_at IS NULL;

            CREATE INDEX IF NOT EXISTS records_lead_tnt_first_assigned_to_jsonpath_idx
            ON public.records (tenant_id, ((data->'first_assigned_to')))
            WHERE entity_type = 'lead'
              AND is_deleted = false
              AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_lead_tnt_assigned_to_jsonpath_idx;
            DROP INDEX IF EXISTS public.records_lead_tnt_lead_source_jsonpath_idx;
            DROP INDEX IF EXISTS public.records_lead_tnt_lead_status_jsonpath_idx;
            DROP INDEX IF EXISTS public.records_lead_tnt_first_assigned_to_jsonpath_idx;
            """,
        ),
    ]
