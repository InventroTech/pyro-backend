from django.db import migrations


class Migration(migrations.Migration):
    """
    Indexes for RmActivityEventListView — the RM PRD analytics dashboard's
    one query. Every page load filters by tenant + a date-range window on
    event_data__started_at, and the RM/ASM views add an event_data__rm_user_id
    filter on top of that.

    rm_activity_events moved everything but tenant/event_type into the
    event_data JSONB blob (migrations 0015-0017), which dropped the real
    btree indexes this query used to get for free. The GIN index added in
    0017 only serves containment/key-existence lookups (`@>`, `?`), not the
    range/equality comparisons these filters actually do, so every load was
    scanning the table. These mirror the real indexes the table had before,
    expressed over the JSON paths — matching Django's actual compiled SQL for
    a JSONField key-transform filter, which compares via `->` (jsonb-to-jsonb),
    not `->>` (text).
    """

    atomic = False

    dependencies = [
        ("analytics", "0018_rename_disposition_to_updated_status"),
    ]

    operations = [
        # Whole-tenant / ASM-hierarchy view: tenant + started_at range, no
        # rm_user_id filter.
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS rm_events_tenant_started_idx
            ON public.rm_activity_events (tenant_id, ((event_data -> 'started_at')))
            WHERE is_deleted = false AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.rm_events_tenant_started_idx;
            """,
        ),
        # Single-RM "my own data" view ("Your Shift" panel, RM-view pages):
        # tenant + rm_user_id (exact/IN) + started_at range, all in one index
        # so Postgres can satisfy the whole WHERE clause as a single range scan.
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS rm_events_tenant_rm_started_idx
            ON public.rm_activity_events (
                tenant_id,
                ((event_data -> 'rm_user_id')),
                ((event_data -> 'started_at'))
            )
            WHERE is_deleted = false AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.rm_events_tenant_rm_started_idx;
            """,
        ),
    ]
