from django.db import migrations


class Migration(migrations.Migration):
    """
    Indexes for RM/CSE analytics board query patterns.

    EventLog analytics filters by tenant + event + timestamp and often by
    payload.user_id. Support-ticket CSE metrics filter records by entity_type
    and JSON fields (cse_name / completed_at / dumped_at).
    """

    atomic = False

    dependencies = [
        ("crm_records", "0038_add_unassigned_leads_index"),
    ]

    operations = [
        # Primary EventLog path used by TeamMetricsService / RmMetricsService:
        # tenant + event__in + timestamp range (without requiring record_id).
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS event_logs_tenant_event_ts_alive_idx
            ON public.event_logs (tenant_id, event, timestamp DESC)
            WHERE is_deleted = false AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.event_logs_tenant_event_ts_alive_idx;
            """,
        ),
        # payload__user_id lookups used for per-RM / per-team analytics.
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS event_logs_tenant_payload_user_ts_alive_idx
            ON public.event_logs (
                tenant_id,
                ((payload->>'user_id')),
                timestamp DESC
            )
            WHERE is_deleted = false
              AND deleted_at IS NULL
              AND COALESCE(TRIM(payload->>'user_id'), '') <> '';
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.event_logs_tenant_payload_user_ts_alive_idx;
            """,
        ),
        # Narrow partial index covering only analytics-tracked lead/agent events.
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS event_logs_analytics_tracked_alive_idx
            ON public.event_logs (
                tenant_id,
                event,
                ((payload->>'user_id')),
                timestamp DESC
            )
            WHERE is_deleted = false
              AND deleted_at IS NULL
              AND event IN (
                'lead.get_next_lead',
                'lead.trial_activated',
                'lead.call_not_connected',
                'lead.call_back_later',
                'agent.take_break',
                'lead.not_interested'
              );
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.event_logs_analytics_tracked_alive_idx;
            """,
        ),
        # CSE analytics: assigned tickets by cse_name within a tenant.
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_support_ticket_cse_name_alive_idx
            ON public.records (tenant_id, ((data->>'cse_name')))
            WHERE entity_type = 'support_ticket'
              AND is_deleted = false
              AND deleted_at IS NULL
              AND COALESCE(TRIM(data->>'cse_name'), '') <> '';
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_support_ticket_cse_name_alive_idx;
            """,
        ),
        # CSE date-window filters on completed_at / dumped_at (stored in JSON).
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_support_ticket_completed_at_alive_idx
            ON public.records (tenant_id, ((data->>'completed_at')))
            WHERE entity_type = 'support_ticket'
              AND is_deleted = false
              AND deleted_at IS NULL
              AND COALESCE(TRIM(data->>'completed_at'), '') <> '';
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_support_ticket_completed_at_alive_idx;
            """,
        ),
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS records_support_ticket_dumped_at_alive_idx
            ON public.records (tenant_id, ((data->>'dumped_at')))
            WHERE entity_type = 'support_ticket'
              AND is_deleted = false
              AND deleted_at IS NULL
              AND COALESCE(TRIM(data->>'dumped_at'), '') <> '';
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.records_support_ticket_dumped_at_alive_idx;
            """,
        ),
    ]
