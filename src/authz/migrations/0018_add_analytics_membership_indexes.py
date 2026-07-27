from django.db import migrations


class Migration(migrations.Migration):
    """
    Membership indexes for Manager I / team hierarchy analytics lookups.
    """

    atomic = False

    dependencies = [
        ("authz", "0017_alter_groupmembership_managers_and_more"),
    ]

    operations = [
        # Active members by tenant + role (CSE/RM listing, Manager I detection).
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS authz_tm_tenant_active_role_alive_idx
            ON public.authz_tenantmembership (tenant_id, role_id, user_id)
            WHERE is_active = true
              AND is_deleted = false
              AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.authz_tm_tenant_active_role_alive_idx;
            """,
        ),
        # Hierarchy walk: active children under a parent membership.
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS authz_tm_tenant_active_parent_alive_idx
            ON public.authz_tenantmembership (tenant_id, user_parent_id_id, user_id)
            WHERE is_active = true
              AND is_deleted = false
              AND deleted_at IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.authz_tm_tenant_active_parent_alive_idx;
            """,
        ),
    ]
