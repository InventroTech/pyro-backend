from django.db import migrations


class Migration(migrations.Migration):
    """
    Repairs a schema drift on the live ``analytics_boards`` table.

    Migration ``0013_remove_analyticsboard_..._role_type_report_uniq_alive_and_more``
    (from the since-reverted "RM Manager I" feature, PR #951/954/955) dropped
    the ``role`` column and re-scoped boards to ``user_id`` on production on
    2026-07-22. Later that day the feature was reverted in code, which deleted
    that migration file — but nobody reversed the actual database change.
    ``models.py`` has needed ``role`` since 0012 and never stopped; Django's
    migration history (built only from files on disk) still ends at 0012 and
    has no record 0013 ever ran, so ``migrate`` has reported "nothing to do"
    on every deploy since, while the live column stayed missing.

    This uses raw SQL rather than AddField/AddConstraint because Django's
    state already believes ``role`` (and its index/constraint) exist — they
    do, per the migration files — so a normal state-changing operation would
    conflict with that history. Only the physical table needs to catch up.
    """

    dependencies = [
        (
            "analytics",
            "0012_remove_analyticsboard_analytics_board_tenant_user_type_report_uniq_alive_and_more",
        ),
    ]

    operations = [
        migrations.RunSQL(
            sql=[
                "ALTER TABLE analytics_boards "
                "ADD COLUMN IF NOT EXISTS role varchar(64) NOT NULL DEFAULT '';",
                # Leftover from the reverted user_id-scoped feature.
                "DROP INDEX IF EXISTS analytics_board_tenant_user_type_report_uniq_alive;",
                "DROP INDEX IF EXISTS analytics_b_tenant__8e06d0_idx;",
                # Restore what 0012 (and the current model) actually define.
                "CREATE INDEX IF NOT EXISTS analytics_b_tenant__4d9d8a_idx "
                "ON analytics_boards (tenant_id, role, board_type);",
                "CREATE UNIQUE INDEX IF NOT EXISTS analytics_board_tenant_role_type_report_uniq_alive "
                "ON analytics_boards (tenant_id, role, board_type, report_id) "
                "WHERE (NOT is_deleted AND deleted_at IS NULL);",
            ],
            reverse_sql=[
                "DROP INDEX IF EXISTS analytics_board_tenant_role_type_report_uniq_alive;",
                "DROP INDEX IF EXISTS analytics_b_tenant__4d9d8a_idx;",
                "ALTER TABLE analytics_boards DROP COLUMN IF EXISTS role;",
            ],
        ),
    ]
