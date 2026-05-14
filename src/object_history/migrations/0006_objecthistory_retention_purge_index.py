from django.db import migrations


class Migration(migrations.Migration):
    """
    Partial btree for ``purge_old_log_rows`` on ``ObjectHistory``:

    ``persistent_history = false``, ``created_at < cutoff``,
    ``ORDER BY created_at, pk`` — see ``core.log_retention``.

    Plain ``CREATE INDEX`` (not ``CONCURRENTLY``) so ``migrate`` runs inside a transaction.
    """

    dependencies = [
        ("object_history", "0005_alter_objecthistory_managers_and_more"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX IF NOT EXISTS object_hist_ephemeral_created_cutoff_idx
            ON public.object_history (created_at ASC, id ASC)
            WHERE persistent_history = false;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.object_hist_ephemeral_created_cutoff_idx;
            """,
        ),
    ]
