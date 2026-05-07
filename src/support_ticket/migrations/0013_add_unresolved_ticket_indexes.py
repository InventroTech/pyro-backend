from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("support_ticket", "0012_alter_supportticket_managers_and_more"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS support_ticket_unresolved_state_idx
            ON public.support_ticket (state)
            WHERE assigned_to IS NULL AND resolution_status IS NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.support_ticket_unresolved_state_idx;
            """,
        ),
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS support_ticket_unresolved_poster_idx
            ON public.support_ticket (poster)
            WHERE assigned_to IS NULL AND resolution_status IS NULL AND poster IS NOT NULL;
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS public.support_ticket_unresolved_poster_idx;
            """,
        ),
    ]
