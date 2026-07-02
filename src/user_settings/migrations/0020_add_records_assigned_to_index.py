from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("user_settings", "0019_backfill_kv_and_drop_legacy_tables"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX IF NOT EXISTS records_assigned_to_idx
            ON records ((data->>'assigned_to'));
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS records_assigned_to_idx;
            """,
        ),
    ]