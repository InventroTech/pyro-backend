# Generated manually for Zoho inbox initial backfill state.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("email_protocol", "0002_zoho_connection_history"),
    ]

    operations = [
        migrations.AddField(
            model_name="zohomailconnection",
            name="backfill_next_start",
            field=models.PositiveIntegerField(default=1),
        ),
        migrations.AddField(
            model_name="zohomailconnection",
            name="initial_backfill_completed",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.RunPython(
            code=lambda apps, schema_editor: apps.get_model(
                "email_protocol", "ZohoMailConnection"
            ).objects.filter(last_synced_at__isnull=False).update(
                initial_backfill_completed=True
            ),
            reverse_code=migrations.RunPython.noop,
        ),
    ]
