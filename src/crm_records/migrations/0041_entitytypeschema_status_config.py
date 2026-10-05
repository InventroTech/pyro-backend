from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("crm_records", "0040_backfill_network_density_pull_strategy"),
    ]

    operations = [
        migrations.AddField(
            model_name="entitytypeschema",
            name="status_config",
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text=(
                    "Request status options and pages: {'pages': [{id, label, order}], "
                    "'statuses': [{value, label, color, page, order, active}]}. Merged over built-in defaults."
                ),
            ),
        ),
    ]
