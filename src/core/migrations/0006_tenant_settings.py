import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0005_systemsettings_recordaggregator"),
    ]

    operations = [
        migrations.CreateModel(
            name="TenantSettings",
            fields=[
                (
                    "tenant",
                    models.OneToOneField(
                        db_column="tenant_id",
                        on_delete=django.db.models.deletion.CASCADE,
                        primary_key=True,
                        related_name="app_settings",
                        serialize=False,
                        to="core.tenant",
                    ),
                ),
                (
                    "persistent_object_history",
                    models.BooleanField(
                        db_index=True,
                        default=False,
                        help_text="If True, object history for this tenant is not purged by retention.",
                    ),
                ),
            ],
            options={
                "db_table": "core_tenant_settings",
            },
        ),
    ]
