from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("user_settings", "0012_add_lead_statuses_to_usersettings"),
    ]

    operations = [
        migrations.CreateModel(
            name="Group",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(help_text="Human-readable group name", max_length=255)),
                ("group_data", models.JSONField(blank=True, default=dict, help_text="Arbitrary group payload (party, lead sources, statuses, limits, etc.)")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("tenant", models.ForeignKey(db_column="tenant_id", help_text="The tenant this group belongs to", on_delete=django.db.models.deletion.CASCADE, to="core.tenant")),
            ],
            options={
                "db_table": "groups",
                "unique_together": {("tenant", "name")},
            },
        ),
        migrations.AddIndex(
            model_name="group",
            index=models.Index(fields=["tenant", "name"], name="groups_tenant__95f522_idx"),
        ),
    ]
