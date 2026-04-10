from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("user_settings", "0014_usersettings_group_id"),
    ]

    operations = [
        migrations.CreateModel(
            name="UserKVSetting",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("key", models.CharField(help_text="Setting key (e.g., 'GROUP', 'DAILY_LIMIT')", max_length=100)),
                ("value", models.JSONField(blank=True, help_text="Setting value (JSON)", null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("tenant", models.ForeignKey(db_column="tenant_id", help_text="The tenant this setting belongs to", on_delete=django.db.models.deletion.CASCADE, to="core.tenant")),
                ("tenant_membership", models.ForeignKey(db_column="tenant_membership_id", help_text="The tenant membership this setting belongs to", on_delete=django.db.models.deletion.CASCADE, to="authz.tenantmembership")),
            ],
            options={
                "db_table": "user_kv_settings",
            },
        ),
        migrations.AlterUniqueTogether(
            name="userkvsetting",
            unique_together={("tenant", "tenant_membership", "key")},
        ),
        migrations.AddIndex(
            model_name="userkvsetting",
            index=models.Index(fields=["tenant", "tenant_membership", "key"], name="user_kv_set_tenant__f8f0c6_idx"),
        ),
        migrations.AddIndex(
            model_name="userkvsetting",
            index=models.Index(fields=["tenant", "key"], name="user_kv_set_tenant__533788_idx"),
        ),
    ]

