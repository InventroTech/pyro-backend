import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    """New table for the deleted-users log; existing tables are not changed."""

    dependencies = [
        ("accounts", "0005_create_auth_schema_and_users_if_missing"),
        ("core", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="DeletedUserLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("membership_id", models.IntegerField(blank=True, db_index=True, null=True)),
                ("name", models.CharField(blank=True, default="", max_length=255)),
                ("email", models.EmailField(max_length=254)),
                ("role_id", models.UUIDField(blank=True, null=True)),
                ("role_name", models.CharField(blank=True, default="", max_length=255)),
                ("manager_name", models.CharField(blank=True, default="", max_length=255)),
                ("manager_email", models.EmailField(blank=True, default="", max_length=254)),
                ("account_created_at", models.DateTimeField(blank=True, null=True)),
                ("deleted_at", models.DateTimeField(db_index=True)),
                ("deleted_by", models.CharField(blank=True, default="", max_length=255)),
                ("reason", models.TextField(blank=True, default="")),
                (
                    "tenant",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="deleted_user_logs",
                        to="core.tenant",
                    ),
                ),
            ],
            options={
                "db_table": "accounts_deleted_user_log",
                "ordering": ("-deleted_at", "-id"),
                "indexes": [models.Index(fields=["tenant", "-deleted_at"], name="acc_deluserlog_tenant_idx")],
            },
        ),
    ]
