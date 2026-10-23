from django.db import migrations, models


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS in_app_notifications (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id VARCHAR(255) NOT NULL,
    notification_type VARCHAR(64) NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    read_at TIMESTAMPTZ NULL,
    record_id BIGINT NULL,
    tenant_id UUID NULL
);
CREATE INDEX IF NOT EXISTS in_app_notifications_user_id_idx ON in_app_notifications (user_id);
CREATE INDEX IF NOT EXISTS in_app_notifications_notification_type_idx ON in_app_notifications (notification_type);
CREATE INDEX IF NOT EXISTS in_app_notifications_read_at_idx ON in_app_notifications (read_at);
CREATE INDEX IF NOT EXISTS in_app_notifications_record_id_idx ON in_app_notifications (record_id);
CREATE INDEX IF NOT EXISTS in_app_notifications_tenant_id_idx ON in_app_notifications (tenant_id);
"""


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.CreateModel(
                    name="InAppNotification",
                    fields=[
                        ("id", models.BigAutoField(primary_key=True, serialize=False)),
                        ("created_at", models.DateTimeField(auto_now_add=True)),
                        ("updated_at", models.DateTimeField(auto_now=True)),
                        ("user_id", models.CharField(db_index=True, max_length=255)),
                        ("notification_type", models.CharField(db_index=True, max_length=64)),
                        ("title", models.CharField(max_length=255)),
                        ("message", models.TextField()),
                        ("read_at", models.DateTimeField(blank=True, db_index=True, null=True)),
                        ("record_id", models.BigIntegerField(blank=True, db_index=True, null=True)),
                        ("tenant_id", models.UUIDField(blank=True, db_index=True, null=True)),
                    ],
                    options={
                        "db_table": "in_app_notifications",
                        "ordering": ["-created_at"],
                    },
                ),
            ],
            database_operations=[
                migrations.RunSQL(CREATE_TABLE_SQL, reverse_sql=migrations.RunSQL.noop),
            ],
        ),
    ]
