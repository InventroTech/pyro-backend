import django.db.models.deletion
from django.db import migrations, models
from django.db.models import Q

import core.models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0006_tenant_settings"),
    ]

    operations = [
        migrations.CreateModel(
            name="EntityTypeDiscoverySyncState",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("updated_at", models.DateTimeField(auto_now=True, db_index=True)),
                (
                    "job_name",
                    models.CharField(
                        default="entity_type_discovery",
                        max_length=100,
                        unique=True,
                    ),
                ),
                (
                    "last_processed_updated_at",
                    models.DateTimeField(blank=True, db_index=True, null=True),
                ),
                ("last_processed_record_id", models.PositiveBigIntegerField(default=0)),
                ("last_success_at", models.DateTimeField(blank=True, null=True)),
                ("last_error", models.TextField(blank=True, null=True)),
            ],
            options={
                "db_table": "entity_type_discovery_sync_state",
            },
        ),
        migrations.CreateModel(
            name="TenantEntityType",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("is_deleted", models.BooleanField(db_index=True, default=False)),
                (
                    "deleted_at",
                    models.DateTimeField(
                        blank=True,
                        db_index=True,
                        default=None,
                        null=True,
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("updated_at", models.DateTimeField(auto_now=True, db_index=True)),
                (
                    "entity_type",
                    models.CharField(
                        db_index=True,
                        help_text="The discovered record entity type, e.g. 'lead' or 'ticket'.",
                        max_length=100,
                    ),
                ),
                (
                    "schema_json",
                    models.JSONField(
                        blank=True,
                        default=core.models.default_entity_type_schema,
                        help_text="Discovered unique data fields and their inferred JSON types.",
                    ),
                ),
                (
                    "fields_count",
                    models.PositiveIntegerField(
                        default=0,
                        help_text="Number of unique fields currently stored in schema_json.fields.",
                    ),
                ),
                ("first_seen_at", models.DateTimeField(blank=True, null=True)),
                (
                    "last_seen_at",
                    models.DateTimeField(blank=True, db_index=True, null=True),
                ),
                ("last_seen_record_id", models.PositiveBigIntegerField(default=0)),
                (
                    "tenant",
                    models.ForeignKey(
                        blank=True,
                        db_column="tenant_id",
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="%(app_label)s_%(class)s_set",
                        to="core.tenant",
                    ),
                ),
            ],
            options={
                "db_table": "tenant_entity_types",
                "indexes": [
                    models.Index(
                        fields=["tenant", "-created_at"],
                        name="tenant_enti_tenant__88f4bf_idx",
                    ),
                    models.Index(
                        fields=["tenant", "entity_type"],
                        name="tenant_enti_tenant__6d9c64_idx",
                    ),
                    models.Index(
                        fields=["tenant", "-last_seen_at"],
                        name="tenant_enti_tenant__189b55_idx",
                    ),
                ],
            },
        ),
        migrations.AddConstraint(
            model_name="tenantentitytype",
            constraint=models.UniqueConstraint(
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                fields=("tenant", "entity_type"),
                name="tenant_entity_types_tenant_entity_type_uniq_alive",
            ),
        ),
    ]
