# AnalyticsRunCore extends BaseModel (PR #614): tenant, updated_at, soft-delete, managers.
# is_deleted / deleted_at indexes: db_index on SoftDeleteMixin fields.

import django.utils.timezone
import django.db.models.deletion
from django.db import migrations, models

import core.soft_delete


class Migration(migrations.Migration):

    dependencies = [
        ("analytics", "0007_delete_supportticket"),
        ("core", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="analyticsruncore",
            name="tenant",
            field=models.ForeignKey(
                blank=True,
                null=True,
                to="core.tenant",
                on_delete=django.db.models.deletion.SET_NULL,
                db_index=True,
                db_column="tenant_id",
            ),
        ),
        migrations.AddField(
            model_name="analyticsruncore",
            name="updated_at",
            field=models.DateTimeField(
                auto_now=True,
                default=django.utils.timezone.now,
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="analyticsruncore",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="analyticsruncore",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddIndex(
            model_name="analyticsruncore",
            index=models.Index(
                fields=["tenant", "-created_at"],
                name="analytics_a_tnt_cr_idx",
            ),
        ),
        migrations.AlterModelManagers(
            name="analyticsruncore",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
    ]
