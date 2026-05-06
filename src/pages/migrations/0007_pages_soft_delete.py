"""
Page (RoleBaseModel) and CustomIcon (BaseModel): soft-delete columns, BaseModel
indexes, partial unique on custom_icons, soft-delete managers.
"""

from django.db import migrations, models
from django.db.models import Q

import core.soft_delete


class Migration(migrations.Migration):

    dependencies = [
        ("pages", "0006_customicon"),
    ]

    operations = [
        migrations.AddField(
            model_name="page",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="page",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddIndex(
            model_name="page",
            index=models.Index(
                fields=["tenant", "-created_at"],
                name="pages_tnt_cr_desc_idx",
            ),
        ),
        migrations.AlterModelManagers(
            name="page",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AddField(
            model_name="customicon",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="customicon",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AlterUniqueTogether(
            name="customicon",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="customicon",
            constraint=models.UniqueConstraint(
                fields=["tenant", "name"],
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="pages_customicon_tenant_name_uniq_alive",
            ),
        ),
        migrations.AddIndex(
            model_name="customicon",
            index=models.Index(
                fields=["tenant", "-created_at"],
                name="custom_icons_tnt_cr_desc_idx",
            ),
        ),
        migrations.AlterModelManagers(
            name="customicon",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
    ]
