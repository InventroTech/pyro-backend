"""
WhatsAppTemplate extends BaseModel: is_deleted, deleted_at, soft-delete managers.
(tenant, -created_at) index already exists from 0001 / 0002.
"""

from django.db import migrations, models

import core.soft_delete


class Migration(migrations.Migration):

    dependencies = [
        ("whatsapp", "0002_rename_whatsapp_te_tenant__idx_whatsapp_te_tenant__f5c841_idx_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsapptemplate",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="whatsapptemplate",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AlterModelManagers(
            name="whatsapptemplate",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
    ]
