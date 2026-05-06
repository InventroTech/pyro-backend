"""
SupportTicket and SupportTicketDump use SoftDeleteMixin: add columns and managers.
"""

from django.db import migrations, models

import core.soft_delete


class Migration(migrations.Migration):

    dependencies = [
        ("support_ticket", "0010_delete_pyrosupport"),
    ]

    operations = [
        migrations.AddField(
            model_name="supportticketdump",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="supportticketdump",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AlterModelManagers(
            name="supportticketdump",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AddField(
            model_name="supportticket",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="supportticket",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AlterModelManagers(
            name="supportticket",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
    ]
