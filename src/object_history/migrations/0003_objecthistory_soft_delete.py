# ObjectHistory(BaseModel): soft-delete columns, partial unique, managers.

from django.db import migrations, models
from django.db.models import Q

import object_history.models as object_history_models


class Migration(migrations.Migration):

    dependencies = [
        ("object_history", "0002_alter_objecthistory_options_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="objecthistory",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="objecthistory",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.RemoveConstraint(
            model_name="objecthistory",
            name="object_hist_unique_version",
        ),
        migrations.AddConstraint(
            model_name="objecthistory",
            constraint=models.UniqueConstraint(
                fields=("content_type", "object_id", "version"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="object_hist_unique_version",
            ),
        ),
        migrations.AlterModelManagers(
            name="objecthistory",
            managers=[
                ("objects", object_history_models.ObjectHistoryManager()),
                ("all_objects", object_history_models.ObjectHistoryAllObjectsManager()),
            ],
        ),
    ]
