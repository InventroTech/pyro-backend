from django.contrib.postgres.operations import AddIndexConcurrently
from django.db import migrations, models


class Migration(migrations.Migration):
    # Required for CREATE INDEX CONCURRENTLY (see AddIndexConcurrently).
    atomic = False

    dependencies = [
        ("object_history", "0005_alter_objecthistory_managers_and_more"),
    ]

    operations = [
        AddIndexConcurrently(
            model_name="objecthistory",
            index=models.Index(
                fields=["tenant", "created_at"],
                name="object_hist_retention_idx",
                condition=models.Q(
                    persistent_history=False,
                    is_deleted=False,
                    deleted_at__isnull=True,
                ),
            ),
        ),
    ]
