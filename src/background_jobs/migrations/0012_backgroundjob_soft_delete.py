from django.db import migrations, models

import core.soft_delete


class Migration(migrations.Migration):

    dependencies = [
        ("background_jobs", "0011_add_snoozed_to_not_connected_midnight_job_type"),
    ]

    operations = [
        migrations.AddField(
            model_name="backgroundjob",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="backgroundjob",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AlterModelManagers(
            name="backgroundjob",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
    ]
