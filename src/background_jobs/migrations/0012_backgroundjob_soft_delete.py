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
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="backgroundjob",
            name="deleted_at",
            field=models.DateTimeField(blank=True, default=None, null=True),
        ),
        migrations.AddIndex(
            model_name="backgroundjob",
            index=models.Index(
                fields=("is_deleted",),
                name="bg_jobs_is_deleted_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="backgroundjob",
            index=models.Index(
                fields=("deleted_at",),
                name="bg_jobs_deleted_at_idx",
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
