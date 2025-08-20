from django.db import migrations, models
from django.utils import timezone

class Migration(migrations.Migration):

    dependencies = [
        ("scheduler", "0001_initial"),
    ]

    operations = [
        # Step 1: add with a default so existing rows get populated; then drop the default
        migrations.AddField(
            model_name="taskpolicy",
            name="created_at",
            field=models.DateTimeField(default=timezone.now, db_index=True),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="taskpolicy",
            name="updated_at",
            field=models.DateTimeField(default=timezone.now, db_index=True),
            preserve_default=False,
        ),

        # Step 2: switch to auto_* semantics for ongoing writes
        migrations.AlterField(
            model_name="taskpolicy",
            name="created_at",
            field=models.DateTimeField(auto_now_add=True, db_index=True),
        ),
        migrations.AlterField(
            model_name="taskpolicy",
            name="updated_at",
            field=models.DateTimeField(auto_now=True, db_index=True),
        ),
    ]
