from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("user_settings", "0013_group"),
    ]

    operations = [
        migrations.AddField(
            model_name="usersettings",
            name="group_id",
            field=models.BigIntegerField(
                blank=True,
                help_text="Assigned Group id for this user's lead assignment",
                null=True,
            ),
        ),
    ]
