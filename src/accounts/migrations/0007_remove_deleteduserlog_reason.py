from django.db import migrations


class Migration(migrations.Migration):
    """Drop the reason column from the deleted-users log; no other table changes."""

    dependencies = [
        ("accounts", "0006_deleteduserlog"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="deleteduserlog",
            name="reason",
        ),
    ]
