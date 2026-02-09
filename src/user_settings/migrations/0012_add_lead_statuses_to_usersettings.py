# Generated migration for adding lead_statuses field to UserSettings

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('user_settings', '0011_rename_routing_rul_tenant__tm_idx_routing_rul_tenant__7b2f52_idx_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='usersettings',
            name='lead_statuses',
            field=models.JSONField(blank=True, help_text='List of lead statuses assigned to this user (for key=LEAD_TYPE_ASSIGNMENT); only these leads are directed to the RM', null=True),
        ),
    ]
