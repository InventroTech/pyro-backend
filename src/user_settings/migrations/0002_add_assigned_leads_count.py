# Generated migration for adding assigned_leads_count field

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('user_settings', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='usersettings',
            name='assigned_leads_count',
            field=models.IntegerField(blank=True, help_text='Number of leads assigned to the user (for LEAD_TYPE_ASSIGNMENT key)', null=True),
        ),
    ]

