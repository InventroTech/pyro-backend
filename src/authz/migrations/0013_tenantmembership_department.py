# Generated migration: add department to TenantMembership (default null)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('authz', '0012_alter_userpermission_unique_together_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='tenantmembership',
            name='department',
            field=models.CharField(blank=True, help_text='Optional department', max_length=255, null=True),
        ),
    ]
