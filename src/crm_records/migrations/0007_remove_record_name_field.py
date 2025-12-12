# Generated migration to remove name field from Record model
# Name field is now stored inside the data JSONB column

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0006_add_rules_to_entity_type_schema'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='record',
            name='name',
        ),
    ]
