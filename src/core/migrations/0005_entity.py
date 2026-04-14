# Generated migration to move Entity model from crm_records to core with new table name

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0004_idle_in_transaction_session_timeout'),
    ]

    operations = [
        migrations.CreateModel(
            name='Entity',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(help_text='The type of entity (e.g., lead, ticket)', max_length=255)),
                ('schema', models.JSONField(blank=True, default=dict)),
                ('last_processed_record_id', models.BigIntegerField(default=0)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('tenant', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='entities', to='core.tenant')),
            ],
            options={
                'db_table': 'core_entity',
                'unique_together': {('tenant', 'name')},
            },
        ),
    ]
