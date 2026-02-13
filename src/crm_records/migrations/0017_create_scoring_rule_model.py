# Generated manually for ScoringRule model

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0001_initial'),
        ('crm_records', '0016_add_call_attempt_matrix'),
    ]

    operations = [
        migrations.CreateModel(
            name='ScoringRule',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated_at', models.DateTimeField(auto_now=True, db_index=True)),
                ('entity_type', models.CharField(db_index=True, default='lead', help_text="The entity type this rule applies to (e.g., 'lead', 'ticket')", max_length=100)),
                ('attribute', models.CharField(help_text="Attribute path in dot notation (e.g., 'data.assigned_to', 'data.affiliated_party')", max_length=255)),
                ('data', models.JSONField(blank=True, default=dict, help_text='Rule configuration data (operator, value, and any other fields). Structure can be anything.')),
                ('weight', models.FloatField(help_text='Score weight/points added when this rule matches')),
                ('order', models.IntegerField(default=0, help_text='Display order for rules (lower numbers appear first)')),
                ('is_active', models.BooleanField(default=True, help_text='Whether this rule is active and should be evaluated')),
                ('description', models.TextField(blank=True, help_text='Optional description of what this rule does', null=True)),
                ('tenant', models.ForeignKey(blank=True, db_column='tenant_id', null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='%(app_label)s_%(class)s_set', to='core.tenant')),
            ],
            options={
                'db_table': 'scoring_rules',
                'indexes': [
                    models.Index(fields=['tenant', 'entity_type', 'is_active'], name='scoring_rules_tenant_entity_active_idx'),
                    models.Index(fields=['tenant', 'entity_type', 'order'], name='scoring_rules_tenant_entity_order_idx'),
                ],
                'ordering': ['order', 'created_at'],
            },
        ),
    ]
