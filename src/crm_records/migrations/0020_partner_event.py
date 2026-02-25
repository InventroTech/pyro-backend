# Partner events table for incoming partner webhook audit trail

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0001_initial'),
        ('crm_records', '0019_record_records_data_gin_idx_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='PartnerEvent',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated_at', models.DateTimeField(auto_now=True, db_index=True)),
                ('partner_slug', models.CharField(db_index=True, max_length=64)),
                ('event', models.CharField(db_index=True, max_length=100)),
                ('payload', models.JSONField(blank=True, default=dict, help_text='Full request payload (praja_id, email_id, etc.)')),
                ('status', models.CharField(db_index=True, default='pending', help_text='pending, processing, completed, failed', max_length=20)),
                ('job_id', models.PositiveIntegerField(blank=True, db_index=True, help_text='Background job id that processed this event', null=True)),
                ('processed_at', models.DateTimeField(blank=True, null=True)),
                ('error_message', models.TextField(blank=True, null=True)),
                ('record', models.ForeignKey(blank=True, help_text='Resolved lead record when applicable', null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='partner_events', to='crm_records.record')),
                ('tenant', models.ForeignKey(blank=True, db_column='tenant_id', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='%(app_label)s_%(class)s_set', to='core.tenant')),
            ],
            options={
                'db_table': 'partner_events',
                'ordering': ['-created_at'],
                'indexes': [
                    models.Index(fields=['tenant', 'partner_slug', '-created_at'], name='partner_ev_tenant_partner_created_idx'),
                    models.Index(fields=['tenant', 'status', '-created_at'], name='partner_ev_tenant_status_created_idx'),
                ],
            },
        ),
    ]
