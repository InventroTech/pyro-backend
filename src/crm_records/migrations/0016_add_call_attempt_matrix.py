# Generated manually for crm_records app

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0015_rename_api_secret_hash_act_idx_api_secret__secret__cdef2f_idx_and_more'),
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='CallAttemptMatrix',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated_at', models.DateTimeField(auto_now=True, db_index=True)),
                ('lead_type', models.CharField(db_index=True, help_text="Lead type (e.g., 'BJP', 'AAP', 'Congress', 'TDP', 'TMC', 'CITU', 'CPIM')", max_length=100)),
                ('max_call_attempts', models.PositiveSmallIntegerField(default=5, help_text='Maximum number of call attempts (m)')),
                ('sla_days', models.PositiveSmallIntegerField(default=2, help_text='SLA in days (n)')),
                ('min_time_between_calls_hours', models.PositiveSmallIntegerField(default=3, help_text='Minimum time difference between calls in hours (K)')),
                ('tenant', models.ForeignKey(blank=True, db_column='tenant_id', db_index=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='crm_records_callattemptmatrix_set', to='core.tenant')),
            ],
            options={
                'db_table': 'call_attempt_matrix',
                'verbose_name': 'Call Attempt Matrix',
                'verbose_name_plural': 'Call Attempt Matrices',
                'indexes': [
                    models.Index(fields=['tenant', 'lead_type'], name='crm_records_callattemptmatrix_tenant_lead_type_idx'),
                ],
                'unique_together': {('tenant', 'lead_type')},
            },
        ),
    ]
