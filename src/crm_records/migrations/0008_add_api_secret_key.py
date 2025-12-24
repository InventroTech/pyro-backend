# Generated manually for ApiSecretKey model

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0001_initial'),
        ('crm_records', '0007_remove_record_name_field'),
    ]

    operations = [
        migrations.CreateModel(
            name='ApiSecretKey',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated_at', models.DateTimeField(auto_now=True, db_index=True)),
                ('secret_key', models.CharField(db_index=True, help_text='The secret key used in X-Secret-Praja header', max_length=255, unique=True)),
                ('description', models.TextField(blank=True, help_text='Optional description for this secret key (e.g., client name, purpose)', null=True)),
                ('is_active', models.BooleanField(db_index=True, default=True, help_text='Whether this secret key is currently active')),
                ('last_used_at', models.DateTimeField(blank=True, help_text='Timestamp of last successful API request using this secret key', null=True)),
                ('tenant', models.ForeignKey(help_text='The tenant this secret key maps to', on_delete=django.db.models.deletion.CASCADE, related_name='api_secret_keys', to='core.tenant')),
            ],
            options={
                'db_table': 'api_secret_keys',
                'verbose_name': 'API Secret Key',
                'verbose_name_plural': 'API Secret Keys',
                'indexes': [
                    models.Index(fields=['secret_key', 'is_active'], name='api_secret_sec_act_idx'),
                    models.Index(fields=['tenant', 'is_active'], name='api_secret_ten_act_idx'),
                ],
            },
        ),
    ]

