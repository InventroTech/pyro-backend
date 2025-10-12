# Generated manually for crm_records app

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Record',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated_at', models.DateTimeField(auto_now=True, db_index=True)),
                ('entity_type', models.CharField(db_index=True, max_length=100)),
                ('name', models.CharField(blank=True, max_length=255, null=True)),
                ('data', models.JSONField(blank=True, default=dict)),
                ('tenant', models.ForeignKey(blank=True, db_column='tenant_id', db_index=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='crm_records_record_set', to='core.tenant')),
            ],
            options={
                'db_table': 'records',
                'indexes': [
                    models.Index(fields=['tenant', 'entity_type', '-created_at'], name='crm_records_record_tenant_entity_created_idx'),
                ],
            },
        ),
    ]
