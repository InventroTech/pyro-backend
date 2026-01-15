# Generated migration to add pyro_data field to Record model

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0012_add_record_json_indexes'),
    ]

    operations = [
        migrations.AddField(
            model_name='record',
            name='pyro_data',
            field=models.JSONField(blank=True, default=dict, null=True, help_text='Additional JSON data for Pyro-specific fields'),
        ),
        migrations.RunSQL(
            sql="""
            -- GIN index on JSONB pyro_data for generic key lookups
            CREATE INDEX IF NOT EXISTS records_pyro_data_gin_idx 
            ON records USING gin (pyro_data);
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS records_pyro_data_gin_idx;
            """,
        ),
    ]

