from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("crm_records", "0011_drop_plaintext_secret_key"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            -- GIN index on JSONB data for generic key lookups
            CREATE INDEX IF NOT EXISTS records_data_gin_idx 
            ON records USING gin (data);
            
            -- Expression indexes on the most commonly queried JSON keys
            CREATE INDEX IF NOT EXISTS records_lead_stage_idx 
            ON records USING btree ((data->>'lead_stage'));

            CREATE INDEX IF NOT EXISTS records_assigned_to_idx 
            ON records USING btree ((data->>'assigned_to'));

            CREATE INDEX IF NOT EXISTS records_affiliated_party_idx 
            ON records USING btree ((data->>'affiliated_party'));

            CREATE INDEX IF NOT EXISTS records_praja_id_idx 
            ON records USING btree ((data->>'praja_id'));

            CREATE INDEX IF NOT EXISTS records_next_call_at_idx 
            ON records USING btree ((data->>'next_call_at'));
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS records_data_gin_idx;
            DROP INDEX IF EXISTS records_lead_stage_idx;
            DROP INDEX IF EXISTS records_assigned_to_idx;
            DROP INDEX IF EXISTS records_affiliated_party_idx;
            DROP INDEX IF EXISTS records_praja_id_idx;
            DROP INDEX IF EXISTS records_next_call_at_idx;
            """,
        ),
    ]


