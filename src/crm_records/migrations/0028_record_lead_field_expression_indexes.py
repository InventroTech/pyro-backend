from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("crm_records", "0027_userbucketassignment_tenant_wide"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            -- lead_stage: records_lead_stage_idx already exists (0012_add_record_json_indexes)

            CREATE INDEX IF NOT EXISTS records_lead_source_idx
            ON records USING btree ((data->>'lead_source'));

            CREATE INDEX IF NOT EXISTS records_lead_status_idx
            ON records USING btree ((data->>'lead_status'));

            CREATE INDEX IF NOT EXISTS records_lead_score_idx
            ON records USING btree (((data->>'lead_score')::double precision));
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS records_lead_source_idx;
            DROP INDEX IF EXISTS records_lead_status_idx;
            DROP INDEX IF EXISTS records_lead_score_idx;
            """,
        ),
    ]
