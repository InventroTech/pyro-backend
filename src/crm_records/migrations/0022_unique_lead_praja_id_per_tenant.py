# Generated manually to prevent duplicate leads with same praja_id per tenant.
# Enforces uniqueness at DB level so concurrent requests cannot both insert.
# Duplicate cleanup (merge/delete) is done manually; this migration only adds the index.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("crm_records", "0021_rename_partner_ev_tenant_partner_created_idx_partner_eve_tenant__0d3ff2_idx_and_more"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE UNIQUE INDEX IF NOT EXISTS records_lead_praja_id_tenant_unique
            ON records (tenant_id, (data->>'praja_id'))
            WHERE entity_type = 'lead'
              AND data->>'praja_id' IS NOT NULL
              AND trim(data->>'praja_id') != '';
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS records_lead_praja_id_tenant_unique;
            """,
        ),
    ]
