# Generated merge migration to resolve conflicting 0017 leaf nodes
# (0017_create_scoring_rule_model and 0017_rename_..._call_attemp_tenant_...)

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0017_create_scoring_rule_model'),
        ('crm_records', '0017_rename_crm_records_callattemptmatrix_tenant_lead_type_idx_call_attemp_tenant__80df9c_idx_and_more'),
    ]

    operations = [
    ]
