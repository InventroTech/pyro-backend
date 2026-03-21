# Generated migration: add department to TenantMembership (default null)
#
# Uses ADD COLUMN IF NOT EXISTS so migrate succeeds when the column was already
# created (e.g. Supabase / manual SQL / drifted DB).

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("authz", "0012_alter_userpermission_unique_together_and_more"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AddField(
                    model_name="tenantmembership",
                    name="department",
                    field=models.CharField(
                        blank=True,
                        help_text="Optional department",
                        max_length=255,
                        null=True,
                    ),
                ),
            ],
            database_operations=[
                migrations.RunSQL(
                    sql="""
                    ALTER TABLE authz_tenantmembership
                    ADD COLUMN IF NOT EXISTS department varchar(255) NULL;
                    """,
                    reverse_sql="""
                    ALTER TABLE authz_tenantmembership
                    DROP COLUMN IF EXISTS department;
                    """,
                ),
            ],
        ),
    ]
