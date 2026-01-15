# Generated migration to allow NULL values in pyro_data column

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0013_add_pyro_data_field'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            -- Alter pyro_data column to allow NULL values
            ALTER TABLE records ALTER COLUMN pyro_data DROP NOT NULL;
            """,
            reverse_sql="""
            -- Revert: Set NOT NULL constraint (use default empty dict for existing NULLs)
            UPDATE records SET pyro_data = '{}'::jsonb WHERE pyro_data IS NULL;
            ALTER TABLE records ALTER COLUMN pyro_data SET NOT NULL;
            """,
        ),
    ]

