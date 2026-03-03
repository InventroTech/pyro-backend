# Enable pg_trgm before any migration (e.g. analytics.0005) that uses gin_trgm_ops.
# Safe for test and production: IF NOT EXISTS and no-op when already enabled.

from django.db import migrations

CREATE_PG_TRGM = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
"""

DROP_PG_TRGM = """
DROP EXTENSION IF EXISTS pg_trgm;
"""


class Migration(migrations.Migration):
    dependencies = [("core", "0002_create_tenants_table_if_missing")]

    operations = [
        migrations.RunSQL(CREATE_PG_TRGM, reverse_sql=DROP_PG_TRGM),
    ]
