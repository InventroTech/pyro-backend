# Create public.tenants so FKs from other apps (e.g. support_ticket) can be applied.
# Tenant model is unmanaged (managed=False), so 0001_initial does not create the table.
# This migration ensures the table exists for fresh DBs (e.g. test) and is a no-op when it already exists.

from django.db import migrations

CREATE_TENANTS_IF_MISSING = """
CREATE TABLE IF NOT EXISTS public.tenants (
    id UUID NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    slug VARCHAR(64) NOT NULL UNIQUE
);
"""

DROP_TENANTS = """
DROP TABLE IF EXISTS public.tenants;
"""


class Migration(migrations.Migration):
    dependencies = [("core", "0001_initial")]

    operations = [
        migrations.RunSQL(CREATE_TENANTS_IF_MISSING, reverse_sql=DROP_TENANTS),
    ]
