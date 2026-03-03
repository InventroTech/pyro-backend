# Create auth schema and auth.users table when missing (e.g. test DB without Supabase).
# SupabaseAuthUser is an unmanaged mirror of auth.users; this migration ensures the
# table exists so FKs from other apps (e.g. support_ticket.0006) can be applied.

from django.db import migrations

CREATE_AUTH_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS auth;
"""

CREATE_AUTH_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS auth.users (
    id UUID NOT NULL PRIMARY KEY,
    email VARCHAR(254) NULL,
    phone TEXT NULL,
    raw_app_meta_data JSONB NULL,
    raw_user_meta_data JSONB NULL,
    created_at TIMESTAMP WITH TIME ZONE NULL,
    updated_at TIMESTAMP WITH TIME ZONE NULL,
    last_sign_in_at TIMESTAMP WITH TIME ZONE NULL,
    is_super_admin BOOLEAN NULL,
    is_sso_user BOOLEAN NULL,
    is_anonymous BOOLEAN NULL,
    deleted_at TIMESTAMP WITH TIME ZONE NULL
);
"""

REVERSE_DROP_AUTH_USERS = """
DROP TABLE IF EXISTS auth.users;
"""

REVERSE_DROP_AUTH_SCHEMA = """
DROP SCHEMA IF EXISTS auth;
"""


class Migration(migrations.Migration):
    dependencies = [("accounts", "0004_drop_legacy_users_and_roles")]

    operations = [
        migrations.RunSQL(CREATE_AUTH_SCHEMA, reverse_sql=REVERSE_DROP_AUTH_SCHEMA),
        migrations.RunSQL(
            CREATE_AUTH_USERS_TABLE,
            reverse_sql=REVERSE_DROP_AUTH_USERS,
        ),
    ]
