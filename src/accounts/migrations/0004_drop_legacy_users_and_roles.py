# Drop public.users and public.roles tables and remove LegacyUser/LegacyRole from Django state.
# Run after data has been migrated to TenantMembership and authz Role.

from django.db import migrations


def drop_legacy_tables(apps, schema_editor):
    with schema_editor.connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS public.roles CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS public.users CASCADE;")


def reverse_drop_legacy_tables(apps, schema_editor):
    # Tables were unmanaged; we do not recreate them. Reverse is no-op.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("accounts", "0003_supabaseauthuser"),
    ]

    operations = [
        migrations.RunPython(drop_legacy_tables, reverse_drop_legacy_tables),
        migrations.DeleteModel(name="LegacyRole"),
        migrations.DeleteModel(name="LegacyUser"),
    ]
