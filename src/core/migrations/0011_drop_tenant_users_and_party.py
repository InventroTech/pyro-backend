# Drop orphan public.tenant_users and public.party tables (no Django models).

from django.db import migrations


def drop_tenant_users_and_party(apps, schema_editor):
    with schema_editor.connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS public.tenant_users CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS public.party CASCADE;")


def reverse_drop_tenant_users_and_party(apps, schema_editor):
    # Tables were unmanaged leftovers; we do not recreate them.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0010_drop_cards_and_card_sets"),
    ]

    operations = [
        migrations.RunPython(drop_tenant_users_and_party, reverse_drop_tenant_users_and_party),
    ]
