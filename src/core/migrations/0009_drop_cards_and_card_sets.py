# Drop orphan public.cards and public.card_sets tables (no Django models).

from django.db import migrations


def drop_cards_tables(apps, schema_editor):
    with schema_editor.connection.cursor() as cursor:
        # card_sets first in case cards references it; CASCADE covers either order.
        cursor.execute("DROP TABLE IF EXISTS public.card_sets CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS public.cards CASCADE;")


def reverse_drop_cards_tables(apps, schema_editor):
    # Tables were unmanaged leftovers; we do not recreate them.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0008_alter_recordaggregator_unique_together_and_more"),
    ]

    operations = [
        migrations.RunPython(drop_cards_tables, reverse_drop_cards_tables),
    ]
