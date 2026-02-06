# Data migration: copy lead_sources from LEAD_SOURCE_ASSIGNMENT rows into
# the lead_sources column of the corresponding LEAD_TYPE_ASSIGNMENT row.

from django.db import migrations


def backfill_lead_sources(apps, schema_editor):
    UserSettings = apps.get_model("user_settings", "UserSettings")
    # For each LEAD_SOURCE_ASSIGNMENT row, find the LEAD_TYPE_ASSIGNMENT row
    # for same tenant+tenant_membership and set its lead_sources from the value.
    source_rows = UserSettings.objects.filter(key="LEAD_SOURCE_ASSIGNMENT")
    for src in source_rows:
        if not isinstance(src.value, list):
            continue
        type_row = UserSettings.objects.filter(
            tenant=src.tenant,
            tenant_membership=src.tenant_membership,
            key="LEAD_TYPE_ASSIGNMENT",
        ).first()
        if type_row is not None:
            type_row.lead_sources = src.value
            type_row.save(update_fields=["lead_sources"])


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("user_settings", "0008_add_lead_sources_to_usersettings"),
    ]

    operations = [
        migrations.RunPython(backfill_lead_sources, noop_reverse),
    ]
