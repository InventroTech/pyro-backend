from django.db import migrations, models


def backfill_persistent_object_history(apps, schema_editor):
    ObjectHistory = apps.get_model("object_history", "ObjectHistory")
    TenantSettings = apps.get_model("core", "TenantSettings")
    tenant_ids = list(
        TenantSettings.objects.filter(persistent_object_history=True).values_list(
            "tenant_id", flat=True
        )
    )
    if tenant_ids:
        ObjectHistory.objects.filter(tenant_id__in=tenant_ids).update(
            persistent_history=True
        )


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0006_tenant_settings"),
        ("object_history", "0002_alter_objecthistory_options_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="objecthistory",
            name="persistent_history",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddIndex(
            model_name="objecthistory",
            index=models.Index(
                fields=["persistent_history", "created_at"],
                name="object_hist_persist_cr_idx",
            ),
        ),
        migrations.RunPython(
            backfill_persistent_object_history, noop_reverse
        ),
    ]
