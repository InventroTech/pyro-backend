from django.db import migrations


def create_inventory_rulesets(apps, schema_editor):
    RuleSet = apps.get_model("crm_records", "RuleSet")
    Tenant = apps.get_model("core", "Tenant")

    for tenant in Tenant.objects.all():
        # Simple example rule: on inventory_request.mark_in_shipping, set data.status = IN_SHIPPING
        RuleSet.objects.get_or_create(
            tenant=tenant,
            event_name="inventory_request.mark_in_shipping",
            defaults={
                "condition": {},
                "actions": [
                    {
                        "type": "update_fields",
                        "field_updates": {
                            "data.status": "IN_SHIPPING",
                        },
                    }
                ],
                "enabled": True,
                "description": "Inventory: mark request as IN_SHIPPING.",
            },
        )


def delete_inventory_rulesets(apps, schema_editor):
    RuleSet = apps.get_model("crm_records", "RuleSet")
    RuleSet.objects.filter(event_name="inventory_request.mark_in_shipping").delete()


class Migration(migrations.Migration):

    dependencies = [
        ("crm_records", "0001_initial"),
        ("core", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(
            create_inventory_rulesets,
            delete_inventory_rulesets,
        ),
    ]

