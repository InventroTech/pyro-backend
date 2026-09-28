from django.db import migrations


def rename_key_forward(apps, schema_editor):
    RmActivityEvent = apps.get_model("analytics", "RmActivityEvent")
    batch = []
    for event in RmActivityEvent.objects.filter(event_data__has_key="disposition").iterator():
        data = dict(event.event_data)
        data["updated_status"] = data.pop("disposition")
        event.event_data = data
        batch.append(event)
    if batch:
        RmActivityEvent.objects.bulk_update(batch, ["event_data"], batch_size=500)


def rename_key_backward(apps, schema_editor):
    RmActivityEvent = apps.get_model("analytics", "RmActivityEvent")
    batch = []
    for event in RmActivityEvent.objects.filter(event_data__has_key="updated_status").iterator():
        data = dict(event.event_data)
        data["disposition"] = data.pop("updated_status")
        event.event_data = data
        batch.append(event)
    if batch:
        RmActivityEvent.objects.bulk_update(batch, ["event_data"], batch_size=500)


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0017_rm_activity_event_drop_columns'),
    ]

    operations = [
        migrations.RunPython(rename_key_forward, rename_key_backward),
    ]
