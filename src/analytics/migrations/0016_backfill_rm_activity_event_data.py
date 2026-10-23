from django.db import migrations


def backfill_event_data(apps, schema_editor):
    RmActivityEvent = apps.get_model("analytics", "RmActivityEvent")
    batch = []
    for event in RmActivityEvent.objects.all().iterator():
        event.event_data = {
            "rm_user_id": str(event.rm_user_id),
            "rm_name": event.rm_name,
            "manager_name": event.manager_name,
            "team": event.team,
            "state": event.state,
            "lead_record_id": event.lead_record_id,
            "disposition": event.disposition,
            "lead_bucket": event.lead_bucket,
            "party": event.party,
            "started_at": event.started_at.isoformat() if event.started_at else None,
            "ended_at": event.ended_at.isoformat() if event.ended_at else None,
            "duration_seconds": event.duration_seconds,
        }
        batch.append(event)
    if batch:
        RmActivityEvent.objects.bulk_update(batch, ["event_data"], batch_size=500)


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0015_rm_activity_event_data_field'),
    ]

    operations = [
        migrations.RunPython(backfill_event_data, noop_reverse),
    ]
