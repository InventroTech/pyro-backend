from django.db import migrations, models


_LEGACY_DUMP_COLUMNS = (
    "ticket_date",
    "user_id",
    "name",
    "phone",
    "source",
    "subscription_status",
    "atleast_paid_once",
    "reason",
    "badge",
    "poster",
    "layout_status",
    "state",
    "praja_dashboard_user_link",
    "display_pic_url",
)


def _serialize_legacy_value(field: str, value):
    if value is None:
        return None
    if field == "ticket_date" and hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def forwards_copy_columns_to_data(apps, schema_editor):
    SupportTicketDump = apps.get_model("support_ticket", "SupportTicketDump")
    for row in SupportTicketDump._default_manager.iterator():
        data = dict(row.data or {})
        changed = False
        for field in _LEGACY_DUMP_COLUMNS:
            if field in data:
                continue
            value = getattr(row, field, None)
            if value is None:
                continue
            data[field] = _serialize_legacy_value(field, value)
            changed = True
        if changed or not row.data:
            row.data = data
            row.save(update_fields=["data"])


def backwards_copy_data_to_columns(apps, schema_editor):
    SupportTicketDump = apps.get_model("support_ticket", "SupportTicketDump")
    for row in SupportTicketDump._default_manager.iterator():
        data = row.data or {}
        updates = {}
        for field in _LEGACY_DUMP_COLUMNS:
            if field not in data:
                continue
            value = data[field]
            if field == "ticket_date" and isinstance(value, str):
                from django.utils.dateparse import parse_datetime

                value = parse_datetime(value)
            updates[field] = value
        if updates:
            for key, val in updates.items():
                setattr(row, key, val)
            row.save(update_fields=list(updates.keys()))


class Migration(migrations.Migration):

    dependencies = [
        ("support_ticket", "0013_add_unresolved_ticket_indexes"),
    ]

    operations = [
        migrations.AddField(
            model_name="supportticketdump",
            name="data",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.RunPython(
            forwards_copy_columns_to_data,
            backwards_copy_data_to_columns,
        ),
        migrations.RemoveField(model_name="supportticketdump", name="ticket_date"),
        migrations.RemoveField(model_name="supportticketdump", name="user_id"),
        migrations.RemoveField(model_name="supportticketdump", name="name"),
        migrations.RemoveField(model_name="supportticketdump", name="phone"),
        migrations.RemoveField(model_name="supportticketdump", name="source"),
        migrations.RemoveField(model_name="supportticketdump", name="subscription_status"),
        migrations.RemoveField(model_name="supportticketdump", name="atleast_paid_once"),
        migrations.RemoveField(model_name="supportticketdump", name="reason"),
        migrations.RemoveField(model_name="supportticketdump", name="badge"),
        migrations.RemoveField(model_name="supportticketdump", name="poster"),
        migrations.RemoveField(model_name="supportticketdump", name="layout_status"),
        migrations.RemoveField(model_name="supportticketdump", name="state"),
        migrations.RemoveField(model_name="supportticketdump", name="praja_dashboard_user_link"),
        migrations.RemoveField(model_name="supportticketdump", name="display_pic_url"),
    ]
