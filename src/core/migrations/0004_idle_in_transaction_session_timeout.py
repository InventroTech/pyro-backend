# Database default for new sessions: terminate if idle inside an open transaction
# for longer than 60s. Revert resets this so the cluster default applies again.


from django.db import migrations


def set_idle_in_transaction_session_timeout(apps, schema_editor):
    connection = schema_editor.connection
    db_name = connection.settings_dict["NAME"]
    quoted = connection.ops.quote_name(db_name)
    with connection.cursor() as cursor:
        cursor.execute(
            f"ALTER DATABASE {quoted} SET idle_in_transaction_session_timeout = '60s';"
        )


def reset_idle_in_transaction_session_timeout(apps, schema_editor):
    connection = schema_editor.connection
    db_name = connection.settings_dict["NAME"]
    quoted = connection.ops.quote_name(db_name)
    with connection.cursor() as cursor:
        cursor.execute(
            f"ALTER DATABASE {quoted} RESET idle_in_transaction_session_timeout;"
        )


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0003_enable_pg_trgm"),
    ]

    operations = [
        migrations.RunPython(
            set_idle_in_transaction_session_timeout,
            reset_idle_in_transaction_session_timeout,
        ),
    ]
