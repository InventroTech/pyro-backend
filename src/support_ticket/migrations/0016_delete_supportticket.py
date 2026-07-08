from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("support_ticket", "0015_supportticketdump_drop_extra_columns"),
    ]

    operations = [
        migrations.DeleteModel(
            name="SupportTicket",
        ),
    ]
