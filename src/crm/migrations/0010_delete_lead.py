from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("crm", "0009_lead_resolved_at"),
    ]

    operations = [
        migrations.DeleteModel(
            name="Lead",
        ),
    ]
