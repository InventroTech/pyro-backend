# Drop legacy user_settings and routing_rules tables (Group/KV is the source of truth).

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("user_settings", "0018_alter_group_options_alter_routingrule_options_and_more"),
    ]

    operations = [
        migrations.DeleteModel(name="RoutingRule"),
        migrations.DeleteModel(name="UserSettings"),
    ]
