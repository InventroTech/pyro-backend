from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("user_settings", "0015_userkvsetting"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="UserKVSetting",
            new_name="TenantMemberSetting",
        ),
    ]

