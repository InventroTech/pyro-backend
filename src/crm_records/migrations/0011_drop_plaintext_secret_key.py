# Generated manually: drop plaintext secret_key column (store only bcrypt hash)

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0010_bcrypt_api_secret_key_hash'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='apisecretkey',
            name='secret_key',
        ),
    ]


