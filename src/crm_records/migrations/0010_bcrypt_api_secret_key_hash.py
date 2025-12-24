# Generated manually: switch secret_key_hash to bcrypt-compatible (non-unique, longer)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0009_hash_api_secret_key'),
    ]

    operations = [
        migrations.AlterField(
            model_name='apisecretkey',
            name='secret_key_hash',
            field=models.CharField(
                db_index=True,
                help_text='bcrypt hash of the raw secret (pgcrypto crypt). Do NOT store plaintext secrets.',
                max_length=128,
                null=True,
            ),
        ),
    ]


