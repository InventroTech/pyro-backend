# Replace secret_sha256 and secret_key_hash with single plaintext secret column

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0021_apisecretkey_secret_sha256'),
    ]

    operations = [
        migrations.AddField(
            model_name='apisecretkey',
            name='secret',
            field=models.CharField(
                blank=True,
                db_index=True,
                help_text='API secret value (plain). Used for X-Secret-Pyro; set by set_raw_secret().',
                max_length=255,
                null=True,
            ),
        ),
        migrations.RemoveIndex(
            model_name='apisecretkey',
            name='api_secret_sha256_act_idx',
        ),
        migrations.RemoveField(
            model_name='apisecretkey',
            name='secret_sha256',
        ),
        migrations.RemoveIndex(
            model_name='apisecretkey',
            name='api_secret__secret__cdef2f_idx',
        ),
        migrations.RemoveField(
            model_name='apisecretkey',
            name='secret_key_hash',
        ),
        migrations.AddIndex(
            model_name='apisecretkey',
            index=models.Index(fields=['secret', 'is_active'], name='api_secret_sec_act_idx'),
        ),
    ]
