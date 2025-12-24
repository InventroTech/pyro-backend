# Generated manually: store only hashed secret keys (no plaintext)

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0001_initial'),
        ('crm_records', '0008_add_api_secret_key'),
    ]

    operations = [
        migrations.AddField(
            model_name='apisecretkey',
            name='secret_key_hash',
            field=models.CharField(
                db_index=True,
                help_text='bcrypt hash of the raw secret (pgcrypto crypt). Do NOT store plaintext secrets.',
                max_length=128,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='apisecretkey',
            name='secret_key_last4',
            field=models.CharField(
                blank=True,
                help_text='Last 4 chars of raw secret for identification (non-sensitive).',
                max_length=4,
                null=True,
            ),
        ),
        migrations.RemoveIndex(
            model_name='apisecretkey',
            name='api_secret_sec_act_idx',
        ),
        migrations.AddIndex(
            model_name='apisecretkey',
            index=models.Index(fields=['secret_key_hash', 'is_active'], name='api_secret_hash_act_idx'),
        ),
        # We keep the old plaintext column (secret_key) from 0008 for now, but it should no longer be used.
        # A follow-up migration can drop it after data has been migrated.
    ]


