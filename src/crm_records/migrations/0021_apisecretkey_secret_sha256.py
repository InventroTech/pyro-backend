# Add SHA-256 lookup for ApiSecretKey (fast path; replaces bcrypt in hot path)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0020_partner_event'),
    ]

    operations = [
        migrations.AddField(
            model_name='apisecretkey',
            name='secret_sha256',
            field=models.CharField(
                blank=True,
                db_index=True,
                help_text='SHA-256 hex of the raw secret. Used for fast lookup; set by set_raw_secret().',
                max_length=64,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name='apisecretkey',
            name='secret_key_hash',
            field=models.CharField(
                blank=True,
                db_index=True,
                help_text='[Legacy] bcrypt hash of the raw secret (pgcrypto). Prefer secret_sha256 for lookup.',
                max_length=128,
                null=True,
            ),
        ),
        migrations.AddIndex(
            model_name='apisecretkey',
            index=models.Index(fields=['secret_sha256', 'is_active'], name='api_secret_sha256_act_idx'),
        ),
    ]
