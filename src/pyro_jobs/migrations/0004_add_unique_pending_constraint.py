from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pyro_jobs', '0003_add_attempts_fields'),
    ]

    operations = [
        # Constraint was added and then reverted — keeping this migration
        # as a no-op to preserve the migration history without a squash.
    ]
