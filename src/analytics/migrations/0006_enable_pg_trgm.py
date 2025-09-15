# Manual Generation on 2025-09-12 19:05PM

import django.contrib.postgres.indexes
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('analytics', '0005_alter_supportticket_options_and_more'),
    ]

    operations = [
        migrations.RunSQL(
            "create extension if not exists pg_trgm with schema extensions;",
            reverse_sql="drop extension if exists pg_trgm;",
        ),
        migrations.RunSQL(
            "alter database postgres set search_path = public, extensions;",
            reverse_sql="-- no-op",
        ),
    ]
