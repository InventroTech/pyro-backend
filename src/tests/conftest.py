import os
import pytest
import django
from django.conf import settings
from django.db import connection
from .test_settings import TEST_SETTINGS


def pytest_configure():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    # Apply test overrides before Django setup completes
    for key, value in TEST_SETTINGS.items():
        setattr(settings, key, value)
    # Initialize Django
    django.setup()

    # Ensure a tenants table exists for unmanaged core.Tenant in SQLite tests
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tenants (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT,
                slug TEXT UNIQUE NOT NULL
            )
            """
        )
