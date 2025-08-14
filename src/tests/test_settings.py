"""
Test settings for running tests with SQLite database
"""
from django.test import override_settings

# Test database configuration - use SQLite to avoid PostgreSQL conflicts
TEST_SETTINGS = {
    'DATABASES': {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',  # Use in-memory database for tests
        }
    },
    'SUPABASE_JWT_SECRET': 'test-secret-key-for-testing',
    'DEBUG': True,
    'LOCAL_TEST_MODE': True,
}

# Decorator for test settings
test_settings = override_settings(**TEST_SETTINGS)
