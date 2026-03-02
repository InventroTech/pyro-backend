"""
Django settings for running the test suite (single source for test config).

- Uses PostgreSQL (same as production). Set LOCAL_DB_* env vars to point at your
  local Postgres; Django creates a test_<DB_NAME> database when running tests.
- All tests should use this module only (conftest sets DJANGO_SETTINGS_MODULE).
- For per-test overrides (e.g. WEBHOOK_SECRET), use @override_settings in that test.

Load with: pytest (conftest sets this) or DJANGO_SETTINGS_MODULE=config.settings_test
"""
import os

# Set required env vars before base settings load (for CI or when .env is missing)
os.environ.setdefault("DJANGO_SECRET_KEY", "test-secret-key-do-not-use-in-production")
os.environ.setdefault("SUPABASE_JWT_SECRET", "test-jwt-secret")
os.environ.setdefault("PYRO_SECRET", "")

from .settings import *  # noqa: F401, F403

# PostgreSQL only (same as production)
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("LOCAL_DB_NAME", "postgres"),
        "USER": os.environ.get("LOCAL_DB_USER", "postgres"),
        "PASSWORD": os.environ.get("LOCAL_DB_PASSWORD", ""),
        "HOST": os.environ.get("LOCAL_DB_HOST", "localhost"),
        "PORT": os.environ.get("LOCAL_DB_PORT", "5432"),
    }
}

DEBUG = True
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "test-jwt-secret")
LOCAL_TEST_MODE = True  # Single source for test flags; tests use config.settings_test only.
