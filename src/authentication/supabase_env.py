"""
Resolve Supabase URL and API keys from environment variables.

When DJANGO_ENV=development, STAGING_* variables are read before SUPABASE_* so a local
.env can keep incorrect legacy SUPABASE_SERVICE_ROLE_KEY while using correct
STAGING_SERVICE_ROLE_KEY / STAGING_SUPABASE_* for the active project.
"""

from __future__ import annotations

import os

_URL_PRIMARY = ("SUPABASE_PROJECT_URL", "SUPABASE_URL")
_URL_STAGING = ("STAGING_SUPABASE_URL",)

_SERVICE_ROLE_PRIMARY = ("SUPABASE_SERVICE_ROLE_KEY",)
_SERVICE_ROLE_STAGING = ("STAGING_SERVICE_ROLE_KEY",)

_ANON_PRIMARY = ("SUPABASE_ANON_KEY",)
_ANON_STAGING = ("STAGING_SUPABASE_KEY",)


def first_nonempty(*names: str) -> str:
    for name in names:
        raw = os.environ.get(name)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return ""


def _dev_prefers_staging_creds() -> bool:
    return os.environ.get("DJANGO_ENV", "").lower() == "development"


def credential_env(primary: tuple[str, ...], staging: tuple[str, ...]) -> str:
    if _dev_prefers_staging_creds():
        return first_nonempty(*staging, *primary)
    return first_nonempty(*primary, *staging)


def supabase_api_base_url() -> str:
    return credential_env(_URL_PRIMARY, _URL_STAGING)


def supabase_service_role_key() -> str:
    return credential_env(_SERVICE_ROLE_PRIMARY, _SERVICE_ROLE_STAGING)


def supabase_anon_key() -> str:
    return credential_env(_ANON_PRIMARY, _ANON_STAGING)
