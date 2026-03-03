"""
Pytest configuration and shared fixtures for Pyro tests.

- Ensures test settings are used (config.settings_test -> PostgreSQL).
- Provides common fixtures: client, api_client, rf,
  plus authenticated variants (tenant, user, auth_headers, auth_client).
"""
import os
import uuid

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings_test")

import django
import jwt
import pytest


def pytest_configure(config):
    django.setup()


# ---------------------------------------------------------------------------
# Low-level DRF fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def api_client():
    """Bare DRF APIClient (no auth)."""
    from rest_framework.test import APIClient
    return APIClient()


@pytest.fixture
def client(api_client):
    """Alias for api_client."""
    return api_client


@pytest.fixture
def rf():
    """DRF APIRequestFactory for unit-testing views directly."""
    from rest_framework.test import APIRequestFactory
    return APIRequestFactory()


# ---------------------------------------------------------------------------
# Tenant / User / Auth fixtures  (require @pytest.mark.django_db)
# ---------------------------------------------------------------------------

@pytest.fixture
def tenant(db):
    from tests.factories import TenantFactory
    return TenantFactory()


@pytest.fixture
def user(db, tenant):
    """Authenticated user linked to the default tenant."""
    from tests.factories import UserFactory, SupabaseAuthUserFactory, RoleFactory, TenantMembershipFactory
    uid = str(uuid.uuid4())
    user = UserFactory(
        supabase_uid=uid,
        email="testuser@example.com",
        tenant_id=str(tenant.id),
    )
    SupabaseAuthUserFactory(id=uuid.UUID(uid), email=user.email)
    role = RoleFactory(tenant=tenant, key="pyro_admin", name="Pyro Admin")
    TenantMembershipFactory(tenant=tenant, user_id=uid, email=user.email, role=role)
    return user


@pytest.fixture
def auth_headers(user, tenant):
    """Dict of HTTP headers for Supabase JWT auth — ready to **kwargs into client calls."""
    from django.conf import settings as dj_settings
    payload = {
        "sub": user.supabase_uid,
        "email": user.email,
        "tenant_id": str(tenant.id),
        "role": "authenticated",
        "aud": "authenticated",
        "user_data": {"tenant_id": str(tenant.id)},
    }
    token = jwt.encode(payload, dj_settings.SUPABASE_JWT_SECRET, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return {
        "HTTP_AUTHORIZATION": f"Bearer {token}",
        "HTTP_X_TENANT_ID": str(tenant.id),
    }


@pytest.fixture
def auth_client(api_client, auth_headers):
    """APIClient that auto-attaches auth headers to every request."""
    api_client.credentials(**{
        k.replace("HTTP_", "").replace("_", "-"): v
        for k, v in auth_headers.items()
    })
    return api_client


# ---------------------------------------------------------------------------
# Multi-tenant pair (for isolation / cross-tenant tests)
# ---------------------------------------------------------------------------

@pytest.fixture
def tenant_b(db):
    from tests.factories import TenantFactory
    return TenantFactory()


@pytest.fixture
def user_b(db, tenant_b):
    from tests.factories import UserFactory, SupabaseAuthUserFactory, RoleFactory, TenantMembershipFactory
    uid = str(uuid.uuid4())
    user = UserFactory(
        supabase_uid=uid,
        email="tenantb@example.com",
        tenant_id=str(tenant_b.id),
    )
    SupabaseAuthUserFactory(id=uuid.UUID(uid), email=user.email)
    role = RoleFactory(tenant=tenant_b, key="pyro_admin", name="Pyro Admin")
    TenantMembershipFactory(tenant=tenant_b, user_id=uid, email=user.email, role=role)
    return user


@pytest.fixture
def auth_headers_b(user_b, tenant_b):
    from django.conf import settings as dj_settings
    payload = {
        "sub": user_b.supabase_uid,
        "email": user_b.email,
        "tenant_id": str(tenant_b.id),
        "role": "authenticated",
        "aud": "authenticated",
        "user_data": {"tenant_id": str(tenant_b.id)},
    }
    token = jwt.encode(payload, dj_settings.SUPABASE_JWT_SECRET, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return {
        "HTTP_AUTHORIZATION": f"Bearer {token}",
        "HTTP_X_TENANT_ID": str(tenant_b.id),
    }


# ---------------------------------------------------------------------------
# Membership fixture (tenant + user + role + membership in one shot)
# ---------------------------------------------------------------------------

@pytest.fixture
def membership(db, tenant, user):
    """Creates a TenantMembership linking the default user to the default tenant with an admin role."""
    from tests.factories import RoleFactory, TenantMembershipFactory
    role = RoleFactory(tenant=tenant, key="pyro_admin", name="Pyro Admin")
    return TenantMembershipFactory(
        tenant=tenant,
        user_id=user.supabase_uid,
        email=user.email,
        role=role,
    )
