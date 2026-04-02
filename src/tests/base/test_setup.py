import uuid

import jwt
from django.conf import settings
from django.test import TestCase
from rest_framework.test import APIClient

from tests.factories import (
    TenantFactory,
    UserFactory,
    SupabaseAuthUserFactory,
    RoleFactory,
    TenantMembershipFactory,
)
from tests.rest.crm_records.bucket_seed import seed_default_lead_buckets
from .assertions import (
    DRFResponseAssertionsMixin,
    StatusCodeAssertionsMixin,
    PaginationAssertionsMixin,
    TenantIsolationAssertionsMixin,
    PerformanceAssertionsMixin,
)


def _make_jwt(supabase_uid, email, tenant_id, role="authenticated"):
    payload = {
        "sub": supabase_uid,
        "email": email,
        "tenant_id": str(tenant_id),
        "role": role,
        "aud": "authenticated",
        "user_data": {"tenant_id": str(tenant_id)},
    }
    token = jwt.encode(payload, settings.SUPABASE_JWT_SECRET, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


class BaseAPITestCase(
    DRFResponseAssertionsMixin,
    StatusCodeAssertionsMixin,
    PaginationAssertionsMixin,
    TenantIsolationAssertionsMixin,
    PerformanceAssertionsMixin,
    TestCase,
):
    """
    Batteries-included base for API tests.

    Provides:
    - self.tenant          (Tenant instance via TenantFactory)
    - self.tenant_id       (str — for backward compat)
    - self.user            (User instance via UserFactory, linked to tenant)
    - self.token           (Supabase-style JWT)
    - self.auth_headers    (dict ready to unpack into client calls)
    - self.client          (DRF APIClient)
    - helper methods to create additional users / tenants with auth
    """

    def setUp(self):
        super().setUp()
        self.client = APIClient()

        self.tenant = TenantFactory()
        self.tenant_id = str(self.tenant.id)

        self.supabase_uid = str(uuid.uuid4())
        self.email = "testuser@example.com"
        self.role = "authenticated"

        self.user = UserFactory(
            supabase_uid=self.supabase_uid,
            email=self.email,
            tenant_id=self.tenant_id,
            role=self.role,
        )

        self.supabase_auth_user = SupabaseAuthUserFactory(
            id=uuid.UUID(self.supabase_uid),
            email=self.email,
        )

        self._default_role = RoleFactory(
            tenant=self.tenant, key="pyro_admin", name="Pyro Admin",
        )
        self.membership = TenantMembershipFactory(
            tenant=self.tenant,
            user_id=self.supabase_uid,
            email=self.email,
            role=self._default_role,
        )

        self.token = _make_jwt(self.supabase_uid, self.email, self.tenant_id, self.role)
        self.auth_headers = {
            "HTTP_AUTHORIZATION": f"Bearer {self.token}",
            "HTTP_X_TENANT_ID": self.tenant_id,
        }

        seed_default_lead_buckets(self.tenant)

    # ------------------------------------------------------------------
    # Helpers for creating secondary users / auth contexts
    # ------------------------------------------------------------------

    def make_authenticated_user(self, email=None, tenant=None, role="authenticated"):
        """Create a new user + JWT pair. Returns (user, auth_headers)."""
        tenant = tenant or self.tenant
        uid = str(uuid.uuid4())
        email = email or f"{uid[:8]}@example.com"
        user = UserFactory(
            supabase_uid=uid,
            email=email,
            tenant_id=str(tenant.id),
            role=role,
        )
        SupabaseAuthUserFactory(id=uuid.UUID(uid), email=email)
        _role = RoleFactory(tenant=tenant, key="pyro_admin", name="Pyro Admin")
        TenantMembershipFactory(
            tenant=tenant, user_id=uid, email=email, role=_role,
        )
        token = _make_jwt(uid, email, tenant.id, role)
        headers = {
            "HTTP_AUTHORIZATION": f"Bearer {token}",
            "HTTP_X_TENANT_ID": str(tenant.id),
        }
        return user, headers

    def make_unauthenticated_headers(self):
        """Return headers with no Authorization — useful for 401 tests."""
        return {"HTTP_X_TENANT_ID": self.tenant_id}


class MultiTenantAPITestCase(BaseAPITestCase):
    """
    Extended base that pre-creates **two** tenants with separate users.

    Provides everything from BaseAPITestCase (tenant A) plus:
    - self.tenant_b, self.user_b, self.auth_headers_b
    """

    def setUp(self):
        super().setUp()

        self.tenant_b = TenantFactory()
        self.user_b, self.auth_headers_b = self.make_authenticated_user(
            email="tenantb@example.com",
            tenant=self.tenant_b,
        )
        seed_default_lead_buckets(self.tenant_b)
