import time
import uuid

import jwt
import pytest
from django.test import RequestFactory, TestCase, override_settings
from rest_framework import exceptions

from authentication.models import User
from authentication.tokens import (
    TokenConfigurationError,
    build_user_data_claims,
    issue_access_token,
    verify_access_token,
)
from authz.models import Role, TenantMembership
from config.supabase_auth import SupabaseJWTAuthentication
from core.models import Tenant
from middleware.tenant import _get_tenant_id_from_jwt

AUTH_SECRET = "test-auth-jwt-secret"
SUPABASE_SECRET = "test-jwt-secret"


def _supabase_token(sub, **extra):
    payload = {
        "sub": str(sub),
        "email": "user@example.com",
        "aud": "authenticated",
        "role": "authenticated",
        "exp": int(time.time()) + 600,
        **extra,
    }
    return jwt.encode(payload, SUPABASE_SECRET, algorithm="HS256")


@override_settings(
    AUTH_JWT_SECRET=AUTH_SECRET,
    SUPABASE_JWT_SECRET=SUPABASE_SECRET,
    AUTH_JWT_ISSUER="pyro-backend",
    AUTH_ACCEPT_SUPABASE_TOKENS=True,
    AUTH_ACCESS_TOKEN_TTL_SECONDS=3600,
)
class AccessTokenTests(TestCase):
    def setUp(self):
        self.tenant = Tenant.objects.create(name="Tenant A", slug="tenant-a")
        self.other_tenant = Tenant.objects.create(name="Tenant B", slug="tenant-b")
        self.role = Role.objects.create(tenant=self.tenant, key="GM", name="General Manager")
        self.other_role = Role.objects.create(tenant=self.other_tenant, key="AGENT", name="Agent")
        self.uid = str(uuid.uuid4())
        self.user = User.objects.create_user(supabase_uid=self.uid, email="User@Example.com")
        self.membership = TenantMembership.objects.create(
            tenant=self.tenant,
            role=self.role,
            email="user@example.com",
            user_id=self.uid,
            is_active=True,
        )

    def test_issued_token_round_trips_with_supabase_claim_layout(self):
        token = issue_access_token(self.user, user_metadata={"full_name": "Test User"})
        claims = verify_access_token(token)

        self.assertEqual(claims["iss"], "pyro-backend")
        self.assertEqual(claims["sub"], self.uid)
        self.assertEqual(claims["email"], "user@example.com")
        self.assertEqual(claims["aud"], "authenticated")
        self.assertEqual(claims["role"], "authenticated")
        self.assertEqual(claims["exp"] - claims["iat"], 3600)
        self.assertEqual(claims["user_metadata"], {"full_name": "Test User"})
        self.assertEqual(
            claims["user_data"],
            {
                "user_id": self.uid,
                "role_id": str(self.role.id),
                "role_key": "GM",
                "tenant_id": str(self.tenant.id),
                "tenant_membership_id": str(self.membership.id),
            },
        )

    def test_user_data_omitted_without_active_membership(self):
        self.membership.is_active = False
        self.membership.save()

        claims = verify_access_token(issue_access_token(self.user))

        self.assertNotIn("user_data", claims)

    def test_user_data_skips_soft_deleted_membership(self):
        self.membership.delete()

        self.assertIsNone(build_user_data_claims(self.uid))

    def test_user_data_picks_oldest_active_membership(self):
        TenantMembership.objects.create(
            tenant=self.other_tenant,
            role=self.other_role,
            email="user@example.com",
            user_id=self.uid,
            is_active=True,
        )

        for _ in range(3):
            self.assertEqual(build_user_data_claims(self.uid)["tenant_id"], str(self.tenant.id))

    def test_supabase_token_still_accepted(self):
        claims = verify_access_token(_supabase_token(self.uid))

        self.assertEqual(claims["sub"], self.uid)

    @override_settings(AUTH_ACCEPT_SUPABASE_TOKENS=False)
    def test_supabase_token_rejected_once_disabled(self):
        with self.assertRaises(jwt.InvalidIssuerError):
            verify_access_token(_supabase_token(self.uid))

    def test_our_issuer_signed_with_supabase_secret_is_rejected(self):
        forged = _supabase_token(self.uid, iss="pyro-backend", iat=int(time.time()))

        with self.assertRaises(jwt.InvalidSignatureError):
            verify_access_token(forged)

    def test_expired_issued_token_rejected(self):
        with override_settings(AUTH_ACCESS_TOKEN_TTL_SECONDS=-10):
            token = issue_access_token(self.user)

        with self.assertRaises(jwt.ExpiredSignatureError):
            verify_access_token(token)

    @override_settings(AUTH_JWT_SECRET="")
    def test_missing_auth_secret_raises_configuration_error(self):
        with self.assertRaises(TokenConfigurationError):
            issue_access_token(self.user)

    def test_drf_authentication_accepts_issued_token(self):
        token = issue_access_token(self.user)
        request = RequestFactory().get("/", HTTP_AUTHORIZATION=f"Bearer {token}")

        user, _ = SupabaseJWTAuthentication().authenticate(request)

        self.assertEqual(user.supabase_uid, self.uid)
        self.assertEqual(request.jwt_claims["user_data"]["tenant_id"], str(self.tenant.id))

    def test_drf_authentication_rejects_tampered_token(self):
        token = issue_access_token(self.user)
        request = RequestFactory().get("/", HTTP_AUTHORIZATION=f"Bearer {token[:-2]}xx")

        with pytest.raises(exceptions.AuthenticationFailed):
            SupabaseJWTAuthentication().authenticate(request)

    def test_tenant_middleware_resolves_tenant_from_issued_token(self):
        token = issue_access_token(self.user)
        request = RequestFactory().get("/", HTTP_AUTHORIZATION=f"Bearer {token}")

        self.assertEqual(_get_tenant_id_from_jwt(request), str(self.tenant.id))
