import time
import uuid

import jwt
from django.core.cache import cache
from django.test import TestCase, override_settings
from rest_framework.test import APIClient

from authentication.models import User
from authentication.tokens import issue_access_token, verify_access_token

SUPABASE_SECRET = "supabase-test-secret"


def _supabase_token(sub, **overrides):
    now = int(time.time())
    claims = {
        "sub": sub,
        "aud": "authenticated",
        "role": "authenticated",
        "iat": now,
        "exp": now + 3600,
        "session_id": str(uuid.uuid4()),
        "iss": "https://example.supabase.co/auth/v1",
        **overrides,
    }
    claims = {k: v for k, v in claims.items() if v is not None}
    return jwt.encode(claims, SUPABASE_SECRET, algorithm="HS256")


@override_settings(
    AUTH_JWT_SECRET="test-auth-jwt-secret",
    SUPABASE_JWT_SECRET=SUPABASE_SECRET,
    AUTH_ACCEPT_SUPABASE_TOKENS=True,
)
class SupabaseSessionExchangeTests(TestCase):
    def setUp(self):
        cache.clear()
        self.client = APIClient()
        self.uid = str(uuid.uuid4())
        self.user = User.objects.create_user(supabase_uid=self.uid, email="moved@example.com")

    def _exchange(self, token):
        return self.client.post("/auth/token/from-supabase/", {"access_token": token}, format="json")

    def test_supabase_login_becomes_backend_session(self):
        response = self._exchange(_supabase_token(self.uid))

        self.assertEqual(response.status_code, 200)
        claims = verify_access_token(response.json()["access_token"])
        self.assertEqual(claims["sub"], self.uid)
        self.assertEqual(claims["iss"], "pyro-backend")
        self.assertTrue(response.json()["refresh_token"])

    def test_spoof_style_token_without_session_id_or_exp_rejected(self):
        for token in (_supabase_token(self.uid, session_id=None), _supabase_token(self.uid, exp=None)):
            response = self._exchange(token)
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response.json()["message"], "Not a Supabase login session")

    def test_our_own_token_rejected(self):
        response = self._exchange(issue_access_token(self.user))
        self.assertEqual(response.status_code, 400)

    def test_wrong_signature_rejected(self):
        forged = jwt.encode(
            {"sub": self.uid, "exp": int(time.time()) + 60, "session_id": "x"}, "wrong", algorithm="HS256"
        )
        self.assertEqual(self._exchange(forged).json()["error"], "invalid_grant")

    def test_unknown_or_inactive_user_rejected(self):
        self.assertEqual(self._exchange(_supabase_token(str(uuid.uuid4()))).status_code, 400)
        self.user.is_active = False
        self.user.save()
        self.assertEqual(self._exchange(_supabase_token(self.uid)).status_code, 400)

    def test_disabled_once_supabase_tokens_turned_off(self):
        with override_settings(AUTH_ACCEPT_SUPABASE_TOKENS=False):
            self.assertEqual(self._exchange(_supabase_token(self.uid)).status_code, 400)


@override_settings(AUTH_JWT_SECRET="test-auth-jwt-secret")
class OwnTokenDoesNotRecreateUserTests(TestCase):
    def test_deleted_users_token_is_rejected_and_user_not_recreated(self):
        user = User.objects.create_user(supabase_uid=str(uuid.uuid4()), email="gone@example.com")
        token = issue_access_token(user)
        user.delete()

        response = APIClient().get("/auth/me/", HTTP_AUTHORIZATION=f"Bearer {token}")

        self.assertIn(response.status_code, (401, 403))
        self.assertFalse(User.objects.filter(email="gone@example.com").exists())

    def test_disabled_users_token_is_rejected(self):
        user = User.objects.create_user(supabase_uid=str(uuid.uuid4()), email="off@example.com", is_active=False)
        response = APIClient().get("/auth/me/", HTTP_AUTHORIZATION=f"Bearer {issue_access_token(user)}")
        self.assertIn(response.status_code, (401, 403))
