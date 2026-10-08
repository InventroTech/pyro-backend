import uuid
from datetime import timedelta

import bcrypt
from django.core.cache import cache
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.test import APIClient

from authentication.models import RefreshToken, User
from authentication.tokens import verify_access_token
from authz.models import Role, TenantMembership
from core.models import Tenant

PASSWORD = "Correct-Horse-42"


@override_settings(AUTH_JWT_SECRET="test-auth-jwt-secret", AUTH_ACCESS_TOKEN_TTL_SECONDS=3600)
class LoginSessionTests(TestCase):
    def setUp(self):
        cache.clear()
        self.client = APIClient()
        self.tenant = Tenant.objects.create(name="Tenant A", slug="tenant-a")
        self.role = Role.objects.create(tenant=self.tenant, key="GM", name="General Manager")
        self.uid = str(uuid.uuid4())
        self.user = User.objects.create_user(
            supabase_uid=self.uid,
            email="user@example.com",
            password=PASSWORD,
            email_verified_at=timezone.now(),
            user_metadata={"full_name": "Test User"},
        )
        TenantMembership.objects.create(
            tenant=self.tenant, role=self.role, email="user@example.com", user_id=self.uid, is_active=True
        )

    def _login(self, email="user@example.com", password=PASSWORD):
        return self.client.post("/auth/login/", {"email": email, "password": password}, format="json")

    def _refresh(self, refresh_token):
        return self.client.post("/auth/token/refresh/", {"refresh_token": refresh_token}, format="json")

    def test_login_returns_supabase_shaped_session(self):
        response = self._login(email="USER@example.com")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["token_type"], "bearer")
        self.assertEqual(body["expires_in"], 3600)
        self.assertEqual(body["user"]["id"], self.uid)
        self.assertEqual(body["user"]["user_metadata"], {"full_name": "Test User"})
        claims = verify_access_token(body["access_token"])
        self.assertEqual(claims["sub"], self.uid)
        self.assertEqual(claims["user_data"]["tenant_id"], str(self.tenant.id))
        self.assertEqual(claims["user_metadata"], {"full_name": "Test User"})
        self.user.refresh_from_db()
        self.assertIsNotNone(self.user.last_login)

    def test_wrong_password_and_unknown_email_get_same_error(self):
        wrong = self._login(password="nope")
        unknown = self._login(email="ghost@example.com")

        for response in (wrong, unknown):
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response.json()["error"], "invalid_credentials")

    def test_unverifiable_password_hash_is_rejected_not_a_server_error(self):
        User.objects.filter(pk=self.user.pk).update(password="argon2$argon2id$v=19$m=102400,t=2,p=8$c2FsdA$aGFzaA")

        with self.assertLogs("authentication.session_views", level="ERROR"):
            response = self._login()

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "invalid_credentials")

    def test_unconfirmed_email_cannot_log_in(self):
        self.user.email_verified_at = None
        self.user.save()

        response = self._login()

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "email_not_confirmed")

    def test_inactive_user_cannot_log_in(self):
        self.user.is_active = False
        self.user.save()

        self.assertEqual(self._login().json()["error"], "invalid_credentials")

    def test_mirrored_user_without_password_cannot_log_in(self):
        User.objects.create_user(supabase_uid=str(uuid.uuid4()), email="mirror@example.com")

        self.assertEqual(self._login(email="mirror@example.com", password="").status_code, 400)
        self.assertEqual(self._login(email="mirror@example.com", password="anything").status_code, 400)

    def test_imported_supabase_bcrypt_password_works_and_is_upgraded(self):
        supabase_hash = bcrypt.hashpw(PASSWORD.encode(), bcrypt.gensalt(rounds=4, prefix=b"2a")).decode()
        User.objects.filter(pk=self.user.pk).update(password=f"bcrypt${supabase_hash}")

        response = self._login()

        self.assertEqual(response.status_code, 200)
        self.user.refresh_from_db()
        self.assertTrue(self.user.password.startswith("pbkdf2_sha256$"))

    @override_settings(AUTH_JWT_SECRET="")
    def test_login_without_secret_returns_503(self):
        self.assertEqual(self._login().status_code, 503)

    def test_refresh_rotates_token(self):
        first = self._login().json()

        response = self._refresh(first["refresh_token"])

        self.assertEqual(response.status_code, 200)
        second = response.json()
        self.assertNotEqual(second["refresh_token"], first["refresh_token"])
        self.assertEqual(verify_access_token(second["access_token"])["sub"], self.uid)
        self.assertEqual(self._refresh(second["refresh_token"]).status_code, 200)

    def test_reusing_old_refresh_token_revokes_the_session(self):
        first = self._login().json()
        second = self._refresh(first["refresh_token"]).json()
        RefreshToken.objects.filter(revoked_at__isnull=False).update(
            revoked_at=timezone.now() - timedelta(seconds=11)
        )

        reused = self._refresh(first["refresh_token"])

        self.assertEqual(reused.status_code, 400)
        self.assertEqual(reused.json()["error"], "invalid_grant")
        self.assertEqual(self._refresh(second["refresh_token"]).status_code, 400)

    def test_reuse_within_grace_window_keeps_the_session(self):
        first = self._login().json()
        second = self._refresh(first["refresh_token"]).json()

        reused = self._refresh(first["refresh_token"])

        self.assertEqual(reused.status_code, 400)
        self.assertEqual(self._refresh(second["refresh_token"]).status_code, 200)

    def test_expired_refresh_token_rejected(self):
        session = self._login().json()
        RefreshToken.objects.update(expires_at=timezone.now() - timedelta(seconds=1))

        self.assertEqual(self._refresh(session["refresh_token"]).status_code, 400)

    def test_unknown_refresh_token_rejected(self):
        self.assertEqual(self._refresh("not-a-real-token").status_code, 400)

    def test_refresh_token_is_stored_hashed(self):
        session = self._login().json()

        self.assertFalse(RefreshToken.objects.filter(token_hash=session["refresh_token"]).exists())
        self.assertEqual(RefreshToken.objects.count(), 1)

    def test_local_logout_ends_only_that_session(self):
        laptop = self._login().json()
        phone = self._login().json()

        response = self.client.post("/auth/logout/", {"refresh_token": laptop["refresh_token"]}, format="json")

        self.assertEqual(response.status_code, 204)
        self.assertEqual(self._refresh(laptop["refresh_token"]).status_code, 400)
        self.assertEqual(self._refresh(phone["refresh_token"]).status_code, 200)

    def test_global_logout_ends_every_session(self):
        laptop = self._login().json()
        phone = self._login().json()

        self.client.post(
            "/auth/logout/", {"refresh_token": laptop["refresh_token"], "scope": "global"}, format="json"
        )

        self.assertEqual(self._refresh(laptop["refresh_token"]).status_code, 400)
        self.assertEqual(self._refresh(phone["refresh_token"]).status_code, 400)

    def test_logout_with_unknown_token_is_noop(self):
        response = self.client.post("/auth/logout/", {"refresh_token": "unknown"}, format="json")

        self.assertEqual(response.status_code, 204)

    def test_login_is_rate_limited(self):
        statuses = [self._login(password="nope").status_code for _ in range(11)]

        self.assertEqual(statuses[-1], 429)
