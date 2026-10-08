import uuid
from datetime import timedelta
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from django.core.cache import cache
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.test import APIClient

from authentication.models import EmailVerificationToken, RefreshToken, User
from authentication.tokens import verify_access_token

PASSWORD = "Correct-Horse-42"
SITE = "http://localhost:8080"


@override_settings(
    AUTH_JWT_SECRET="test-auth-jwt-secret",
    AUTH_ALLOWED_REDIRECT_ORIGINS=[SITE],
    AUTH_SITE_URL=SITE,
)
class SignupTests(TestCase):
    def setUp(self):
        cache.clear()
        self.client = APIClient()
        patcher = patch("authentication.email_verification.send_email", return_value=(True, "sent"))
        self.send_email = patcher.start()
        self.addCleanup(patcher.stop)

    def _signup(self, **overrides):
        body = {"email": "new@example.com", "password": PASSWORD, **overrides}
        return self.client.post("/auth/signup/", body, format="json")

    def _emailed_link(self):
        return self.send_email.call_args.kwargs["message"].split("\n")[3]

    def _emailed_token(self):
        return parse_qs(urlsplit(self._emailed_link()).query)["confirmation_token"][0]

    def test_signup_creates_unconfirmed_user_and_emails_link(self):
        response = self._signup(email="New@Example.com", data={"tenant_slug": "acme"})

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["session"])
        user = User.objects.get(email="new@example.com")
        self.assertIsNone(user.email_verified_at)
        self.assertTrue(user.check_password(PASSWORD))
        self.assertEqual(user.user_metadata, {"tenant_slug": "acme"})
        self.assertEqual(self.send_email.call_args.kwargs["to_emails"], "new@example.com")
        self.assertTrue(self._emailed_link().startswith(SITE + "?confirmation_token="))

    def test_unconfirmed_user_cannot_log_in_until_verified(self):
        self._signup()
        login = self.client.post("/auth/login/", {"email": "new@example.com", "password": PASSWORD}, format="json")
        self.assertEqual(login.json()["error"], "email_not_confirmed")

        verify = self.client.post("/auth/verify-email/", {"token": self._emailed_token()}, format="json")

        self.assertEqual(verify.status_code, 200)
        self.assertIsNotNone(verify.json()["user"]["email_confirmed_at"])
        self.assertEqual(verify_access_token(verify.json()["access_token"])["email"], "new@example.com")
        login = self.client.post("/auth/login/", {"email": "new@example.com", "password": PASSWORD}, format="json")
        self.assertEqual(login.status_code, 200)

    def test_confirmation_token_is_single_use(self):
        self._signup()
        token = self._emailed_token()
        self.client.post("/auth/verify-email/", {"token": token}, format="json")

        again = self.client.post("/auth/verify-email/", {"token": token}, format="json")

        self.assertEqual(again.status_code, 400)
        self.assertEqual(again.json()["error"], "invalid_token")

    def test_expired_confirmation_token_rejected(self):
        self._signup()
        token = self._emailed_token()
        EmailVerificationToken.objects.update(expires_at=timezone.now() - timedelta(seconds=1))

        response = self.client.post("/auth/verify-email/", {"token": token}, format="json")

        self.assertEqual(response.status_code, 400)
        self.assertIn("expired", response.json()["message"])

    def test_token_stored_hashed(self):
        self._signup()
        token = self._emailed_token()
        self.assertFalse(EmailVerificationToken.objects.filter(token_hash=token).exists())

    def test_redirect_to_allowed_origin_is_kept(self):
        self._signup(redirect_to=f"{SITE}/app/acme/auth/callback?x=1")
        self.assertTrue(self._emailed_link().startswith(f"{SITE}/app/acme/auth/callback?x=1&confirmation_token="))

    def test_redirect_to_unknown_origin_falls_back_to_site_url(self):
        self._signup(redirect_to="https://evil.example.com/steal")
        self.assertTrue(self._emailed_link().startswith(SITE + "?confirmation_token="))

    def test_existing_confirmed_email_gets_same_response_and_no_email(self):
        User.objects.create_user(
            supabase_uid=str(uuid.uuid4()), email="new@example.com", password="Other-Pass-99",
            email_verified_at=timezone.now(),
        )
        response = self._signup()

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["user"])
        self.send_email.assert_not_called()
        self.assertEqual(User.objects.filter(email__iexact="new@example.com").count(), 1)

    def test_repeat_signup_resends_link_without_changing_password(self):
        self._signup()
        first_token = self._emailed_token()

        self._signup(password="Different-Pass-77")

        self.assertTrue(User.objects.get(email="new@example.com").check_password(PASSWORD))
        self.assertEqual(self.send_email.call_count, 2)
        old = self.client.post("/auth/verify-email/", {"token": first_token}, format="json")
        self.assertEqual(old.status_code, 400)

    def test_mirrored_supabase_user_is_not_taken_over(self):
        User.objects.create_user(supabase_uid=str(uuid.uuid4()), email="new@example.com")

        self._signup()

        self.send_email.assert_not_called()
        self.assertFalse(User.objects.get(email="new@example.com").has_usable_password())

    def test_password_shorter_than_six_rejected(self):
        response = self._signup(password="12345")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "weak_password")
        self.assertFalse(User.objects.filter(email="new@example.com").exists())

    def test_six_digit_password_accepted_like_supabase(self):
        response = self._signup(password="123456")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(User.objects.get(email="new@example.com").check_password("123456"))

    def test_invalid_email_rejected(self):
        response = self._signup(email="not-an-email")
        self.assertEqual(response.json()["error"], "invalid_email")

    def test_email_failure_returns_503(self):
        self.send_email.return_value = (False, "smtp down")
        response = self._signup()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"], "email_send_failed")

    def test_resend_confirmation(self):
        self._signup()
        self.client.post("/auth/resend-confirmation/", {"email": "new@example.com"}, format="json")
        self.assertEqual(self.send_email.call_count, 2)

        unknown = self.client.post("/auth/resend-confirmation/", {"email": "ghost@example.com"}, format="json")
        self.assertEqual(unknown.json(), {"ok": True})
        self.assertEqual(self.send_email.call_count, 2)

    def test_signup_is_rate_limited(self):
        for i in range(5):
            self._signup(email=f"user{i}@example.com")
        self.assertEqual(self._signup(email="user5@example.com").status_code, 429)


@override_settings(AUTH_JWT_SECRET="test-auth-jwt-secret")
class MeAndChangePasswordTests(TestCase):
    def setUp(self):
        cache.clear()
        self.client = APIClient()
        self.user = User.objects.create_user(
            supabase_uid=str(uuid.uuid4()),
            email="me@example.com",
            password=PASSWORD,
            email_verified_at=timezone.now(),
            user_metadata={"full_name": "Me"},
        )
        login = self.client.post("/auth/login/", {"email": "me@example.com", "password": PASSWORD}, format="json")
        self.session = login.json()
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {self.session['access_token']}")

    def test_me_requires_login(self):
        self.assertIn(APIClient().get("/auth/me/").status_code, (401, 403))

    def test_get_me(self):
        body = self.client.get("/auth/me/").json()
        self.assertEqual(body["id"], self.user.supabase_uid)
        self.assertEqual(body["user_metadata"], {"full_name": "Me"})

    def test_patch_me_merges_metadata(self):
        response = self.client.patch("/auth/me/", {"data": {"department": "Sales"}}, format="json")

        self.assertEqual(response.status_code, 200)
        self.user.refresh_from_db()
        self.assertEqual(self.user.user_metadata, {"full_name": "Me", "department": "Sales"})

    def test_patch_me_rejects_non_object(self):
        response = self.client.patch("/auth/me/", {"data": ["x"]}, format="json")
        self.assertEqual(response.status_code, 400)

    def test_change_password_needs_current_password(self):
        response = self.client.post(
            "/auth/change-password/", {"current_password": "wrong", "password": "Brand-New-Pass-1"}, format="json"
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "invalid_credentials")

    def test_change_password_logs_out_other_sessions(self):
        response = self.client.post(
            "/auth/change-password/",
            {"current_password": PASSWORD, "password": "Brand-New-Pass-1"},
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password("Brand-New-Pass-1"))
        old_refresh = self.client.post(
            "/auth/token/refresh/", {"refresh_token": self.session["refresh_token"]}, format="json"
        )
        self.assertEqual(old_refresh.status_code, 400)
        new_refresh = self.client.post(
            "/auth/token/refresh/", {"refresh_token": response.json()["refresh_token"]}, format="json"
        )
        self.assertEqual(new_refresh.status_code, 200)
        self.assertEqual(RefreshToken.objects.filter(user=self.user, revoked_at__isnull=True).count(), 1)

    def test_account_without_password_can_set_one(self):
        self.user.set_unusable_password()
        self.user.save()

        response = self.client.post("/auth/change-password/", {"password": "Brand-New-Pass-1"}, format="json")

        self.assertEqual(response.status_code, 200)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password("Brand-New-Pass-1"))

    def test_short_new_password_rejected(self):
        response = self.client.post(
            "/auth/change-password/", {"current_password": PASSWORD, "password": "12345"}, format="json"
        )
        self.assertEqual(response.json()["error"], "weak_password")

    def test_six_digit_new_password_accepted_like_supabase(self):
        response = self.client.post(
            "/auth/change-password/", {"current_password": PASSWORD, "password": "123456"}, format="json"
        )
        self.assertEqual(response.status_code, 200)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password("123456"))
