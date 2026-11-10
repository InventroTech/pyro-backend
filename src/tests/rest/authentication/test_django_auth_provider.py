"""Password reset and user deletion with AUTH_PROVIDER="django" (no Supabase calls)."""
import time
import uuid
from datetime import timedelta
from unittest.mock import patch

import jwt
from django.conf import settings
from django.core.cache import cache
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.exceptions import AuthenticationFailed
from rest_framework.test import APIClient

from accounts.models import SupabaseAuthUser
from accounts.services.delete_user_everywhere import delete_user_everywhere
from config.supabase_auth import _get_or_create_profile
from authentication.models import PasswordResetOTP, RefreshToken, User
from authentication.password_reset import otp_hmac_digest
from authz.models import TenantMembership
from tests.factories import RoleFactory, TenantFactory, TenantMembershipFactory

OTP_SLOT = "%%PYRO_OTP%%"
NEW_PASSWORD = "Brand-New-Pass-1"


def _refresh_row(user):
    return RefreshToken.objects.create(
        user=user,
        token_hash=uuid.uuid4().hex + uuid.uuid4().hex,
        family_id=uuid.uuid4(),
        expires_at=timezone.now() + timedelta(days=1),
    )


@override_settings(AUTH_PROVIDER="django")
@patch("authentication.views.admin_update_user_password", side_effect=AssertionError("Supabase called"))
@patch("authentication.views.find_supabase_user_id_for_password_reset", side_effect=AssertionError("Supabase called"))
class DjangoPasswordResetTests(TestCase):
    def setUp(self):
        cache.clear()
        self.client = APIClient()
        self.uid = str(uuid.uuid4())
        self.user = User.objects.create_user(supabase_uid=self.uid, email="reset@example.com", password="Old-Pass-123")

    def _forgot(self, email):
        return self.client.post(
            "/auth/forgot-password/",
            {"email": email, "subject": "Reset", "message": f"Code {OTP_SLOT}", "html_message": f"<p>{OTP_SLOT}</p>"},
            format="json",
        )

    def _confirm(self, code="123456", password=NEW_PASSWORD):
        PasswordResetOTP.objects.create(
            email="reset@example.com",
            otp_hash=otp_hmac_digest("reset@example.com", "123456"),
            expires_at=timezone.now() + timedelta(minutes=4),
        )
        return self.client.post(
            "/auth/reset-password/confirm/",
            {"email": "reset@example.com", "otp": code, "password": password, "password_confirm": password},
            format="json",
        )

    @patch("authentication.views.send_email", return_value=(True, "sent"))
    def test_forgot_password_uses_django_user(self, mock_send, *_):
        response = self._forgot("Reset@Example.com")

        self.assertEqual(response.json(), {"ok": True})
        mock_send.assert_called_once()
        self.assertTrue(PasswordResetOTP.objects.filter(email="reset@example.com").exists())

    @patch("authentication.views.send_email")
    def test_forgot_password_unknown_or_inactive_sends_nothing(self, mock_send, *_):
        self.user.is_active = False
        self.user.save()

        self.assertEqual(self._forgot("reset@example.com").json(), {"ok": True})
        self.assertEqual(self._forgot("ghost@example.com").json(), {"ok": True})
        mock_send.assert_not_called()

    def test_confirm_sets_django_password_and_logs_out_everywhere(self, *_):
        old_session = _refresh_row(self.user)

        response = self._confirm()

        self.assertEqual(response.json(), {"ok": True})
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password(NEW_PASSWORD))
        self.assertIsNotNone(self.user.email_verified_at)
        old_session.refresh_from_db()
        self.assertIsNotNone(old_session.revoked_at)
        self.assertFalse(PasswordResetOTP.objects.exists())

    def test_confirm_wrong_code_keeps_password(self, *_):
        response = self._confirm(code="000000")

        self.assertEqual(response.status_code, 400)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password("Old-Pass-123"))

    def test_confirm_rejects_short_password(self, *_):
        response = self._confirm(password="12345")

        self.assertEqual(response.status_code, 400)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password("Old-Pass-123"))

    def test_confirm_accepts_six_digit_password_like_supabase(self, *_):
        response = self._confirm(password="123456")

        self.assertEqual(response.json(), {"ok": True})
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password("123456"))

    def test_reset_changes_only_the_account_with_that_email_in_any_case(self, *_):
        other = User.objects.create_user(
            supabase_uid=str(uuid.uuid4()), email="other@example.com", password="Other-Pass-123"
        )
        PasswordResetOTP.objects.create(
            email="reset@example.com",
            otp_hash=otp_hmac_digest("reset@example.com", "123456"),
            expires_at=timezone.now() + timedelta(minutes=4),
        )

        response = self.client.post(
            "/auth/reset-password/confirm/",
            {"email": "RESET@Example.com", "otp": "123456", "password": NEW_PASSWORD, "password_confirm": NEW_PASSWORD},
            format="json",
        )

        self.assertEqual(response.json(), {"ok": True})
        self.user.refresh_from_db()
        other.refresh_from_db()
        self.assertTrue(self.user.check_password(NEW_PASSWORD))
        self.assertTrue(other.check_password("Other-Pass-123"))

    @patch("authentication.views.send_email", return_value=(True, "sent"))
    def test_forgot_password_is_rate_limited_per_email_and_per_ip(self, *_):
        for _attempt in range(3):
            self.assertEqual(self._forgot("reset@example.com").status_code, 200)
        self.assertEqual(self._forgot("Reset@Example.com").status_code, 429)

        cache.clear()
        for n in range(5):
            self.assertEqual(self._forgot(f"person{n}@example.com").status_code, 200)
        self.assertEqual(self._forgot("someone-else@example.com").status_code, 429)

    def test_confirm_code_guessing_is_rate_limited_per_email(self, *_):
        for n in range(5):
            self.assertEqual(self._confirm(code=f"00000{n}").status_code, 400)

        # Even the right code is refused once the limit is hit, and from another IP too.
        response = self.client.post(
            "/auth/reset-password/confirm/",
            {"email": "reset@example.com", "otp": "123456", "password": NEW_PASSWORD, "password_confirm": NEW_PASSWORD},
            format="json",
            REMOTE_ADDR="203.0.113.9",
        )
        self.assertEqual(response.status_code, 429)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password("Old-Pass-123"))


def _supabase_token(uid, email, **claims):
    return jwt.encode(
        {
            "sub": uid,
            "email": email,
            "aud": "authenticated",
            "role": "authenticated",
            "iss": "https://project.supabase.co/auth/v1",
            "session_id": str(uuid.uuid4()),
            "exp": int(time.time()) + 3600,
            **claims,
        },
        settings.SUPABASE_JWT_SECRET,
        algorithm="HS256",
    )


@override_settings(AUTH_PROVIDER="django", AUTH_ACCEPT_SUPABASE_TOKENS=True)
@patch("accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally", return_value={"revoked": True})
class LeftoverSupabaseTokenTests(TestCase):
    """A Supabase token issued before a user was deleted or disabled must not get them back in."""

    def setUp(self):
        cache.clear()
        self.client = APIClient()
        self.tenant = TenantFactory()
        self.role = RoleFactory(tenant=self.tenant)
        self.uid = str(uuid.uuid4())
        self.user = User.objects.create_user(supabase_uid=self.uid, email="gone@example.com", password="Pass-Word-77")
        TenantMembershipFactory(
            tenant=self.tenant, role=self.role, email="gone@example.com", user_id=self.uid, is_active=True
        )
        self.token = _supabase_token(self.uid, "gone@example.com")
        self.spoof_token = jwt.encode(
            {"sub": self.uid, "email": "gone@example.com", "role": "authenticated"},
            settings.SUPABASE_JWT_SECRET,
            algorithm="HS256",
        )

    def _me(self, token):
        return self.client.get("/auth/me/", HTTP_AUTHORIZATION=f"Bearer {token}")

    def _assert_locked_out(self):
        for token in (self.token, self.spoof_token):
            self.assertIn(self._me(token).status_code, (401, 403))
        exchange = self.client.post("/auth/token/from-supabase/", {"access_token": self.token}, format="json")
        self.assertEqual(exchange.status_code, 400)

    def test_token_works_while_the_account_is_active(self, _):
        self.assertEqual(self._me(self.token).status_code, 200)
        exchange = self.client.post("/auth/token/from-supabase/", {"access_token": self.token}, format="json")
        self.assertEqual(exchange.status_code, 200)

    def test_deleted_user_cannot_log_back_in_or_be_recreated(self, _):
        delete_user_everywhere(tenant=self.tenant, uid=self.uid)

        self._assert_locked_out()
        self.assertFalse(User.objects.filter(supabase_uid=self.uid).exists())
        self.assertFalse(User.objects.filter(email__iexact="gone@example.com").exists())

    def test_disabled_user_cannot_log_back_in(self, _):
        self.user.is_active = False
        self.user.save(update_fields=["is_active"])

        self._assert_locked_out()
        self.assertEqual(User.objects.filter(email__iexact="gone@example.com").count(), 1)
        self.assertFalse(User.objects.get(supabase_uid=self.uid).is_active)


class DeleteUserLoginAccountTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()
        self.role = RoleFactory(tenant=self.tenant)
        self.uid = str(uuid.uuid4())
        self.user = User.objects.create_user(supabase_uid=self.uid, email="gone@example.com", password="Pass-Word-77")
        self.session = _refresh_row(self.user)
        TenantMembershipFactory(
            tenant=self.tenant, role=self.role, email="gone@example.com", user_id=self.uid, is_active=True
        )

    @override_settings(AUTH_PROVIDER="django")
    @patch(
        "accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally",
        return_value={"revoked": True},
    )
    def test_django_provider_deletes_django_account_and_leftover_supabase_login(self, mock_supabase_revoke):
        SupabaseAuthUser.objects.create(id=self.uid, email="gone@example.com")

        report = delete_user_everywhere(tenant=self.tenant, uid=self.uid)

        mock_supabase_revoke.assert_called_once_with(self.uid)
        self.assertEqual(report["deleted"]["auth_users"], 1)
        self.assertEqual(report["sessions_revoked"][0]["django_sessions_revoked"], 1)
        self.assertFalse(User.objects.filter(supabase_uid=self.uid).exists())
        self.assertFalse(RefreshToken.objects.filter(pk=self.session.pk).exists())
        self.assertFalse(SupabaseAuthUser.objects.filter(id=self.uid).exists())

    @override_settings(AUTH_PROVIDER="django")
    @patch(
        "accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally",
        return_value={"revoked": True},
    )
    def test_failed_account_delete_rolls_everything_back(self, _):
        with patch.object(User, "delete", side_effect=RuntimeError("db down")):
            with self.assertRaises(RuntimeError):
                delete_user_everywhere(tenant=self.tenant, uid=self.uid)

        membership = TenantMembership.objects.get(tenant=self.tenant, email="gone@example.com")
        self.assertTrue(membership.is_active)
        self.assertEqual(str(membership.user_id), self.uid)
        self.assertTrue(User.objects.filter(supabase_uid=self.uid, is_active=True).exists())
        self.session.refresh_from_db()
        self.assertIsNone(self.session.revoked_at)

    @override_settings(AUTH_PROVIDER="django")
    @patch(
        "accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally",
        return_value={"revoked": True},
    )
    def test_django_provider_delete_works_without_supabase_account(self, _):
        report = delete_user_everywhere(tenant=self.tenant, uid=self.uid)

        self.assertEqual(report["deleted"]["auth_users"], 1)
        self.assertFalse(User.objects.filter(supabase_uid=self.uid).exists())

    @override_settings(AUTH_PROVIDER="django", AUTH_ACCEPT_SUPABASE_TOKENS=True)
    @patch(
        "accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally",
        return_value={"revoked": True},
    )
    def test_leftover_supabase_token_cannot_recreate_deleted_user(self, _):
        delete_user_everywhere(tenant=self.tenant, uid=self.uid)

        with self.assertRaises(AuthenticationFailed):
            _get_or_create_profile({"sub": self.uid, "email": "gone@example.com", "iss": "supabase"})
        self.assertFalse(User.objects.filter(supabase_uid=self.uid).exists())

    @patch(
        "accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally",
        return_value={"revoked": True},
    )
    def test_supabase_provider_also_revokes_django_sessions(self, mock_supabase_revoke):
        delete_user_everywhere(tenant=self.tenant, uid=self.uid)

        mock_supabase_revoke.assert_called_once_with(self.uid)
        self.session.refresh_from_db()
        self.assertIsNotNone(self.session.revoked_at)
        self.assertTrue(User.objects.filter(supabase_uid=self.uid).exists())
