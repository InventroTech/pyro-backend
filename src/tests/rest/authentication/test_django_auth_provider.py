"""Password reset and user deletion with AUTH_PROVIDER="django" (no Supabase calls)."""
import uuid
from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.test import APIClient

from accounts.services.delete_user_everywhere import delete_user_everywhere
from authentication.models import PasswordResetOTP, RefreshToken, User
from authentication.password_reset import otp_hmac_digest
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
    @patch("accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally")
    def test_django_provider_deletes_django_account_without_supabase(self, mock_supabase_revoke):
        report = delete_user_everywhere(tenant=self.tenant, uid=self.uid)

        mock_supabase_revoke.assert_not_called()
        self.assertEqual(report["deleted"]["auth_users"], 1)
        self.assertEqual(report["sessions_revoked"][0]["django_sessions_revoked"], 1)
        self.assertFalse(User.objects.filter(supabase_uid=self.uid).exists())
        self.assertFalse(RefreshToken.objects.filter(pk=self.session.pk).exists())

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
