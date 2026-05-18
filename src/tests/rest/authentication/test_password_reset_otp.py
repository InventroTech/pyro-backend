import os
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone
from rest_framework.test import APIClient

from authentication.models import PasswordResetOTP
from authentication.password_reset import otp_hmac_digest

User = get_user_model()

_OTP_SLOT = "%%PYRO_OTP%%"


def _valid_email_payload(email: str) -> dict:
    return {
        "email": email,
        "subject": "[Pyro Test] Password reset",
        "message": f"Your code:\n{_OTP_SLOT}",
        "html_message": f'<p style="margin:0">Your code:</p><div>{_OTP_SLOT}</div>',
    }


class SupabasePasswordRecoverOTPTests(TestCase):
    def setUp(self):
        self.client = APIClient()

    @patch("authentication.views.send_email")
    @patch("authentication.views.find_supabase_user_id_for_email")
    def test_issues_otp_and_sends_mail(self, mock_find, mock_send):
        mock_find.return_value = "00000000-0000-0000-0000-000000000001"
        mock_send.return_value = (True, "sent")

        response = self.client.post(
            "/auth/forgot-password/",
            _valid_email_payload(" User@Example.com "),
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        mock_send.assert_called_once()
        _args, kw = mock_send.call_args
        self.assertEqual(kw["to_emails"], "user@example.com")

        html = kw["html_message"]
        self.assertRegex(html, r"\d{6}")

        row = PasswordResetOTP.objects.get(email="user@example.com")
        self.assertGreater(row.expires_at, timezone.now())
        self.assertNotIn(_OTP_SLOT, html)

    @patch("authentication.views.find_supabase_user_id_for_email")
    def test_no_user_returns_ok_without_row(self, mock_find):
        mock_find.return_value = None
        response = self.client.post(
            "/auth/forgot-password/",
            _valid_email_payload("ghost@example.com"),
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(PasswordResetOTP.objects.filter(email__iexact="ghost@example.com").exists())

    def test_requires_email_and_templates(self):
        self.assertEqual(self.client.post("/auth/forgot-password/", {}, format="json").status_code, 400)
        self.assertEqual(
            self.client.post(
                "/auth/forgot-password/",
                {"email": "a@b.com", "subject": "", "message": "", "html_message": ""},
                format="json",
            ).status_code,
            400,
        )

    @patch("authentication.views.find_supabase_user_id_for_email")
    def test_rejects_templates_without_placeholder(self, mock_find):
        mock_find.return_value = None
        response = self.client.post(
            "/auth/forgot-password/",
            {
                "email": "x@y.com",
                "subject": "Hi",
                "message": "no code",
                "html_message": "<p>no code</p>",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 400)

    @patch("authentication.views.send_email")
    @patch("authentication.views.find_supabase_user_id_for_email")
    def test_send_failure_returns_503(self, mock_find, mock_send):
        mock_find.return_value = "00000000-0000-0000-0000-000000000002"
        mock_send.return_value = (False, "smtp down")
        response = self.client.post(
            "/auth/forgot-password/",
            _valid_email_payload("fail@example.com"),
            format="json",
        )
        self.assertEqual(response.status_code, 503)
        self.assertFalse(PasswordResetOTP.objects.filter(email__iexact="fail@example.com").exists())


class PasswordResetConfirmOTPTests(TestCase):
    def setUp(self):
        self.client = APIClient()

    @patch.dict(
        os.environ,
        {
            "SUPABASE_PROJECT_URL": "https://example.supabase.co",
            "SUPABASE_SERVICE_ROLE_KEY": "service-key",
        },
        clear=False,
    )
    @patch("authentication.views.admin_update_user_password")
    @patch("authentication.views.find_supabase_user_id_for_email")
    def test_confirm_with_valid_otp_updates_password(self, mock_find, mock_admin):
        uid = "22222222-2222-2222-2222-222222222222"
        email = "otp@example.com"
        User.objects.create(supabase_uid=uid, email=email)

        code = "847291"
        digest = otp_hmac_digest(email, code)
        PasswordResetOTP.objects.create(
            email=email,
            otp_hash=digest,
            expires_at=timezone.now() + timedelta(minutes=4),
        )

        mock_find.return_value = uid
        mock_admin.return_value = (True, "ok")

        response = self.client.post(
            "/auth/reset-password/confirm/",
            {
                "email": email,
                "otp": code,
                "password": "new-secret-9",
                "password_confirm": "new-secret-9",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        mock_admin.assert_called_once_with(uid, "new-secret-9")
        self.assertFalse(PasswordResetOTP.objects.filter(email__iexact=email).exists())

    def test_confirm_rejects_mismatched_passwords(self):
        response = self.client.post(
            "/auth/reset-password/confirm/",
            {
                "email": "a@b.com",
                "otp": "123456",
                "password": "abcdef",
                "password_confirm": "abcdeg",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 400)

    @patch.dict(
        os.environ,
        {
            "SUPABASE_PROJECT_URL": "https://example.supabase.co",
            "SUPABASE_SERVICE_ROLE_KEY": "service-key",
        },
        clear=False,
    )
    @patch("authentication.views.find_supabase_user_id_for_email")
    def test_confirm_rejects_expired_otp(self, mock_find):
        email = "exp@example.com"
        mock_find.return_value = "33333333-3333-3333-3333-333333333333"
        PasswordResetOTP.objects.create(
            email=email,
            otp_hash=otp_hmac_digest(email, "111111"),
            expires_at=timezone.now() - timedelta(minutes=1),
        )

        response = self.client.post(
            "/auth/reset-password/confirm/",
            {
                "email": email,
                "otp": "111111",
                "password": "newpass1",
                "password_confirm": "newpass1",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 400)
