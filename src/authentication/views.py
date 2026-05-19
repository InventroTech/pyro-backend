import logging
import re
import secrets
from datetime import timedelta

import requests
from django.db import transaction
from django.utils import timezone
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from authentication.client_email_templates import apply_otp_to_client_email, ensure_single_otp_placeholder
from authentication.models import PasswordResetOTP
from authentication.supabase_env import supabase_anon_key, supabase_api_base_url
from authentication.password_reset import (
    OTP_TTL_SECONDS,
    admin_update_user_password,
    find_supabase_user_id_for_password_reset,
    otp_codes_match,
    otp_hmac_digest,
    _email_log_tag,
)

from email_protocol.services import send_email

logger = logging.getLogger(__name__)

SUPABASE_PROJECT_URL = supabase_api_base_url() or None
SUPABASE_ANON_KEY = supabase_anon_key() or None


def _client_correlation_id(request) -> str:
    """Prefer reverse-proxy request id headers for log correlation."""
    rid = (
        request.META.get("HTTP_X_REQUEST_ID")
        or request.META.get("HTTP_X_CORRELATION_ID")
        or request.META.get("HTTP_X_AMZN_TRACE_ID")
    )
    if rid:
        return str(rid).strip()[:200]
    return "no-client-request-id"


class SupabaseAuthCheckView(APIView):
    """
    Authenticates with Supabase using email & password.
    Returns user info if valid, else error.
    """

    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        email = request.data.get("email")
        password = request.data.get("password")
        if not email or not password:
            return Response({"error": "Email and password are required."}, status=status.HTTP_400_BAD_REQUEST)

        url = f"{SUPABASE_PROJECT_URL}/auth/v1/token?grant_type=password"
        headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Content-Type": "application/json",
        }
        data = {"email": email, "password": password}
        try:
            r = requests.post(url, json=data, headers=headers)
        except Exception as e:
            logger.exception("Failed to call Supabase: %s", e)
            return Response({"error": "Failed to connect to Supabase."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

        if r.status_code == 200:
            out = r.json()
            return Response(
                {
                    "valid": True,
                    "user_id": out.get("user", {}).get("id"),
                    "access_token": out.get("access_token"),
                    "email": out.get("user", {}).get("email"),
                }
            )
        error_body = {}
        try:
            error_body = r.json()
        except Exception:
            pass
        return Response(
            {
                "valid": False,
                "error": error_body.get("error", "Login failed"),
                "message": error_body.get("msg") or error_body.get("message"),
            },
            status=status.HTTP_401_UNAUTHORIZED,
        )


def _generate_six_digit_otp() -> str:
    """Cryptographically suitable 6-digit string, zero-padded."""
    n = secrets.randbelow(1_000_000)
    return f"{n:06d}"


OTP_PATTERN = re.compile(r"^\d{6}$")


class SupabasePasswordRecoverView(APIView):
    """
    Sends a 6-digit OTP by email. Email subject/bodies are authored in the frontend and
    posted here; bodies must contain %%PYRO_OTP%% exactly once for server-side OTP injection.
    """

    authentication_classes = []
    permission_classes = [AllowAny]

    MAX_TEMPLATE_LEN = 400_000

    def post(self, request):
        email = (request.data.get("email") or "").strip()
        subject = (request.data.get("subject") or "").strip()
        message = request.data.get("message") or ""
        html_message = request.data.get("html_message") or ""

        if not isinstance(message, str) or not isinstance(html_message, str):
            return Response(
                {"error": "Invalid message payloads."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if len(subject) > 998 or len(message) > self.MAX_TEMPLATE_LEN or len(html_message) > self.MAX_TEMPLATE_LEN:
            return Response({"error": "Email template too large."}, status=status.HTTP_400_BAD_REQUEST)

        if not email:
            return Response({"error": "Email is required."}, status=status.HTTP_400_BAD_REQUEST)

        if not subject or not message.strip() or not html_message.strip():
            return Response(
                {"error": "subject, message, and html_message are required (from email templates)."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        msg_stripped = message.strip()
        html_stripped = html_message.strip()

        try:
            ensure_single_otp_placeholder(msg_stripped, html_stripped)
        except ValueError as exc:
            return Response({"error": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        normalized = email.lower()
        rid = _client_correlation_id(request)

        logger.info(
            "[PasswordReset][forgot-password] start rid=%s %s",
            rid,
            _email_log_tag(normalized),
        )

        uid, lookup_failed = find_supabase_user_id_for_password_reset(normalized)
        if lookup_failed:
            logger.error(
                "[PasswordReset][forgot-password] resolve_failed_503 rid=%s %s "
                "(check resolve_user + supabase_admin_list logs for error_id)",
                rid,
                _email_log_tag(normalized),
            )
            return Response(
                {
                    "error": "Unable to verify account right now. Please try again in a few minutes.",
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        if not uid:
            logger.info(
                "[PasswordReset][forgot-password] no_supabase_user_ack rid=%s %s",
                rid,
                _email_log_tag(normalized),
            )
            return Response({"ok": True})

        otp_plain = _generate_six_digit_otp()
        digest = otp_hmac_digest(normalized, otp_plain)
        expires_at = timezone.now() + timedelta(seconds=OTP_TTL_SECONDS)

        try:
            subject_out, text_out, html_out = apply_otp_to_client_email(
                subject,
                msg_stripped,
                html_stripped,
                otp_plain,
            )
        except ValueError as exc:
            return Response({"error": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        ok, msg = send_email(
            to_emails=normalized,
            subject=subject_out,
            message=text_out,
            html_message=html_out,
            client_name="password-reset-otp",
        )

        if not ok:
            logger.error(
                "[PasswordReset][forgot-password] send_email_failed rid=%s %s err=%s",
                rid,
                _email_log_tag(normalized),
                msg,
            )
            return Response(
                {"error": "Unable to send reset email. Try again later."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        try:
            with transaction.atomic():
                PasswordResetOTP.objects.filter(email__iexact=normalized).delete()
                PasswordResetOTP.objects.create(
                    email=normalized, otp_hash=digest, expires_at=expires_at
                )
        except Exception:
            logger.exception(
                "[PasswordReset][forgot-password] otp_db_save_failed rid=%s %s",
                rid,
                _email_log_tag(normalized),
            )
            return Response(
                {"error": "Unable to finalize reset request. Try again later."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        logger.info(
            "[PasswordReset][forgot-password] otp_issued rid=%s %s",
            rid,
            _email_log_tag(normalized),
        )
        return Response({"ok": True})


class PasswordResetConfirmView(APIView):
    """
    Confirms email + OTP + new password; updates Supabase Auth user password via Admin API.
    """

    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        email = (request.data.get("email") or "").strip().lower()
        rid = _client_correlation_id(request)
        otp = (request.data.get("otp") or "").strip().replace(" ", "")
        password = request.data.get("password") or ""
        password_confirm = request.data.get("password_confirm") or ""

        if not email:
            return Response({"error": "Email is required."}, status=status.HTTP_400_BAD_REQUEST)
        if not otp or not OTP_PATTERN.match(otp):
            return Response(
                {"error": "Enter the 6-digit code from your email."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if not password or len(password) < 6:
            return Response(
                {"error": "Password must be at least 6 characters."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if password != password_confirm:
            return Response(
                {"error": "Passwords do not match."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        logger.info(
            "[PasswordReset][confirm-password] start rid=%s %s",
            rid,
            _email_log_tag(email),
        )

        uid, lookup_failed = find_supabase_user_id_for_password_reset(email)
        if lookup_failed:
            logger.error(
                "[PasswordReset][confirm-password] resolve_failed_503 rid=%s %s",
                rid,
                _email_log_tag(email),
            )
            return Response(
                {
                    "error": "Unable to verify account right now. Please try again in a few minutes.",
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        if not uid:
            logger.info(
                "[PasswordReset][confirm-password] no_uid_bad_request rid=%s %s",
                rid,
                _email_log_tag(email),
            )
            return Response(
                {"error": "Invalid email or code."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        now = timezone.now()
        row = (
            PasswordResetOTP.objects.filter(email__iexact=email, expires_at__gt=now)
            .order_by("-created_at")
            .first()
        )
        if not row or not otp_codes_match(email, otp, row.otp_hash):
            logger.info(
                "[PasswordReset][confirm-password] invalid_or_expired_otp rid=%s %s",
                rid,
                _email_log_tag(email),
            )
            return Response(
                {"error": "Invalid or expired code. Request a new reset email."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        success, err_msg = admin_update_user_password(uid, password)
        PasswordResetOTP.objects.filter(email__iexact=email).delete()

        if not success:
            logger.error(
                "[PasswordReset][confirm-password] admin_set_password_failed rid=%s %s err=%s",
                rid,
                _email_log_tag(email),
                err_msg,
            )
            return Response(
                {"error": err_msg or "Could not update password."},
                status=status.HTTP_502_BAD_GATEWAY,
            )

        logger.info(
            "[PasswordReset][confirm-password] success rid=%s %s",
            rid,
            _email_log_tag(email),
        )
        return Response({"ok": True})
