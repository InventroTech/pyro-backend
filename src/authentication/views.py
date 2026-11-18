import logging
import re
import secrets
import time
from datetime import datetime, timedelta, timezone as dt_timezone

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import DatabaseError, transaction
from django.db.models import Q
from django.utils import timezone
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from authentication.client_email_templates import apply_otp_to_client_email, ensure_single_otp_placeholder
from authentication.account_views import validate_user_password
from authentication.session_views import AuthEmailRateThrottle, AuthRateThrottle
from authentication.models import PasswordResetOTP, User
from authentication.password_reset import (
    OTP_TTL_SECONDS,
    admin_update_user_password,
    auth_provider_is_django,
    find_django_user_id_for_password_reset,
    find_supabase_user_id_for_password_reset,
    otp_codes_match,
    otp_hmac_digest,
    set_django_user_password,
    _email_log_tag,
)

from email_protocol.services import send_email

logger = logging.getLogger(__name__)


def _find_reset_user_id(normalized_email: str) -> tuple[str | None, bool]:
    """``(user_id, lookup_failed)`` from whichever system owns login accounts."""
    if auth_provider_is_django():
        return find_django_user_id_for_password_reset(normalized_email), False
    return find_supabase_user_id_for_password_reset(normalized_email)


def _set_reset_password(user_id: str, password: str) -> tuple[bool, str]:
    if auth_provider_is_django():
        return set_django_user_password(user_id, password)
    return admin_update_user_password(user_id, password)


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
    throttle_classes = [AuthRateThrottle, AuthEmailRateThrottle]
    throttle_scope = "auth_password_reset"
    email_throttle_scope = "auth_password_reset_email"

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

        uid, lookup_failed = _find_reset_user_id(normalized)
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
    Confirms email + OTP + new password; updates the password in Supabase Auth (Admin API)
    or in Django, depending on AUTH_PROVIDER.
    """

    authentication_classes = []
    permission_classes = [AllowAny]
    throttle_classes = [AuthRateThrottle, AuthEmailRateThrottle]
    throttle_scope = "auth_password_reset_confirm"
    email_throttle_scope = "auth_password_reset_confirm_email"

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
        if auth_provider_is_django():
            try:
                validate_user_password(password, User(email=email))
            except ValidationError as exc:
                return Response({"error": " ".join(exc.messages)}, status=status.HTTP_400_BAD_REQUEST)

        logger.info(
            "[PasswordReset][confirm-password] start rid=%s %s",
            rid,
            _email_log_tag(email),
        )

        uid, lookup_failed = _find_reset_user_id(email)
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

        success, _ = _set_reset_password(uid, password)
        PasswordResetOTP.objects.filter(email__iexact=email).delete()

        if not success:
            # err_msg can come from the auth service's reply to the password change; not logged.
            logger.error(
                "[PasswordReset][confirm-password] admin_set_password_failed rid=%s %s",
                rid,
                _email_log_tag(email),
            )
            return Response(
                {"error": "Could not update password."},
                status=status.HTTP_502_BAD_GATEWAY,
            )

        logger.info(
            "[PasswordReset][confirm-password] success rid=%s %s",
            rid,
            _email_log_tag(email),
        )
        return Response({"ok": True})


# ---------------------------------------------------------------------------
# Last sign-in: config.supabase_auth calls these on every authenticated request
# to keep authentication_user.last_login current for the settings user list.
# ---------------------------------------------------------------------------

# Sign-in times further ahead than this are treated as bogus, not clock skew.
SIGN_IN_MAX_CLOCK_SKEW_SECONDS = 300
# Reused access tokens (tab close/reopen) keep the same iat; bump last_login at
# most this often so activity is visible without a write on every API call.
LAST_LOGIN_TOUCH_INTERVAL_SECONDS = 60


def _valid_unix_timestamp(value, latest_allowed):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and 0 < value <= latest_allowed
    )


def _signed_in_at(claims):
    """
    Activity time for last_login: newer of Supabase amr and token iat.

    amr ([{"method": ..., "timestamp": ...}]) is required so spoof tokens without
    it do not record. Refreshed tokens keep the original amr timestamp but get a
    new iat when the user reopens the app; using the newer of the two keeps the
    settings "last sign in" current without requiring logout. Malformed,
    non-positive or far-future timestamps are ignored.
    """
    amr = claims.get("amr")
    if not isinstance(amr, (list, tuple)):
        return None
    latest_allowed = time.time() + SIGN_IN_MAX_CLOCK_SKEW_SECONDS
    amr_stamps = [
        entry.get("timestamp")
        for entry in amr
        if isinstance(entry, dict)
    ]
    amr_stamps = [s for s in amr_stamps if _valid_unix_timestamp(s, latest_allowed)]
    # Require a real amr marker; iat alone must not record a sign-in.
    if not amr_stamps:
        return None
    stamps = list(amr_stamps)
    iat = claims.get("iat")
    if _valid_unix_timestamp(iat, latest_allowed):
        stamps.append(iat)
    signed_in_at = datetime.fromtimestamp(max(stamps), tz=dt_timezone.utc)
    # With USE_TZ=False, DB datetimes are naive; an aware value could not be compared to them.
    return signed_in_at if settings.USE_TZ else timezone.make_naive(signed_in_at)


def _record_sign_in(user, signed_in_at):
    """
    Move last_login forward to signed_in_at. The newer-than check is in SQL so
    concurrent requests can't move it backwards. A failed write never blocks auth.
    """
    if user.last_login is not None and signed_in_at <= user.last_login:
        return
    try:
        with transaction.atomic():
            updated = (
                User.objects.filter(pk=user.pk)
                .filter(Q(last_login__isnull=True) | Q(last_login__lt=signed_in_at))
                .update(last_login=signed_in_at)
            )
    except DatabaseError:
        logger.warning("Could not record sign-in for user %s", user.pk, exc_info=True)
        return
    if updated:
        user.last_login = signed_in_at


def touch_last_login(user):
    """
    Bump last_login to now when it is missing or older than the touch interval.

    Django auth has no amr claim; tab close/reopen often reuses the same access
    token. Calling this on authenticated requests keeps the settings time moving
    without writing on every request.
    """
    now = timezone.now()
    if user.last_login is not None:
        age = (now - user.last_login).total_seconds()
        if age < LAST_LOGIN_TOUCH_INTERVAL_SECONDS:
            return
    _record_sign_in(user, now)


def record_sign_in_from_token(user, claims):
    """Copy token activity onto last_login, then touch now() if still stale."""
    signed_in_at = _signed_in_at(claims)
    if not signed_in_at:
        return
    _record_sign_in(user, signed_in_at)
    # Same access token after tab reopen: amr/iat unchanged; bump when stale.
    touch_last_login(user)
