"""
Sign-up email confirmation: a single-use link token is emailed to the user; the
frontend posts it back to ``/auth/verify-email/`` to confirm the address.
"""
from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import timedelta
from html import escape

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from authentication.models import EmailVerificationToken, User
from authentication.redirects import safe_redirect_url, with_query
from email_protocol.services import send_email

logger = logging.getLogger(__name__)

TOKEN_QUERY_PARAM = "confirmation_token"


class InvalidVerificationToken(Exception):
    """Token is unknown, already used, or expired."""


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _issue_token(user: User) -> str:
    plain = secrets.token_urlsafe(32)
    with transaction.atomic():
        EmailVerificationToken.objects.filter(user=user, used_at__isnull=True).delete()
        EmailVerificationToken.objects.create(
            user=user,
            token_hash=_hash(plain),
            expires_at=timezone.now() + timedelta(seconds=settings.AUTH_EMAIL_CONFIRM_TTL_SECONDS),
        )
    return plain


def send_confirmation_email(user: User, redirect_to: str | None = None) -> bool:
    """Issue a fresh token (older unused ones stop working) and email the link."""
    link = with_query(safe_redirect_url(redirect_to), {TOKEN_QUERY_PARAM: _issue_token(user)})
    hours = max(1, settings.AUTH_EMAIL_CONFIRM_TTL_SECONDS // 3600)
    ok, msg = send_email(
        to_emails=user.email,
        subject="Confirm your Pyro account",
        message=(
            "Welcome to Pyro!\n\n"
            f"Confirm your email address by opening this link:\n{link}\n\n"
            f"The link expires in {hours} hours. If you didn't sign up, you can ignore this email."
        ),
        html_message=(
            "<p>Welcome to Pyro!</p>"
            f'<p><a href="{escape(link)}">Confirm your email address</a></p>'
            f"<p>The link expires in {hours} hours. If you didn't sign up, you can ignore this email.</p>"
        ),
        client_name="signup-confirmation",
    )
    if not ok:
        logger.error("[Auth][signup] confirmation email failed user_id=%s err=%s", user.pk, msg)
    return ok


def confirm_email(token: str) -> User:
    """Mark the token used and the user's email verified; return the user."""
    if not token:
        raise InvalidVerificationToken("Missing confirmation token")
    now = timezone.now()
    with transaction.atomic():
        row = (
            EmailVerificationToken.objects.select_for_update()
            .select_related("user")
            .filter(token_hash=_hash(token))
            .first()
        )
        if row is None or row.used_at is not None:
            raise InvalidVerificationToken("Confirmation link is invalid or has already been used")
        if row.expires_at <= now:
            raise InvalidVerificationToken("Confirmation link has expired")
        user = row.user
        if not user.is_active:
            raise InvalidVerificationToken("Confirmation link is invalid or has already been used")

        row.used_at = now
        row.save(update_fields=["used_at"])
        if user.email_verified_at is None:
            user.email_verified_at = now
            user.save(update_fields=["email_verified_at"])
    return user
