"""
Login sessions: access token + rotating refresh token.

Refresh tokens are random strings; only their SHA-256 is stored (``RefreshToken``).
Revoking a session does not invalidate access tokens already issued — they expire
on their own after ``AUTH_ACCESS_TOKEN_TTL_SECONDS``, as with Supabase Auth.
"""
from __future__ import annotations

import hashlib
import secrets
import uuid
from dataclasses import dataclass
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from authentication.models import RefreshToken, User
from authentication.tokens import issue_access_token


class InvalidRefreshToken(Exception):
    """Refresh token is unknown, expired, revoked, or belongs to an inactive user."""


@dataclass(frozen=True)
class Session:
    user: User
    access_token: str
    refresh_token: str
    expires_in: int


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _new_refresh_row(user, family_id, *, ip_address=None, user_agent="") -> tuple[RefreshToken, str]:
    plain = secrets.token_urlsafe(48)
    row = RefreshToken.objects.create(
        user=user,
        token_hash=_hash(plain),
        family_id=family_id,
        expires_at=timezone.now() + timedelta(seconds=settings.AUTH_REFRESH_TOKEN_TTL_SECONDS),
        ip_address=ip_address,
        user_agent=(user_agent or "")[:1000],
    )
    return row, plain


def _session_for(user, refresh_plain: str) -> Session:
    return Session(
        user=user,
        access_token=issue_access_token(user, user_metadata=user.user_metadata or None),
        refresh_token=refresh_plain,
        expires_in=int(settings.AUTH_ACCESS_TOKEN_TTL_SECONDS),
    )


def start_session(user, *, ip_address=None, user_agent="") -> Session:
    _, plain = _new_refresh_row(user, uuid.uuid4(), ip_address=ip_address, user_agent=user_agent)
    return _session_for(user, plain)


def _revoke_family(family_id) -> None:
    RefreshToken.objects.filter(family_id=family_id, revoked_at__isnull=True).update(revoked_at=timezone.now())


def refresh_session(refresh_token: str, *, ip_address=None, user_agent="") -> Session:
    if not refresh_token:
        raise InvalidRefreshToken("Missing refresh token")

    reused_family_id = None
    with transaction.atomic():
        row = (
            RefreshToken.objects.select_for_update()
            .select_related("user")
            .filter(token_hash=_hash(refresh_token))
            .first()
        )
        if row is None:
            raise InvalidRefreshToken("Invalid refresh token")
        if row.revoked_at is not None:
            # Two browser tabs refreshing at the same moment is not theft; within the
            # grace window just refuse (the tab picks up the other tab's new token).
            grace = timedelta(seconds=settings.AUTH_REFRESH_REUSE_GRACE_SECONDS)
            if timezone.now() - row.revoked_at <= grace:
                raise InvalidRefreshToken("Refresh token already used")
            reused_family_id = row.family_id
        else:
            if row.expires_at <= timezone.now() or not row.user.is_active:
                raise InvalidRefreshToken("Refresh token expired")

            row.revoked_at = timezone.now()
            row.save(update_fields=["revoked_at"])
            _, plain = _new_refresh_row(row.user, row.family_id, ip_address=ip_address, user_agent=user_agent)

    if reused_family_id is not None:
        # Outside the atomic block so the revocation isn't rolled back by the raise.
        _revoke_family(reused_family_id)
        raise InvalidRefreshToken("Refresh token already used")

    return _session_for(row.user, plain)


def end_session(refresh_token: str, *, everywhere: bool = False) -> None:
    """Log out the session owning ``refresh_token`` (or every session of its user)."""
    row = RefreshToken.objects.filter(token_hash=_hash(refresh_token or "")).first()
    if row is None:
        return
    if everywhere:
        revoke_all_sessions(row.user)
    else:
        _revoke_family(row.family_id)


def revoke_all_sessions(user) -> int:
    return RefreshToken.objects.filter(user=user, revoked_at__isnull=True).update(revoked_at=timezone.now())
