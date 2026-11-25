"""
Google / Zoho sign-in (OAuth 2.0 authorization code flow, server side).

1. ``/auth/oauth/<provider>/authorize/`` redirects the browser to the provider.
2. The provider redirects back to ``/auth/oauth/<provider>/callback/``; we exchange the
   code for the user's profile, find or create the user, and redirect to the frontend
   with a one-time ``code``.
3. The frontend posts that code to ``/auth/oauth/exchange/`` and gets a session.
"""
from __future__ import annotations

import hashlib
import logging
import secrets
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from urllib.parse import urlencode, urlsplit

import requests
from django.conf import settings
from django.core import signing
from django.db import IntegrityError, transaction
from django.utils import timezone

from authentication.models import OAuthIdentity, OAuthLoginCode, User
from authentication.sessions import revoke_all_sessions

logger = logging.getLogger(__name__)

STATE_SALT = "authentication.oauth.state"
STATE_MAX_AGE_SECONDS = 10 * 60
LOGIN_CODE_TTL_SECONDS = 60
HTTP_TIMEOUT_SECONDS = 15

# Zoho sends users from other data centres back with ``accounts-server``; only these are trusted.
ZOHO_ACCOUNTS_HOSTS = {
    "accounts.zoho.com",
    "accounts.zoho.in",
    "accounts.zoho.eu",
    "accounts.zoho.com.au",
    "accounts.zoho.jp",
    "accounts.zohocloud.ca",
    "accounts.zoho.sa",
    "accounts.zoho.uk",
}


class OAuthError(Exception):
    """Login can't be completed; ``code`` is passed to the frontend as ``error``."""

    def __init__(self, message: str, code: str = "server_error"):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Provider:
    name: str
    client_id: str
    client_secret: str
    authorize_url: str
    token_url: str
    userinfo_url: str
    scope: str = "openid email profile"


@dataclass(frozen=True)
class Profile:
    subject: str
    email: str
    email_verified: bool
    metadata: dict = field(default_factory=dict)


def _zoho_provider(accounts_url: str) -> Provider:
    base = accounts_url.rstrip("/")
    return Provider(
        name="zoho",
        client_id=settings.ZOHO_LOGIN_CLIENT_ID,
        client_secret=settings.ZOHO_LOGIN_CLIENT_SECRET,
        authorize_url=f"{base}/oauth/v2/auth",
        token_url=f"{base}/oauth/v2/token",
        userinfo_url=f"{base}/oauth/v2/userinfo",
    )


def get_provider(name: str, *, zoho_accounts_server: str | None = None) -> Provider | None:
    """Return the configured provider, or None if unknown / not configured."""
    if name == "google":
        provider = Provider(
            name="google",
            client_id=settings.GOOGLE_LOGIN_CLIENT_ID,
            client_secret=settings.GOOGLE_LOGIN_CLIENT_SECRET,
            authorize_url="https://accounts.google.com/o/oauth2/v2/auth",
            token_url="https://oauth2.googleapis.com/token",
            userinfo_url="https://openidconnect.googleapis.com/v1/userinfo",
        )
    elif name == "zoho":
        accounts_url = settings.ZOHO_LOGIN_ACCOUNTS_URL
        if zoho_accounts_server:
            parts = urlsplit(zoho_accounts_server)
            if parts.scheme == "https" and parts.hostname in ZOHO_ACCOUNTS_HOSTS:
                accounts_url = f"https://{parts.hostname}"
        provider = _zoho_provider(accounts_url)
    else:
        return None
    if not provider.client_id or not provider.client_secret:
        return None
    return provider


def build_state(provider: str, redirect_to: str, nonce: str) -> str:
    return signing.dumps({"p": provider, "r": redirect_to, "n": nonce}, salt=STATE_SALT)


def parse_state(state: str) -> dict:
    try:
        data = signing.loads(state or "", salt=STATE_SALT, max_age=STATE_MAX_AGE_SECONDS)
    except signing.BadSignature as exc:
        raise OAuthError("Sign-in link expired. Please try again.", "invalid_request") from exc
    if not isinstance(data, dict) or not {"p", "r", "n"} <= data.keys():
        raise OAuthError("Sign-in link expired. Please try again.", "invalid_request")
    return data


def authorize_url(provider: Provider, *, redirect_uri: str, state: str) -> str:
    params = {
        "client_id": provider.client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": provider.scope,
        "state": state,
    }
    if provider.name == "google":
        params["prompt"] = "select_account"
    return f"{provider.authorize_url}?{urlencode(params)}"


def fetch_profile(provider: Provider, *, code: str, redirect_uri: str) -> Profile:
    try:
        token_resp = requests.post(
            provider.token_url,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": provider.client_id,
                "client_secret": provider.client_secret,
            },
            headers={"Accept": "application/json"},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        token_body = token_resp.json() if token_resp.content else {}
        access_token = token_body.get("access_token")
        if token_resp.status_code != 200 or not access_token:
            logger.warning(
                "[Auth][oauth] %s token exchange failed status=%s error=%s",
                provider.name,
                token_resp.status_code,
                token_body.get("error"),
            )
            raise OAuthError(f"Unable to exchange external code ({provider.name})")

        info_resp = requests.get(
            provider.userinfo_url,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        info = info_resp.json() if info_resp.content else {}
    except (requests.RequestException, ValueError) as exc:
        logger.warning("[Auth][oauth] %s request failed: %s", provider.name, exc)
        raise OAuthError(f"Could not reach {provider.name}. Please try again.") from exc

    if info_resp.status_code != 200 or not info.get("sub"):
        logger.warning("[Auth][oauth] %s userinfo failed status=%s", provider.name, info_resp.status_code)
        raise OAuthError(f"Unable to read {provider.name} profile")

    name = info.get("name") or " ".join(filter(None, [info.get("given_name"), info.get("family_name")]))
    picture = info.get("picture")
    metadata = {k: v for k, v in {"full_name": name, "name": name, "avatar_url": picture, "picture": picture}.items() if v}
    return Profile(
        subject=str(info["sub"]),
        email=(info.get("email") or "").strip().lower(),
        # A missing flag is not proof of ownership; it would let the login take over a password account.
        email_verified=str(info.get("email_verified")).lower() == "true",
        metadata=metadata,
    )


def resolve_user(provider_name: str, profile: Profile) -> User:
    """Return the user for this provider account, linking or creating one by email."""
    now = timezone.now()
    with transaction.atomic():
        identity = (
            OAuthIdentity.objects.select_for_update()
            .select_related("user")
            .filter(provider=provider_name, subject=profile.subject)
            .first()
        )
        if identity is not None:
            if not identity.user.is_active:
                raise OAuthError("This account has been disabled", "access_denied")
            identity.last_login_at = now
            if profile.email:
                identity.email = profile.email
            identity.save(update_fields=["last_login_at", "email"])
            return identity.user

        if not profile.email or not profile.email_verified:
            raise OAuthError(f"Your {provider_name} email address is not verified", "access_denied")

        user = User.objects.filter(email__iexact=profile.email).first()
        created = False
        if user is None:
            try:
                with transaction.atomic():
                    user = User.objects.create_user(
                        supabase_uid=str(uuid.uuid4()),
                        email=profile.email,
                        email_verified_at=now,
                        user_metadata=profile.metadata,
                    )
                created = True
            except IntegrityError:
                # A concurrent sign-up or login created the account with this email first.
                user = User.objects.get(email__iexact=profile.email)
        if not created:
            if not user.is_active:
                raise OAuthError("This account has been disabled", "access_denied")
            if user.email_verified_at is None:
                # Whoever set a password on this unconfirmed account never proved they own the
                # address; the provider just did, so their password must not keep working.
                if user.has_usable_password():
                    user.set_unusable_password()
                    revoke_all_sessions(user)
                user.email_verified_at = now
                user.save(update_fields=["password", "email_verified_at"])

        OAuthIdentity.objects.create(
            user=user, provider=provider_name, subject=profile.subject, email=profile.email, last_login_at=now
        )
    return user


def _hash(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def issue_login_code(user: User) -> str:
    plain = secrets.token_urlsafe(32)
    OAuthLoginCode.objects.create(
        user=user,
        code_hash=_hash(plain),
        expires_at=timezone.now() + timedelta(seconds=LOGIN_CODE_TTL_SECONDS),
    )
    return plain


def redeem_login_code(code: str) -> User:
    now = timezone.now()
    with transaction.atomic():
        row = (
            OAuthLoginCode.objects.select_for_update()
            .select_related("user")
            .filter(code_hash=_hash(code or ""))
            .first()
        )
        if row is None or row.used_at is not None or row.expires_at <= now or not row.user.is_active:
            raise OAuthError("Login code is invalid or expired", "invalid_grant")
        row.used_at = now
        row.save(update_fields=["used_at"])
    return row.user
