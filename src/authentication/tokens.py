"""
Access-token issuing and verification.

Two issuers are accepted while login moves off Supabase Auth:

- tokens minted by this backend (``iss == AUTH_JWT_ISSUER``), signed with ``AUTH_JWT_SECRET``
  (logins, and spoof tokens once ``AUTH_PROVIDER`` is "django");
- Supabase Auth tokens (and spoof tokens before that), signed with ``SUPABASE_JWT_SECRET``,
  only while ``AUTH_ACCEPT_SUPABASE_TOKENS`` is on.

Minted tokens keep the Supabase claim layout (``sub``, ``email``, ``aud``, ``role``,
``user_data``) so the frontend and tenant resolution read them unchanged.
"""
from __future__ import annotations

import time
import uuid

import jwt
from django.conf import settings

ALGORITHM = "HS256"
AUDIENCE = "authenticated"


class TokenConfigurationError(Exception):
    """A signing secret needed for this token is not configured."""


def _issuer() -> str:
    return getattr(settings, "AUTH_JWT_ISSUER", "pyro-backend")


def verify_access_token(token: str) -> dict:
    """
    Verify ``token`` and return its claims.

    Raises ``jwt.ExpiredSignatureError`` / ``jwt.InvalidTokenError`` for bad tokens and
    ``TokenConfigurationError`` when the matching secret is missing.
    """
    # The unverified issuer only selects which secret to verify with; the signature
    # (and, for our own tokens, the issuer) is then fully verified.
    unverified = jwt.decode(token, options={"verify_signature": False})
    decode_options = {"verify_aud": False}

    if unverified.get("iss") == _issuer():
        secret = getattr(settings, "AUTH_JWT_SECRET", "")
        if not secret:
            raise TokenConfigurationError("AUTH_JWT_SECRET is not configured")
        return jwt.decode(
            token,
            secret,
            algorithms=[ALGORITHM],
            issuer=_issuer(),
            options={**decode_options, "require": ["exp", "iat", "sub"]},
        )

    if not getattr(settings, "AUTH_ACCEPT_SUPABASE_TOKENS", True):
        raise jwt.InvalidIssuerError("Supabase-issued tokens are no longer accepted")

    secret = getattr(settings, "SUPABASE_JWT_SECRET", "")
    if not secret:
        raise TokenConfigurationError("SUPABASE_JWT_SECRET is not configured")
    return jwt.decode(token, secret, algorithms=[ALGORITHM], options=decode_options)


def build_user_data_claims(user_id) -> dict | None:
    """
    ``user_data`` claim for ``user_id``: same keys the Supabase access-token hook
    (``public.add_membership_claims_to_jwt``) sets, plus ``role_key`` and
    ``tenant_membership_id`` as in spoof tokens. ``None`` when the user has no active
    membership.

    Unlike the hook's unordered ``LIMIT 1``, the oldest active membership wins so
    multi-tenant users always get the same tenant.
    """
    from authz.models import TenantMembership

    membership = (
        TenantMembership.objects.filter(user_id=user_id, is_active=True)
        .select_related("role")
        .order_by("created_at", "id")
        .first()
    )
    if membership is None:
        return None
    return {
        "user_id": str(membership.user_id),
        "role_id": str(membership.role_id),
        "role_key": membership.role.key,
        "tenant_id": str(membership.tenant_id),
        "tenant_membership_id": str(membership.id),
    }


def sign_access_token(claims: dict) -> str:
    """
    Sign ``claims`` (must include ``sub``) as one of our access tokens, adding issuer,
    audience, role and a lifetime of ``AUTH_ACCESS_TOKEN_TTL_SECONDS``.
    """
    secret = getattr(settings, "AUTH_JWT_SECRET", "")
    if not secret:
        raise TokenConfigurationError("AUTH_JWT_SECRET is not configured")

    issued_at = int(time.time())
    return jwt.encode(
        {
            "aud": AUDIENCE,
            "role": "authenticated",
            **claims,
            "iss": _issuer(),
            "iat": issued_at,
            "exp": issued_at + int(getattr(settings, "AUTH_ACCESS_TOKEN_TTL_SECONDS", 3600)),
            "jti": uuid.uuid4().hex,
        },
        secret,
        algorithm=ALGORITHM,
    )


def issue_access_token(user, *, user_metadata: dict | None = None) -> str:
    """Mint a short-lived access token for ``user`` (an ``authentication.User``)."""
    sub = str(user.supabase_uid)
    claims = {"sub": sub}
    if user.email:
        claims["email"] = user.email.lower()
    if user_metadata:
        claims["user_metadata"] = user_metadata

    user_data = build_user_data_claims(sub)
    if user_data:
        claims["user_data"] = user_data

    return sign_access_token(claims)
