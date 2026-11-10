import logging
import time
from datetime import datetime, timezone as dt_timezone

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import DatabaseError, IntegrityError, transaction
from django.db.models import Q
from django.utils import timezone
from jwt import ExpiredSignatureError, InvalidTokenError
from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions

from authentication.tokens import TokenConfigurationError, verify_access_token

logger = logging.getLogger(__name__)

User = get_user_model()

# Sign-in times further ahead than this are treated as bogus, not clock skew.
SIGN_IN_MAX_CLOCK_SKEW_SECONDS = 300

def _get_bearer(request):
    auth = request.META.get("HTTP_AUTHORIZATION") or request.headers.get("Authorization", "")
    if not auth or not auth.lower().startswith("bearer "):
        return None
    return auth.split(" ", 1)[1].strip()

def _verify_jwt(token: str) -> dict:
    try:
        return verify_access_token(token)
    except TokenConfigurationError:
        raise exceptions.AuthenticationFailed("JWT secret not configured")
    except ExpiredSignatureError:
        raise exceptions.AuthenticationFailed("Token expired")
    except InvalidTokenError as e:
        raise exceptions.AuthenticationFailed(f"Invalid token: {e}")

def _signed_in_at(claims):
    """
    Sign-in time from Supabase's amr claim ([{"method": ..., "timestamp": ...}]).
    Refreshed tokens keep the original timestamp; spoof tokens have no amr.
    Malformed, non-positive or far-future timestamps are ignored.
    """
    amr = claims.get("amr")
    if not isinstance(amr, (list, tuple)):
        return None
    latest_allowed = time.time() + SIGN_IN_MAX_CLOCK_SKEW_SECONDS
    stamps = [
        entry.get("timestamp")
        for entry in amr
        if isinstance(entry, dict)
    ]
    stamps = [
        s for s in stamps
        if isinstance(s, (int, float)) and not isinstance(s, bool) and 0 < s <= latest_allowed
    ]
    if not stamps:
        return None
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

def _get_or_create_profile(claims):
    sub = claims.get("sub") or claims.get("user_id")
    if not sub:
        raise exceptions.AuthenticationFailed("Token missing 'sub'")
    email = (claims.get("email") or "").lower() or None

    if claims.get("iss") == settings.AUTH_JWT_ISSUER or settings.AUTH_PROVIDER == "django":
        # Django owns the accounts: a token (ours, or a leftover Supabase one) only works for
        # an existing active user, so a deleted or disabled account is never recreated.
        user = User.objects.filter(supabase_uid=sub, is_active=True).first()
        if user is None:
            raise exceptions.AuthenticationFailed("User not found")
        return user

    # Mirror user locally (no password; identity of record is Supabase)
    if email and User.objects.filter(email__iexact=email).exclude(supabase_uid=sub).exists():
        # A stale row for a previous Supabase account holds this address; emails are unique.
        logger.warning("[Auth] Email of Supabase user %s belongs to another local user", sub)
        email = None
    try:
        with transaction.atomic():
            user, _ = User.objects.get_or_create(
                supabase_uid=sub,
                defaults={"email": email, "is_active": True},
            )
    except IntegrityError:
        user = User.objects.get(supabase_uid=sub)
    if email and user.email != email:
        user.email = email
        user.save(update_fields=["email"])
    signed_in_at = _signed_in_at(claims)
    if signed_in_at:
        _record_sign_in(user, signed_in_at)
    return user

class SupabaseJWTAuthentication(BaseAuthentication):
    def authenticate(self, request):
        token = _get_bearer(request)
        if not token:
            return None
        claims = _verify_jwt(token)
        user = _get_or_create_profile(claims)

        request.jwt_claims = claims
        request.token = token
        return (user, None)
