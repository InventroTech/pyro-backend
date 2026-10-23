import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from jwt import ExpiredSignatureError, InvalidTokenError
from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions

from authentication.tokens import TokenConfigurationError, verify_access_token

logger = logging.getLogger(__name__)

User = get_user_model()

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
