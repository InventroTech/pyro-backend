import os
import jwt
from jwt import ExpiredSignatureError, InvalidTokenError
from django.contrib.auth import get_user_model
from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions

SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET")
User = get_user_model()

def _get_bearer(request):
    auth = request.META.get("HTTP_AUTHORIZATION") or request.headers.get("Authorization", "")
    if not auth or not auth.lower().startswith("bearer "):
        return None
    return auth.split(" ", 1)[1].strip()

def _verify_jwt(token: str) -> dict:
    if not SUPABASE_JWT_SECRET:
        raise exceptions.AuthenticationFailed("JWT secret not configured")
    try:
        # Supabase tokens have aud='authenticated'; we’re not enforcing audience.
        return jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], options={"verify_aud": False})
    except ExpiredSignatureError:
        raise exceptions.AuthenticationFailed("Token expired")
    except InvalidTokenError as e:
        raise exceptions.AuthenticationFailed(f"Invalid token: {e}")

def _get_or_create_profile(claims):
    sub = claims.get("sub") or claims.get("user_id")
    if not sub:
        raise exceptions.AuthenticationFailed("Token missing 'sub'")
    email = (claims.get("email") or "").lower() or None

    # Mirror user locally (no password; identity of record is Supabase)
    user, created = User.objects.get_or_create(
        supabase_uid=sub,
        defaults={"email": email, "is_active": True},
    )
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
