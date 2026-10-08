"""
Password login, token refresh and logout. Response bodies mirror Supabase Auth
sessions (``access_token``, ``refresh_token``, ``expires_in``, ``user``…) so the
frontend can switch providers with minimal changes.
"""
import logging
import time

import jwt
from django.conf import settings
from django.utils import timezone
from drf_spectacular.utils import OpenApiResponse, extend_schema
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.throttling import ScopedRateThrottle
from rest_framework.views import APIView

from authentication.models import User
from authentication.serializers import (
    AuthErrorSerializer,
    LoginRequestSerializer,
    LogoutRequestSerializer,
    RefreshRequestSerializer,
    SessionSerializer,
    SupabaseExchangeRequestSerializer,
)
from authentication.sessions import (
    InvalidRefreshToken,
    end_session,
    refresh_session,
    start_session,
)
from authentication.tokens import TokenConfigurationError, verify_access_token

logger = logging.getLogger(__name__)


class AuthRateThrottle(ScopedRateThrottle):
    # ScopedRateThrottle.THROTTLE_RATES is captured at import time, which can predate
    # REST_FRAMEWORK being loaded (e.g. under pytest); read the rate per request instead.
    def get_rate(self):
        return settings.REST_FRAMEWORK.get("DEFAULT_THROTTLE_RATES", {}).get(self.scope)


def _client_ip(request):
    forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR", "")
    return forwarded_for.split(",")[0].strip() if forwarded_for else request.META.get("REMOTE_ADDR")


def _user_agent(request) -> str:
    return request.META.get("HTTP_USER_AGENT", "")


def _error(code: str, message: str, http_status: int) -> Response:
    return Response({"error": code, "message": message}, status=http_status)


def user_payload(user) -> dict:
    return {
        "id": str(user.supabase_uid),
        "email": user.email,
        "email_confirmed_at": user.email_verified_at,
        "user_metadata": user.user_metadata or {},
    }


def _session_response(session) -> Response:
    return Response(
        {
            "access_token": session.access_token,
            "token_type": "bearer",
            "expires_in": session.expires_in,
            "expires_at": int(time.time()) + session.expires_in,
            "refresh_token": session.refresh_token,
            "user": user_payload(session.user),
        },
        status=status.HTTP_200_OK,
    )


_AUTH_ERRORS = {
    400: OpenApiResponse(AuthErrorSerializer, description="Request rejected; see `error` code"),
    429: OpenApiResponse(description="Too many attempts"),
    503: OpenApiResponse(AuthErrorSerializer, description="AUTH_JWT_SECRET is not configured"),
}


def password_matches(user, password: str) -> bool:
    try:
        return user.check_password(password)
    except ValueError:
        # The stored hash uses an algorithm whose library isn't installed (e.g. argon2).
        logger.exception("[Auth] Cannot verify password hash for user pk=%s", user.pk)
        return False


def _authenticate(email: str, password: str):
    candidates = list(User.objects.filter(email__iexact=email, is_active=True))
    if not candidates:
        # Hash anyway so response time doesn't reveal whether the email exists.
        User().set_password(password)
        return None
    for user in candidates:
        if password_matches(user, password):
            return user
    return None


class LoginView(APIView):
    """POST {email, password} -> session."""

    permission_classes = [AllowAny]
    authentication_classes = []
    throttle_classes = [AuthRateThrottle]
    throttle_scope = "auth_login"

    @extend_schema(
        tags=["auth"],
        summary="Log in with email and password",
        auth=[],
        request=LoginRequestSerializer,
        responses={200: SessionSerializer, **_AUTH_ERRORS},
    )
    def post(self, request):
        email = (request.data.get("email") or "").strip().lower()
        password = request.data.get("password") or ""
        if not email or not password:
            return _error("missing_credentials", "Email and password are required", status.HTTP_400_BAD_REQUEST)

        user = _authenticate(email, password)
        if user is None:
            return _error("invalid_credentials", "Invalid login credentials", status.HTTP_400_BAD_REQUEST)
        if user.email_verified_at is None:
            return _error("email_not_confirmed", "Email not confirmed", status.HTTP_400_BAD_REQUEST)

        try:
            session = start_session(user, ip_address=_client_ip(request), user_agent=_user_agent(request))
        except TokenConfigurationError:
            logger.error("[Auth][login] AUTH_JWT_SECRET is not configured")
            return _error("auth_not_configured", "Login is not available", status.HTTP_503_SERVICE_UNAVAILABLE)

        # .update() keeps routine logins out of object history.
        User.objects.filter(pk=user.pk).update(last_login=timezone.now())
        return _session_response(session)


class TokenRefreshView(APIView):
    """POST {refresh_token} -> new session (the old refresh token stops working)."""

    permission_classes = [AllowAny]
    authentication_classes = []
    throttle_classes = [AuthRateThrottle]
    throttle_scope = "auth_refresh"

    @extend_schema(
        tags=["auth"],
        summary="Exchange a refresh token for a new session",
        auth=[],
        request=RefreshRequestSerializer,
        responses={200: SessionSerializer, **_AUTH_ERRORS},
    )
    def post(self, request):
        try:
            session = refresh_session(
                request.data.get("refresh_token") or "",
                ip_address=_client_ip(request),
                user_agent=_user_agent(request),
            )
        except InvalidRefreshToken as exc:
            return _error("invalid_grant", str(exc), status.HTTP_400_BAD_REQUEST)
        except TokenConfigurationError:
            logger.error("[Auth][refresh] AUTH_JWT_SECRET is not configured")
            return _error("auth_not_configured", "Login is not available", status.HTTP_503_SERVICE_UNAVAILABLE)
        return _session_response(session)


class SupabaseSessionExchangeView(APIView):
    """
    POST {access_token: <Supabase access token>} -> session. Lets browsers that were
    logged in through Supabase Auth switch to backend sessions without logging in again.
    Only works while AUTH_ACCEPT_SUPABASE_TOKENS is on.
    """

    permission_classes = [AllowAny]
    authentication_classes = []
    throttle_classes = [AuthRateThrottle]
    throttle_scope = "auth_refresh"

    @extend_schema(
        tags=["auth"],
        summary="Exchange a Supabase login for a backend session (one-time migration)",
        auth=[],
        request=SupabaseExchangeRequestSerializer,
        responses={200: SessionSerializer, **_AUTH_ERRORS},
    )
    def post(self, request):
        token = (request.data.get("access_token") or "").strip()
        if not token:
            return _error("invalid_grant", "access_token is required", status.HTTP_400_BAD_REQUEST)
        try:
            claims = verify_access_token(token)
        except TokenConfigurationError:
            logger.error("[Auth][supabase-exchange] JWT secret is not configured")
            return _error("auth_not_configured", "Login is not available", status.HTTP_503_SERVICE_UNAVAILABLE)
        except jwt.InvalidTokenError as exc:
            return _error("invalid_grant", f"Invalid token: {exc}", status.HTTP_400_BAD_REQUEST)

        # Real Supabase logins carry exp + session_id; spoof tokens and our own tokens don't qualify.
        if claims.get("iss") == settings.AUTH_JWT_ISSUER or not claims.get("exp") or not claims.get("session_id"):
            return _error("invalid_grant", "Not a Supabase login session", status.HTTP_400_BAD_REQUEST)

        user = User.objects.filter(supabase_uid=claims.get("sub"), is_active=True).first()
        if user is None:
            return _error("invalid_grant", "Account not found", status.HTTP_400_BAD_REQUEST)
        try:
            session = start_session(user, ip_address=_client_ip(request), user_agent=_user_agent(request))
        except TokenConfigurationError:
            logger.error("[Auth][supabase-exchange] AUTH_JWT_SECRET is not configured")
            return _error("auth_not_configured", "Login is not available", status.HTTP_503_SERVICE_UNAVAILABLE)
        return _session_response(session)


class LogoutView(APIView):
    """POST {refresh_token, scope: "local" | "global"} -> 204. Unknown tokens are ignored."""

    permission_classes = [AllowAny]
    authentication_classes = []

    @extend_schema(
        tags=["auth"],
        summary="Log out (this session, or every session with scope=global)",
        auth=[],
        request=LogoutRequestSerializer,
        responses={204: OpenApiResponse(description="Logged out"), 400: AuthErrorSerializer},
    )
    def post(self, request):
        scope = (request.data.get("scope") or "local").lower()
        if scope not in ("local", "global"):
            return _error("invalid_scope", "scope must be 'local' or 'global'", status.HTTP_400_BAD_REQUEST)
        end_session(request.data.get("refresh_token") or "", everywhere=scope == "global")
        return Response(status=status.HTTP_204_NO_CONTENT)
