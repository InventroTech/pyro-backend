"""Google / Zoho sign-in endpoints. See ``authentication.oauth`` for the flow."""
import hmac
import logging
import secrets

from django.conf import settings
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.utils import timezone
from drf_spectacular.utils import OpenApiParameter, OpenApiResponse, extend_schema
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.views import APIView

from authentication.models import User
from authentication.oauth import (
    OAuthError,
    authorize_url,
    build_state,
    fetch_profile,
    get_provider,
    issue_login_code,
    parse_state,
    redeem_login_code,
    resolve_user,
)
from authentication.redirects import safe_redirect_url, with_query
from authentication.serializers import AuthErrorSerializer, OAuthExchangeRequestSerializer, SessionSerializer
from authentication.session_views import (
    AuthRateThrottle,
    _client_ip,
    _error,
    _session_response,
    _user_agent,
)
from authentication.sessions import start_session
from authentication.tokens import TokenConfigurationError

logger = logging.getLogger(__name__)

NONCE_COOKIE = "pyro_oauth_nonce"
NONCE_COOKIE_PATH = "/auth/oauth/"


def _callback_uri(request, provider: str) -> str:
    path = reverse("authentication:oauth-callback", args=[provider])
    base = settings.AUTH_PUBLIC_BASE_URL.rstrip("/")
    return f"{base}{path}" if base else request.build_absolute_uri(path)


def _error_redirect(redirect_to: str, exc: OAuthError) -> HttpResponseRedirect:
    response = HttpResponseRedirect(with_query(redirect_to, {"error": exc.code, "error_description": str(exc)}))
    response.delete_cookie(NONCE_COOKIE, path=NONCE_COOKIE_PATH)
    return response


_PROVIDER_PARAM = OpenApiParameter("provider", str, OpenApiParameter.PATH, enum=["google", "zoho"])


class OAuthAuthorizeView(APIView):
    """GET ?redirect_to=<frontend callback URL> -> redirect to Google / Zoho."""

    permission_classes = [AllowAny]
    authentication_classes = []

    @extend_schema(
        tags=["auth"],
        summary="Start Google / Zoho sign-in (open in the browser, not via fetch)",
        auth=[],
        parameters=[_PROVIDER_PARAM, OpenApiParameter("redirect_to", str, description="Frontend callback URL")],
        responses={302: OpenApiResponse(description="Redirect to the provider")},
    )
    def get(self, request, provider):
        redirect_to = safe_redirect_url(request.query_params.get("redirect_to"))
        config = get_provider(provider)
        if config is None:
            return _error_redirect(redirect_to, OAuthError(f"{provider} sign-in is not enabled", "invalid_request"))

        nonce = secrets.token_urlsafe(16)
        callback_uri = _callback_uri(request, provider)
        response = HttpResponseRedirect(
            authorize_url(config, redirect_uri=callback_uri, state=build_state(provider, redirect_to, nonce))
        )
        # Ties the callback to the browser that started the login (stops login CSRF).
        response.set_cookie(
            NONCE_COOKIE,
            nonce,
            max_age=600,
            path=NONCE_COOKIE_PATH,
            httponly=True,
            secure=callback_uri.startswith("https://"),
            samesite="Lax",
        )
        return response


class OAuthCallbackView(APIView):
    """Provider redirects here; we redirect to the frontend with ?code=… or ?error=…"""

    permission_classes = [AllowAny]
    authentication_classes = []

    @extend_schema(
        tags=["auth"],
        summary="Google / Zoho redirect target (called by the provider)",
        auth=[],
        parameters=[_PROVIDER_PARAM],
        responses={302: OpenApiResponse(description="Redirect to the frontend")},
    )
    def get(self, request, provider):
        params = request.query_params
        try:
            state = parse_state(params.get("state"))
        except OAuthError as exc:
            return _error_redirect(settings.AUTH_SITE_URL, exc)
        redirect_to = safe_redirect_url(state["r"])

        try:
            if state["p"] != provider or not hmac.compare_digest(
                request.COOKIES.get(NONCE_COOKIE, ""), state["n"]
            ):
                raise OAuthError("Sign-in session expired. Please try again.", "invalid_request")
            if params.get("error"):
                raise OAuthError(params.get("error_description") or params["error"], "access_denied")
            config = get_provider(provider, zoho_accounts_server=params.get("accounts-server"))
            if config is None or not params.get("code"):
                raise OAuthError(f"{provider} sign-in is not enabled", "invalid_request")

            profile = fetch_profile(config, code=params["code"], redirect_uri=_callback_uri(request, provider))
            user = resolve_user(provider, profile)
        except OAuthError as exc:
            logger.info("[Auth][oauth] %s callback rejected: %s", provider, exc)
            return _error_redirect(redirect_to, exc)

        response = HttpResponseRedirect(with_query(redirect_to, {"code": issue_login_code(user)}))
        response.delete_cookie(NONCE_COOKIE, path=NONCE_COOKIE_PATH)
        return response


class OAuthExchangeView(APIView):
    """POST {code} -> session."""

    permission_classes = [AllowAny]
    authentication_classes = []
    throttle_classes = [AuthRateThrottle]
    throttle_scope = "auth_verify"

    @extend_schema(
        tags=["auth"],
        summary="Exchange the one-time code from a Google / Zoho sign-in for a session",
        auth=[],
        request=OAuthExchangeRequestSerializer,
        responses={200: SessionSerializer, 400: AuthErrorSerializer, 503: AuthErrorSerializer},
    )
    def post(self, request):
        try:
            user = redeem_login_code((request.data.get("code") or "").strip())
        except OAuthError as exc:
            return _error(exc.code, str(exc), status.HTTP_400_BAD_REQUEST)
        try:
            session = start_session(user, ip_address=_client_ip(request), user_agent=_user_agent(request))
        except TokenConfigurationError:
            logger.error("[Auth][oauth-exchange] AUTH_JWT_SECRET is not configured")
            return _error("auth_not_configured", "Login is not available", status.HTTP_503_SERVICE_UNAVAILABLE)
        User.objects.filter(pk=user.pk).update(last_login=timezone.now())
        return _session_response(session)
