"""
Sign-up with email confirmation, the current user's profile, and password change.
Companion to ``session_views`` (login / refresh / logout).
"""
import json
import logging
import uuid

from django.conf import settings
from django.contrib.auth.password_validation import get_password_validators, validate_password
from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from django.db import transaction
from drf_spectacular.utils import OpenApiResponse, extend_schema
from rest_framework import status
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from authentication.email_verification import (
    InvalidVerificationToken,
    confirm_email,
    send_confirmation_email,
)
from authentication.models import User
from authentication.serializers import (
    AuthErrorSerializer,
    AuthUserSerializer,
    ChangePasswordRequestSerializer,
    OkSerializer,
    ResendConfirmationRequestSerializer,
    SessionSerializer,
    SignupRequestSerializer,
    SignupResponseSerializer,
    UpdateMeRequestSerializer,
    VerifyEmailRequestSerializer,
)
from authentication.session_views import (
    AuthRateThrottle,
    _client_ip,
    _error,
    _session_response,
    _user_agent,
    password_matches,
    user_payload,
)
from authentication.sessions import revoke_all_sessions, start_session
from authentication.tokens import TokenConfigurationError
from config.supabase_auth import SupabaseJWTAuthentication

logger = logging.getLogger(__name__)

MAX_METADATA_BYTES = 16_000
SIGNUP_ACK = "If this email can be registered, a confirmation link has been sent."


def validate_user_password(password: str, user: User) -> None:
    """Raise ValidationError if ``password`` breaks the customer password policy."""
    validate_password(
        password,
        user=user,
        password_validators=get_password_validators(settings.AUTH_USER_PASSWORD_VALIDATORS),
    )


def _password_error(password: str, user: User):
    try:
        validate_user_password(password, user)
    except ValidationError as exc:
        return _error("weak_password", " ".join(exc.messages), status.HTTP_400_BAD_REQUEST)
    return None


def _metadata_error(data):
    if not isinstance(data, dict):
        return _error("invalid_data", "data must be a JSON object", status.HTTP_400_BAD_REQUEST)
    if len(json.dumps(data)) > MAX_METADATA_BYTES:
        return _error("invalid_data", "data is too large", status.HTTP_400_BAD_REQUEST)
    return None


def _can_receive_confirmation(user: User) -> bool:
    # Users mirrored from Supabase have no usable password; they already have an account there.
    return user.is_active and user.email_verified_at is None and user.has_usable_password()


class SignupView(APIView):
    """
    POST {email, password, data?, redirect_to?}. Creates an unconfirmed account and emails
    a confirmation link. The response is the same whether or not the email is already
    registered, so it can't be used to discover accounts.
    """

    permission_classes = [AllowAny]
    authentication_classes = []
    throttle_classes = [AuthRateThrottle]
    throttle_scope = "auth_signup"

    @extend_schema(
        tags=["auth"],
        summary="Sign up with email and password (sends a confirmation email)",
        auth=[],
        request=SignupRequestSerializer,
        responses={
            200: SignupResponseSerializer,
            400: AuthErrorSerializer,
            429: OpenApiResponse(description="Too many attempts"),
            503: OpenApiResponse(AuthErrorSerializer, description="Confirmation email could not be sent"),
        },
    )
    def post(self, request):
        email = (request.data.get("email") or "").strip().lower()
        password = request.data.get("password") or ""
        data = request.data.get("data") or {}
        redirect_to = request.data.get("redirect_to")

        if not email or not password:
            return _error("missing_credentials", "Email and password are required", status.HTTP_400_BAD_REQUEST)
        try:
            validate_email(email)
        except ValidationError:
            return _error("invalid_email", "Enter a valid email address", status.HTTP_400_BAD_REQUEST)
        error = _metadata_error(data) or _password_error(password, User(email=email))
        if error:
            return error

        existing = User.objects.filter(email__iexact=email).order_by("pk").first()
        if existing is None:
            with transaction.atomic():
                user = User.objects.create_user(
                    supabase_uid=str(uuid.uuid4()), email=email, password=password, user_metadata=data
                )
            if not send_confirmation_email(user, redirect_to):
                return _error(
                    "email_send_failed",
                    "Error sending confirmation email",
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                )
        elif _can_receive_confirmation(existing):
            # Re-signing up never changes the stored password; it only re-sends the link.
            send_confirmation_email(existing, redirect_to)

        return Response({"user": None, "session": None, "message": SIGNUP_ACK}, status=status.HTTP_200_OK)


class VerifyEmailView(APIView):
    """POST {token} -> marks the email confirmed and logs the user in."""

    permission_classes = [AllowAny]
    authentication_classes = []
    throttle_classes = [AuthRateThrottle]
    throttle_scope = "auth_verify"

    @extend_schema(
        tags=["auth"],
        summary="Confirm an email address with the emailed link token",
        auth=[],
        request=VerifyEmailRequestSerializer,
        responses={
            200: SessionSerializer,
            400: AuthErrorSerializer,
            429: OpenApiResponse(description="Too many attempts"),
            503: AuthErrorSerializer,
        },
    )
    def post(self, request):
        try:
            user = confirm_email((request.data.get("token") or "").strip())
        except InvalidVerificationToken as exc:
            return _error("invalid_token", str(exc), status.HTTP_400_BAD_REQUEST)
        try:
            session = start_session(user, ip_address=_client_ip(request), user_agent=_user_agent(request))
        except TokenConfigurationError:
            logger.error("[Auth][verify-email] AUTH_JWT_SECRET is not configured")
            return _error("auth_not_configured", "Login is not available", status.HTTP_503_SERVICE_UNAVAILABLE)
        return _session_response(session)


class ResendConfirmationView(APIView):
    """POST {email, redirect_to?} -> always {"ok": true}."""

    permission_classes = [AllowAny]
    authentication_classes = []
    throttle_classes = [AuthRateThrottle]
    throttle_scope = "auth_signup"

    @extend_schema(
        tags=["auth"],
        summary="Re-send the sign-up confirmation email",
        auth=[],
        request=ResendConfirmationRequestSerializer,
        responses={200: OkSerializer, 429: OpenApiResponse(description="Too many attempts")},
    )
    def post(self, request):
        email = (request.data.get("email") or "").strip().lower()
        if email:
            user = User.objects.filter(email__iexact=email).order_by("pk").first()
            if user is not None and _can_receive_confirmation(user):
                send_confirmation_email(user, request.data.get("redirect_to"))
        return Response({"ok": True})


class MeView(APIView):
    """GET -> the logged-in user. PATCH {data} -> merge keys into user_metadata."""

    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(tags=["auth"], summary="Get the logged-in user", responses={200: AuthUserSerializer})
    def get(self, request):
        return Response(user_payload(request.user))

    @extend_schema(
        tags=["auth"],
        summary="Update the logged-in user's profile data",
        request=UpdateMeRequestSerializer,
        responses={200: AuthUserSerializer, 400: AuthErrorSerializer},
    )
    def patch(self, request):
        data = request.data.get("data")
        if data is None:
            return _error("invalid_data", "data is required", status.HTTP_400_BAD_REQUEST)
        merged = {**(request.user.user_metadata or {}), **data} if isinstance(data, dict) else data
        error = _metadata_error(merged)
        if error:
            return error
        user = request.user
        user.user_metadata = merged
        user.save(update_fields=["user_metadata"])
        return Response(user_payload(user))


class ChangePasswordView(APIView):
    """
    POST {current_password, password}. Logs out every other session and returns a new one.
    ``current_password`` is not needed for accounts without a password yet (e.g. Google sign-in).
    """

    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsAuthenticated]
    throttle_classes = [AuthRateThrottle]
    throttle_scope = "auth_login"

    @extend_schema(
        tags=["auth"],
        summary="Change the logged-in user's password",
        request=ChangePasswordRequestSerializer,
        responses={
            200: SessionSerializer,
            400: AuthErrorSerializer,
            429: OpenApiResponse(description="Too many attempts"),
            503: AuthErrorSerializer,
        },
    )
    def post(self, request):
        user = request.user
        password = request.data.get("password") or ""
        if not password:
            return _error("missing_password", "New password is required", status.HTTP_400_BAD_REQUEST)
        if user.has_usable_password() and not password_matches(user, request.data.get("current_password") or ""):
            return _error("invalid_credentials", "Current password is incorrect", status.HTTP_400_BAD_REQUEST)
        error = _password_error(password, user)
        if error:
            return error

        user.set_password(password)
        user.save(update_fields=["password"])
        revoke_all_sessions(user)
        try:
            session = start_session(user, ip_address=_client_ip(request), user_agent=_user_agent(request))
        except TokenConfigurationError:
            logger.error("[Auth][change-password] AUTH_JWT_SECRET is not configured")
            return _error("auth_not_configured", "Login is not available", status.HTTP_503_SERVICE_UNAVAILABLE)
        return _session_response(session)
