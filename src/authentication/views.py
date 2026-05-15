import logging
import os
import requests
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny

# Create your views here.
# Logging and config
logger = logging.getLogger(__name__)
SUPABASE_PROJECT_URL = os.environ.get('SUPABASE_PROJECT_URL')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY')

class SupabaseAuthCheckView(APIView):
    """
    Authenticates with Supabase using email & password.
    Returns user info if valid, else error.
    """
    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        email = request.data.get("email")
        password = request.data.get("password")
        if not email or not password:
            return Response({"error": "Email and password are required."}, status=status.HTTP_400_BAD_REQUEST)

        url = f"{SUPABASE_PROJECT_URL}/auth/v1/token?grant_type=password"
        headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Content-Type": "application/json",
        }
        data = {"email": email, "password": password}
        try:
            r = requests.post(url, json=data, headers=headers)
        except Exception as e:
            logger.exception("Failed to call Supabase: %s", e)
            return Response({"error": "Failed to connect to Supabase."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

        if r.status_code == 200:
            out = r.json()
            return Response({
                "valid": True,
                "user_id": out.get("user", {}).get("id"),
                "access_token": out.get("access_token"),
                "email": out.get("user", {}).get("email")
            })
        else:
            error_body = {}
            try:
                error_body = r.json()
            except Exception:
                pass
            return Response({
                "valid": False,
                "error": error_body.get("error", "Login failed"),
                "message": error_body.get("msg") or error_body.get("message")
            }, status=status.HTTP_401_UNAUTHORIZED)


class SupabasePasswordRecoverView(APIView):
    """
    Triggers Supabase GoTrue POST /auth/v1/recover (password recovery email).
    Uses server-side anon credentials; optional redirect_to is forwarded as a query param.
    """

    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        if not SUPABASE_PROJECT_URL or not SUPABASE_ANON_KEY:
            logger.error("SUPABASE_PROJECT_URL or SUPABASE_ANON_KEY not configured")
            return Response(
                {"error": "Password recovery is not configured."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        email = (request.data.get("email") or "").strip()
        redirect_to = (request.data.get("redirect_to") or "").strip() or None

        if not email:
            return Response({"error": "Email is required."}, status=status.HTTP_400_BAD_REQUEST)

        url = f"{SUPABASE_PROJECT_URL.rstrip('/')}/auth/v1/recover"
        headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "Content-Type": "application/json",
        }
        payload = {"email": email}
        params = {}
        if redirect_to:
            params["redirect_to"] = redirect_to

        try:
            r = requests.post(url, json=payload, headers=headers, params=params or None, timeout=15)
        except requests.RequestException as e:
            logger.exception("Failed to call Supabase recover: %s", e)
            return Response(
                {"error": "Failed to connect to authentication service."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        # Supabase returns 200 with {} even when user is unknown (avoid email enumeration)
        if r.status_code == 200:
            return Response({"ok": True})

        error_body = {}
        try:
            error_body = r.json()
        except Exception:
            pass
        logger.warning(
            "Supabase recover failed status=%s body=%s",
            r.status_code,
            error_body or r.text[:500],
        )
        return Response(
            {
                "error": error_body.get("error") or error_body.get("msg") or error_body.get("message") or "Request failed",
            },
            status=r.status_code if r.status_code < 500 else status.HTTP_502_BAD_GATEWAY,
        )
