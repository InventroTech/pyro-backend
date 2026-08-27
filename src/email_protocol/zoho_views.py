"""
Zoho Mail OAuth connect / status / disconnect for shipment email auto-fill.
"""

from __future__ import annotations

import logging
from urllib.parse import urlencode

from django.conf import settings
from django.shortcuts import redirect
from drf_spectacular.utils import OpenApiResponse, extend_schema
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from authz.permissions import IsTenantAuthenticated
from config.supabase_auth import SupabaseJWTAuthentication
from core.models import Tenant

from .models import ZohoMailConnection
from .zoho_mail_client import ZohoMailClient
from .zoho_oauth import (
    ZohoOAuthError,
    build_authorize_url,
    exchange_code_for_tokens,
    parse_oauth_state,
    resolve_mail_api_base,
    token_expiry_from_payload,
    zoho_oauth_configured,
)
from .zoho_shipment_sync import ensure_account_and_inbox, ensure_fresh_access_token

logger = logging.getLogger(__name__)


def _user_email(request) -> str:
    user = getattr(request, "user", None)
    return (getattr(user, "email", None) or "").strip()


class ZohoMailConnectView(APIView):
    """
    GET /email/zoho/connect/

    Returns the Zoho OAuth authorize URL for one-time mailbox consent.
    Frontend should redirect the browser to ``authorize_url``.
    """

    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    @extend_schema(
        summary="Start Zoho Mail OAuth connect",
        responses={200: OpenApiResponse(description="Authorize URL")},
        tags=["Email / Zoho"],
    )
    def get(self, request, *args, **kwargs):
        if not zoho_oauth_configured():
            return Response(
                {
                    "error": (
                        "Zoho OAuth is not configured on the server. "
                        "Set ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_OAUTH_REDIRECT_URI."
                    )
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        try:
            url = build_authorize_url(
                tenant_id=str(request.tenant.id),
                user_email=_user_email(request),
            )
        except ZohoOAuthError as exc:
            return Response({"error": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        return Response(
            {
                "authorize_url": url,
                "configured": True,
                "scopes": (getattr(settings, "ZOHO_OAUTH_SCOPES", "") or "").strip(),
            },
            status=status.HTTP_200_OK,
        )


class ZohoMailCallbackView(APIView):
    """
    GET /email/zoho/callback/?code=...&state=...&accounts-server=...&location=...

    Zoho redirects here after consent. Stores refresh token for the tenant in ``state``.
    """

    authentication_classes = []
    permission_classes = [AllowAny]

    @extend_schema(
        summary="Zoho Mail OAuth callback",
        responses={302: OpenApiResponse(description="Redirect to frontend")},
        tags=["Email / Zoho"],
    )
    def get(self, request, *args, **kwargs):
        error = request.query_params.get("error")
        if error:
            return self._finish(ok=False, detail=error)

        code = (request.query_params.get("code") or "").strip()
        state = (request.query_params.get("state") or "").strip()
        if not code or not state:
            return self._finish(ok=False, detail="missing_code_or_state")

        try:
            state_data = parse_oauth_state(state)
            tenant_id = state_data["tenant_id"]
            tenant = Tenant.objects.filter(id=tenant_id).first()
            if not tenant:
                return self._finish(ok=False, detail="unknown_tenant")

            accounts_server = request.query_params.get("accounts-server") or request.query_params.get(
                "accounts_server"
            )
            location = request.query_params.get("location")
            accounts_base, mail_api = resolve_mail_api_base(accounts_server, location)

            tokens = exchange_code_for_tokens(code=code, accounts_base_url=accounts_base)
            refresh = (tokens.get("refresh_token") or "").strip()
            access = (tokens.get("access_token") or "").strip()
            if not refresh:
                existing = ZohoMailConnection.objects.filter(tenant=tenant).first()
                if existing and existing.refresh_token:
                    refresh = existing.refresh_token
                else:
                    return self._finish(
                        ok=False,
                        detail="no_refresh_token_prompt_consent",
                    )

            connection, _created = ZohoMailConnection.objects.update_or_create(
                tenant=tenant,
                defaults={
                    "refresh_token": refresh,
                    "access_token": access,
                    "access_token_expires_at": token_expiry_from_payload(tokens),
                    "accounts_base_url": accounts_base,
                    "mail_api_base_url": mail_api,
                    "is_active": True,
                    "connected_by_email": (state_data.get("user_email") or "")[:254],
                },
            )

            try:
                token = ensure_fresh_access_token(connection)
                client = ZohoMailClient(
                    access_token=token,
                    mail_api_base_url=connection.mail_api_base_url,
                )
                ensure_account_and_inbox(connection, client)
            except Exception:
                logger.exception("Zoho connect: account resolve failed tenant=%s", tenant_id)

            return self._finish(ok=True, detail="connected", email=connection.email_address)
        except ZohoOAuthError as exc:
            logger.warning("Zoho OAuth callback failed: %s", exc)
            return self._finish(ok=False, detail=str(exc))
        except Exception:
            logger.exception("Zoho OAuth callback unexpected error")
            return self._finish(ok=False, detail="server_error")

    def _finish(self, *, ok: bool, detail: str = "", email: str = ""):
        frontend = (getattr(settings, "ZOHO_OAUTH_SUCCESS_REDIRECT", "") or "").strip()
        if frontend:
            sep = "&" if "?" in frontend else "?"
            qs = urlencode(
                {
                    "zoho_mail": "ok" if ok else "error",
                    "detail": detail[:200],
                    "email": email,
                }
            )
            return redirect(f"{frontend}{sep}{qs}")
        if ok:
            return Response(
                {"success": True, "detail": detail, "email": email},
                status=status.HTTP_200_OK,
            )
        return Response(
            {"success": False, "error": detail},
            status=status.HTTP_400_BAD_REQUEST,
        )


class ZohoMailStatusView(APIView):
    """GET /email/zoho/status/ — connection status for current tenant."""

    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    @extend_schema(summary="Zoho Mail connection status", tags=["Email / Zoho"])
    def get(self, request, *args, **kwargs):
        conn = ZohoMailConnection.objects.filter(tenant=request.tenant).first()
        return Response(
            {
                "configured": zoho_oauth_configured(),
                "connected": bool(conn and conn.is_active and conn.refresh_token),
                "email_address": (conn.email_address if conn else "") or "",
                "is_active": bool(conn.is_active) if conn else False,
                "last_synced_at": conn.last_synced_at.isoformat() if conn and conn.last_synced_at else None,
                "connected_by_email": (conn.connected_by_email if conn else "") or "",
            },
            status=status.HTTP_200_OK,
        )


class ZohoMailDisconnectView(APIView):
    """POST /email/zoho/disconnect/ — deactivate and clear tokens for tenant."""

    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    @extend_schema(summary="Disconnect Zoho Mail", tags=["Email / Zoho"])
    def post(self, request, *args, **kwargs):
        conn = ZohoMailConnection.objects.filter(tenant=request.tenant).first()
        if not conn:
            return Response({"success": True, "detail": "not_connected"}, status=status.HTTP_200_OK)
        conn.is_active = False
        conn.refresh_token = ""
        conn.access_token = ""
        conn.access_token_expires_at = None
        conn.save(
            update_fields=[
                "is_active",
                "refresh_token",
                "access_token",
                "access_token_expires_at",
                "updated_at",
            ]
        )
        return Response({"success": True, "detail": "disconnected"}, status=status.HTTP_200_OK)


class ZohoMailSyncNowView(APIView):
    """
    POST /email/zoho/sync-now/

    Manually trigger one inbox sync for the current tenant (useful for testing).
    """

    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    @extend_schema(summary="Sync Zoho shipment emails now", tags=["Email / Zoho"])
    def post(self, request, *args, **kwargs):
        from .zoho_shipment_sync import sync_zoho_shipment_emails

        conn = ZohoMailConnection.objects.filter(tenant=request.tenant, is_active=True).first()
        if not conn or not conn.refresh_token:
            return Response(
                {"error": "Zoho Mail is not connected for this tenant."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            result = sync_zoho_shipment_emails(
                conn, max_messages=int(request.data.get("max_messages") or 40)
            )
        except ZohoOAuthError as exc:
            return Response({"error": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception:
            logger.exception("Zoho sync-now failed tenant=%s", request.tenant.id)
            return Response(
                {"error": "Zoho sync failed."},
                status=status.HTTP_502_BAD_GATEWAY,
            )
        return Response(result, status=status.HTTP_200_OK)
