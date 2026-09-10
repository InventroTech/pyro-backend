"""
Zoho Mail OAuth 2.0 (authorization code + offline refresh token).

Register a Server-based Application in the Zoho API Console and set:
  ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_OAUTH_REDIRECT_URI
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

import requests
from django.conf import settings
from django.core import signing
from django.utils import timezone

logger = logging.getLogger(__name__)

OAUTH_STATE_SALT = "email_protocol.zoho_oauth"
OAUTH_STATE_MAX_AGE = 60 * 15  # 15 minutes

DEFAULT_SCOPES = (
    "ZohoMail.accounts.READ,"
    "ZohoMail.messages.READ,"
    "ZohoMail.folders.READ"
)

# Map Zoho OAuth `location` / accounts-server host → Mail API base.
_MAIL_API_BY_ACCOUNTS_HOST = {
    "accounts.zoho.com": "https://mail.zoho.com/api",
    "accounts.zoho.in": "https://mail.zoho.in/api",
    "accounts.zoho.eu": "https://mail.zoho.eu/api",
    "accounts.zoho.com.au": "https://mail.zoho.com.au/api",
    "accounts.zoho.jp": "https://mail.zoho.jp/api",
    "accounts.zohocloud.ca": "https://mail.zohocloud.ca/api",
}


class ZohoOAuthError(Exception):
    """Client / config error for Zoho OAuth."""


def zoho_oauth_configured() -> bool:
    return bool(
        (getattr(settings, "ZOHO_CLIENT_ID", "") or "").strip()
        and (getattr(settings, "ZOHO_CLIENT_SECRET", "") or "").strip()
        and (getattr(settings, "ZOHO_OAUTH_REDIRECT_URI", "") or "").strip()
    )


def _accounts_base() -> str:
    return (getattr(settings, "ZOHO_ACCOUNTS_BASE_URL", "") or "https://accounts.zoho.com").rstrip(
        "/"
    )


def _scopes() -> str:
    return (getattr(settings, "ZOHO_OAUTH_SCOPES", "") or DEFAULT_SCOPES).strip()


def build_oauth_state(*, tenant_id: str, user_email: str = "") -> str:
    return signing.dumps(
        {"tenant_id": str(tenant_id), "user_email": (user_email or "").strip()},
        salt=OAUTH_STATE_SALT,
    )


def parse_oauth_state(state: str) -> Dict[str, Any]:
    try:
        data = signing.loads(state, salt=OAUTH_STATE_SALT, max_age=OAUTH_STATE_MAX_AGE)
    except signing.BadSignature as exc:
        raise ZohoOAuthError("Invalid or expired OAuth state.") from exc
    if not isinstance(data, dict) or not data.get("tenant_id"):
        raise ZohoOAuthError("Invalid OAuth state payload.")
    return data


def build_authorize_url(*, tenant_id: str, user_email: str = "") -> str:
    if not zoho_oauth_configured():
        raise ZohoOAuthError(
            "Zoho OAuth is not configured. Set ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, "
            "and ZOHO_OAUTH_REDIRECT_URI."
        )
    params = {
        "client_id": settings.ZOHO_CLIENT_ID.strip(),
        "response_type": "code",
        "redirect_uri": settings.ZOHO_OAUTH_REDIRECT_URI.strip(),
        "scope": _scopes(),
        "access_type": "offline",
        "prompt": "consent",
        "state": build_oauth_state(tenant_id=tenant_id, user_email=user_email),
    }
    return f"{_accounts_base()}/oauth/v2/auth?{urlencode(params)}"


def resolve_mail_api_base(accounts_server: Optional[str], location: Optional[str] = None) -> Tuple[str, str]:
    """
    Return (accounts_base_url, mail_api_base_url) for the tenant's Zoho DC.
    """
    accounts = (accounts_server or "").strip().rstrip("/") or _accounts_base()
    if accounts.startswith("http"):
        host = accounts.split("://", 1)[-1].split("/", 1)[0].lower()
    else:
        host = accounts.lower()
        accounts = f"https://{host}"

    mail_api = _MAIL_API_BY_ACCOUNTS_HOST.get(host)
    if not mail_api and location:
        loc = location.strip().lower()
        guess = {
            "us": "https://mail.zoho.com/api",
            "in": "https://mail.zoho.in/api",
            "eu": "https://mail.zoho.eu/api",
            "au": "https://mail.zoho.com.au/api",
            "jp": "https://mail.zoho.jp/api",
            "ca": "https://mail.zohocloud.ca/api",
        }.get(loc)
        if guess:
            mail_api = guess
    if not mail_api:
        mail_api = (getattr(settings, "ZOHO_MAIL_API_BASE_URL", "") or "https://mail.zoho.com/api").rstrip(
            "/"
        )
    return accounts, mail_api


def exchange_code_for_tokens(
    *,
    code: str,
    accounts_base_url: Optional[str] = None,
) -> Dict[str, Any]:
    if not zoho_oauth_configured():
        raise ZohoOAuthError("Zoho OAuth is not configured.")
    base = (accounts_base_url or _accounts_base()).rstrip("/")
    url = f"{base}/oauth/v2/token"
    data = {
        "code": code,
        "grant_type": "authorization_code",
        "client_id": settings.ZOHO_CLIENT_ID.strip(),
        "client_secret": settings.ZOHO_CLIENT_SECRET.strip(),
        "redirect_uri": settings.ZOHO_OAUTH_REDIRECT_URI.strip(),
        "scope": _scopes(),
    }
    try:
        resp = requests.post(url, data=data, timeout=30)
    except requests.RequestException as exc:
        raise ZohoOAuthError(f"Could not reach Zoho token endpoint: {exc}") from exc
    payload = {}
    try:
        payload = resp.json()
    except Exception:
        payload = {"error": resp.text[:300]}
    if resp.status_code >= 400 or not payload.get("access_token"):
        err = payload.get("error") or payload.get("error_description") or resp.text[:200]
        logger.warning("Zoho token exchange failed status=%s err=%s", resp.status_code, err)
        raise ZohoOAuthError(f"Zoho token exchange failed: {err}")
    return payload


def refresh_access_token(
    *,
    refresh_token: str,
    accounts_base_url: Optional[str] = None,
) -> Dict[str, Any]:
    if not zoho_oauth_configured():
        raise ZohoOAuthError("Zoho OAuth is not configured.")
    base = (accounts_base_url or _accounts_base()).rstrip("/")
    url = f"{base}/oauth/v2/token"
    data = {
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "client_id": settings.ZOHO_CLIENT_ID.strip(),
        "client_secret": settings.ZOHO_CLIENT_SECRET.strip(),
    }
    try:
        resp = requests.post(url, data=data, timeout=30)
    except requests.RequestException as exc:
        raise ZohoOAuthError(f"Could not reach Zoho token endpoint: {exc}") from exc
    payload = {}
    try:
        payload = resp.json()
    except Exception:
        payload = {"error": resp.text[:300]}
    if resp.status_code >= 400 or not payload.get("access_token"):
        err = payload.get("error") or payload.get("error_description") or resp.text[:200]
        raise ZohoOAuthError(f"Zoho token refresh failed: {err}")
    return payload


def token_expiry_from_payload(payload: Dict[str, Any]):
    expires_in = int(payload.get("expires_in") or 3600)
    # Refresh a minute early.
    return timezone.now() + timedelta(seconds=max(60, expires_in - 60))
