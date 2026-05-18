"""
Password reset helpers: resolve Supabase user id, admin password update, and OTP hashing.
Updates Supabase Auth password via Admin API (GoTrue hashes stored_password — encrypted_password in DB).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
from typing import Optional, Tuple

import requests
from django.conf import settings

from authentication.models import User
from authentication.supabase_env import supabase_api_base_url, supabase_service_role_key
from authz.models import TenantMembership

logger = logging.getLogger(__name__)

OTP_TTL_SECONDS = 300  # 5 minutes


def _supabase_jwt_role(jwt_like: str) -> Optional[str]:
    """Decode role claim without verifying signature (sanity-check for mis-pasted anon key)."""
    try:
        parts = jwt_like.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1]
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
        role = payload.get("role")
        return str(role) if role is not None else None
    except (ValueError, json.JSONDecodeError, TypeError, OSError):
        return None


def otp_hmac_digest(normalized_email: str, otp_code: str) -> str:
    raw = f"{normalized_email.strip().lower()}:{otp_code.strip()}".encode("utf-8")
    key = settings.SECRET_KEY.encode("utf-8")
    return hmac.new(key, raw, hashlib.sha256).hexdigest()


def otp_codes_match(normalized_email: str, otp_code: str, stored_digest: str) -> bool:
    digest = otp_hmac_digest(normalized_email, otp_code)
    return hmac.compare_digest(digest, stored_digest)


def admin_headers() -> Optional[Tuple[str, dict]]:
    base = supabase_api_base_url().rstrip("/")
    key = supabase_service_role_key()
    if not base or not key:
        return None
    role = _supabase_jwt_role(key)
    if role == "anon":
        logger.warning(
            "Service-role env decodes as role=anon (publishable key). "
            "In development, STAGING_SERVICE_ROLE_KEY is tried before SUPABASE_SERVICE_ROLE_KEY. "
            "Use the Dashboard service_role JWT for the same project as the resolved URL."
        )
    hdrs = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    return base, hdrs


def find_supabase_user_id_for_email(normalized_email: str) -> Optional[str]:
    """
    Resolve Supabase Auth user UUID: Django auth User, TenantMembership.user_id,
    then scan Supabase Admin list (paginated) as fallback.
    """
    e = normalized_email.strip().lower()

    du = User.objects.filter(email__iexact=e).first()
    if du and getattr(du, "supabase_uid", None):
        return str(du.supabase_uid).strip()

    tm = (
        TenantMembership.objects.filter(email__iexact=e, is_active=True)
        .exclude(user_id__isnull=True)
        .first()
    )
    if tm and tm.user_id:
        return str(tm.user_id)

    bh = admin_headers()
    if not bh:
        return None
    base, headers = bh
    page = 1
    per_page = 200

    try:
        while page <= 50:
            r = requests.get(
                f"{base}/auth/v1/admin/users",
                headers=headers,
                params={"page": page, "per_page": per_page},
                timeout=20,
            )
            if r.status_code != 200:
                logger.warning("Supabase admin list users HTTP %s: %s", r.status_code, r.text[:300])
                return None
            body = r.json()
            users = body if isinstance(body, list) else body.get("users") or []
            for u in users:
                if (u.get("email") or "").strip().lower() == e:
                    uid = u.get("id")
                    return str(uid) if uid else None
            if len(users) < per_page:
                break
            page += 1
    except requests.RequestException:
        logger.exception("Supabase admin list users failed")
    return None


def admin_update_user_password(user_id: str, new_password: str) -> Tuple[bool, str]:
    """
    Sets the user's password via Supabase Admin API (updates auth.users password hash server-side).
    """
    bh = admin_headers()
    if not bh:
        return False, "Password reset is not configured (missing service credentials)."
    base, headers = bh
    try:
        r = requests.put(
            f"{base}/auth/v1/admin/users/{user_id}",
            headers=headers,
            json={"password": new_password},
            timeout=25,
        )
    except requests.RequestException as exc:
        logger.exception("Supabase admin update user failed")
        return False, f"Could not reach auth service: {exc}"

    if r.status_code in (200, 201):
        return True, "ok"

    try:
        err = r.json()
        msg = (
            err.get("message")
            or err.get("msg")
            or err.get("error_description")
            or err.get("error")
            or str(err)
        )
        hint = err.get("hint")
        if hint and hint not in (msg or ""):
            msg = f"{msg} ({hint})" if msg else str(hint)
    except Exception:
        msg = r.text[:500] or f"HTTP {r.status_code}"
    logger.warning("Supabase admin set password failed: %s %s", r.status_code, msg)
    return False, msg or "Update failed"


__all__ = [
    "OTP_TTL_SECONDS",
    "admin_update_user_password",
    "find_supabase_user_id_for_email",
    "otp_codes_match",
    "otp_hmac_digest",
]
