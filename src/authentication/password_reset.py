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
from typing import Any, Optional, Tuple
from urllib.parse import urlparse

import requests
from django.conf import settings
from django.db import connection as django_connection

from authentication.models import User
from authentication.supabase_env import supabase_api_base_url, supabase_service_role_key
from authz.models import TenantMembership

logger = logging.getLogger(__name__)

OTP_TTL_SECONDS = 300  # 5 minutes


def _email_log_tag(normalized_email: str) -> str:
    """Grep-friendly label (domain + short hash) for log correlation without repeating full PII."""
    e = (normalized_email or "").strip().lower()
    if "@" not in e:
        return "invalid-email"
    local, domain = e.rsplit("@", 1)
    short = hashlib.sha256(e.encode("utf-8")).hexdigest()[:10]
    return f"email_hash={short} domain={domain} local_len={len(local)}"


def _supabase_error_details(response: requests.Response) -> dict[str, Any]:
    """Structured fields from GoTrue/Supabase JSON error bodies (for support tickets)."""
    details: dict[str, Any] = {"http_status": response.status_code}
    body = (response.text or "")[:2000]
    details["body_snippet"] = body
    try:
        j = response.json()
        if isinstance(j, dict):
            for key in (
                "error_id",
                "msg",
                "message",
                "code",
                "error_code",
                "hint",
                "error_description",
            ):
                if key in j and j[key] is not None:
                    details[key] = j[key]
    except (ValueError, json.JSONDecodeError):
        pass
    return details


def _supabase_host(base_url: str) -> str:
    try:
        return urlparse(base_url).netloc or base_url[:80]
    except Exception:
        return "invalid-base"


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


def _find_user_id_via_supabase_admin_list(
    base: str, headers: dict, normalized_email: str
) -> tuple[Optional[str], bool]:
    """
    Paginated scan of auth users in the *current* Supabase project.

    Returns:
        ``(user_id, upstream_error).`` When ``upstream_error`` is True, Supabase Admin list
        did not succeed (network or non-HTTP-200 response); callers must **not** treat a
        missing ``user_id`` as proof the email has no Auth user — it can be infra / Supabase
        ("Database error finding users", outages, etc.).
    """
    e = normalized_email.strip().lower()
    host = _supabase_host(base)
    tag = _email_log_tag(e)
    page = 1
    per_page = 200
    logger.info(
        "[PasswordReset][resolve_user][supabase_admin_list] start host=%s %s per_page=%s max_pages=50",
        host,
        tag,
        per_page,
    )
    try:
        while page <= 50:
            r = requests.get(
                f"{base}/auth/v1/admin/users",
                headers=headers,
                params={"page": page, "per_page": per_page},
                timeout=20,
            )
            if r.status_code != 200:
                details = _supabase_error_details(r)
                lvl = logging.ERROR if r.status_code >= 500 else logging.WARNING
                logger.log(
                    lvl,
                    "[PasswordReset][resolve_user][supabase_admin_list] host=%s %s page=%s "
                    "path=/auth/v1/admin/users http_status=%s error_id=%s error_code=%s msg=%r hint=%r",
                    host,
                    tag,
                    page,
                    details.get("http_status"),
                    details.get("error_id"),
                    details.get("error_code") or details.get("code"),
                    details.get("msg") or details.get("message"),
                    details.get("hint"),
                )
                body_snip = (details.get("body_snippet") or "")[:1200]
                if body_snip:
                    logger.log(
                        lvl,
                        "[PasswordReset][resolve_user][supabase_admin_list] raw_response_body=%s",
                        body_snip,
                    )
                return None, True
            body = r.json()
            users = body if isinstance(body, list) else body.get("users") or []
            for u in users:
                if (u.get("email") or "").strip().lower() == e:
                    uid = u.get("id")
                    uid_str = str(uid) if uid else None
                    if uid_str:
                        logger.info(
                            "[PasswordReset][resolve_user][supabase_admin_list] matched host=%s %s "
                            "user_id_prefix=%s page=%s batch_size=%s",
                            host,
                            tag,
                            uid_str[:8],
                            page,
                            len(users),
                        )
                    return (uid_str if uid else None), False
            if len(users) < per_page:
                logger.info(
                    "[PasswordReset][resolve_user][supabase_admin_list] scanned_all_pages host=%s %s "
                    "last_page=%s batch_size=%s result=no_match",
                    host,
                    tag,
                    page,
                    len(users),
                )
                break
            page += 1
        else:
            logger.info(
                "[PasswordReset][resolve_user][supabase_admin_list] hit_page_cap host=%s %s "
                "cap_pages=50 per_page=%s result=no_match",
                host,
                tag,
                per_page,
            )
    except requests.RequestException:
        logger.exception(
            "[PasswordReset][resolve_user][supabase_admin_list] request_exception host=%s %s",
            host,
            tag,
        )
        return None, True
    return None, False


def _find_user_id_from_auth_users_sql(normalized_email: str) -> Optional[str]:
    """
    Resolve Supabase Auth user UUID by reading ``auth.users`` through Django's DB connection.

    Use when Admin HTTP ``/admin/users`` is flaky (500) or when paginated list missed the user.
    Requires the configured ``DATABASES['default']`` role to ``SELECT`` from ``auth.users``
    (typical when Django uses the same Supabase Postgres as Auth).
    """
    e = normalized_email.strip().lower()
    if not e:
        return None
    tag = _email_log_tag(e)
    db_host = settings.DATABASES.get("default", {}).get("HOST") or "unknown"
    logger.info("[PasswordReset][resolve_user][auth_users_sql] try db_host=%s %s", db_host, tag)
    try:
        with django_connection.cursor() as cursor:
            cursor.execute(
                "SELECT id::text FROM auth.users WHERE lower(email) = lower(%s) LIMIT 1",
                [e],
            )
            row = cursor.fetchone()
            if row and row[0]:
                uid = str(row[0]).strip()
                logger.info(
                    "[PasswordReset][resolve_user][auth_users_sql] found db_host=%s %s user_id_prefix=%s",
                    db_host,
                    tag,
                    uid[:8],
                )
                return uid
            logger.info(
                "[PasswordReset][resolve_user][auth_users_sql] no_row db_host=%s %s",
                db_host,
                tag,
            )
    except Exception as exc:
        logger.warning(
            "[PasswordReset][resolve_user][auth_users_sql] query_failed db_host=%s %s error=%s",
            db_host,
            tag,
            exc,
            exc_info=True,
        )
    return None


def find_supabase_user_id_for_password_reset(normalized_email: str) -> tuple[Optional[str], bool]:
    """
    Resolve Supabase Auth user UUID for the configured project.

    Tries **Admin HTTP** ``/admin/users`` first, then **PostgreSQL** ``auth.users`` on Django's
    ``default`` database (same Supabase Postgres). The DB path covers Admin API outages, HTTP
    500 "Database error finding users", and very large tenants where pagination may not reach
    every user in one sweep.

    Returns ``(user_id, upstream_lookup_error).`` When ``upstream_lookup_error`` is True,
    both HTTP and SQL failed to resolve a user — caller should respond with ``503``.

    See :func:`find_supabase_user_id_for_email` — convenience wrapper without the upstream flag.
    """
    e = normalized_email.strip().lower()

    bh = admin_headers()
    if bh:
        base, _headers = bh
        host = _supabase_host(base)
        tag = _email_log_tag(e)
        uid, upstream_err = _find_user_id_via_supabase_admin_list(base, _headers, e)
        if uid:
            logger.info(
                "[PasswordReset][resolve_user] outcome=admin_http_ok host=%s %s",
                host,
                tag,
            )
            return uid, False

        sql_uid = _find_user_id_from_auth_users_sql(e)
        if sql_uid:
            logger.info(
                "[PasswordReset][resolve_user] outcome=sql_fallback_ok host=%s %s admin_had_upstream_error=%s",
                host,
                tag,
                upstream_err,
            )
            return sql_uid, False

        if upstream_err:
            logger.error(
                "[PasswordReset][resolve_user] outcome=FAILED host=%s %s reason=admin_http_error_and_sql_miss "
                "(open Supabase ticket with error_id from admin_list logs above if 5xx)",
                host,
                tag,
            )
            return None, True
        logger.info(
            "[PasswordReset][resolve_user] outcome=no_auth_user host=%s %s (Admin list scanned, SQL no row)",
            host,
            tag,
        )
        return None, False

    logger.warning(
        "[PasswordReset][resolve_user] no_supabase_admin_env_using_django_fallback %s",
        _email_log_tag(e),
    )
    du = User.objects.filter(email__iexact=e).first()
    if du and getattr(du, "supabase_uid", None):
        return str(du.supabase_uid).strip(), False

    tm = (
        TenantMembership.objects.filter(email__iexact=e, is_active=True)
        .exclude(user_id__isnull=True)
        .first()
    )
    if tm and tm.user_id:
        return str(tm.user_id), False

    return None, False


def find_supabase_user_id_for_email(normalized_email: str) -> Optional[str]:
    """
    Convenience wrapper returning only the UUID (or None).

    If you need to distinguish Supabase outages from genuine "no Auth user",
    use :func:`find_supabase_user_id_for_password_reset` instead.
    """
    uid, _ = find_supabase_user_id_for_password_reset(normalized_email)
    return uid


def admin_update_user_password(user_id: str, new_password: str) -> Tuple[bool, str]:
    """
    Sets the user's password via Supabase Admin API (updates auth.users password hash server-side).
    """
    bh = admin_headers()
    if not bh:
        return False, "Password reset is not configured (missing service credentials)."
    base, headers = bh
    host = _supabase_host(base)
    uid_pfx = (user_id or "").strip()[:8] or "?"
    try:
        r = requests.put(
            f"{base}/auth/v1/admin/users/{user_id}",
            headers=headers,
            json={"password": new_password},
            timeout=25,
        )
    except requests.RequestException as exc:
        logger.exception(
            "[PasswordReset][admin_update_password] request_exception host=%s user_id_prefix=%s",
            host,
            uid_pfx,
        )
        return False, f"Could not reach auth service: {exc}"

    if r.status_code in (200, 201):
        return True, "ok"

    details = _supabase_error_details(r)
    err_id = details.get("error_id")
    msg = (
        details.get("msg")
        or details.get("message")
        or details.get("error_description")
        or (details.get("error") if not isinstance(details.get("error"), dict) else None)
    )
    if msg is not None and not isinstance(msg, str):
        msg = str(msg)
    hint = details.get("hint")
    if isinstance(hint, str) and hint and msg and hint not in msg:
        msg = f"{msg} ({hint})"
    elif isinstance(hint, str) and hint and not msg:
        msg = hint
    if not msg:
        msg = (details.get("body_snippet") or r.text or "")[:500] or f"HTTP {r.status_code}"

    code = details.get("code") or details.get("error_code")
    raw = (details.get("body_snippet") or "")[:1200]
    logger.warning(
        "[PasswordReset][admin_update_password] failed host=%s http_status=%s user_id_prefix=%s "
        "error_id=%s code=%s msg=%s raw_response_body=%r",
        host,
        r.status_code,
        uid_pfx,
        err_id,
        code,
        msg,
        raw,
    )
    return False, msg if isinstance(msg, str) else str(msg)


__all__ = [
    "OTP_TTL_SECONDS",
    "admin_update_user_password",
    "find_supabase_user_id_for_email",
    "find_supabase_user_id_for_password_reset",
    "otp_codes_match",
    "otp_hmac_digest",
]
