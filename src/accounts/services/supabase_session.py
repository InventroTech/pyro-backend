"""
Revoke Supabase Auth sessions via GoTrue Admin API (global sign-out).
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import requests

from authentication.password_reset import admin_headers

logger = logging.getLogger(__name__)


def revoke_supabase_sessions_globally(user_id: str) -> dict[str, Any]:
    """
    Sign the user out of all devices by revoking refresh tokens (scope=global).

    Returns a small result dict for delete audit logs. Failures are logged but
    do not raise — user deletion should still proceed.
    """
    uid = str(user_id).strip()
    if not uid:
        return {"user_id": uid, "revoked": False, "reason": "empty_user_id"}

    admin = admin_headers()
    if not admin:
        logger.warning(
            "Skipping Supabase global sign-out: missing SUPABASE URL or service role key",
            extra={"user_id": uid},
        )
        return {"user_id": uid, "revoked": False, "reason": "supabase_not_configured"}

    base, headers = admin
    url = f"{base.rstrip('/')}/auth/v1/admin/users/{uid}/logout"
    try:
        response = requests.post(
            url,
            headers=headers,
            json={"scope": "global"},
            timeout=20,
        )
        if response.status_code in (200, 204):
            logger.info("Supabase global sign-out succeeded", extra={"user_id": uid})
            return {"user_id": uid, "revoked": True, "status_code": response.status_code}

        logger.warning(
            "Supabase global sign-out failed",
            extra={
                "user_id": uid,
                "status_code": response.status_code,
                "body": (response.text or "")[:500],
            },
        )
        return {
            "user_id": uid,
            "revoked": False,
            "status_code": response.status_code,
            "reason": "supabase_api_error",
        }
    except requests.RequestException as exc:
        logger.warning(
            "Supabase global sign-out request error: %s",
            exc,
            extra={"user_id": uid},
            exc_info=True,
        )
        return {"user_id": uid, "revoked": False, "reason": "request_error", "error": str(exc)}
