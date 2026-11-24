from __future__ import annotations

from urllib.parse import parse_qs

from channels.db import database_sync_to_async
from django.contrib.auth.models import AnonymousUser

from config.supabase_auth import _get_or_create_profile, _verify_jwt


def _tenant_id_from_claims(claims: dict) -> str | None:
    user_data = claims.get("user_data") or {}
    tenant_id = user_data.get("tenant_id")
    if tenant_id:
        return str(tenant_id)

    sub = claims.get("sub")
    if not sub:
        return None

    from authz.models import TenantMembership

    membership = (
        TenantMembership.objects.filter(user_id=sub, is_active=True)
        .values_list("tenant_id", flat=True)
        .first()
    )
    return str(membership) if membership else None


def _role_key_for_user(sub: str | None, tenant_id: str | None, claims: dict) -> str | None:
    user_data = claims.get("user_data") or {}
    from_claims = user_data.get("role_key") or user_data.get("role")
    if from_claims:
        return str(from_claims)

    if not sub or not tenant_id:
        return None

    from authz.models import TenantMembership

    role_key = (
        TenantMembership.objects.filter(
            user_id=sub,
            tenant_id=tenant_id,
            is_active=True,
        )
        .values_list("role__key", flat=True)
        .first()
    )
    return str(role_key) if role_key else None


@database_sync_to_async
def resolve_websocket_user_and_tenant(token: str | None):
    if not token:
        return AnonymousUser(), None, None

    try:
        claims = _verify_jwt(token)
        user = _get_or_create_profile(claims)
        tenant_id = _tenant_id_from_claims(claims)
        sub = claims.get("sub") or claims.get("user_id")
        role_key = _role_key_for_user(
            str(sub) if sub else None,
            str(tenant_id) if tenant_id else None,
            claims,
        )
        return user, str(tenant_id) if tenant_id else None, role_key
    except Exception:
        return AnonymousUser(), None, None


def extract_bearer_token(scope) -> str | None:
    query_string = scope.get("query_string", b"").decode()
    params = parse_qs(query_string)
    token = (params.get("token") or [None])[0]
    if token:
        return token.strip()

    for header_name, header_value in scope.get("headers", []):
        if header_name.lower() == b"authorization":
            auth = header_value.decode()
            if auth.lower().startswith("bearer "):
                return auth.split(" ", 1)[1].strip()
            break
    return None


class JWTAuthMiddleware:
    """Authenticate WebSocket connections using the same Supabase JWT as REST."""

    def __init__(self, inner):
        self.inner = inner

    async def __call__(self, scope, receive, send):
        if scope["type"] != "websocket":
            return await self.inner(scope, receive, send)

        token = extract_bearer_token(scope)
        user, tenant_id, role_key = await resolve_websocket_user_and_tenant(token)
        scope["user"] = user
        scope["tenant_id"] = tenant_id
        scope["role_key"] = role_key
        return await self.inner(scope, receive, send)
