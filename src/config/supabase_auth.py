import os
import jwt
from jwt import ExpiredSignatureError, InvalidTokenError
from django.contrib.auth import get_user_model
from django.conf import settings
from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions
from authz.service import get_effective_permissions
from core.models import Tenant
from authz.models import Role as AuthzRole

SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET")
User = get_user_model()

def _get_bearer(request):
    auth = request.META.get("HTTP_AUTHORIZATION") or request.headers.get("Authorization", "")
    if not auth or not auth.lower().startswith("bearer "):
        return None
    return auth.split(" ", 1)[1].strip()

def _resolve_tenant_slug(request) -> str:
    """Resolve tenant slug from request headers or path (same logic as TenantResolver middleware)"""
    # 1) Header
    slug = request.headers.get("X-Tenant-Slug") or request.headers.get("X-Tenant")
    if slug:
        return slug.strip()

    # 2) Path: /t/<slug>/...
    if request.path.startswith("/t/"):
        parts = request.path.split("/", 3)
        if len(parts) >= 3 and parts[2]:
            return parts[2].strip()

    # 3) Subdomain (enable by setting TENANCY_BASE_DOMAIN)
    base = getattr(settings, "TENANCY_BASE_DOMAIN", None)
    if base:
        host = (request.get_host() or "").split(":")[0]
        if host.endswith("." + base):
            subdomain = host[:-(len(base) + 1)].split(".")[0].strip()
            if subdomain:
                return subdomain

    # 4) Default tenant slug
    return getattr(settings, "DEFAULT_TENANT_SLUG", "bibhab-thepyro-ai")

def _verify_jwt(token: str) -> dict:
    if not SUPABASE_JWT_SECRET:
        raise exceptions.AuthenticationFailed("JWT secret not configured")
    try:
        # Supabase tokens have aud='authenticated'; we're not enforcing audience.
        return jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], options={"verify_aud": False})
    except ExpiredSignatureError:
        raise exceptions.AuthenticationFailed("Token expired")
    except InvalidTokenError as e:
        raise exceptions.AuthenticationFailed(f"Invalid token: {e}")

def _get_or_create_profile(claims):
    sub = claims.get("sub") or claims.get("user_id")
    if not sub:
        raise exceptions.AuthenticationFailed("Token missing 'sub'")
    email = (claims.get("email") or "").lower() or None

    # Mirror user locally (no password; identity of record is Supabase)
    user, created = User.objects.get_or_create(
        supabase_uid=sub,
        defaults={"email": email, "is_active": True},
    )
    if email and user.email != email:
        user.email = email
        user.save(update_fields=["email"])
    return user

def _enrich_claims(claims: dict, request) -> dict:
    """Enrich JWT claims with tenant_id, user_id, role_id, role_key, and tenant_slug"""
    user_id = claims.get("sub") or claims.get("user_id")
    if not user_id:
        return claims  # Can't enrich without user_id
    
    # Resolve tenant slug from request
    tenant_slug = _resolve_tenant_slug(request)
    
    # Resolve tenant
    try:
        tenant = Tenant.objects.filter(slug=tenant_slug).first()
        if not tenant:
            # If tenant not found, return claims without enrichment
            # This allows authentication to proceed but without tenant-specific data
            return claims
    except Exception:
        # If tenant lookup fails, return original claims
        return claims
    
    # Get role information
    try:
        perm_info = get_effective_permissions(user_id, tenant)
        role_key = perm_info.get('role_key')
        role_id = None
        
        # Get role ID if role_key exists
        if role_key:
            role = AuthzRole.objects.filter(tenant=tenant, key=role_key).first()
            if role:
                role_id = str(role.id)
        
        # Enrich claims with tenant and role information
        enriched_claims = {
            **claims,  # Include all original claims
            "tenant_id": str(tenant.id),
            "user_id": user_id,
            "role_id": role_id,
            "role_key": role_key,
            "tenant_slug": tenant_slug,
        }
        
        return enriched_claims
    except Exception:
        # If role lookup fails, return original claims
        return claims

class SupabaseJWTAuthentication(BaseAuthentication):
    def authenticate(self, request):
        token = _get_bearer(request)
        if not token:
            return None
        claims = _verify_jwt(token)
        user = _get_or_create_profile(claims)
        
        # Enrich claims with tenant and role information
        enriched_claims = _enrich_claims(claims, request)

        request.jwt_claims = enriched_claims
        request.token = token
        return (user, None)
