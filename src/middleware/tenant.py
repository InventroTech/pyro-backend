from django.utils.deprecation import MiddlewareMixin
from django.core.cache import cache
from django.conf import settings
from core.models import Tenant
import jwt
from jwt import ExpiredSignatureError, InvalidTokenError

SKIP_PATH_PREFIXES = ("/admin", "/health", "/_health", "/metrics", "/docs", "/schema",
                      "/auth", "/authentication", "/api/auth", "/sentry", "/favicon.ico")
CACHE_TTL = 60  # seconds

def _get_bearer_token(request) -> str | None:
    """Extract Bearer token from Authorization header."""
    auth = request.META.get("HTTP_AUTHORIZATION") or request.headers.get("Authorization", "")
    if not auth or not auth.lower().startswith("bearer "):
        return None
    return auth.split(" ", 1)[1].strip()

def _get_tenant_id_from_jwt(request) -> str | None:
    """
    Extract tenant_id from JWT token if present.
    Returns tenant_id (UUID string) or None.
    """
    token = _get_bearer_token(request)
    if not token:
        return None
    
    jwt_secret = getattr(settings, "SUPABASE_JWT_SECRET", None)
    if not jwt_secret:
        return None
    
    try:
        claims = jwt.decode(token, jwt_secret, algorithms=["HS256"], options={"verify_aud": False})
        # Extract tenant_id from user_data.tenant_id
        user_data = claims.get("user_data", {})
        tenant_id = user_data.get("tenant_id")
        if tenant_id:
            return str(tenant_id)
    except (ExpiredSignatureError, InvalidTokenError, Exception):
        # If JWT is invalid/expired, fall back to slug-based resolution
        pass
    
    return None

def _resolve_slug(request) -> str | None:
    """
    Resolve tenant slug from path or subdomain.
    Used as fallback when JWT tenant_id is not available.
    Note: X-Tenant-Slug header is NOT used for security reasons.
    """
    # 1) Path: /t/<slug>/...
    if request.path.startswith("/t/"):
        parts = request.path.split("/", 3)
        if len(parts) >= 3 and parts[2]:
            return parts[2].strip()

    # 2) Subdomain (enable by setting TENANCY_BASE_DOMAIN)
    base = getattr(settings, "TENANCY_BASE_DOMAIN", None)
    if base:
        host = (request.get_host() or "").split(":")[0]
        if host.endswith("." + base):
            return host[:-(len(base) + 1)].split(".")[0].strip() or None

    return getattr(settings, "DEFAULT_TENANT_SLUG", "bibhab-thepyro-ai")

class TenantResolver(MiddlewareMixin):
    """
    Resolves tenant from JWT token (preferred) or slug-based methods (fallback).
    
    Priority order:
    1. JWT token tenant_id (most secure, can't be spoofed)
    2. Path-based (/t/<slug>/...)
    3. Subdomain-based
    4. Default tenant slug from settings
    
    Note: X-Tenant-Slug header is NOT used for security reasons.
    """
    def process_request(self, request):
        for pfx in SKIP_PATH_PREFIXES:
            if request.path.startswith(pfx):
                request.tenant = None
                return

        request.tenant = None
        
        # Priority 1: Try to get tenant_id from JWT token (most secure)
        tenant_id = _get_tenant_id_from_jwt(request)
        if tenant_id:
            cache_key = f"tenant:id:{tenant_id}"
            tenant = cache.get(cache_key)
            if tenant is None:
                tenant = Tenant.objects.only("id", "slug", "name").filter(id=tenant_id).first()
                if tenant:
                    cache.set(cache_key, tenant, CACHE_TTL)
            
            if tenant:
                request.tenant = tenant
                return
        
        # Priority 2: Fallback to slug-based resolution (path/subdomain/default)
        slug = _resolve_slug(request)
        if not slug:
            return  # tenant-aware permissions will 403 later

        cache_key = f"tenant:slug:{slug}"
        tenant = cache.get(cache_key)
        if tenant is None:
            tenant = Tenant.objects.only("id", "slug", "name").filter(slug=slug).first()
            if tenant:
                cache.set(cache_key, tenant, CACHE_TTL)

        request.tenant = tenant  # can be None if not found
