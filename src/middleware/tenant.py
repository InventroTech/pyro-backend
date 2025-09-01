from django.utils.deprecation import MiddlewareMixin
from django.core.cache import cache
from django.conf import settings
from core.models import Tenant

SKIP_PATH_PREFIXES = ("/admin", "/health", "/_health", "/metrics", "/docs", "/schema",
                      "/auth", "/authentication", "/api/auth", "/sentry", "/favicon.ico")
CACHE_TTL = 60  # seconds

def _resolve_slug(request) -> str | None:
    # 1) Header (recommended for APIs)
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
            return host[:-(len(base) + 1)].split(".")[0].strip() or None

    # 4) Optional default for dev/single-tenant
    return getattr(settings, "DEFAULT_TENANT_SLUG", None)

class TenantResolver(MiddlewareMixin):
    def process_request(self, request):
        for pfx in SKIP_PATH_PREFIXES:
            if request.path.startswith(pfx):
                request.tenant = None
                return

        slug = _resolve_slug(request)
        print(slug)
        request.tenant = None

        if not slug:
            return  # tenant-aware permissions will 403 later

        cache_key = f"tenant:slug:{slug}"
        tenant = cache.get(cache_key)
        if tenant is None:
            tenant = Tenant.objects.only("id", "slug", "name").filter(slug=slug).first()
            cache.set(cache_key, tenant, CACHE_TTL)

        request.tenant = tenant  # can be None if not found
