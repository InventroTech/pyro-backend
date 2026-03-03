from rest_framework.permissions import BasePermission
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
import hashlib
import logging

logger = logging.getLogger(__name__)

# Cache successful API secret lookups 10-15 min to avoid slow crypt() query on every request
API_SECRET_CACHE_TTL = getattr(settings, "API_SECRET_CACHE_TTL", 600)  # 10 min; set 900 for 15 min
API_SECRET_CACHE_KEY_PREFIX = "entity_api_secret:"


class HasAPISecret(BasePermission):
    """
    Permission class that checks for X-Secret-Pyro header.
    Used for external API access without tenant authentication.
    Cache: successful DB lookups are cached 10-15 min so repeat requests skip the slow crypt() query.
    """

    def has_permission(self, request, view):
        """
        Check if request has valid X-Secret-Pyro header.
        Checks both API_SECRET from settings and ApiSecretKey model in database.
        """
        secret_header = (
            request.headers.get("X-Secret-Pyro", "")
            or request.headers.get("x-secret-pyro", "")
            or request.META.get("HTTP_X_SECRET_PYRO", "")
        )

        if not secret_header:
            logger.warning("[HasAPISecret] X-Secret-Pyro header missing from request")
            logger.warning("[HasAPISecret] Available headers: %s", list(request.headers.keys()))
            return False

        secret_header = secret_header.strip()
        logger.info(
            "[HasAPISecret] Checking permission - Header value: '%s...' (length: %d)",
            secret_header[:10],
            len(secret_header),
        )

        # Default secret from settings
        api_secret = getattr(settings, "PYRO_SECRET", None)
        if api_secret == "":
            api_secret = None
        if api_secret and secret_header == api_secret:
            logger.info("[HasAPISecret] Valid API secret from settings - permission granted")
            request.api_secret_key = secret_header
            request.is_default_secret = True
            return True

        # Database: cache first, then full crypt() lookup on miss
        try:
            from .models import ApiSecretKey

            cache_key = API_SECRET_CACHE_KEY_PREFIX + hashlib.sha256(secret_header.encode()).hexdigest()

            # Cache hit -> one fast PK lookup, no crypt()
            cached = cache.get(cache_key)
            if cached is not None:
                api_secret_obj = (
                    ApiSecretKey.objects.filter(
                        pk=cached["api_secret_key_id"],
                        is_active=True,
                    )
                    .select_related("tenant")
                    .first()
                )
                if api_secret_obj:
                    logger.debug("[HasAPISecret] Cache hit (tenant: %s)", api_secret_obj.tenant.slug)
                    request.api_secret_key = secret_header
                    request.is_default_secret = False
                    request.api_secret_obj = api_secret_obj
                    return True
                cache.delete(cache_key)

            # Cache miss: full-table crypt() lookup (slow)
            api_secret_obj = (
                ApiSecretKey.objects.filter(is_active=True)
                .extra(
                    where=["secret_key_hash = crypt(%s, secret_key_hash)"],
                    params=[secret_header],
                )
                .select_related("tenant")
                .first()
            )

            if api_secret_obj:
                api_secret_obj.last_used_at = timezone.now()
                api_secret_obj.save(update_fields=["last_used_at"])
                cache.set(
                    cache_key,
                    {"api_secret_key_id": api_secret_obj.id, "tenant_id": str(api_secret_obj.tenant_id)},
                    API_SECRET_CACHE_TTL,
                )
                logger.info(
                    "[HasAPISecret] Valid secret key from database - permission granted (tenant: %s)",
                    api_secret_obj.tenant.slug,
                )
                request.api_secret_key = secret_header
                request.is_default_secret = False
                request.api_secret_obj = api_secret_obj
                return True

        except Exception as e:
            logger.warning("[HasAPISecret] Error checking database for secret key: %s", e)

        logger.warning("[HasAPISecret] Invalid secret - Header: '%s...'", secret_header[:10])
        return False
