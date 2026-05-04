from rest_framework.permissions import BasePermission
from django.conf import settings
from django.utils import timezone
from django.core.cache import cache
import logging
import hashlib

logger = logging.getLogger(__name__)

# Cache configuration for API secret validation
API_SECRET_CACHE_KEY_PREFIX = "api_secret_"
API_SECRET_CACHE_TTL = 3600  # 1 hour


def _cache_key_for_secret(secret: str) -> str:
    return API_SECRET_CACHE_KEY_PREFIX + hashlib.sha256(secret.encode()).hexdigest()


class HasAPISecret(BasePermission):
    """
    Permission class that checks for X-Secret-Pyro header.
    Compares header to PYRO_SECRET (settings) or ApiSecretKey.secret (DB); simple match, no hashing.
    """

    def has_permission(self, request, view):
        secret_header = (
            request.headers.get("X-Secret-Pyro", "")
            or request.headers.get("x-secret-pyro", "")
            or request.META.get("HTTP_X_SECRET_PYRO", "")
        )

        if not secret_header:
            logger.warning("[HasAPISecret] X-Secret-Pyro header missing")
            return False

        secret_header = secret_header.strip()

        # Settings
        api_secret = getattr(settings, "PYRO_SECRET", None)
        if api_secret and api_secret != "" and secret_header == api_secret:
            request.api_secret_key = secret_header
            request.is_default_secret = True
            return True

        cache_key = _cache_key_for_secret(secret_header)
        cached_entry = cache.get(cache_key)

        try:
            from .models import ApiSecretKey

            if cached_entry:
                cached_secret = (
                    ApiSecretKey.objects.filter(
                        pk=cached_entry.get("api_secret_key_id"), is_active=True
                    )
                    .select_related("tenant")
                    .first()
                )
                if cached_secret:
                    cached_secret.last_used_at = timezone.now()
                    cached_secret.save(update_fields=["last_used_at"])
                    request.api_secret_key = secret_header
                    request.is_default_secret = False
                    request.api_secret_obj = cached_secret
                    return True

                cache.delete(cache_key)

            # Database: simple match on secret column
            api_secret_obj = (
                ApiSecretKey.objects.filter(secret=secret_header, is_active=True)
                .select_related("tenant")
                .first()
            )
            if api_secret_obj:
                api_secret_obj.last_used_at = timezone.now()
                api_secret_obj.save(update_fields=["last_used_at"])
                cache.set(
                    cache_key,
                    {
                        "api_secret_key_id": api_secret_obj.id,
                        "tenant_id": str(api_secret_obj.tenant_id),
                    },
                    API_SECRET_CACHE_TTL,
                )
                request.api_secret_key = secret_header
                request.is_default_secret = False
                request.api_secret_obj = api_secret_obj
                return True
        except Exception as e:
            logger.warning("[HasAPISecret] Error checking database: %s", e)

        logger.warning("[HasAPISecret] Invalid secret")
        return False
