from rest_framework.permissions import BasePermission
from django.conf import settings
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)

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

        # Database: simple match on secret column
        try:
            from .models import ApiSecretKey

            api_secret_obj = (
                ApiSecretKey.objects.filter(secret=secret_header, is_active=True)
                .select_related("tenant")
                .first()
            )
            if api_secret_obj:
                api_secret_obj.last_used_at = timezone.now()
                api_secret_obj.save(update_fields=["last_used_at"])
                request.api_secret_key = secret_header
                request.is_default_secret = False
                request.api_secret_obj = api_secret_obj
                return True
        except Exception as e:
            logger.warning("[HasAPISecret] Error checking database: %s", e)

        logger.warning("[HasAPISecret] Invalid secret")
        return False
