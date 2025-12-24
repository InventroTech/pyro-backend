from rest_framework.permissions import BasePermission
from django.conf import settings
from django.utils import timezone
import logging
logger = logging.getLogger(__name__)


class HasAPISecret(BasePermission):
    """
    Permission class that checks for X-Secret-Pyro header.
    Used for external API access without tenant authentication.
    Supports multiple secret keys mapped to different tenants via database.
    """
    
    def has_permission(self, request, view):
        """
        Check if request has valid X-Secret-Pyro header.
        Checks both API_SECRET from settings and ApiSecretKey model in database.
        Stores the matched secret key in request.api_secret_key for tenant resolution.
        """
        # Check header with different possible formats
        secret_header = (
            request.headers.get('X-Secret-Pyro', '') or
            request.headers.get('x-secret-pyro', '') or
            request.META.get('HTTP_X_SECRET_PYRO', '')
        )
        
        if not secret_header:
            logger.warning("[HasAPISecret] X-Secret-Pyro header missing from request")
            logger.warning(f"[HasAPISecret] Available headers: {list(request.headers.keys())}")
            return False
        
        secret_header = secret_header.strip()
        
        logger.info(f"[HasAPISecret] Checking permission - Header value: '{secret_header[:10]}...' (length: {len(secret_header)})")
        
        # First, check API_SECRET from settings (default/primary secret)
        api_secret = getattr(settings, 'PYRO_SECRET', None)
        if api_secret == "":
            api_secret = None
        
        if api_secret and secret_header == api_secret:
            logger.info("[HasAPISecret] Valid API secret from settings provided - permission granted")
            request.api_secret_key = secret_header
            request.is_default_secret = True  # Flag to indicate it's the default secret
            return True
        
        # Then, check database for secret keys
        try:
            from .models import ApiSecretKey
            # Verify using pgcrypto: secret_key_hash = crypt(provided_secret, secret_key_hash)
            # This avoids storing plaintext and works with per-row salts.
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
                # Update last_used_at timestamp
                api_secret_obj.last_used_at = timezone.now()
                api_secret_obj.save(update_fields=['last_used_at'])
                
                logger.info(f"[HasAPISecret] Valid secret key found in database - permission granted (tenant: {api_secret_obj.tenant.slug})")
                request.api_secret_key = secret_header
                request.is_default_secret = False  # Flag to indicate it's a custom secret
                request.api_secret_obj = api_secret_obj  # Store the object for tenant resolution
                return True
        except Exception as e:
            # If model doesn't exist yet (during migrations) or other error, log and continue
            logger.warning(f"[HasAPISecret] Error checking database for secret key: {e}")
        
        # No valid secret found
        logger.warning(f"[HasAPISecret] Invalid secret provided - Header: '{secret_header[:10]}...'")
        return False

