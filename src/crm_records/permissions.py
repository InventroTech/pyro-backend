from rest_framework.permissions import BasePermission
from django.conf import settings
from django.utils import timezone
import logging
logger = logging.getLogger(__name__)


class HasPrajaSecret(BasePermission):
    """
    Permission class that checks for X-Secret-Praja header.
    Used for external API access without tenant authentication.
    Supports multiple secret keys mapped to different tenants via database.
    """
    
    def has_permission(self, request, view):
        """
        Check if request has valid X-Secret-Praja header.
        Checks both PRAJA_SECRET from settings and ApiSecretKey model in database.
        Stores the matched secret key in request.praja_secret_key for tenant resolution.
        """
        # Check header with different possible formats
        secret_header = (
            request.headers.get('X-Secret-Praja', '') or
            request.headers.get('x-secret-praja', '') or
            request.META.get('HTTP_X_SECRET_PRAJA', '')
        )
        
        if not secret_header:
            logger.warning("[HasPrajaSecret] X-Secret-Praja header missing from request")
            logger.warning(f"[HasPrajaSecret] Available headers: {list(request.headers.keys())}")
            return False
        
        secret_header = secret_header.strip()
        
        logger.info(f"[HasPrajaSecret] Checking permission - Header value: '{secret_header[:10]}...' (length: {len(secret_header)})")
        
        # First, check PRAJA_SECRET from settings (default/primary secret)
        praja_secret = getattr(settings, 'PRAJA_SECRET', None)
        if praja_secret == "":
            praja_secret = None
        
        if praja_secret and secret_header == praja_secret:
            logger.info("[HasPrajaSecret] Valid PRAJA_SECRET provided - permission granted")
            request.praja_secret_key = secret_header
            request.is_praja_secret = True  # Flag to indicate it's the default secret
            return True
        
        # Then, check database for secret keys
        try:
            from .models import ApiSecretKey
            # Verify using pgcrypto: secret_key_hash = crypt(provided_secret, secret_key_hash)
            # This avoids storing plaintext and works with per-row salts.
            api_secret = (
                ApiSecretKey.objects.filter(is_active=True)
                .extra(
                    where=["secret_key_hash = crypt(%s, secret_key_hash)"],
                    params=[secret_header],
                )
                .select_related("tenant")
                .first()
            )
            
            if api_secret:
                # Update last_used_at timestamp
                api_secret.last_used_at = timezone.now()
                api_secret.save(update_fields=['last_used_at'])
                
                logger.info(f"[HasPrajaSecret] Valid secret key found in database - permission granted (tenant: {api_secret.tenant.slug})")
                request.praja_secret_key = secret_header
                request.is_praja_secret = False  # Flag to indicate it's a custom secret
                request.api_secret_obj = api_secret  # Store the object for tenant resolution
                return True
        except Exception as e:
            # If model doesn't exist yet (during migrations) or other error, log and continue
            logger.warning(f"[HasPrajaSecret] Error checking database for secret key: {e}")
        
        # No valid secret found
        logger.warning(f"[HasPrajaSecret] Invalid secret provided - Header: '{secret_header[:10]}...'")
        return False

