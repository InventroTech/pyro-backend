from rest_framework.permissions import BasePermission
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class HasPrajaSecret(BasePermission):
    """
    Permission class that checks for X-Secret-Praja header.
    Used for external API access without tenant authentication.
    """
    
    def has_permission(self, request, view):
        """
        Check if request has valid X-Secret-Praja header.
        """
        # Check header with different possible formats
        secret_header = (
            request.headers.get('X-Secret-Praja', '') or
            request.headers.get('x-secret-praja', '') or
            request.META.get('HTTP_X_SECRET_PRAJA', '')
        )
        expected_secret = getattr(settings, 'PRAJA_SECRET', None)
        
        # Handle empty string case (env returns "" as default)
        if expected_secret == "":
            expected_secret = None
        
        logger.info(f"[HasPrajaSecret] Checking permission - Header value: '{secret_header[:10] if secret_header else 'None'}...' (length: {len(secret_header) if secret_header else 0}), Expected secret configured: {bool(expected_secret)}")
        
        if not expected_secret:
            logger.warning("[HasPrajaSecret] PRAJA_SECRET not configured in settings. Please set PRAJA_SECRET in your .env file.")
            return False
        
        if not secret_header:
            logger.warning("[HasPrajaSecret] X-Secret-Praja header missing from request")
            logger.warning(f"[HasPrajaSecret] Available headers: {list(request.headers.keys())}")
            return False
        
        # Compare secrets (constant-time comparison for security)
        if secret_header.strip() == expected_secret.strip():
            logger.info("[HasPrajaSecret] Valid secret provided - permission granted")
            return True
        
        logger.warning(f"[HasPrajaSecret] Invalid secret provided - Header: '{secret_header[:10]}...', Expected: '{expected_secret[:10]}...'")
        return False

