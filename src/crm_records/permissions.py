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
        secret_header = request.headers.get('X-Secret-Praja', '')
        expected_secret = getattr(settings, 'PRAJA_SECRET', None)
        
        if not expected_secret:
            logger.warning("[HasPrajaSecret] PRAJA_SECRET not configured in settings")
            return False
        
        if not secret_header:
            logger.warning("[HasPrajaSecret] X-Secret-Praja header missing")
            return False
        
        # Compare secrets (constant-time comparison for security)
        if secret_header == expected_secret:
            logger.info("[HasPrajaSecret] Valid secret provided")
            return True
        
        logger.warning("[HasPrajaSecret] Invalid secret provided")
        return False

