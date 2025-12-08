from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional

from django.utils.deprecation import MiddlewareMixin

from accounts.models import SupabaseAuthUser
from .engine import clear_request_context, set_request_context

logger = logging.getLogger(__name__)


class HistoryMiddleware(MiddlewareMixin):
    """
    Captures actor + request metadata so history writes stay automatic.
    """

    def process_request(self, request):
        actor_user = self._resolve_actor_user(request)
        actor_identifier = self._build_actor_identifier(request)
        request_metadata = self._build_metadata(request)
        
        request._actor_user = actor_user
        request._actor_identifier = actor_identifier
        request._request_metadata = request_metadata
        
        # Log actor resolution for debugging
        logger.info(
            f"HistoryMiddleware: Resolved actor for {request.path} | "
            f"actor_user_id={actor_user.id if actor_user else None} | "
            f"actor_label={actor_identifier} | "
            f"user_authenticated={getattr(request.user, 'is_authenticated', False) if hasattr(request, 'user') else False}"
        )
        
        if hasattr(request, 'user') and request.user.is_authenticated:
            user = request.user
            supabase_uid = getattr(user, 'supabase_uid', None)
            user_email = getattr(user, 'email', None)
            logger.debug(
                f"HistoryMiddleware: request.user details | "
                f"email={user_email} | "
                f"supabase_uid={supabase_uid} | "
                f"actor_user_found={actor_user is not None}"
            )
        
        set_request_context(request)

    def process_response(self, request, response):
        clear_request_context()
        return response

    def process_exception(self, request, exception):
        clear_request_context()
        return None

    def _resolve_actor_user(self, request) -> Optional[SupabaseAuthUser]:
        """
        Resolve SupabaseAuthUser from request.user or JWT claims.
        
        Since DRF authentication happens after middleware, we also check JWT claims
        directly from the Authorization header as a fallback.
        """
        # First, try to get from request.user (if already authenticated)
        user = getattr(request, "user", None)
        supabase_uid = None
        
        if user and getattr(user, "is_authenticated", False):
            supabase_uid = getattr(user, "supabase_uid", None)
            if supabase_uid:
                logger.debug(f"HistoryMiddleware._resolve_actor_user: Got supabase_uid from request.user: {supabase_uid}")
        
        # If not found, try to extract from JWT token directly (for DRF views)
        if not supabase_uid:
            supabase_uid = self._extract_supabase_uid_from_jwt(request)
            if supabase_uid:
                logger.debug(f"HistoryMiddleware._resolve_actor_user: Got supabase_uid from JWT: {supabase_uid}")
        
        if not supabase_uid:
            if user:
                logger.debug(f"HistoryMiddleware._resolve_actor_user: User exists but no supabase_uid (user={user}, is_authenticated={getattr(user, 'is_authenticated', False)})")
            else:
                logger.debug("HistoryMiddleware._resolve_actor_user: No request.user found and no JWT supabase_uid")
            return None
        
        try:
            # SupabaseAuthUser mirrors auth.users table (unmanaged model)
            # The id field is a UUID that matches supabase_uid
            logger.debug(f"HistoryMiddleware._resolve_actor_user: Looking up SupabaseAuthUser for supabase_uid={supabase_uid}")
            actor_user = SupabaseAuthUser.objects.filter(id=supabase_uid).first()
            
            if actor_user:
                logger.info(
                    f"HistoryMiddleware._resolve_actor_user: Found SupabaseAuthUser | "
                    f"id={actor_user.id} | email={getattr(actor_user, 'email', 'N/A')}"
                )
            else:
                # Log for debugging - this might happen if the user exists in authentication.User
                # but not yet in SupabaseAuthUser (unmanaged mirror of auth.users)
                user_email = getattr(user, "email", None) if user else None
                logger.warning(
                    f"HistoryMiddleware._resolve_actor_user: SupabaseAuthUser NOT found | "
                    f"supabase_uid={supabase_uid} | "
                    f"user_email={user_email} | "
                    f"This user exists in authentication.User but not in SupabaseAuthUser (unmanaged mirror)"
                )
            
            return actor_user
        except Exception as e:
            logger.error(
                f"HistoryMiddleware._resolve_actor_user: Exception resolving SupabaseAuthUser | "
                f"supabase_uid={supabase_uid} | error={e}",
                exc_info=True
            )
            return None
    
    def _extract_supabase_uid_from_jwt(self, request) -> Optional[str]:
        """
        Extract supabase_uid from JWT token in Authorization header.
        This is needed because DRF authentication happens after middleware.
        """
        try:
            from config.supabase_auth import _get_bearer, _verify_jwt
            
            token = _get_bearer(request)
            if not token:
                return None
            
            claims = _verify_jwt(token)
            supabase_uid = claims.get("sub") or claims.get("user_id")
            
            if supabase_uid:
                logger.debug(f"HistoryMiddleware._extract_supabase_uid_from_jwt: Extracted supabase_uid={supabase_uid} from JWT")
            
            return supabase_uid
        except Exception as e:
            # Don't log errors for missing/invalid tokens - that's normal for unauthenticated requests
            logger.debug(f"HistoryMiddleware._extract_supabase_uid_from_jwt: Could not extract from JWT: {e}")
            return None

    def _build_actor_identifier(self, request) -> Optional[str]:
        """
        Build actor identifier string. Always returns something if user is authenticated.
        Priority: actor_user email > user email > JWT email > supabase_uid > service name
        """
        # If we found a SupabaseAuthUser, use its email
        actor_user = getattr(request, "_actor_user", None)
        if actor_user:
            return getattr(actor_user, "email", None) or str(actor_user.id)
        
        # Fallback to authentication.User email/uid
        user = getattr(request, "user", None)
        if user and getattr(user, "is_authenticated", False):
            # Prefer email, fallback to supabase_uid
            email = getattr(user, "email", None)
            if email:
                return email
            supabase_uid = getattr(user, "supabase_uid", None)
            if supabase_uid:
                return f"user:{supabase_uid}"
        
        # Try to get email from JWT claims as fallback
        try:
            from config.supabase_auth import _get_bearer, _verify_jwt
            token = _get_bearer(request)
            if token:
                claims = _verify_jwt(token)
                email = claims.get("email")
                if email:
                    return email
        except Exception:
            pass  # Ignore JWT errors here
        
        # Check for service name header
        service_name = request.META.get("HTTP_X_SERVICE_NAME")
        if service_name:
            return service_name
        
        return None

    def _build_metadata(self, request) -> Dict[str, Any]:
        request_id = (
            request.META.get("HTTP_X_REQUEST_ID")
            or request.headers.get("X-Request-ID")
            or str(uuid.uuid4())
        )
        forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR", "")
        ip = forwarded_for.split(",")[0].strip() if forwarded_for else request.META.get("REMOTE_ADDR")
        user_agent = request.META.get("HTTP_USER_AGENT")
        endpoint = request.get_full_path()
        source = request.META.get("HTTP_X_SOURCE") or request.META.get("HTTP_X_AUTOMATION_SOURCE")

        metadata = {
            "request_id": request_id,
            "ip": ip,
            "user_agent": user_agent,
            "endpoint": endpoint,
            "source": source,
        }
        return {key: value for key, value in metadata.items() if value}


__all__ = ["HistoryMiddleware"]


