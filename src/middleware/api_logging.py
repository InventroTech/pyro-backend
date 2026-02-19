"""
API Request/Response Logging Middleware

Logs detailed information about every API call including:
- Endpoint and HTTP method
- Request payload (body, query params, headers)
- Response status code and body
- Request timing
- User and tenant information
- IP address and user agent

HOW IT WORKS:
1. process_request() runs when request comes in - stores start time
2. View processes the request
3. process_response() runs when response is ready - captures everything and logs once
4. Formatter (in log_formatters.py) formats the log_data into readable output

SECURITY:
- All sensitive fields (password, token, etc.) are automatically masked before logging
- This happens recursively through nested dictionaries and lists
"""
import json
import logging
import time
from typing import Any, Dict, Optional
from django.utils.deprecation import MiddlewareMixin
from django.http import HttpRequest, HttpResponse, StreamingHttpResponse
from django.conf import settings

logger = logging.getLogger('api_logging')

SKIP_LOGGING_PREFIXES = (
    "/admin",
    "/health",
    "/_health",
    "/metrics",
    "/docs",
    "/schema",
    "/static",
    "/media",
    "/favicon.ico",
    "/sentry",
)

SENSITIVE_FIELDS = {
    'password', 'token', 'secret', 'api_key', 'apikey', 'authorization',
    'access_token', 'refresh_token', 'bearer', 'credential', 'auth',
    'private_key', 'privatekey', 'secret_key', 'secretkey'
}


def _is_sensitive_field(key: str) -> bool:
    """Check if a field name contains sensitive information."""
    key_lower = key.lower()
    return any(sensitive in key_lower for sensitive in SENSITIVE_FIELDS)


def _mask_sensitive_data(data: Any, depth: int = 0) -> Any:
    """
    Recursively mask sensitive fields in data structures.
    Prevents logging passwords, tokens, and other sensitive information.
    """
    if depth > 10:
        return "[MAX_DEPTH_REACHED]"
    
    if isinstance(data, dict):
        masked = {}
        for key, value in data.items():
            if _is_sensitive_field(key):
                masked[key] = "[MASKED]"
            else:
                masked[key] = _mask_sensitive_data(value, depth + 1)
        return masked
    elif isinstance(data, list):
        return [_mask_sensitive_data(item, depth + 1) for item in data]
    elif isinstance(data, str) and len(data) > 100:
        return data[:100] + "...[TRUNCATED]"
    else:
        return data


def _get_request_body(request: HttpRequest) -> Optional[Dict[str, Any]]:
    """Extract and parse request body."""
    try:
        content_type = request.content_type or ''
        
        if 'application/json' in content_type:
            if hasattr(request, '_body'):
                body_str = request._body.decode('utf-8') if isinstance(request._body, bytes) else request._body
            else:
                body_str = request.body.decode('utf-8') if request.body else '{}'
            try:
                return json.loads(body_str) if body_str else {}
            except json.JSONDecodeError:
                return {"raw": body_str[:500]}
        elif 'application/x-www-form-urlencoded' in content_type or 'multipart/form-data' in content_type:
            return dict(request.POST)
        else:
            if request.body:
                body_str = request.body.decode('utf-8', errors='ignore')[:500]
                return {"raw": body_str}
            return None
    except Exception as e:
        logger.warning(f"Error parsing request body: {e}")
        return None


def _get_query_params(request: HttpRequest) -> Dict[str, Any]:
    """Extract query parameters."""
    try:
        return dict(request.GET)
    except Exception:
        return {}


def _get_response_body(response: HttpResponse) -> Optional[Dict[str, Any]]:
    """Extract response body if available."""
    try:
        if isinstance(response, StreamingHttpResponse):
            return {"streaming": True}
        
        if hasattr(response, 'content'):
            content = response.content
            if content:
                try:
                    content_str = content.decode('utf-8')
                    try:
                        return json.loads(content_str)
                    except json.JSONDecodeError:
                        return {"text": content_str[:500]}
                except Exception:
                    return {"binary": True}
        return None
    except Exception as e:
        logger.warning(f"Error parsing response body: {e}")
        return None


def _get_user_info(request: HttpRequest) -> Dict[str, Any]:
    """Extract user information from request."""
    user_info = {
        "is_authenticated": False,
        "user_id": None,
        "email": None,
        "username": None,
    }
    
    if hasattr(request, 'user') and request.user.is_authenticated:
        user_info["is_authenticated"] = True
        user_info["user_id"] = str(request.user.id) if hasattr(request.user, 'id') else None
        user_info["email"] = getattr(request.user, 'email', None)
        user_info["username"] = getattr(request.user, 'username', None)
        user_info["supabase_uid"] = getattr(request.user, 'supabase_uid', None)
    
    return user_info


def _get_tenant_info(request: HttpRequest) -> Dict[str, Any]:
    """Extract tenant information from request."""
    tenant_info = {
        "tenant_id": None,
        "tenant_slug": None,
        "tenant_name": None,
    }
    
    if hasattr(request, 'tenant') and request.tenant:
        tenant_info["tenant_id"] = str(request.tenant.id)
        tenant_info["tenant_slug"] = getattr(request.tenant, 'slug', None)
        tenant_info["tenant_name"] = getattr(request.tenant, 'name', None)
    
    return tenant_info


def _should_skip_logging(path: str) -> bool:
    """Check if logging should be skipped for this path."""
    return any(path.startswith(prefix) for prefix in SKIP_LOGGING_PREFIXES)


class APILoggingMiddleware(MiddlewareMixin):
    """
    Middleware to log detailed information about API requests and responses.
    
    Logs:
    - Endpoint and HTTP method
    - Request headers (with sensitive data masked)
    - Request body/payload (with sensitive data masked)
    - Query parameters
    - Response status code
    - Response body (truncated if large)
    - Request timing
    - User and tenant information
    - IP address and user agent
    """
    
    def process_request(self, request: HttpRequest):
        """Capture request start time and log request details."""
        if _should_skip_logging(request.path):
            return None
        
        request._api_log_start_time = time.time()
        
        request_info = {
            "method": request.method,
            "path": request.path,
            "endpoint": request.get_full_path(),
            "query_params": _mask_sensitive_data(_get_query_params(request)),
            "headers": _mask_sensitive_data({
                key.replace('HTTP_', '').replace('_', '-').title(): value
                for key, value in request.META.items()
                if key.startswith('HTTP_') or key in ('CONTENT_TYPE', 'CONTENT_LENGTH')
            }),
            "ip_address": self._get_client_ip(request),
            "user_agent": request.META.get('HTTP_USER_AGENT', 'Unknown'),
        }
        
        if request.method in ('POST', 'PUT', 'PATCH', 'DELETE'):
            body = _get_request_body(request)
            if body:
                request_info["payload"] = _mask_sensitive_data(body)
        
        request_info["user"] = _get_user_info(request)
        request_info["tenant"] = _get_tenant_info(request)
        
        return None
    
    def process_response(self, request: HttpRequest, response: HttpResponse):
        """Log response details and calculate request duration."""
        if _should_skip_logging(request.path):
            return response
        
        duration = None
        if hasattr(request, '_api_log_start_time'):
            duration = round((time.time() - request._api_log_start_time) * 1000, 2)
        
        response_info = {
            "status_code": response.status_code,
            "status_text": response.reason_phrase if hasattr(response, 'reason_phrase') else None,
            "content_type": response.get('Content-Type', 'Unknown'),
        }
        
        if not isinstance(response, StreamingHttpResponse):
            response_body = _get_response_body(response)
            if response_body:
                response_info["body"] = _mask_sensitive_data(response_body)
        
        log_data = {
            "method": request.method,
            "path": request.path,
            "endpoint": request.get_full_path(),
            "status_code": response.status_code,
            "duration_ms": duration,
            "user": _get_user_info(request),
            "tenant": _get_tenant_info(request),
            "query_params": _mask_sensitive_data(_get_query_params(request)),
            "headers": _mask_sensitive_data({
                key.replace('HTTP_', '').replace('_', '-').title(): value
                for key, value in request.META.items()
                if key.startswith('HTTP_') or key in ('CONTENT_TYPE', 'CONTENT_LENGTH')
            }),
            "ip_address": self._get_client_ip(request),
        }
        
        if request.method in ('POST', 'PUT', 'PATCH', 'DELETE'):
            body = _get_request_body(request)
            if body:
                log_data["payload"] = _mask_sensitive_data(body)
        
        log_data["response"] = response_info
        if response_info.get("body"):
            log_data["response_body"] = response_info["body"]
        
        if response.status_code >= 500:
            logger.error(
                "",
                extra={
                    "log_data": log_data,
                    "log_type": "api_response"
                }
            )
        elif response.status_code >= 400:
            logger.warning(
                "",
                extra={
                    "log_data": log_data,
                    "log_type": "api_response"
                }
            )
        else:
            logger.info(
                "",
                extra={
                    "log_data": log_data,
                    "log_type": "api_response"
                }
            )
        
        return response
    
    def process_exception(self, request: HttpRequest, exception: Exception):
        """Log exceptions that occur during request processing."""
        if _should_skip_logging(request.path):
            return None
        
        duration = None
        if hasattr(request, '_api_log_start_time'):
            duration = round((time.time() - request._api_log_start_time) * 1000, 2)
        
        logger.error(
            "",
            extra={
                "method": request.method,
                "path": request.path,
                "exception_type": type(exception).__name__,
                "exception_message": str(exception),
                "duration_ms": duration,
                "log_type": "api_exception"
            },
            exc_info=True
        )
        
        return None
    
    def _get_client_ip(self, request: HttpRequest) -> str:
        """Extract client IP address from request."""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0].strip()
        else:
            ip = request.META.get('REMOTE_ADDR', 'Unknown')
        return ip
