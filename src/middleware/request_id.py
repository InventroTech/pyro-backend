import uuid
from contextvars import ContextVar

from django.http import HttpRequest, HttpResponse
from django.utils.deprecation import MiddlewareMixin

_request_id_var: ContextVar[str] = ContextVar('request_id', default='-')

REQUEST_ID_HEADER = 'X-Request-ID'


def get_request_id() -> str:
    """Return the current request's correlation ID (safe to call from anywhere)."""
    return _request_id_var.get()


class RequestIDMiddleware(MiddlewareMixin):
    """
    Assigns a unique ID to every request for log correlation.

    - Reuses an incoming X-Request-ID header (from a load balancer or client)
      if present; otherwise generates a UUID4.
    - Stores the ID on ``request.id`` and in a context variable so that the
      logging filter can pick it up automatically.
    - Echoes the ID back in the X-Request-ID response header.
    """

    def process_request(self, request: HttpRequest) -> None:
        rid = request.META.get('HTTP_X_REQUEST_ID') or uuid.uuid4().hex[:12]
        request.id = rid
        _request_id_var.set(rid)

    def process_response(self, request: HttpRequest, response: HttpResponse) -> HttpResponse:
        rid = getattr(request, 'id', None) or _request_id_var.get()
        response[REQUEST_ID_HEADER] = rid
        _request_id_var.set('-')
        return response
