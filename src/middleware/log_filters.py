import logging

from middleware.request_id import get_request_id


class RequestIDFilter(logging.Filter):
    """
    Logging filter that injects the current request ID into every log record.

    Attach this filter to a handler (or root logger) in Django's LOGGING
    config and the ``{request_id}`` placeholder becomes available in any
    formatter that uses ``{``-style formatting.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id()
        return True
