"""Frontend redirect targets for auth emails and provider logins."""
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from django.conf import settings


def safe_redirect_url(redirect_to: str | None) -> str:
    """Return ``redirect_to`` if its origin is allow-listed, else ``AUTH_SITE_URL``."""
    if redirect_to:
        parts = urlsplit(redirect_to)
        origin = f"{parts.scheme}://{parts.netloc}"
        if parts.scheme in ("http", "https") and origin in settings.AUTH_ALLOWED_REDIRECT_ORIGINS:
            return redirect_to
    return settings.AUTH_SITE_URL


def with_query(url: str, params: dict) -> str:
    parts = urlsplit(url)
    query = parse_qsl(parts.query, keep_blank_values=True) + list(params.items())
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
