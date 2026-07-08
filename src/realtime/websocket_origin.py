from django.conf import settings
from channels.security.websocket import OriginValidator


def wrap_websocket_origin_validator(inner):
    """Allow browser origins from CORS_ALLOWED_ORIGINS (not only API hostnames)."""
    if getattr(settings, "CORS_ALLOW_ALL_ORIGINS", False) or getattr(settings, "IS_DEV", False):
        return inner

    origins = list(getattr(settings, "CORS_ALLOWED_ORIGINS", []))
    if getattr(settings, "IS_DEV", False):
        origins.extend(
            [
                "http://localhost:8080",
                "http://127.0.0.1:8080",
                "http://localhost:5173",
                "http://127.0.0.1:5173",
                "http://localhost:8000",
                "http://127.0.0.1:8000",
            ]
        )

    origins = list(dict.fromkeys(origins))
    if not origins:
        return inner
    return OriginValidator(inner, origins)
