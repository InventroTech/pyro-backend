"""
ASGI config for config project.

Routes HTTP to Django and WebSocket connections to Channels consumers.
"""

import os

from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

django_asgi_app = get_asgi_application()

from realtime.asgi_loop import MainEventLoopMiddleware  # noqa: E402
from realtime.auth import JWTAuthMiddleware  # noqa: E402
from realtime.routing import websocket_urlpatterns  # noqa: E402
from realtime.websocket_origin import wrap_websocket_origin_validator  # noqa: E402

_asgi_inner = ProtocolTypeRouter(
    {
        "http": django_asgi_app,
        "websocket": wrap_websocket_origin_validator(
            JWTAuthMiddleware(URLRouter(websocket_urlpatterns))
        ),
    }
)

application = MainEventLoopMiddleware(_asgi_inner)
