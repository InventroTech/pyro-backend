from django.urls import path, re_path

from .consumers import NotificationConsumer

websocket_urlpatterns = [
    path("ws/notifications/", NotificationConsumer.as_asgi()),
    re_path(r"^/?ws/notifications/?$", NotificationConsumer.as_asgi()),
]
