from __future__ import annotations

import asyncio

from .event_loop import set_main_event_loop


class MainEventLoopMiddleware:
    """Capture the ASGI server event loop so sync threads can publish to Channels."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        set_main_event_loop(asyncio.get_running_loop())
        return await self.app(scope, receive, send)
