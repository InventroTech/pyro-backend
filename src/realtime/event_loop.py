from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_main_loop: Optional[asyncio.AbstractEventLoop] = None


def set_main_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    global _main_loop
    _main_loop = loop


def get_main_event_loop() -> Optional[asyncio.AbstractEventLoop]:
    return _main_loop


def schedule_on_main_loop(coro, *, timeout: float = 5.0) -> bool:
    """
    Run an async coroutine on the Daphne/ASGI event loop from a sync thread
    (post_save, pg_listener, management commands). Required for InMemoryChannelLayer.
    """
    loop = _main_loop
    if loop is None or not loop.is_running():
        return False

    try:
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        future.result(timeout=timeout)
        return True
    except Exception:
        logger.exception("Failed to schedule realtime broadcast on main event loop")
        return False
