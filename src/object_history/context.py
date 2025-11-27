from __future__ import annotations

import contextlib
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, Optional


@dataclass
class HistoryContext:
    actor_user: Any = None
    actor_identifier: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


_context_state = threading.local()


def _get_state() -> HistoryContext:
    ctx = getattr(_context_state, "value", None)
    if ctx is None:
        ctx = HistoryContext()
        _context_state.value = ctx
    return ctx


def set_context(*, actor_user: Any = None, actor_identifier: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
    _context_state.value = HistoryContext(
        actor_user=actor_user,
        actor_identifier=actor_identifier,
        metadata=metadata or {},
    )


def update_metadata(**kwargs: Any) -> None:
    ctx = _get_state()
    ctx.metadata.update(kwargs)


def clear_context() -> None:
    if hasattr(_context_state, "value"):
        delattr(_context_state, "value")


def get_context() -> HistoryContext:
    return _get_state()


@contextlib.contextmanager
def actor_context(*, actor_label: str, metadata: Optional[Dict[str, Any]] = None) -> Iterator[None]:
    previous = get_context()
    set_context(actor_user=None, actor_identifier=actor_label, metadata=metadata or {})
    try:
        yield
    finally:
        _context_state.value = previous

