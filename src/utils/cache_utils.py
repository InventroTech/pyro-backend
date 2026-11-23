"""
Reusable Redis/LocMem cache helpers for expensive API payloads.

Import from ``utils.cache_utils`` in any app (analytics, CRM, tickets, ...).
Uses Django's configured CACHES backend (Redis when REDIS_URL is set,
LocMem otherwise).

Handles:
- unique keys (+ tenant version for invalidation)
- TTL
- miss → compute from DB
- Redis failures → fall back to compute (app still works)
- stampede lock so concurrent misses don't all rebuild
- hit/miss/error counters for monitoring
- invalidation via version bump (no Redis KEYS scan)

Key shape: ``{prefix}:v{version}:{tenant_id}:{stable_parts…}``

Do not cache secrets, JWTs, or passwords — only JSON-safe aggregates.

Example::

    from utils.cache_utils import cached_payload, DEFAULT_TTL_SECONDS, wants_refresh

    data = cached_payload(
        prefix="cse:overview",
        tenant_id=request.tenant.id,
        parts={"date": "2026-01-15"},
        compute=lambda: expensive_query(),
        ttl=DEFAULT_TTL_SECONDS,
        refresh=wants_refresh(request),
    )
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Optional

from django.core.cache import cache

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 45
FILTER_OPTIONS_TTL_SECONDS = 60
LOCK_TTL_SECONDS = 10
LOCK_WAIT_SECONDS = 1.0
LOCK_POLL_SECONDS = 0.05

# Process-local counters (per Gunicorn/Daphne worker). Good for logs/tests;
# aggregate across workers via log shipping or Redis metrics later.
_STATS_LOCK = threading.Lock()
_STATS = {
    "hit": 0,
    "miss": 0,
    "refresh": 0,
    "get_error": 0,
    "set_error": 0,
    "lock_wait": 0,
    "lock_timeout": 0,
    "compute": 0,
    "invalidate": 0,
}


def get_page_cache_stats() -> dict[str, int]:
    with _STATS_LOCK:
        return dict(_STATS)


def reset_page_cache_stats() -> None:
    with _STATS_LOCK:
        for key in _STATS:
            _STATS[key] = 0


def _bump_stat(name: str, n: int = 1) -> None:
    with _STATS_LOCK:
        _STATS[name] = _STATS.get(name, 0) + n


def _stable_part(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        items = sorted(_stable_part(v) for v in value)
        return "[" + ",".join(items) + "]"
    if isinstance(value, Mapping):
        items = []
        for key in sorted(value.keys(), key=lambda k: str(k)):
            items.append(f"{_stable_part(key)}={_stable_part(value[key])}")
        return "{" + ",".join(items) + "}"
    text = str(value).strip()
    return text if text else "-"


def cache_family(prefix: str) -> str:
    """Map ``cse:overview`` → ``cse`` for versioned invalidation."""
    return str(prefix).strip(":").split(":", 1)[0] or "page"


def _version_key(family: str, tenant_id) -> str:
    return f"pcver:{family}:{tenant_id}"


def get_cache_version(family: str, tenant_id) -> int:
    try:
        value = cache.get(_version_key(family, tenant_id))
        if value is None:
            return 1
        return max(1, int(value))
    except Exception:
        _bump_stat("get_error")
        logger.exception(
            "cache_utils version get failed family=%s tenant=%s", family, tenant_id
        )
        return 1


def invalidate_cache_family(tenant_id, family: str) -> None:
    """
    Invalidate all keys for a tenant+family by bumping the version.

    Old keys expire via TTL; new reads use the new version (cache miss).
    Works on LocMem and Redis without pattern deletes.
    """
    family = cache_family(family)
    key = _version_key(family, tenant_id)
    try:
        try:
            cache.incr(key)
        except ValueError:
            # Key missing or non-int — seed at 2 so existing v1 keys are skipped.
            cache.set(key, 2, timeout=None)
        _bump_stat("invalidate")
        logger.info("cache_utils invalidate family=%s tenant=%s", family, tenant_id)
    except Exception:
        logger.exception(
            "cache_utils invalidate failed family=%s tenant=%s", family, tenant_id
        )


def invalidate_cache_families(tenant_id, families: Iterable[str]) -> None:
    for family in families:
        invalidate_cache_family(tenant_id, family)


def invalidate_cse_analytics_cache(tenant_id) -> None:
    invalidate_cache_family(tenant_id, "cse")


def invalidate_team_analytics_cache(tenant_id) -> None:
    invalidate_cache_family(tenant_id, "team")


def invalidate_rm_prd_cache(tenant_id) -> None:
    invalidate_cache_family(tenant_id, "rm")


def invalidate_ticket_filter_options_cache(tenant_id) -> None:
    invalidate_cache_family(tenant_id, "ticket")


def build_cache_key(prefix: str, tenant_id, **parts: Any) -> str:
    """Build a stable Redis/LocMem key including the tenant family version."""
    family = cache_family(prefix)
    version = get_cache_version(family, tenant_id)
    chunks = [str(prefix).strip(":"), f"v{version}", str(tenant_id)]
    for name in sorted(parts.keys()):
        raw = _stable_part(parts[name])
        if len(raw) > 64:
            raw = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
        chunks.append(f"{name}={raw}")
    return ":".join(chunks)


def wants_refresh(request) -> bool:
    """True when caller asks to bypass cache (``?refresh=1``)."""
    if request is None:
        return False
    raw = request.query_params.get("refresh") if hasattr(request, "query_params") else None
    if raw is None and hasattr(request, "GET"):
        raw = request.GET.get("refresh")
    if raw is None:
        return False
    return str(raw).strip().lower() in ("1", "true", "yes")


def _safe_get(key: str) -> Any:
    try:
        return cache.get(key)
    except Exception:
        _bump_stat("get_error")
        logger.exception("cache_utils get failed key=%s", key)
        return None


def _safe_set(key: str, value: Any, ttl: int) -> None:
    try:
        cache.set(key, value, timeout=ttl)
    except Exception:
        _bump_stat("set_error")
        logger.exception("cache_utils set failed key=%s", key)


def _safe_add(key: str, value: Any, ttl: int) -> bool:
    try:
        return bool(cache.add(key, value, timeout=ttl))
    except Exception:
        _bump_stat("set_error")
        logger.exception("cache_utils add failed key=%s", key)
        # Fail open: allow this worker to compute rather than stall everyone.
        return True


def _safe_delete(key: str) -> None:
    try:
        cache.delete(key)
    except Exception:
        logger.exception("cache_utils delete failed key=%s", key)


def _wait_for_cached_value(key: str) -> Any:
    """Poll briefly while another worker holds the stampede lock."""
    _bump_stat("lock_wait")
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    while time.monotonic() < deadline:
        time.sleep(LOCK_POLL_SECONDS)
        cached = _safe_get(key)
        if cached is not None:
            _bump_stat("hit")
            logger.debug("cache_utils HIT after lock wait key=%s", key)
            return cached
    _bump_stat("lock_timeout")
    return None


def cached_payload(
    *,
    prefix: str,
    tenant_id,
    parts: Optional[Mapping[str, Any]] = None,
    compute: Callable[[], Any],
    ttl: int = DEFAULT_TTL_SECONDS,
    refresh: bool = False,
) -> Any:
    """
    Return cached JSON-serializable payload, or compute + store it.

    ``compute`` must return something Django's cache backend can pickle
    (dicts/lists/primitives — typical DRF ``serializer.data``).
    Never pass secrets/JWTs into ``compute`` results.
    """
    key = build_cache_key(prefix, tenant_id, **(parts or {}))
    lock_key = f"{key}:lock"

    if not refresh:
        cached = _safe_get(key)
        if cached is not None:
            _bump_stat("hit")
            logger.debug("cache_utils HIT key=%s", key)
            return cached
    else:
        _bump_stat("refresh")

    got_lock = _safe_add(lock_key, "1", LOCK_TTL_SECONDS)
    if not got_lock and not refresh:
        waited = _wait_for_cached_value(key)
        if waited is not None:
            return waited

    logger.debug("cache_utils MISS key=%s refresh=%s", key, refresh)
    _bump_stat("miss")
    _bump_stat("compute")
    try:
        payload = compute()
    finally:
        # Always release lock even if compute raises.
        _safe_delete(lock_key)

    _safe_set(key, payload, ttl)
    return payload


def cache_key_fingerprint(value: Any) -> str:
    """Short fingerprint for bulky filter blobs in keys."""
    raw = json.dumps(value, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
