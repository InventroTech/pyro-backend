"""
Prometheus HTTP metrics middleware and /metrics scrape endpoint.

Emits:
  - http_request_duration_seconds  (histogram) — latency by method/endpoint/status
  - http_requests_total            (counter)   — request count by method/endpoint/status

Endpoint labels prefer Django URL routes (low cardinality). Raw paths with UUIDs/IDs
are normalized as a fallback.

Auth: set METRICS_AUTH_TOKEN and scrape with Authorization: Bearer <token>
      or X-Metrics-Token: <token>. In non-dev, an empty token hides the endpoint (404).
"""
from __future__ import annotations

import os
import re
import time
from typing import Optional

from django.conf import settings
from django.http import HttpRequest, HttpResponse, HttpResponseNotFound, HttpResponseForbidden
from django.utils.deprecation import MiddlewareMixin
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
    multiprocess,
    REGISTRY,
)

SKIP_METRIC_PREFIXES = (
    "/metrics",
    "/health",
    "/_health",
    "/static",
    "/media",
    "/favicon.ico",
    "/admin/jsi18n",
)

# Replace UUID / numeric path segments so fallback labels stay low-cardinality.
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_NUMERIC_RE = re.compile(r"^\d+$")

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    labelnames=("method", "endpoint", "status"),
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    labelnames=("method", "endpoint", "status"),
)


def _should_skip(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in SKIP_METRIC_PREFIXES)


def _normalize_path(path: str) -> str:
    parts = []
    for segment in path.strip("/").split("/"):
        if not segment:
            continue
        if _UUID_RE.match(segment):
            parts.append(":id")
        elif _NUMERIC_RE.match(segment):
            parts.append(":id")
        else:
            parts.append(segment)
    return "/" + "/".join(parts) if parts else "/"


def _endpoint_label(request: HttpRequest) -> str:
    match = getattr(request, "resolver_match", None)
    if match is not None:
        route = getattr(match, "route", None) or ""
        if route:
            return route if route.startswith("/") else f"/{route}"
        view_name = getattr(match, "view_name", None)
        if view_name:
            return view_name
    return _normalize_path(request.path)


def _expected_token() -> str:
    return getattr(settings, "METRICS_AUTH_TOKEN", "") or os.environ.get("METRICS_AUTH_TOKEN", "")


def _extract_metrics_token(request: HttpRequest) -> Optional[str]:
    header_token = request.META.get("HTTP_X_METRICS_TOKEN") or request.headers.get("X-Metrics-Token")
    if header_token:
        return header_token.strip()

    auth = request.META.get("HTTP_AUTHORIZATION") or request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return None


def _metrics_authorized(request: HttpRequest) -> bool:
    expected = _expected_token()
    if not expected:
        # Local/dev convenience: open scrape when no token is configured.
        return bool(getattr(settings, "IS_DEV", False))
    provided = _extract_metrics_token(request)
    return bool(provided) and provided == expected


def metrics_view(request: HttpRequest) -> HttpResponse:
    """Prometheus scrape endpoint (GET /metrics)."""
    expected = _expected_token()
    if not expected and not getattr(settings, "IS_DEV", False):
        return HttpResponseNotFound()

    if not _metrics_authorized(request):
        return HttpResponseForbidden("Forbidden")

    multiproc_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR", "")
    if multiproc_dir:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
    else:
        registry = REGISTRY

    payload = generate_latest(registry)
    return HttpResponse(payload, content_type=CONTENT_TYPE_LATEST)


class PrometheusMetricsMiddleware(MiddlewareMixin):
    """Record per-request latency and count for Prometheus."""

    def process_request(self, request: HttpRequest):
        if _should_skip(request.path):
            return None
        request._prometheus_start_time = time.perf_counter()
        return None

    def process_response(self, request: HttpRequest, response: HttpResponse):
        start = getattr(request, "_prometheus_start_time", None)
        if start is None:
            return response

        duration = time.perf_counter() - start
        method = request.method or "GET"
        endpoint = _endpoint_label(request)
        status = str(getattr(response, "status_code", 0))

        REQUEST_LATENCY.labels(method=method, endpoint=endpoint, status=status).observe(duration)
        REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status).inc()
        return response
