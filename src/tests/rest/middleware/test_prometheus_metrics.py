"""
Tests for Prometheus /metrics endpoint and path label helpers.

Run:
    pytest src/tests/rest/middleware/test_prometheus_metrics.py -v
"""
from __future__ import annotations

import pytest
from django.test import override_settings
from django.http import HttpResponse

from middleware.prometheus_metrics import (
    PrometheusMetricsMiddleware,
    _endpoint_label,
    _normalize_path,
    metrics_view,
)


@pytest.mark.parametrize(
    "path,expected",
    [
        ("/support-ticket/abc/", "/support-ticket/abc"),
        (
            "/support-ticket/11111111-1111-1111-1111-111111111111/",
            "/support-ticket/:id",
        ),
        ("/crm-records/leads/42/", "/crm-records/leads/:id"),
        ("/", "/"),
    ],
)
def test_normalize_path(path, expected):
    assert _normalize_path(path) == expected


def test_endpoint_label_uses_resolver_route(rf):
    request = rf.get("/unused/")
    request.resolver_match = type(
        "Match",
        (),
        {"route": "support-ticket/<uuid:pk>/", "view_name": "unused"},
    )()
    assert _endpoint_label(request) == "/support-ticket/<uuid:pk>/"


@override_settings(IS_DEV=False, METRICS_AUTH_TOKEN="")
def test_metrics_hidden_without_token_in_non_dev(rf):
    response = metrics_view(rf.get("/metrics"))
    assert response.status_code == 404


@override_settings(IS_DEV=True, METRICS_AUTH_TOKEN="")
def test_metrics_open_in_dev_without_token(rf):
    response = metrics_view(rf.get("/metrics"))
    assert response.status_code == 200
    body = response.content.decode("utf-8")
    assert "http_request_duration_seconds" in body
    assert "http_requests_total" in body


@override_settings(IS_DEV=False, METRICS_AUTH_TOKEN="secret-metrics-token")
def test_metrics_requires_token_when_configured(rf):
    assert metrics_view(rf.get("/metrics")).status_code == 403

    request = rf.get("/metrics", HTTP_AUTHORIZATION="Bearer secret-metrics-token")
    ok = metrics_view(request)
    assert ok.status_code == 200
    assert b"http_request_duration_seconds" in ok.content


@override_settings(IS_DEV=False, METRICS_AUTH_TOKEN="secret-metrics-token")
def test_metrics_accepts_x_metrics_token_header(rf):
    request = rf.get("/metrics", HTTP_X_METRICS_TOKEN="secret-metrics-token")
    response = metrics_view(request)
    assert response.status_code == 200


@override_settings(IS_DEV=True, METRICS_AUTH_TOKEN="")
def test_middleware_records_request(rf):
    middleware = PrometheusMetricsMiddleware(get_response=lambda r: HttpResponse("ok"))
    request = rf.get("/crm-records/leads/")
    middleware.process_request(request)
    response = middleware.process_response(request, HttpResponse("ok"))
    assert response.status_code == 200

    scrape = metrics_view(rf.get("/metrics"))
    body = scrape.content.decode("utf-8")
    assert "http_requests_total" in body
    assert 'method="GET"' in body
    assert "/crm-records/leads" in body
