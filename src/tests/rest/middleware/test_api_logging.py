"""
Tests for API logging skip rules (fast 200s vs errors / slow successes).

Run:
    pytest src/tests/rest/middleware/test_api_logging.py -v
"""
from __future__ import annotations

from unittest.mock import patch

from django.http import HttpResponse, JsonResponse
from django.test import override_settings

from middleware.api_logging import APILoggingMiddleware, _should_emit_api_log


@override_settings(API_LOGGING_SKIP_SUCCESS=True, API_LOGGING_SLOW_MS=1500)
def test_skip_fast_success():
    assert _should_emit_api_log(200, 12.0) is False
    assert _should_emit_api_log(201, 40.0) is False
    assert _should_emit_api_log(204, 5.0) is False
    assert _should_emit_api_log(304, 8.0) is False


@override_settings(API_LOGGING_SKIP_SUCCESS=True, API_LOGGING_SLOW_MS=1500)
def test_always_emit_errors():
    assert _should_emit_api_log(400, 3.0) is True
    assert _should_emit_api_log(403, 8.0) is True
    assert _should_emit_api_log(500, 20.0) is True


@override_settings(API_LOGGING_SKIP_SUCCESS=True, API_LOGGING_SLOW_MS=1500)
def test_emit_slow_success():
    assert _should_emit_api_log(200, 1500.0) is True
    assert _should_emit_api_log(200, 1499.0) is False


@override_settings(API_LOGGING_SKIP_SUCCESS=False)
def test_can_restore_log_every_request():
    assert _should_emit_api_log(200, 1.0) is True


@override_settings(API_LOGGING_SKIP_SUCCESS=True, API_LOGGING_SLOW_MS=0)
def test_slow_ms_zero_means_errors_only():
    assert _should_emit_api_log(200, 10_000.0) is False
    assert _should_emit_api_log(500, 1.0) is True


def _run(mw, request, status=200, body=b'{"ok":true}'):
    mw.process_request(request)
    response = HttpResponse(body, status=status, content_type="application/json")
    return mw.process_response(request, response)


@override_settings(API_LOGGING_SKIP_SUCCESS=True, API_LOGGING_SLOW_MS=1500)
def test_fast_200_does_not_log_or_parse_bodies(rf):
    mw = APILoggingMiddleware(get_response=lambda r: HttpResponse("ok"))
    request = rf.get("/crm-records/records/")
    with (
        patch("middleware.api_logging.logger") as logger,
        patch("middleware.api_logging._get_request_body") as get_req,
        patch("middleware.api_logging._get_response_body") as get_resp,
    ):
        response = _run(mw, request, status=200)
    assert response.status_code == 200
    logger.info.assert_not_called()
    logger.warning.assert_not_called()
    logger.error.assert_not_called()
    get_req.assert_not_called()
    get_resp.assert_not_called()


@override_settings(API_LOGGING_SKIP_SUCCESS=True, API_LOGGING_SLOW_MS=1500)
def test_400_logs_warning(rf):
    mw = APILoggingMiddleware(get_response=lambda r: HttpResponse("no"))
    request = rf.get("/crm-records/records/")
    with patch("middleware.api_logging.logger") as logger:
        _run(mw, request, status=400, body=b'{"detail":"bad"}')
    logger.warning.assert_called_once()
    logger.info.assert_not_called()
    extra = logger.warning.call_args.kwargs["extra"]
    assert extra["log_data"]["status_code"] == 400
    assert extra["log_data"]["path"] == "/crm-records/records/"


@override_settings(API_LOGGING_SKIP_SUCCESS=True, API_LOGGING_SLOW_MS=1500)
def test_500_logs_error(rf):
    mw = APILoggingMiddleware(get_response=lambda r: HttpResponse("err"))
    request = rf.post(
        "/crm-records/records/",
        data={"password": "secret", "name": "x"},
        content_type="application/json",
    )
    with patch("middleware.api_logging.logger") as logger:
        _run(mw, request, status=500, body=b'{"error":"boom"}')
    logger.error.assert_called_once()
    extra = logger.error.call_args.kwargs["extra"]
    assert extra["log_data"]["status_code"] == 500


@override_settings(API_LOGGING_SKIP_SUCCESS=True, API_LOGGING_SLOW_MS=1500)
def test_health_path_never_logs(rf):
    mw = APILoggingMiddleware(get_response=lambda r: HttpResponse("err"))
    request = rf.get("/health/")
    with patch("middleware.api_logging.logger") as logger:
        _run(mw, request, status=500)
    logger.error.assert_not_called()
    logger.warning.assert_not_called()
    logger.info.assert_not_called()


@override_settings(API_LOGGING_SKIP_SUCCESS=True, API_LOGGING_SLOW_MS=1500)
def test_slow_200_logs_info(rf):
    mw = APILoggingMiddleware(get_response=lambda r: JsonResponse({"ok": True}))
    request = rf.get("/crm-records/leads/next/")
    with (
        patch("middleware.api_logging.time.time", side_effect=[1000.0, 1002.0]),
        patch("middleware.api_logging.logger") as logger,
    ):
        _run(mw, request, status=200)
    logger.info.assert_called_once()
    extra = logger.info.call_args.kwargs["extra"]
    assert extra["log_data"]["status_code"] == 200
    assert extra["log_data"]["duration_ms"] == 2000.0
