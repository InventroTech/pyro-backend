"""
Tests for the Render metrics monitoring module.
  - background_jobs.render_metrics_monitor (Render REST API polling)

Run:
    pytest src/tests/rest/background_jobs/test_monitoring.py -v
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import background_jobs.render_metrics_monitor as rm

# send_email is imported lazily inside functions, so patch at the source.
SEND_EMAIL_PATH = "email_protocol.services.send_email"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_render_series(value: float) -> list:
    return [{"labels": [], "unit": "test", "values": [{"timestamp": "2026-01-01T00:00:00Z", "value": value}]}]


def _reset_render_monitor():
    rm._last_render_check_at[0] = None
    rm._last_render_alert_sent.clear()


def _render_cfg(
    api_key="rnd_test",
    service_id="srv-test",
    cpu_threshold=85.0,
    memory_threshold=90.0,
    latency_threshold_ms=3000.0,
):
    return {
        "api_key": api_key,
        "service_id": service_id,
        "cpu_threshold": cpu_threshold,
        "memory_threshold": memory_threshold,
        "latency_threshold_ms": latency_threshold_ms,
    }


def _render_side_effect(cpu=5.0, mem_usage=300_000_000, mem_limit=2_147_483_648, latency=50.0):
    def side_effect(url, **kwargs):
        r = MagicMock()
        r.raise_for_status = MagicMock()
        if "memory-limit" in url:
            r.json.return_value = _make_render_series(mem_limit)
        elif "memory" in url:
            r.json.return_value = _make_render_series(mem_usage)
        elif "latency" in url:
            r.json.return_value = _make_render_series(latency)
        else:
            r.json.return_value = _make_render_series(cpu)
        return r
    return side_effect


# ===========================================================================
# render_metrics_monitor — guards
# ===========================================================================

class TestRenderMetricsMonitorGuards:
    def setup_method(self):
        _reset_render_monitor()

    def test_skips_when_api_key_missing(self):
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg(api_key="")), \
             patch("requests.get") as mock_get:
            result = rm.check_render_metrics()
        mock_get.assert_not_called()
        assert result == {}

    def test_skips_when_service_id_missing(self):
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg(service_id="")), \
             patch("requests.get") as mock_get:
            result = rm.check_render_metrics()
        mock_get.assert_not_called()
        assert result == {}

    def test_check_interval_throttles_api_calls(self):
        rm._last_render_check_at[0] = time.monotonic()
        with patch("requests.get") as mock_get:
            result = rm.check_render_metrics()
        mock_get.assert_not_called()
        assert result == {}


# ===========================================================================
# render_metrics_monitor — alerts
# ===========================================================================

class TestRenderMetricsMonitorAlerts:
    def setup_method(self):
        _reset_render_monitor()

    def test_no_alert_when_all_healthy(self):
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg()), \
             patch("requests.get", side_effect=_render_side_effect(cpu=5.0, latency=100.0)), \
             patch(SEND_EMAIL_PATH) as mock_send:
            rm.check_render_metrics()
        mock_send.assert_not_called()

    def test_cpu_alert_above_threshold(self):
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg(cpu_threshold=80.0)), \
             patch("requests.get", side_effect=_render_side_effect(cpu=90.0)), \
             patch(SEND_EMAIL_PATH, return_value=(True, "ok")) as mock_send:
            result = rm.check_render_metrics()
        assert result["cpu_percent"] == 90.0
        assert any("CPU" in c[1]["subject"] for c in mock_send.call_args_list)

    def test_cpu_below_threshold_no_alert(self):
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg(cpu_threshold=85.0)), \
             patch("requests.get", side_effect=_render_side_effect(cpu=50.0)), \
             patch(SEND_EMAIL_PATH) as mock_send:
            rm.check_render_metrics()
        assert not any("CPU" in c[1]["subject"] for c in mock_send.call_args_list)

    def test_memory_calculated_as_percentage(self):
        # 1.8 GB / 2 GB ≈ 90%
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg(memory_threshold=89.0)), \
             patch("requests.get", side_effect=_render_side_effect(
                 mem_usage=1_932_735_283, mem_limit=2_147_483_648)), \
             patch(SEND_EMAIL_PATH, return_value=(True, "ok")) as mock_send:
            result = rm.check_render_metrics()
        assert result["memory_percent"] == pytest.approx(90.0, abs=1.0)
        assert any("Memory" in c[1]["subject"] for c in mock_send.call_args_list)

    def test_latency_alert_above_threshold(self):
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg()), \
             patch("requests.get", side_effect=_render_side_effect(latency=4500.0)), \
             patch(SEND_EMAIL_PATH, return_value=(True, "ok")) as mock_send:
            result = rm.check_render_metrics()
        assert result["latency_p95_ms"] == 4500.0
        assert any("Latency" in c[1]["subject"] or "P95" in c[1]["subject"]
                   for c in mock_send.call_args_list)

    def test_cooldown_suppresses_repeated_cpu_alert(self):
        rm._last_render_alert_sent["render_cpu"] = time.monotonic()
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg(cpu_threshold=80.0)), \
             patch("requests.get", side_effect=_render_side_effect(cpu=95.0)), \
             patch(SEND_EMAIL_PATH) as mock_send:
            rm.check_render_metrics()
        assert not any("CPU" in c[1]["subject"] for c in mock_send.call_args_list)

    def test_alert_fires_again_after_cooldown(self):
        rm._last_render_alert_sent["render_cpu"] = time.monotonic() - rm.RENDER_ALERT_COOLDOWN - 1
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg(cpu_threshold=80.0)), \
             patch("requests.get", side_effect=_render_side_effect(cpu=95.0)), \
             patch(SEND_EMAIL_PATH, return_value=(True, "ok")) as mock_send:
            rm.check_render_metrics()
        assert any("CPU" in c[1]["subject"] for c in mock_send.call_args_list)

    def test_api_failure_does_not_raise(self):
        import requests as req_lib
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg()), \
             patch("requests.get", side_effect=req_lib.exceptions.ConnectionError("timeout")):
            rm.check_render_metrics()  # must not raise

    def test_cc_included_in_email(self):
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg(cpu_threshold=80.0)), \
             patch("background_jobs.render_metrics_monitor._get_alert_recipients",
                   return_value=["support@thepyro.ai"]), \
             patch("background_jobs.render_metrics_monitor._get_alert_cc",
                   return_value=["ritam@thepyro.ai", "bibhab@thepyro.ai"]), \
             patch("requests.get", side_effect=_render_side_effect(cpu=90.0)), \
             patch(SEND_EMAIL_PATH, return_value=(True, "ok")) as mock_send:
            rm.check_render_metrics()
        cc = mock_send.call_args[1]["cc"]
        assert "ritam@thepyro.ai" in cc
        assert "bibhab@thepyro.ai" in cc

    def test_send_failure_does_not_raise(self):
        with patch("background_jobs.render_metrics_monitor._get_render_config",
                   return_value=_render_cfg(cpu_threshold=80.0)), \
             patch("requests.get", side_effect=_render_side_effect(cpu=90.0)), \
             patch(SEND_EMAIL_PATH, return_value=(False, "SMTP error")):
            rm.check_render_metrics()  # must not raise


# ===========================================================================
# render_metrics_monitor — utilities
# ===========================================================================

class TestLatestMaxValue:
    def test_returns_max_across_multiple_points(self):
        series = [
            {"values": [{"timestamp": "t1", "value": 10}, {"timestamp": "t2", "value": 50}]},
            {"values": [{"timestamp": "t3", "value": 30}]},
        ]
        assert rm._latest_max_value(series) == 50

    def test_empty_series_returns_none(self):
        assert rm._latest_max_value([]) is None

    def test_empty_values_list_returns_none(self):
        assert rm._latest_max_value([{"values": []}]) is None

    def test_single_point(self):
        assert rm._latest_max_value([{"values": [{"timestamp": "t1", "value": 42.5}]}]) == 42.5

    def test_negative_values(self):
        series = [{"values": [{"timestamp": "t1", "value": -5}, {"timestamp": "t2", "value": -1}]}]
        assert rm._latest_max_value(series) == -1


class TestTimeWindow:
    def test_timestamps_end_with_z(self):
        start, end = rm._time_window(minutes=5)
        assert start.endswith("Z") and end.endswith("Z")

    def test_start_before_end(self):
        start, end = rm._time_window(minutes=5)
        assert start < end

    def test_window_is_approximately_n_minutes(self):
        start, end = rm._time_window(minutes=10)
        s = datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        e = datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        assert 590 <= (e - s).total_seconds() <= 610
