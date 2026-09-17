"""
Tests for the Supabase metrics monitoring module.
  - background_jobs.supabase_metrics_monitor (Supabase Prometheus metrics endpoint polling)

Run:
    pytest src/tests/rest/background_jobs/test_supabase_monitoring.py -v
"""
from __future__ import annotations

import time
from unittest.mock import patch

import pytest

import background_jobs.supabase_metrics_monitor as sm

SEND_EMAIL_PATH = "email_protocol.services.send_email"


def _memory_text(total=8_000_000_000, available=5_600_000_000):
    return f"""\
# HELP node_memory_MemTotal_bytes Memory information field MemTotal_bytes
node_memory_MemTotal_bytes {total}
# HELP node_memory_MemAvailable_bytes Memory information field MemAvailable_bytes
node_memory_MemAvailable_bytes {available}
"""


def _cpu_text(idle=0.0, user=0.0, system=0.0):
    return f"""\
# HELP node_cpu_seconds_total Seconds the CPUs spent in each mode
node_cpu_seconds_total{{cpu="0",mode="idle"}} {idle / 2}
node_cpu_seconds_total{{cpu="1",mode="idle"}} {idle / 2}
node_cpu_seconds_total{{cpu="0",mode="user"}} {user / 2}
node_cpu_seconds_total{{cpu="1",mode="user"}} {user / 2}
node_cpu_seconds_total{{cpu="0",mode="system"}} {system / 2}
node_cpu_seconds_total{{cpu="1",mode="system"}} {system / 2}
"""


def _reset_supabase_monitor():
    sm._last_supabase_check_at[0] = None
    sm._last_supabase_alert_sent.clear()
    sm._last_cpu_sample[0] = None


def _supabase_cfg(project_url="https://proj.supabase.co", metrics_secret_key="sb_secret_test",
                   cpu_threshold=90.0, memory_threshold=90.0):
    return {
        "project_url": project_url,
        "metrics_secret_key": metrics_secret_key,
        "cpu_threshold": cpu_threshold,
        "memory_threshold": memory_threshold,
    }


@pytest.fixture(autouse=True)
def _reset():
    _reset_supabase_monitor()
    yield
    _reset_supabase_monitor()


# ===========================================================================
# _parse_prometheus_metrics
# ===========================================================================

class TestParsePrometheusMetrics:
    def test_parses_labeled_and_unlabeled_metrics(self):
        metrics = sm._parse_prometheus_metrics(_memory_text(total=1000.0, available=400.0))
        assert metrics["node_memory_MemTotal_bytes"] == [({}, 1000.0)]
        assert metrics["node_memory_MemAvailable_bytes"] == [({}, 400.0)]

    def test_ignores_comments_and_blank_lines(self):
        metrics = sm._parse_prometheus_metrics("# just a comment\n\n")
        assert metrics == {}


# ===========================================================================
# fetch_memory_percent
# ===========================================================================

class TestFetchMemoryPercent:
    def test_computes_percent_used(self):
        with patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_memory_text(total=1000.0, available=300.0)):
            pct = sm.fetch_memory_percent("https://proj.supabase.co", "key")
        assert pct == pytest.approx(70.0)

    def test_returns_none_when_metrics_missing(self):
        with patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value="# empty\n"):
            pct = sm.fetch_memory_percent("https://proj.supabase.co", "key")
        assert pct is None

    def test_returns_none_on_fetch_failure(self):
        with patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=None):
            pct = sm.fetch_memory_percent("https://proj.supabase.co", "key")
        assert pct is None


# ===========================================================================
# fetch_cpu_percent
# ===========================================================================

class TestFetchCpuPercent:
    def test_first_call_returns_none_and_caches_sample(self):
        with patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_cpu_text(idle=100.0, user=0.0, system=0.0)):
            pct = sm.fetch_cpu_percent("https://proj.supabase.co", "key")
        assert pct is None
        assert sm._last_cpu_sample[0] is not None

    def test_second_call_computes_rate_from_delta(self):
        with patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_cpu_text(idle=100.0, user=0.0, system=0.0)):
            sm.fetch_cpu_percent("https://proj.supabase.co", "key")
        # 5 more seconds pass: 4s idle, 1s busy => 20% CPU
        with patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_cpu_text(idle=104.0, user=1.0, system=0.0)):
            pct = sm.fetch_cpu_percent("https://proj.supabase.co", "key")
        assert pct == pytest.approx(20.0)

    def test_returns_none_when_metric_missing(self):
        with patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value="# empty\n"):
            pct = sm.fetch_cpu_percent("https://proj.supabase.co", "key")
        assert pct is None

    def test_counter_reset_is_skipped_not_negative(self):
        with patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_cpu_text(idle=100.0, user=10.0, system=0.0)):
            sm.fetch_cpu_percent("https://proj.supabase.co", "key")
        # Counters dropped (e.g. host restart) — must not raise or go negative-crazy.
        with patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_cpu_text(idle=5.0, user=1.0, system=0.0)):
            pct = sm.fetch_cpu_percent("https://proj.supabase.co", "key")
        assert pct is None  # all deltas skipped => total_delta == 0


# ===========================================================================
# check_supabase_metrics — guards
# ===========================================================================

class TestCheckSupabaseMetricsGuards:
    def test_skips_when_not_configured(self):
        with patch("background_jobs.supabase_metrics_monitor._get_supabase_config",
                   return_value=_supabase_cfg(project_url="", metrics_secret_key="")):
            result = sm.check_supabase_metrics()
        assert result == {}

    def test_rate_limited_within_check_interval(self):
        with patch("background_jobs.supabase_metrics_monitor._get_supabase_config",
                   return_value=_supabase_cfg()), \
             patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_memory_text()):
            result1 = sm.check_supabase_metrics()
            result2 = sm.check_supabase_metrics()
        assert result1 != {}
        assert result2 == {}  # second call within interval is a no-op


# ===========================================================================
# check_supabase_metrics — alerts
# ===========================================================================

class TestCheckSupabaseMetricsAlerts:
    def test_no_alert_below_threshold(self):
        with patch("background_jobs.supabase_metrics_monitor._get_supabase_config",
                   return_value=_supabase_cfg(memory_threshold=90.0)), \
             patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_memory_text(total=1000.0, available=300.0)), \
             patch(SEND_EMAIL_PATH, return_value=(True, "ok")) as mock_send:
            result = sm.check_supabase_metrics()
        assert result["memory_percent"] == pytest.approx(70.0)
        mock_send.assert_not_called()

    def test_memory_alert_above_threshold(self):
        with patch("background_jobs.supabase_metrics_monitor._get_supabase_config",
                   return_value=_supabase_cfg(memory_threshold=50.0)), \
             patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_memory_text(total=1000.0, available=300.0)), \
             patch(SEND_EMAIL_PATH, return_value=(True, "ok")) as mock_send:
            result = sm.check_supabase_metrics()
        assert result["memory_percent"] == pytest.approx(70.0)
        assert mock_send.call_count == 1
        assert "Memory" in mock_send.call_args_list[0][1]["subject"]

    def test_cooldown_suppresses_repeated_alert(self):
        sm._last_supabase_alert_sent["supabase_memory"] = time.monotonic()
        with patch("background_jobs.supabase_metrics_monitor._get_supabase_config",
                   return_value=_supabase_cfg(memory_threshold=50.0)), \
             patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_memory_text(total=1000.0, available=300.0)), \
             patch(SEND_EMAIL_PATH, return_value=(True, "ok")) as mock_send:
            sm.check_supabase_metrics()
        mock_send.assert_not_called()

    def test_no_recipients_does_not_raise(self):
        with patch("background_jobs.supabase_metrics_monitor._get_supabase_config",
                   return_value=_supabase_cfg(memory_threshold=50.0)), \
             patch("background_jobs.supabase_metrics_monitor._fetch_metrics_text",
                   return_value=_memory_text(total=1000.0, available=300.0)), \
             patch("background_jobs.supabase_metrics_monitor._get_alert_recipients",
                   return_value=[]), \
             patch(SEND_EMAIL_PATH, return_value=(True, "ok")) as mock_send:
            sm.check_supabase_metrics()  # must not raise
        mock_send.assert_not_called()
