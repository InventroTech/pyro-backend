"""
Render metrics monitor — polls the Render REST API every 5 minutes and sends
email alerts when key metrics cross configured thresholds.

Metrics checked:
  - CPU usage      : max CPU % across the last 5 minutes
  - Memory usage   : max memory % across the last 5 minutes
  - HTTP P95 latency: 95th-percentile response time in ms

Required env vars:
  RENDER_API_KEY      — Render API key (Dashboard → Account Settings → API Keys)
  RENDER_SERVICE_ID   — your service ID (e.g. srv-xxxxxxxx)
                        Run: python manage.py find_render_service to look it up

Optional thresholds:
  RENDER_CPU_THRESHOLD        — % CPU before alert (default: 85)
  RENDER_MEMORY_THRESHOLD     — % Memory before alert (default: 90)
  RENDER_LATENCY_P95_THRESHOLD — HTTP P95 latency in ms before alert (default: 3000)
"""

import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

RENDER_API_BASE = "https://api.render.com/v1"

# How often to poll Render metrics (seconds)
RENDER_CHECK_INTERVAL = 300  # 5 minutes

# Cooldown between same-type alerts (seconds)
RENDER_ALERT_COOLDOWN = 1800  # 30 minutes

_last_render_check_at: Optional[float] = None
_last_render_alert_sent: dict[str, float] = {}


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _cfg(key: str, default: str = "") -> str:
    from django.conf import settings
    return getattr(settings, key, None) or os.environ.get(key, default)


def _get_render_config() -> dict:
    return {
        "api_key":             _cfg("RENDER_API_KEY"),
        "service_id":          _cfg("RENDER_SERVICE_ID"),
        "cpu_threshold":       float(_cfg("RENDER_CPU_THRESHOLD", "85")),
        "memory_threshold":    float(_cfg("RENDER_MEMORY_THRESHOLD", "90")),
        "latency_threshold_ms": float(_cfg("RENDER_LATENCY_P95_THRESHOLD", "3000")),
    }


def _parse_emails(setting_name: str, env_var: str) -> list[str]:
    from django.conf import settings
    raw = getattr(settings, setting_name, None) or os.environ.get(env_var, "")
    return [e.strip() for e in raw.split(",") if e.strip()]


def _get_alert_recipients() -> list[str]:
    recipients = _parse_emails("HEALTH_ALERT_RECIPIENTS", "HEALTH_ALERT_RECIPIENTS")
    if recipients:
        return recipients
    from django.conf import settings
    from_email = getattr(settings, "DEFAULT_FROM_EMAIL", "") or os.environ.get("DEFAULT_FROM_EMAIL", "")
    return [from_email] if from_email else []


def _get_alert_cc() -> list[str]:
    return _parse_emails("HEALTH_ALERT_CC", "HEALTH_ALERT_CC")


# ---------------------------------------------------------------------------
# Render API calls
# ---------------------------------------------------------------------------

def _headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}


def _time_window(minutes: int = 5) -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=minutes)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start.strftime(fmt), now.strftime(fmt)


def _latest_max_value(series: list) -> Optional[float]:
    """Extract the max value across all data points in a time series response."""
    max_val = None
    for item in series:
        for point in item.get("values", []):
            v = point.get("value")
            if v is not None:
                max_val = v if max_val is None else max(max_val, v)
    return max_val


def fetch_cpu_percent(api_key: str, service_id: str) -> Optional[float]:
    start, end = _time_window(minutes=5)
    try:
        resp = requests.get(
            f"{RENDER_API_BASE}/metrics/cpu",
            headers=_headers(api_key),
            params={
                "resource": service_id,
                "startTime": start,
                "endTime": end,
                "resolutionSeconds": 60,
                "aggregationMethod": "MAX",
            },
            timeout=10,
        )
        resp.raise_for_status()
        return _latest_max_value(resp.json())
    except Exception as e:
        logger.warning("[RenderMonitor] Failed to fetch CPU: %s", e)
        return None


def fetch_memory_percent(api_key: str, service_id: str) -> Optional[float]:
    """Returns memory usage as a percentage (0-100) by fetching usage and limit separately."""
    start, end = _time_window(minutes=5)
    params = {"resource": service_id, "startTime": start, "endTime": end, "resolutionSeconds": 60}
    try:
        usage_resp = requests.get(
            f"{RENDER_API_BASE}/metrics/memory",
            headers=_headers(api_key), params=params, timeout=10,
        )
        usage_resp.raise_for_status()
        limit_resp = requests.get(
            f"{RENDER_API_BASE}/metrics/memory-limit",
            headers=_headers(api_key), params=params, timeout=10,
        )
        limit_resp.raise_for_status()
        usage_bytes = _latest_max_value(usage_resp.json())
        limit_bytes = _latest_max_value(limit_resp.json())
        if usage_bytes is None or limit_bytes is None or limit_bytes == 0:
            return None
        return (usage_bytes / limit_bytes) * 100
    except Exception as e:
        logger.warning("[RenderMonitor] Failed to fetch memory: %s", e)
        return None


def fetch_http_latency_p95_ms(api_key: str, service_id: str) -> Optional[float]:
    start, end = _time_window(minutes=5)
    try:
        resp = requests.get(
            f"{RENDER_API_BASE}/metrics/http-latency",
            headers=_headers(api_key),
            params={
                "resource": service_id,
                "startTime": start,
                "endTime": end,
                "resolutionSeconds": 60,
                "quantile": 0.95,
            },
            timeout=10,
        )
        resp.raise_for_status()
        return _latest_max_value(resp.json())
    except Exception as e:
        logger.warning("[RenderMonitor] Failed to fetch HTTP latency: %s", e)
        return None


# ---------------------------------------------------------------------------
# Alert helpers
# ---------------------------------------------------------------------------

def _cooldown_active(alert_key: str) -> bool:
    last = _last_render_alert_sent.get(alert_key)
    return last is not None and (time.monotonic() - last) < RENDER_ALERT_COOLDOWN


def _build_email(title: str, color: str, icon: str, rows: list[tuple[str, str, bool]], timestamp: str) -> tuple[str, str]:
    plain_rows = "\n".join(f"{label}: {value}" for label, value, _ in rows)
    plain = f"{title}\nTime: {timestamp}\n{plain_rows}\n\nThis alert will not repeat for 30 minutes."

    html_rows = ""
    for label, value, highlight in rows:
        bg = 'style="background:#fff5f5;"' if highlight else ""
        val_style = f'style="color:{color};font-weight:bold;"' if highlight else ""
        html_rows += f'<tr {bg}><td style="padding:6px 16px;font-weight:bold;">{label}</td><td style="padding:6px 16px;" {val_style}>{value}</td></tr>\n'

    html = f"""<html><body style="font-family:sans-serif;">
<h2 style="color:{color};">{icon} {title} &mdash; Pyro</h2>
<table style="border-collapse:collapse;font-family:monospace;font-size:14px;">
  <tr><td style="padding:6px 16px;font-weight:bold;">Time</td><td style="padding:6px 16px;">{timestamp}</td></tr>
  {html_rows}
</table>
<p style="color:#718096;font-size:12px;margin-top:16px;">This alert will not repeat for 30 minutes.</p>
</body></html>"""
    return plain, html


def _send_alert(subject: str, plain: str, html: str, alert_key: str) -> None:
    if _cooldown_active(alert_key):
        logger.debug("[RenderMonitor] Alert '%s' suppressed (cooldown active)", alert_key)
        return

    recipients = _get_alert_recipients()
    if not recipients:
        logger.warning("[RenderMonitor] No alert recipients configured — set HEALTH_ALERT_RECIPIENTS")
        return

    cc = _get_alert_cc()

    try:
        from email_protocol.services import send_email
        success, msg = send_email(
            to_emails=recipients,
            subject=subject,
            message=plain,
            html_message=html,
            cc=cc or None,
            client_name="RenderMonitor",
        )
        if success:
            _last_render_alert_sent[alert_key] = time.monotonic()
            logger.warning("[RenderMonitor] Alert sent: %s", subject)
        else:
            logger.error("[RenderMonitor] Failed to send alert: %s", msg)
    except Exception as e:
        logger.error("[RenderMonitor] Exception sending alert: %s", e, exc_info=True)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def check_render_metrics() -> dict:
    """
    Poll Render API metrics and send alerts if thresholds exceeded.
    Rate-limited to once per RENDER_CHECK_INTERVAL. Safe to call every worker tick.
    """
    global _last_render_check_at

    now = time.monotonic()
    if _last_render_check_at is not None and (now - _last_render_check_at) < RENDER_CHECK_INTERVAL:
        return {}
    _last_render_check_at = now

    cfg = _get_render_config()

    if not cfg["api_key"] or not cfg["service_id"]:
        logger.debug("[RenderMonitor] Skipping — RENDER_API_KEY or RENDER_SERVICE_ID not set")
        return {}

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = {}

    # CPU
    cpu = fetch_cpu_percent(cfg["api_key"], cfg["service_id"])
    if cpu is not None:
        result["cpu_percent"] = cpu
        logger.debug("[RenderMonitor] CPU=%.1f%% (threshold=%.0f%%)", cpu, cfg["cpu_threshold"])
        if cpu >= cfg["cpu_threshold"]:
            plain, html = _build_email(
                title="High CPU Usage (Render)",
                color="#e53e3e", icon="&#9888;",
                rows=[
                    ("CPU Usage", f"{cpu:.1f}%", True),
                    ("Threshold", f"{cfg['cpu_threshold']:.0f}%", False),
                    ("Service", cfg["service_id"], False),
                ],
                timestamp=timestamp,
            )
            _send_alert(
                subject=f"[ALERT] High CPU: {cpu:.1f}% on Pyro (Render)",
                plain=plain, html=html, alert_key="render_cpu",
            )

    # Memory
    mem = fetch_memory_percent(cfg["api_key"], cfg["service_id"])
    if mem is not None:
        result["memory_percent"] = mem
        logger.debug("[RenderMonitor] Memory=%.1f%% (threshold=%.0f%%)", mem, cfg["memory_threshold"])
        if mem >= cfg["memory_threshold"]:
            plain, html = _build_email(
                title="High Memory Usage (Render)",
                color="#e53e3e", icon="&#9888;",
                rows=[
                    ("Memory Usage", f"{mem:.1f}%", True),
                    ("Threshold", f"{cfg['memory_threshold']:.0f}%", False),
                    ("Service", cfg["service_id"], False),
                ],
                timestamp=timestamp,
            )
            _send_alert(
                subject=f"[ALERT] High Memory: {mem:.1f}% on Pyro (Render)",
                plain=plain, html=html, alert_key="render_memory",
            )
        logger.info("[RenderMonitor] Memory=%.1f%%", mem)

    # HTTP P95 Latency
    latency = fetch_http_latency_p95_ms(cfg["api_key"], cfg["service_id"])
    if latency is not None:
        result["latency_p95_ms"] = latency
        logger.debug("[RenderMonitor] P95 latency=%.0fms (threshold=%.0fms)", latency, cfg["latency_threshold_ms"])
        if latency >= cfg["latency_threshold_ms"]:
            plain, html = _build_email(
                title="High P95 Response Time (Render)",
                color="#d69e2e", icon="&#9201;",
                rows=[
                    ("P95 Latency", f"{latency:.0f} ms", True),
                    ("Threshold", f"{cfg['latency_threshold_ms']:.0f} ms", False),
                    ("Service", cfg["service_id"], False),
                ],
                timestamp=timestamp,
            )
            _send_alert(
                subject=f"[ALERT] High P95 Latency: {latency:.0f}ms on Pyro (Render)",
                plain=plain, html=html, alert_key="render_latency",
            )

    return result
