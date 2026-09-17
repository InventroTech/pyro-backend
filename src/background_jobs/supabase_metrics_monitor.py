"""
Supabase metrics monitor — polls the project's Prometheus-compatible metrics
endpoint every 5 minutes and sends email alerts when CPU or memory usage
crosses a configured threshold.

Supabase exposes ~200 Postgres/platform metrics in Prometheus exposition
format at:
    https://<project-ref>.supabase.co/customer/v1/privileged/metrics
secured with HTTP Basic Auth: username "service_role", password = a
*Secret API key* (new format, "sb_secret_...") — NOT the legacy
SUPABASE_SERVICE_ROLE_KEY JWT, which this endpoint rejects with 401.
Generate one at Dashboard → Project Settings → API Keys → Secret keys.
See: https://supabase.com/docs/guides/telemetry/metrics

Metrics checked (node_exporter-style host metrics, confirmed against
https://github.com/supabase/supabase-grafana/blob/main/docs/metrics.md):
  - Memory usage : (MemTotal - MemAvailable) / MemTotal, from
                   node_memory_MemTotal_bytes / node_memory_MemAvailable_bytes
                   (a simple gauge ratio — one scrape is enough).
  - CPU usage    : 1 - (idle time delta / total time delta), from the
                   node_cpu_seconds_total counter (summed across cores,
                   grouped by `mode` label). This is a counter, not a gauge,
                   so it needs two scrapes: we cache each cycle's per-mode
                   totals and diff them against the previous cycle
                   (~SUPABASE_CHECK_INTERVAL apart). The first check after
                   startup has no prior sample and reports no CPU value.

Required env vars:
  SUPABASE_METRICS_SECRET_KEY — Secret API key for the metrics endpoint (sb_secret_...)
  SUPABASE_PROJECT_URL        — resolved via authentication.supabase_env
                                 (respects its dev/staging fallback order)

Optional thresholds:
  SUPABASE_CPU_THRESHOLD    — % CPU before alert (default: 90)
  SUPABASE_MEMORY_THRESHOLD — % memory before alert (default: 90)

Disk usage/IO and connection-pool metrics are also available from this same
endpoint (node_filesystem_*_bytes, node_disk_io_time_seconds_total,
db_sql_connection_open/max_open) but aren't wired up here — add a new
fetch_*/check block below following the same pattern if needed later.
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)

# How often to poll Supabase metrics (seconds)
SUPABASE_CHECK_INTERVAL = 300  # 5 minutes

# Cooldown between same-type alerts (seconds)
SUPABASE_ALERT_COOLDOWN = 1800  # 30 minutes

_last_supabase_check_at: list[Optional[float]] = [None]  # [0] = last check timestamp
_last_supabase_alert_sent: dict[str, float] = {}

# Previous cycle's summed-by-mode node_cpu_seconds_total, for rate calculation.
_last_cpu_sample: list[Optional[dict[str, float]]] = [None]


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _cfg(key: str, default: str = "") -> str:
    from django.conf import settings
    return getattr(settings, key, None) or os.environ.get(key, default)


def _get_supabase_config() -> dict:
    from authentication.supabase_env import supabase_api_base_url
    return {
        "project_url":        supabase_api_base_url(),
        "metrics_secret_key": _cfg("SUPABASE_METRICS_SECRET_KEY"),
        "cpu_threshold":      float(_cfg("SUPABASE_CPU_THRESHOLD", "90")),
        "memory_threshold":   float(_cfg("SUPABASE_MEMORY_THRESHOLD", "90")),
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
# Supabase metrics endpoint
# ---------------------------------------------------------------------------

def _fetch_metrics_text(project_url: str, metrics_secret_key: str) -> Optional[str]:
    base = project_url.rstrip("/")
    try:
        resp = requests.get(
            f"{base}/customer/v1/privileged/metrics",
            auth=("service_role", metrics_secret_key),
            timeout=10,
        )
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning("[SupabaseMonitor] Failed to fetch metrics: %s", e)
        return None


def _parse_prometheus_metrics(text: str) -> dict[str, list[tuple[dict, float]]]:
    """Minimal Prometheus text-exposition-format parser: {metric_name: [(labels, value), ...]}."""
    metrics: dict[str, list[tuple[dict, float]]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "{" in line:
            name, _, rest = line.partition("{")
            label_str, _, value_str = rest.rpartition("}")
            labels = {}
            for pair in label_str.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    labels[k.strip()] = v.strip().strip('"')
        else:
            name, _, value_str = line.rpartition(" ")
            labels = {}
        name = name.strip()
        try:
            value = float(value_str.split()[0])
        except (ValueError, IndexError):
            continue
        if name:
            metrics.setdefault(name, []).append((labels, value))
    return metrics


def fetch_memory_percent(project_url: str, metrics_secret_key: str) -> Optional[float]:
    """Memory in use as a % of total, from node_memory_MemTotal_bytes / MemAvailable_bytes."""
    text = _fetch_metrics_text(project_url, metrics_secret_key)
    if text is None:
        return None
    metrics = _parse_prometheus_metrics(text)

    total_points = metrics.get("node_memory_MemTotal_bytes")
    avail_points = metrics.get("node_memory_MemAvailable_bytes")
    if not total_points or not avail_points:
        logger.warning(
            "[SupabaseMonitor] Expected metrics not found in response "
            "(node_memory_MemTotal_bytes / node_memory_MemAvailable_bytes) — "
            "check the raw endpoint output for this project's metric names"
        )
        return None

    total = total_points[0][1]
    avail = avail_points[0][1]
    if not total:
        return None
    return (1 - avail / total) * 100


def _sum_by_mode(points: list[tuple[dict, float]]) -> dict[str, float]:
    sums: dict[str, float] = {}
    for labels, value in points:
        mode = labels.get("mode", "unknown")
        sums[mode] = sums.get(mode, 0.0) + value
    return sums


def fetch_cpu_percent(project_url: str, metrics_secret_key: str) -> Optional[float]:
    """
    CPU usage % since the previous check, from node_cpu_seconds_total (a
    counter). Returns None on the first call after startup since there's no
    prior sample to diff against yet.
    """
    text = _fetch_metrics_text(project_url, metrics_secret_key)
    if text is None:
        return None
    metrics = _parse_prometheus_metrics(text)

    cpu_points = metrics.get("node_cpu_seconds_total")
    if not cpu_points:
        logger.warning(
            "[SupabaseMonitor] Expected metric not found in response "
            "(node_cpu_seconds_total) — check the raw endpoint output for this project's metric names"
        )
        return None

    current = _sum_by_mode(cpu_points)
    previous = _last_cpu_sample[0]
    _last_cpu_sample[0] = current

    if previous is None:
        return None

    total_delta = 0.0
    idle_delta = 0.0
    for mode, curr_val in current.items():
        prev_val = previous.get(mode, curr_val)
        delta = curr_val - prev_val
        if delta < 0:
            continue  # counter reset (e.g. host restart) — skip this cycle
        total_delta += delta
        if mode == "idle":
            idle_delta += delta

    if total_delta <= 0:
        return None
    return (1 - idle_delta / total_delta) * 100


# ---------------------------------------------------------------------------
# Alert helpers
# ---------------------------------------------------------------------------

def _cooldown_active(alert_key: str) -> bool:
    last = _last_supabase_alert_sent.get(alert_key)
    return last is not None and (time.monotonic() - last) < SUPABASE_ALERT_COOLDOWN


def _format_alert_timestamp() -> str:
    now_ist = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Kolkata"))
    return now_ist.strftime("%Y-%m-%d %H:%M:%S IST")


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
        logger.debug("[SupabaseMonitor] Alert '%s' suppressed (cooldown active)", alert_key)
        return

    recipients = _get_alert_recipients()
    if not recipients:
        logger.warning("[SupabaseMonitor] No alert recipients configured — set HEALTH_ALERT_RECIPIENTS")
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
            client_name="SupabaseMonitor",
        )
        if success:
            _last_supabase_alert_sent[alert_key] = time.monotonic()
            logger.warning("[SupabaseMonitor] Alert sent: %s", subject)
        else:
            logger.error("[SupabaseMonitor] Failed to send alert: %s", msg)
    except Exception as e:
        logger.error("[SupabaseMonitor] Exception sending alert: %s", e, exc_info=True)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def check_supabase_metrics() -> dict:
    """
    Poll Supabase's Prometheus metrics endpoint and send alerts if CPU or
    memory usage exceeds its threshold. Rate-limited to once per
    SUPABASE_CHECK_INTERVAL. Safe to call every worker tick.
    """
    now = time.monotonic()
    if _last_supabase_check_at[0] is not None and (now - _last_supabase_check_at[0]) < SUPABASE_CHECK_INTERVAL:
        return {}
    _last_supabase_check_at[0] = now

    cfg = _get_supabase_config()

    if not cfg["project_url"] or not cfg["metrics_secret_key"]:
        logger.debug("[SupabaseMonitor] Skipping — SUPABASE_PROJECT_URL or SUPABASE_METRICS_SECRET_KEY not set")
        return {}

    timestamp = _format_alert_timestamp()
    result = {}

    mem = fetch_memory_percent(cfg["project_url"], cfg["metrics_secret_key"])
    if mem is not None:
        result["memory_percent"] = mem
        logger.debug("[SupabaseMonitor] Memory=%.1f%% (threshold=%.0f%%)", mem, cfg["memory_threshold"])
        if mem >= cfg["memory_threshold"]:
            plain, html = _build_email(
                title="High Memory Usage (Supabase)",
                color="#e53e3e", icon="&#9888;",
                rows=[
                    ("Memory Usage", f"{mem:.1f}%", True),
                    ("Threshold", f"{cfg['memory_threshold']:.0f}%", False),
                    ("Project", cfg["project_url"], False),
                ],
                timestamp=timestamp,
            )
            _send_alert(
                subject=f"[ALERT] High Memory: {mem:.1f}% on Pyro (Supabase)",
                plain=plain, html=html, alert_key="supabase_memory",
            )

    cpu = fetch_cpu_percent(cfg["project_url"], cfg["metrics_secret_key"])
    if cpu is not None:
        result["cpu_percent"] = cpu
        logger.debug("[SupabaseMonitor] CPU=%.1f%% (threshold=%.0f%%)", cpu, cfg["cpu_threshold"])
        if cpu >= cfg["cpu_threshold"]:
            plain, html = _build_email(
                title="High CPU Usage (Supabase)",
                color="#e53e3e", icon="&#9888;",
                rows=[
                    ("CPU Usage", f"{cpu:.1f}%", True),
                    ("Threshold", f"{cfg['cpu_threshold']:.0f}%", False),
                    ("Project", cfg["project_url"], False),
                ],
                timestamp=timestamp,
            )
            _send_alert(
                subject=f"[ALERT] High CPU: {cpu:.1f}% on Pyro (Supabase)",
                plain=plain, html=html, alert_key="supabase_cpu",
            )

    return result
