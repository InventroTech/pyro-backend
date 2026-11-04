"""Generate dashboards/api-timings-by-endpoint.json (import it in Grafana)."""

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "${datasource}"}
SEL = 'job=~"$job",endpoint=~"$endpoint"'


def target(ref, expr, legend="", instant=False):
    t = {"refId": ref, "datasource": DS, "expr": expr, "legendFormat": legend}
    if instant:
        t.update({"instant": True, "range": False, "format": "table"})
    return t


def timeseries(panel_id, title, expr, unit, y, legend="{{method}} {{endpoint}}"):
    return {
        "id": panel_id,
        "type": "timeseries",
        "title": title,
        "datasource": DS,
        "gridPos": {"h": 10, "w": 12, "x": 0 if panel_id % 2 else 12, "y": y},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "right", "calcs": ["mean", "max"]}},
        "targets": [target("A", expr, legend)],
    }


by = "sum by (method, endpoint)"
table_targets = [
    target("A", f'histogram_quantile(0.50, {by[:-1]}, le) (increase(http_request_duration_seconds_bucket{{{SEL}}}[$__range])))', instant=True),
    target("B", f'histogram_quantile(0.95, {by[:-1]}, le) (increase(http_request_duration_seconds_bucket{{{SEL}}}[$__range])))', instant=True),
    target("C", f'histogram_quantile(0.99, {by[:-1]}, le) (increase(http_request_duration_seconds_bucket{{{SEL}}}[$__range])))', instant=True),
    target("D", f'{by} (increase(http_request_duration_seconds_sum{{{SEL}}}[$__range])) / {by} (increase(http_request_duration_seconds_count{{{SEL}}}[$__range]))', instant=True),
    target("E", f'{by} (increase(http_requests_total{{{SEL}}}[$__range]))', instant=True),
    target("F", f'{by} (increase(http_requests_total{{{SEL},status=~"5.."}}[$__range]))', instant=True),
]

seconds = lambda name: {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "unit", "value": "s"}, {"id": "decimals", "value": 3}]}
count = lambda name: {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "unit", "value": "short"}, {"id": "decimals", "value": 0}]}

table = {
    "id": 1,
    "type": "table",
    "title": "Every API: time taken in the selected range (sorted by slowest p95)",
    "datasource": DS,
    "gridPos": {"h": 16, "w": 24, "x": 0, "y": 0},
    "targets": table_targets,
    "transformations": [
        {"id": "merge", "options": {}},
        {
            "id": "organize",
            "options": {
                "excludeByName": {"Time": True},
                "indexByName": {"method": 0, "endpoint": 1, "Value #A": 2, "Value #B": 3, "Value #C": 4, "Value #D": 5, "Value #E": 6, "Value #F": 7},
                "renameByName": {
                    "method": "Method",
                    "endpoint": "API",
                    "Value #A": "p50 (median)",
                    "Value #B": "p95",
                    "Value #C": "p99",
                    "Value #D": "Average",
                    "Value #E": "Requests",
                    "Value #F": "5xx errors",
                },
            },
        },
        {"id": "sortBy", "options": {"sort": [{"field": "p95", "desc": True}]}},
    ],
    "fieldConfig": {
        "defaults": {"custom": {"filterable": True}},
        "overrides": [
            seconds("p50 (median)"),
            {
                "matcher": {"id": "byName", "options": "p95"},
                "properties": [
                    {"id": "unit", "value": "s"},
                    {"id": "decimals", "value": 3},
                    {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                    {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                        {"color": "green", "value": None},
                        {"color": "yellow", "value": 0.5},
                        {"color": "orange", "value": 1},
                        {"color": "red", "value": 2.5},
                    ]}},
                ],
            },
            seconds("p99"),
            seconds("Average"),
            count("Requests"),
            count("5xx errors"),
        ],
    },
    "options": {"showHeader": True, "footer": {"show": False}},
}

panels = [
    table,
    timeseries(3, "p95 time per API", f'histogram_quantile(0.95, {by[:-1]}, le) (rate(http_request_duration_seconds_bucket{{{SEL}}}[$__rate_interval])))', "s", 16),
    timeseries(4, "Average time per API", f'{by} (rate(http_request_duration_seconds_sum{{{SEL}}}[$__rate_interval])) / {by} (rate(http_request_duration_seconds_count{{{SEL}}}[$__rate_interval]))', "s", 16),
    timeseries(5, "Requests per second per API", f'{by} (rate(http_requests_total{{{SEL}}}[$__rate_interval]))', "reqps", 26),
    timeseries(6, "5xx errors per second per API", f'{by} (rate(http_requests_total{{{SEL},status=~"5.."}}[$__rate_interval]))', "reqps", 26),
]

dashboard = {
    "title": "Pyro API timings by endpoint",
    "uid": "pyro-api-timings",
    "tags": ["pyro", "api", "latency"],
    "timezone": "browser",
    "schemaVersion": 39,
    "refresh": "1m",
    "time": {"from": "now-24h", "to": "now"},
    "templating": {
        "list": [
            {"name": "datasource", "label": "Data source", "type": "datasource", "query": "prometheus"},
            {
                "name": "job",
                "label": "Service",
                "type": "query",
                "datasource": DS,
                "query": {"query": "label_values(http_requests_total, job)", "refId": "job"},
                "definition": "label_values(http_requests_total, job)",
                "refresh": 2,
                "includeAll": True,
                "multi": True,
                "current": {"text": "All", "value": "$__all"},
            },
            {
                "name": "endpoint",
                "label": "API",
                "type": "query",
                "datasource": DS,
                "query": {"query": 'label_values(http_requests_total{job=~"$job"}, endpoint)', "refId": "endpoint"},
                "definition": 'label_values(http_requests_total{job=~"$job"}, endpoint)',
                "refresh": 2,
                "includeAll": True,
                "multi": True,
                "current": {"text": "All", "value": "$__all"},
            },
        ]
    },
    "panels": panels,
}

out = Path(__file__).parent / "dashboards" / "api-timings-by-endpoint.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(out)
