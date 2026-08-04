# Pyro Grafana + Prometheus (API latency)

## Quick start (local)

1. Run the Django API on port 8000 (dev has open `/metrics`).
2. From this folder:

```bash
cd grafana
docker compose up -d
```

3. Open Grafana: http://localhost:3000  
   Login: `admin` / `admin`  
   Dashboard: **Pyro → Pyro API Latency**

4. Confirm Prometheus is scraping: http://localhost:9090/targets

Generate a bit of traffic against the API, wait ~15s, then refresh the dashboard.

## Staging / production scrape

1. Set `METRICS_AUTH_TOKEN` on the Render service.
2. Copy `prometheus/prometheus.remote.yml.example` into `prometheus/prometheus.yml`
   (or merge the remote job) and replace:
   - `REPLACE_WITH_METRICS_AUTH_TOKEN`
   - `api.thepyro.ai` (or your staging host)
3. Restart Prometheus: `docker compose restart prometheus`

## Import into existing Grafana

If you already have Grafana Cloud / a shared Grafana:

1. Add a Prometheus datasource that can reach the scraper (or Grafana Cloud Metrics).
2. **Dashboards → Import → Upload** `dashboards/api-latency.json`
3. Select your Prometheus datasource when prompted.

## Useful PromQL

```promql
# p95 by endpoint
histogram_quantile(0.95, sum by (endpoint, le) (rate(http_request_duration_seconds_bucket[5m])))

# request rate
sum by (endpoint) (rate(http_requests_total[5m]))
```
