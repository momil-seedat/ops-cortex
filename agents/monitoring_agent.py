"""
Monitoring agent — polls Prometheus every 10 seconds, writes metrics to Redis
(current-state hash + time-series list), computes amber/red status, and
publishes alerts to the Redis alert stream.

Also exposes POST /webhook for Alertmanager integration.
"""
import asyncio
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI, Request, Response
import uvicorn

sys.path.insert(0, "/app")
import config
from memory.redis_store import RedisStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("monitoring_agent")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", config.PROMETHEUS_URL)
REDIS_HOST     = os.getenv("REDIS_HOST",     config.REDIS_HOST)
REDIS_PORT     = int(os.getenv("REDIS_PORT", str(config.REDIS_PORT)))

# (prometheus_metric_name, graph_metric_name, category)
# Adding a new service type only requires adding its Prometheus metric names here.
# The topology agent will auto-create the nodes in the graph from the Redis keys
# that the monitoring agent writes — no other code change needed.
PROMETHEUS_POLLS: list[tuple[str, str, str]] = [
    (config.PROM_ERROR_RATE_METRIC,        "error_rate",          "performance"),
    (config.PROM_LATENCY_P99_METRIC,       "latency_p99",         "performance"),
    (config.PROM_REQUEST_RATE_METRIC,      "request_rate",        "performance"),
    (config.PROM_DB_CONNECTIONS_METRIC,    "db_connection_count", "database"),
    (config.PROM_KAFKA_LAG_METRIC,         "consumer_lag",        "kafka"),
    (config.PROM_RECORDS_PROCESSED_METRIC, "records_processed",   "batch"),
    (config.PROM_BATCH_JOB_RUNNING_METRIC, "batch_job_running",   "batch"),
]

ALERT_COOLDOWN_SECONDS = 60


# ── Prometheus helpers ─────────────────────────────────────────────────────────

async def query_prometheus(session: aiohttp.ClientSession, metric: str) -> list[dict]:
    url = f"{PROMETHEUS_URL}/api/v1/query"
    try:
        async with session.get(url, params={"query": metric}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("data", {}).get("result", [])
    except Exception as e:
        logger.warning(f"Prometheus query failed for {metric}: {e}")
    return []


def extract_service(result: dict) -> str:
    labels = result.get("metric", {})
    return labels.get("service", labels.get("job", "unknown"))


# ── Main poll cycle ────────────────────────────────────────────────────────────

async def poll_prometheus(redis_store: RedisStore, sqlite_store=None):
    now = time.time()
    # Track which (service, metric) pairs are currently green so we can auto-resolve
    green_now: set[tuple[str, str]] = set()

    async with aiohttp.ClientSession() as session:
        for prom_name, graph_metric, category in PROMETHEUS_POLLS:
            results = await query_prometheus(session, prom_name)
            for result in results:
                service = extract_service(result)
                if not service or service == "unknown":
                    continue
                value_pair = result.get("value", [None, None])
                if value_pair[1] is None:
                    continue
                value = float(value_pair[1])
                status = config.compute_metric_status(graph_metric, value)
                thresholds = config.METRIC_THRESHOLDS.get(graph_metric, {"amber": 0, "red": 0})

                # Write current-state hash
                redis_store.set_metric_state(
                    service=service,
                    category=category,
                    metric=graph_metric,
                    value=value,
                    status=status,
                    threshold_low=thresholds["amber"],
                    threshold_high=thresholds["red"],
                    timestamp=now,
                )
                # Append to time series
                redis_store.push_timeseries(service, category, graph_metric, value, timestamp=now)
                # Legacy key for backward compat with dashboard timeseries charts
                redis_store.push_metric(service, prom_name, value, timestamp=now)

                if status == "green":
                    green_now.add((service, graph_metric))

                # Fire alert if status is red and cooldown passed
                if status == "red":
                    last = redis_store.get_last_alert_time(service, graph_metric)
                    if last is None or (now - last) > ALERT_COOLDOWN_SECONDS:
                        severity = "critical" if status == "red" else "warning"
                        redis_store.publish_alert_stream(
                            service=service,
                            category=category,
                            metric=graph_metric,
                            value=value,
                            threshold=thresholds["red"],
                            severity=severity,
                            timestamp=now,
                        )
                        redis_store.set_last_alert_time(service, graph_metric)
                        logger.info(f"Alert: {service}/{category}/{graph_metric}={value:.3f} [{severity}]")
                elif status == "amber":
                    last = redis_store.get_last_alert_time(service, f"{graph_metric}_amber")
                    if last is None or (now - last) > ALERT_COOLDOWN_SECONDS * 2:
                        redis_store.publish_alert_stream(
                            service=service,
                            category=category,
                            metric=graph_metric,
                            value=value,
                            threshold=thresholds["amber"],
                            severity="warning",
                            timestamp=now,
                        )
                        redis_store.set_last_alert_time(service, f"{graph_metric}_amber")

    # Auto-resolve: any unresolved incident whose metric is now back to green
    if sqlite_store and green_now:
        try:
            unresolved = sqlite_store.get_unresolved_incidents()
            for inc in unresolved:
                svc    = inc.get("service") or inc.get("faulted_service")
                metric = inc.get("metric")  or inc.get("alert_metric")
                if (svc, metric) in green_now:
                    sqlite_store.resolve_incident(inc["id"])
                    logger.info(f"Auto-resolved incident #{inc['id']} — {svc}/{metric} is back to green")
        except Exception as e:
            logger.warning(f"Auto-resolve check failed: {e}")


async def prometheus_loop(redis_store: RedisStore, sqlite_store=None):
    while True:
        try:
            await poll_prometheus(redis_store, sqlite_store)
        except Exception as e:
            logger.error(f"Prometheus poll error: {e}")
        await asyncio.sleep(config.MONITORING_PROMETHEUS_INTERVAL_SECONDS)


# ── Alertmanager webhook ───────────────────────────────────────────────────────

_redis_store: RedisStore = None  # set in lifespan


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis_store
    _redis_store = RedisStore(host=REDIS_HOST, port=REDIS_PORT)
    asyncio.create_task(prometheus_loop(_redis_store))
    yield


app = FastAPI(title="monitoring-agent", lifespan=lifespan)


@app.post("/webhook")
async def alertmanager_webhook(request: Request):
    """Receives Alertmanager alerts and writes them to the Redis alert stream."""
    try:
        body = await request.json()
    except Exception:
        return Response(status_code=400)

    alerts = body.get("alerts", [])
    written = 0
    for alert in alerts:
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        service  = labels.get("service", labels.get("job", "unknown"))
        metric   = labels.get("metric", annotations.get("metric", "unknown"))
        category = config.METRIC_CATEGORY_MAP.get(metric, "performance")
        severity = labels.get("severity", "warning")
        try:
            value     = float(annotations.get("value", 0.0))
            threshold = float(annotations.get("threshold", 0.0))
        except (TypeError, ValueError):
            value, threshold = 0.0, 0.0

        if service and service != "unknown":
            _redis_store.publish_alert_stream(
                service=service,
                category=category,
                metric=metric,
                value=value,
                threshold=threshold,
                severity=severity,
            )
            written += 1

    logger.info(f"Alertmanager webhook: {len(alerts)} received, {written} written to stream")
    return {"status": "ok", "written": written}


@app.get("/health")
async def health():
    return {"status": "ok", "redis": _redis_store.ping() if _redis_store else False}


# ── Entry point ────────────────────────────────────────────────────────────────

async def standalone_main():
    """Run without FastAPI when no webhook is needed."""
    logger.info("Monitoring agent starting (standalone mode)…")
    redis_store = RedisStore(host=REDIS_HOST, port=REDIS_PORT)
    for attempt in range(30):
        if redis_store.ping():
            break
        logger.info(f"Waiting for Redis… attempt {attempt + 1}")
        await asyncio.sleep(2)
    else:
        logger.error("Redis not reachable — exiting")
        sys.exit(1)

    redis_store.ensure_alert_consumer_group(config.ALERT_STREAM_CONSUMER_GROUP)
    logger.info("Redis connected — starting poll loops")
    await asyncio.gather(prometheus_loop(redis_store))


if __name__ == "__main__":
    mode = os.getenv("MONITORING_MODE", "server")
    if mode == "server":
        uvicorn.run("agents.monitoring_agent:app", host="0.0.0.0", port=8080, reload=False)
    else:
        asyncio.run(standalone_main())
