"""
report_service — a scheduled batch service that runs a daily report job.

Behaviour:
  - Idles quietly for most of the day (low/zero metrics)
  - Once per day at REPORT_RUN_HOUR (default 02:00), runs the batch job for
    REPORT_DURATION_MINUTES (default 5 minutes)
  - During the job: emits real OTel traces, pumps Prometheus gauges (request_rate,
    latency, error_rate, records_processed), so the topology agent discovers it,
    Neo4j snapshots it, and the baseline model learns the daily schedule

This service intentionally has NO downstream callers in the initial setup.
The topology agent will discover it via its Redis node:* keys and the baseline
model will learn that it only has meaningful activity once per day at ~02:00.

Endpoints:
  GET  /health        — liveness + current job state
  GET  /metrics       — Prometheus scrape target
  POST /control       — trigger job manually, set error rate, set delay (testing)
  GET  /job/status    — current job run details
  POST /job/run       — trigger an immediate job run (used by scenario 06)
"""
import asyncio
import os
import time
import random
import logging
from contextlib import asynccontextmanager
from typing import Optional
import datetime

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("report_service")

OTEL_ENDPOINT        = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
REPORT_RUN_HOUR      = int(os.getenv("REPORT_RUN_HOUR", "2"))        # 02:00 daily
REPORT_DURATION_SECS = int(os.getenv("REPORT_DURATION_MINUTES", "5")) * 60
RECORDS_PER_BATCH    = int(os.getenv("RECORDS_PER_BATCH", "1000"))

resource = Resource.create({"service.name": "report_service"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces")
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("report_service")

# ── Prometheus metrics ─────────────────────────────────────────────────────────
REQUEST_COUNT   = Counter("service_requests_total", "Total requests",
                          ["service", "endpoint", "status"])
REQUEST_LATENCY = Histogram("service_request_duration_seconds", "Request duration",
                            ["service", "endpoint"],
                            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0])
ERROR_RATE_GAUGE    = Gauge("service_error_rate",               "Current error rate",       ["service"])
LATENCY_P99_GAUGE   = Gauge("service_request_duration_p99",    "P99 latency ms",            ["service"])
REQUEST_RATE_GAUGE  = Gauge("service_request_rate",             "Requests per second",       ["service"])
RECORDS_GAUGE       = Gauge("service_records_processed",        "Records processed in job",  ["service"])
JOB_RUNNING_GAUGE   = Gauge("service_batch_job_running",        "1 if batch job is running", ["service"])
JOB_DURATION_GAUGE  = Gauge("service_batch_job_duration_secs",  "Last job duration seconds", ["service"])

# ── Runtime state ──────────────────────────────────────────────────────────────
_job_state = {
    "running":          False,
    "run_count":        0,
    "last_started_at":  None,   # epoch float
    "last_finished_at": None,
    "last_duration_s":  0.0,
    "last_records":     0,
    "current_records":  0,      # live counter updated every chunk during a run
    "last_error":       None,
}

_control = {
    "extra_error_rate": 0.0,
    "artificial_delay_ms": 0,
}

# These counters track JOB activity only — not infrastructure HTTP requests.
# _emit_request_metric() is the only writer; it is called exclusively from
# inside _run_report_job(). Infrastructure endpoints (/health, /metrics, etc.)
# never touch these, so the gauges are zero when the job is not running.
_job_request_times: list[float] = []
_job_latencies:     list[float] = []
_error_count = 0
_job_op_count = 0   # job operations, not HTTP requests


# ── Job logic ──────────────────────────────────────────────────────────────────

async def _run_report_job(triggered_by: str = "scheduler"):
    """
    Simulates a data pipeline batch job:
      1. Fetch phase  — simulate reading records from upstream (order_service DB)
      2. Process phase — CPU-bound aggregation simulation
      3. Write phase  — simulate writing report output
      4. Notify phase — simulate sending a completion webhook

    Emits OTel spans and updates Prometheus gauges throughout so the topology
    agent sees real activity and the baseline model learns the run window.
    """
    global _error_count, _job_op_count

    if _job_state["running"]:
        logger.info("Job already running — skipping duplicate trigger")
        return

    _job_state["running"]          = True
    _job_state["last_started_at"]  = time.time()
    _job_state["run_count"]       += 1
    _job_state["last_error"]       = None
    _job_state["current_records"]  = 0
    JOB_RUNNING_GAUGE.labels(service="report_service").set(1)
    logger.info(f"report_job started (triggered_by={triggered_by}, run #{_job_state['run_count']})")

    start = time.time()
    records_done = 0

    try:
        with tracer.start_as_current_span("report_job.run") as root_span:
            root_span.set_attribute("triggered_by", triggered_by)
            root_span.set_attribute("run_number", _job_state["run_count"])

            # ── Phase 1: Fetch ────────────────────────────────────────────────
            with tracer.start_as_current_span("report_job.fetch"):
                fetch_delay = 0.8 + random.uniform(0.0, 0.4)
                if _control["artificial_delay_ms"] > 0:
                    fetch_delay += _control["artificial_delay_ms"] / 1000.0
                await asyncio.sleep(fetch_delay)
                records = RECORDS_PER_BATCH + random.randint(-100, 200)
                # Latency = time per record fetched (not the total sleep duration)
                _emit_request_metric(fetch_delay * 1000 / max(1, records))
                logger.info(f"report_job.fetch: {records} records")

            # ── Phase 2: Process (chunked so metrics pump continuously) ───────
            chunk_size  = max(1, records // 10)
            chunk_delay = REPORT_DURATION_SECS / 10
            with tracer.start_as_current_span("report_job.process"):
                for chunk_idx in range(10):
                    if random.random() < _control["extra_error_rate"]:
                        _error_count += 1
                        _job_state["last_error"] = f"chunk {chunk_idx} processing error"
                    chunk_recs = min(chunk_size, records - records_done)
                    records_done += chunk_recs
                    # Update live counter immediately so /job/status reflects progress
                    _job_state["current_records"] = records_done
                    RECORDS_GAUGE.labels(service="report_service").set(records_done)
                    # Emit metric BEFORE sleeping so Prometheus sees it on next scrape
                    _emit_request_metric(random.uniform(50, 200))
                    logger.info(f"report_job.process chunk {chunk_idx+1}/10 — {records_done}/{records}")
                    await asyncio.sleep(chunk_delay)

            # ── Phase 3: Write ────────────────────────────────────────────────
            with tracer.start_as_current_span("report_job.write"):
                write_delay = 0.5 + random.uniform(0.0, 0.3)
                await asyncio.sleep(write_delay)
                # Latency = time per record written
                _emit_request_metric(write_delay * 1000 / max(1, records_done))
                logger.info(f"report_job.write: {records_done} records written")

            # ── Phase 4: Notify ───────────────────────────────────────────────
            with tracer.start_as_current_span("report_job.notify"):
                await asyncio.sleep(0.1)
                logger.info("report_job.notify: completion event sent")

            root_span.set_attribute("records_processed", records_done)

    except Exception as e:
        _job_state["last_error"] = str(e)
        logger.error(f"report_job error: {e}")
    finally:
        duration = time.time() - start
        _job_state["running"]          = False
        _job_state["last_finished_at"] = time.time()
        _job_state["last_duration_s"]  = round(duration, 2)
        _job_state["last_records"]     = records_done
        _job_state["current_records"]  = 0
        JOB_RUNNING_GAUGE.labels(service="report_service").set(0)
        JOB_DURATION_GAUGE.labels(service="report_service").set(duration)
        # Clear the rate window so REQUEST_RATE drops to 0 immediately
        _job_request_times.clear()
        REQUEST_RATE_GAUGE.labels(service="report_service").set(0.0)
        LATENCY_P99_GAUGE.labels(service="report_service").set(0.0)
        logger.info(
            f"report_job finished: duration={duration:.1f}s "
            f"records={records_done} error={_job_state['last_error']}"
        )


def _emit_request_metric(latency_ms: float):
    """
    Called exclusively from inside the batch job to record one processing
    operation. Updates REQUEST_RATE_GAUGE, LATENCY_P99_GAUGE, ERROR_RATE_GAUGE.
    Never called from HTTP request handlers — infrastructure traffic is invisible
    to these gauges so they stay at 0.0 when the job is not running.
    """
    global _job_op_count
    _job_op_count += 1
    _job_latencies.append(latency_ms)
    if len(_job_latencies) > 500:
        _job_latencies.pop(0)
    _job_request_times.append(time.time())
    if len(_job_request_times) > 500:
        _job_request_times.pop(0)
    now = time.time()
    recent = [t for t in _job_request_times if now - t < 10]
    REQUEST_RATE_GAUGE.labels(service="report_service").set(len(recent) / 10.0)
    if _job_latencies:
        s = sorted(_job_latencies)
        idx = int(0.99 * len(s))
        LATENCY_P99_GAUGE.labels(service="report_service").set(s[min(idx, len(s)-1)])
    err_rate = _error_count / max(1, _job_op_count)
    ERROR_RATE_GAUGE.labels(service="report_service").set(err_rate)


# ── Scheduler ─────────────────────────────────────────────────────────────────

async def _scheduler_loop():
    """
    Wakes once per minute, checks if current hour == REPORT_RUN_HOUR and the
    job hasn't already run today, then fires the job.
    Also calls REQUEST_RATE_GAUGE(0) during idle so topology agent sees flatline.
    """
    last_run_date: Optional[datetime.date] = None
    logger.info(f"Scheduler: report job will fire daily at {REPORT_RUN_HOUR:02d}:00")

    while True:
        await asyncio.sleep(60)
        now_dt = datetime.datetime.now()
        today  = now_dt.date()

        # Keep metrics zeroed during idle so the service is still visible in Prometheus
        if not _job_state["running"]:
            REQUEST_RATE_GAUGE.labels(service="report_service").set(0.0)
            ERROR_RATE_GAUGE.labels(service="report_service").set(0.0)

        if now_dt.hour == REPORT_RUN_HOUR and last_run_date != today:
            last_run_date = today
            logger.info(f"Scheduler: triggering daily report job at {now_dt.strftime('%H:%M')}")
            asyncio.create_task(_run_report_job(triggered_by="scheduler"))


# ── FastAPI app ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize all gauges to zero immediately so Prometheus scrapes them on the
    # very first poll and the monitoring agent writes Redis node:* keys right away.
    # Without this, a gauge that has never been set produces no metric output at all
    # and the topology agent cannot discover the service until the first job runs.
    REQUEST_RATE_GAUGE.labels(service="report_service").set(0.0)
    ERROR_RATE_GAUGE.labels(service="report_service").set(0.0)
    LATENCY_P99_GAUGE.labels(service="report_service").set(0.0)
    RECORDS_GAUGE.labels(service="report_service").set(0.0)
    JOB_RUNNING_GAUGE.labels(service="report_service").set(0.0)
    JOB_DURATION_GAUGE.labels(service="report_service").set(0.0)

    asyncio.create_task(_scheduler_loop())
    logger.info(
        f"report_service started — daily job at {REPORT_RUN_HOUR:02d}:00, "
        f"duration ~{REPORT_DURATION_SECS//60} min"
    )
    yield
    logger.info("report_service shutting down")


app = FastAPI(title="report_service", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


# Endpoints that are infrastructure-only and must NOT contribute to business
# request_rate or latency gauges. Prometheus scrapes /metrics every 10s,
# healthchecks hit /health every 10s, the scenario polls /job/status every 10s.
# Counting those would make the service look busy when it is idle.
_INFRA_PATHS = {"/health", "/metrics", "/job/status", "/job/run", "/control"}


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start    = time.time()
    response = await call_next(request)
    duration = time.time() - start

    # Always count raw request totals for Prometheus (useful for debugging)
    REQUEST_COUNT.labels(
        service="report_service",
        endpoint=request.url.path,
        status=str(response.status_code),
    ).inc()
    REQUEST_LATENCY.labels(
        service="report_service",
        endpoint=request.url.path,
    ).observe(duration)

    # Do NOT update business request_rate or latency gauges for infra endpoints.
    # Those gauges should only move when the batch job is actually processing data.
    return response


@app.get("/health")
async def health():
    return {
        "service":            "report_service",
        "status":             "healthy",
        "job_running":        _job_state["running"],
        "run_count":          _job_state["run_count"],
        "last_started_at":    _job_state["last_started_at"],
        "last_duration_s":    _job_state["last_duration_s"],
        "last_records":       _job_state["last_records"],
        "last_error":         _job_state["last_error"],
        "next_run_hour":      REPORT_RUN_HOUR,
        "error_rate":         _error_count / max(1, _job_op_count),
        "job_op_count":       _job_op_count,
    }


@app.get("/job/status")
async def job_status():
    return {
        **_job_state,
        # Expose live progress counter — non-zero only while running
        "records_so_far": _job_state["current_records"] if _job_state["running"] else _job_state["last_records"],
        "control": _control,
    }


class ControlRequest(BaseModel):
    extra_error_rate:    Optional[float] = None
    artificial_delay_ms: Optional[int]   = None


@app.post("/control")
async def control(req: ControlRequest):
    if req.extra_error_rate is not None:
        _control["extra_error_rate"] = req.extra_error_rate
    if req.artificial_delay_ms is not None:
        _control["artificial_delay_ms"] = req.artificial_delay_ms
    return {"status": "ok", "control": _control}


@app.post("/job/run")
async def trigger_job():
    """Trigger an immediate job run — used by scenario 06 and for manual testing."""
    if _job_state["running"]:
        return {"status": "already_running", "run_count": _job_state["run_count"]}
    asyncio.create_task(_run_report_job(triggered_by="api"))
    return {"status": "started", "run_count": _job_state["run_count"] + 1}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8004, reload=False)
