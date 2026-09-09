import asyncio
import os
import time
import random
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inventory_service")

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "/data/service_brain.db")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_INVENTORY", "inventory-events")

resource = Resource.create({"service.name": "inventory_service"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces")
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("inventory_service")

REQUEST_COUNT = Counter("service_requests_total", "Total requests", ["service", "endpoint", "status"])
REQUEST_LATENCY = Histogram(
    "service_request_duration_seconds",
    "Request duration",
    ["service", "endpoint"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)
ERROR_RATE_GAUGE = Gauge("service_error_rate", "Current error rate", ["service"])
LATENCY_P99_GAUGE = Gauge("service_request_duration_p99", "P99 latency ms", ["service"])
REQUEST_RATE_GAUGE = Gauge("service_request_rate", "Requests per second", ["service"])
DB_CONNECTIONS_GAUGE = Gauge("service_db_connections_active", "Active DB connections", ["service"])
KAFKA_LAG_GAUGE = Gauge("service_kafka_consumer_lag", "Simulated Kafka consumer lag", ["service"])

Base = declarative_base()

class Reservation(Base):
    __tablename__ = "reservations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    item = Column(String(100))
    quantity = Column(Integer)
    status = Column(String(50))
    created_at = Column(Float)

engine = create_engine(
    f"sqlite:///{SQLITE_DB_PATH}",
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=5,
    pool_timeout=10,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine)

_error_count = 0
_total_count = 0
_latencies: list[float] = []
_request_times: list[float] = []

_kafka_queue: list[dict] = []
_kafka_consumer_running = True
_kafka_lag = 0

_control = {
    "artificial_delay_ms": 0,
    "extra_error_rate": 0.0,
    "kafka_consumer_paused": False,
}


async def kafka_consumer_loop():
    global _kafka_lag, _kafka_consumer_running
    while _kafka_consumer_running:
        await asyncio.sleep(0.5)
        if _control["kafka_consumer_paused"]:
            continue
        batch = _kafka_queue[:10]
        if batch:
            del _kafka_queue[:len(batch)]
        _kafka_lag = len(_kafka_queue)
        KAFKA_LAG_GAUGE.labels(service="inventory_service").set(_kafka_lag)


def produce_kafka_event(item: str, quantity: int):
    _kafka_queue.append({"item": item, "quantity": quantity, "ts": time.time()})
    KAFKA_LAG_GAUGE.labels(service="inventory_service").set(len(_kafka_queue))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _kafka_consumer_running
    Base.metadata.create_all(engine)
    _kafka_consumer_running = True
    asyncio.create_task(kafka_consumer_loop())
    logger.info("inventory_service started")
    yield
    _kafka_consumer_running = False
    logger.info("inventory_service shutting down")


app = FastAPI(title="inventory_service", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    global _error_count, _total_count
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    endpoint = request.url.path
    status = str(response.status_code)
    REQUEST_COUNT.labels(service="inventory_service", endpoint=endpoint, status=status).inc()
    REQUEST_LATENCY.labels(service="inventory_service", endpoint=endpoint).observe(duration)
    _total_count += 1
    _latencies.append(duration * 1000)
    if len(_latencies) > 500:
        _latencies.pop(0)
    _request_times.append(time.time())
    if len(_request_times) > 500:
        _request_times.pop(0)
    if response.status_code >= 500:
        _error_count += 1
    window = max(1, _total_count)
    ERROR_RATE_GAUGE.labels(service="inventory_service").set(_error_count / window)
    now = time.time()
    recent = [t for t in _request_times if now - t < 10]
    REQUEST_RATE_GAUGE.labels(service="inventory_service").set(len(recent) / 10.0)
    if _latencies:
        sorted_lat = sorted(_latencies)
        idx = int(0.99 * len(sorted_lat))
        LATENCY_P99_GAUGE.labels(service="inventory_service").set(sorted_lat[min(idx, len(sorted_lat) - 1)])
    DB_CONNECTIONS_GAUGE.labels(service="inventory_service").set(engine.pool.checkedout())
    return response


class ReserveRequest(BaseModel):
    item: str
    quantity: int


class ControlRequest(BaseModel):
    artificial_delay_ms: Optional[int] = None
    extra_error_rate: Optional[float] = None
    kafka_consumer_paused: Optional[bool] = None


@app.get("/health")
async def health():
    return {
        "service": "inventory_service",
        "status": "healthy",
        "error_rate": _error_count / max(1, _total_count),
        "request_count": _total_count,
        "kafka_lag": _kafka_lag,
        "kafka_queue_depth": len(_kafka_queue),
        "control": _control,
    }


@app.post("/control")
async def control(req: ControlRequest):
    if req.artificial_delay_ms is not None:
        _control["artificial_delay_ms"] = req.artificial_delay_ms
    if req.extra_error_rate is not None:
        _control["extra_error_rate"] = req.extra_error_rate
    if req.kafka_consumer_paused is not None:
        _control["kafka_consumer_paused"] = req.kafka_consumer_paused
    return {"status": "ok", "control": _control}


@app.post("/reserve")
async def reserve(req: ReserveRequest):
    global _error_count

    if _control["artificial_delay_ms"] > 0:
        await asyncio.sleep(_control["artificial_delay_ms"] / 1000.0)

    if random.random() < _control["extra_error_rate"]:
        _error_count += 1
        raise HTTPException(status_code=500, detail="Inventory reservation error")

    with tracer.start_as_current_span("inventory.reserve") as span:
        span.set_attribute("inventory.item", req.item)
        span.set_attribute("inventory.quantity", req.quantity)

        with tracer.start_as_current_span("inventory.db_write"):
            session = SessionLocal()
            try:
                reservation = Reservation(
                    item=req.item,
                    quantity=req.quantity,
                    status="reserved",
                    created_at=time.time(),
                )
                session.add(reservation)
                session.commit()
                reservation_id = reservation.id
            finally:
                session.close()

        with tracer.start_as_current_span("inventory.kafka_produce"):
            produce_kafka_event(req.item, req.quantity)

        span.set_attribute("inventory.reservation_id", reservation_id)
        span.set_attribute("inventory.kafka_lag", _kafka_lag)
        return {
            "reservation_id": reservation_id,
            "item": req.item,
            "quantity": req.quantity,
            "status": "reserved",
            "kafka_lag": _kafka_lag,
        }


@app.get("/kafka/status")
async def kafka_status():
    return {
        "queue_depth": len(_kafka_queue),
        "lag": _kafka_lag,
        "consumer_paused": _control["kafka_consumer_paused"],
    }


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8003, reload=False)
