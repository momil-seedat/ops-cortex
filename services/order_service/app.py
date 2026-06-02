import asyncio
import os
import time
import random
import logging
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("order_service")

PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://payment-service:8002")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://inventory-service:8003")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "/data/service_brain.db")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
DB_POOL_SIZE = int(os.getenv("ORDER_DB_POOL_SIZE", "20"))

resource = Resource.create({"service.name": "order_service"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces")
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("order_service")

HTTPXClientInstrumentor().instrument()

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

Base = declarative_base()

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    item = Column(String(100))
    quantity = Column(Integer)
    amount = Column(Float)
    status = Column(String(50))
    created_at = Column(Float)

engine = create_engine(
    f"sqlite:///{SQLITE_DB_PATH}",
    poolclass=QueuePool,
    pool_size=DB_POOL_SIZE,
    max_overflow=0,
    pool_timeout=10,
    connect_args={"check_same_thread": False},
)

SessionLocal = sessionmaker(bind=engine)

_request_times: list[float] = []
_error_count = 0
_total_count = 0
_latencies: list[float] = []

_control = {
    "artificial_delay_ms": 0,
    "extra_error_rate": 0.0,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    logger.info("order_service started")
    yield
    logger.info("order_service shutting down")


app = FastAPI(title="order_service", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    global _error_count, _total_count
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    endpoint = request.url.path
    status = str(response.status_code)
    REQUEST_COUNT.labels(service="order_service", endpoint=endpoint, status=status).inc()
    REQUEST_LATENCY.labels(service="order_service", endpoint=endpoint).observe(duration)
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
    ERROR_RATE_GAUGE.labels(service="order_service").set(_error_count / window)
    now = time.time()
    recent = [t for t in _request_times if now - t < 10]
    REQUEST_RATE_GAUGE.labels(service="order_service").set(len(recent) / 10.0)
    if _latencies:
        sorted_lat = sorted(_latencies)
        idx = int(0.99 * len(sorted_lat))
        LATENCY_P99_GAUGE.labels(service="order_service").set(sorted_lat[min(idx, len(sorted_lat) - 1)])
    pool = engine.pool
    DB_CONNECTIONS_GAUGE.labels(service="order_service").set(pool.checkedout())
    return response


class OrderRequest(BaseModel):
    item: str
    quantity: int
    amount: float


class ControlRequest(BaseModel):
    artificial_delay_ms: Optional[int] = None
    extra_error_rate: Optional[float] = None


@app.get("/health")
async def health():
    pool = engine.pool
    return {
        "service": "order_service",
        "status": "healthy",
        "error_rate": _error_count / max(1, _total_count),
        "request_count": _total_count,
        "db_connections_active": pool.checkedout(),
        "db_pool_size": DB_POOL_SIZE,
    }


@app.post("/control")
async def control(req: ControlRequest):
    if req.artificial_delay_ms is not None:
        _control["artificial_delay_ms"] = req.artificial_delay_ms
    if req.extra_error_rate is not None:
        _control["extra_error_rate"] = req.extra_error_rate
    return {"status": "ok", "control": _control}


@app.post("/order")
async def create_order(req: OrderRequest):
    global _error_count

    if _control["artificial_delay_ms"] > 0:
        await asyncio.sleep(_control["artificial_delay_ms"] / 1000.0)

    if random.random() < _control["extra_error_rate"]:
        _error_count += 1
        raise HTTPException(status_code=500, detail="Injected error")

    with tracer.start_as_current_span("order.process") as span:
        span.set_attribute("order.item", req.item)
        span.set_attribute("order.quantity", req.quantity)
        span.set_attribute("order.amount", req.amount)

        async with httpx.AsyncClient(timeout=10.0) as client:
            with tracer.start_as_current_span("order.call_payment"):
                pay_resp = await client.post(
                    f"{PAYMENT_SERVICE_URL}/charge",
                    json={"order_id": 0, "amount": req.amount},
                )
            if pay_resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Payment failed")

            with tracer.start_as_current_span("order.call_inventory"):
                inv_resp = await client.post(
                    f"{INVENTORY_SERVICE_URL}/reserve",
                    json={"item": req.item, "quantity": req.quantity},
                )
            if inv_resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Inventory reservation failed")

        with tracer.start_as_current_span("order.db_write"):
            session = SessionLocal()
            try:
                order = Order(
                    item=req.item,
                    quantity=req.quantity,
                    amount=req.amount,
                    status="confirmed",
                    created_at=time.time(),
                )
                session.add(order)
                session.commit()
                order_id = order.id
            finally:
                session.close()

        span.set_attribute("order.id", order_id)
        return {"order_id": order_id, "status": "confirmed"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8001, reload=False)
