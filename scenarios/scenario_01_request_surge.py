"""
Scenario 01 — Request Surge / DB Connection Pool Exhaustion

Phase 1: 30s normal traffic at 10 req/s to build baseline.
Phase 2: 60s ramp to 150 req/s — DB pool (size 20) exhausts, latency spikes,
         error rate climbs above threshold.
Phase 3: 60s hold at 150 req/s — diagnosis agent fires, traces back to
         order_service DB connection exhaustion as root cause.

Expected diagnosis: order_service is the fault origin (DB connections crossed
threshold before error rate), payment and inventory are downstream victims.
"""
import asyncio
import time
import logging
import os
import sys

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://localhost:8001")
PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://localhost:8002")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://localhost:8003")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scenario_01")

_running = True
_sent = 0
_errors = 0
_request_counter_lock = asyncio.Lock()


async def send_order(client: httpx.AsyncClient) -> bool:
    global _sent, _errors
    try:
        resp = await client.post(
            f"{ORDER_SERVICE_URL}/order",
            json={"item": "widget", "quantity": 1, "amount": 9.99},
            timeout=5.0,
        )
        async with _request_counter_lock:
            _sent += 1
            if resp.status_code >= 500:
                _errors += 1
        return resp.status_code == 200
    except Exception:
        async with _request_counter_lock:
            _sent += 1
            _errors += 1
        return False


async def traffic_generator(rate_per_second: float, duration_seconds: float, label: str):
    global _sent, _errors
    interval = 1.0 / rate_per_second
    end_time = time.time() + duration_seconds
    tasks_started = 0
    logger.info(f"[{label}] Starting {rate_per_second} req/s for {duration_seconds}s")

    async with httpx.AsyncClient() as client:
        while time.time() < end_time and _running:
            asyncio.create_task(send_order(client))
            tasks_started += 1
            await asyncio.sleep(interval)

    logger.info(f"[{label}] Done — {tasks_started} requests dispatched")


async def print_stats_loop():
    while _running:
        await asyncio.sleep(10)
        async with _request_counter_lock:
            total = _sent
            errs = _errors
        rate = errs / max(1, total)
        logger.info(f"Stats — total={total} errors={errs} error_rate={rate:.3f}")


async def wait_for_service(url: str, label: str, retries: int = 30):
    async with httpx.AsyncClient() as client:
        for i in range(retries):
            try:
                resp = await client.get(f"{url}/health", timeout=3.0)
                if resp.status_code == 200:
                    logger.info(f"{label} is up")
                    return
            except Exception:
                pass
            logger.info(f"Waiting for {label}… ({i + 1}/{retries})")
            await asyncio.sleep(2)
    raise RuntimeError(f"{label} not reachable after {retries} attempts")


async def reset_controls():
    async with httpx.AsyncClient() as client:
        for url in [ORDER_SERVICE_URL, PAYMENT_SERVICE_URL, INVENTORY_SERVICE_URL]:
            try:
                await client.post(
                    f"{url}/control",
                    json={"artificial_delay_ms": 0, "extra_error_rate": 0.0},
                    timeout=3.0,
                )
            except Exception:
                pass


async def main():
    global _running

    logger.info("=" * 60)
    logger.info("Scenario 01: Request Surge → DB Pool Exhaustion")
    logger.info("=" * 60)

    await wait_for_service(ORDER_SERVICE_URL, "order_service")
    await wait_for_service(PAYMENT_SERVICE_URL, "payment_service")
    await wait_for_service(INVENTORY_SERVICE_URL, "inventory_service")

    await reset_controls()

    stats_task = asyncio.create_task(print_stats_loop())

    # Phase 1: normal baseline
    logger.info("--- Phase 1: Building baseline (10 req/s, 30s) ---")
    await traffic_generator(rate_per_second=10.0, duration_seconds=30.0, label="Phase1-Normal")
    logger.info("Phase 1 complete — baseline established in Redis")

    # Phase 2: surge
    logger.info("--- Phase 2: Traffic surge (150 req/s, 60s) — DB pool will exhaust ---")
    await traffic_generator(rate_per_second=150.0, duration_seconds=60.0, label="Phase2-Surge")
    logger.info("Phase 2 complete — error rate should be climbing")

    # Phase 3: hold
    logger.info("--- Phase 3: Hold surge (150 req/s, 60s) — waiting for diagnosis agent ---")
    await traffic_generator(rate_per_second=150.0, duration_seconds=60.0, label="Phase3-Hold")

    _running = False
    stats_task.cancel()

    async with _request_counter_lock:
        total = _sent
        errs = _errors
    logger.info("=" * 60)
    logger.info(f"Scenario 01 complete — {total} requests, {errs} errors ({errs/max(1,total)*100:.1f}%)")
    logger.info("Check diagnosis_agent logs and SQLite incidents table for RCA result")
    logger.info("Expected root cause: order_service (DB connection pool exhaustion)")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
