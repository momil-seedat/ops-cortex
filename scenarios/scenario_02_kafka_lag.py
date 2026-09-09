"""
Scenario 02 — Kafka Consumer Lag

Phase 1: 30s normal traffic at 10 req/s to build baseline.
Phase 2: Pause inventory_service Kafka consumer while traffic continues.
         Lag counter climbs past threshold (1000 messages).
         Monitoring agent fires a kafka_lag alert on inventory_service.
Phase 3: Re-enable consumer, wait for lag to drain, mark incident resolved.

Expected diagnosis: kafka_lag is a leaf-node fault on inventory_service.
Order and payment remain healthy — no upstream degradation.
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
logger = logging.getLogger("scenario_02")

_running = True
_sent = 0
_errors = 0
_lock = asyncio.Lock()


async def send_order(client: httpx.AsyncClient):
    global _sent, _errors
    try:
        resp = await client.post(
            f"{ORDER_SERVICE_URL}/order",
            json={"item": "gadget", "quantity": 2, "amount": 24.99},
            timeout=5.0,
        )
        async with _lock:
            _sent += 1
            if resp.status_code >= 500:
                _errors += 1
    except Exception:
        async with _lock:
            _sent += 1
            _errors += 1


async def traffic_generator(rate_per_second: float, duration_seconds: float, label: str):
    interval = 1.0 / rate_per_second
    end_time = time.time() + duration_seconds
    logger.info(f"[{label}] Starting {rate_per_second} req/s for {duration_seconds}s")
    async with httpx.AsyncClient() as client:
        while time.time() < end_time and _running:
            asyncio.create_task(send_order(client))
            await asyncio.sleep(interval)
    logger.info(f"[{label}] Done")


async def set_inventory_consumer(paused: bool):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{INVENTORY_SERVICE_URL}/control",
            json={"kafka_consumer_paused": paused},
            timeout=5.0,
        )
        state = "paused" if paused else "running"
        logger.info(f"Kafka consumer set to: {state} — {resp.status_code}")


async def get_kafka_lag() -> int:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{INVENTORY_SERVICE_URL}/kafka/status", timeout=3.0)
            if resp.status_code == 200:
                return resp.json().get("lag", 0)
    except Exception:
        pass
    return 0


async def wait_for_lag(target_lag: int, timeout_seconds: float = 60.0, direction: str = "above"):
    start = time.time()
    while time.time() - start < timeout_seconds:
        lag = await get_kafka_lag()
        logger.info(f"Kafka lag: {lag}")
        if direction == "above" and lag >= target_lag:
            logger.info(f"Lag {lag} reached target {target_lag}")
            return lag
        if direction == "below" and lag <= target_lag:
            logger.info(f"Lag {lag} drained below {target_lag}")
            return lag
        await asyncio.sleep(2)
    lag = await get_kafka_lag()
    return lag


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
    raise RuntimeError(f"{label} not reachable")


async def print_stats_loop():
    while _running:
        await asyncio.sleep(10)
        lag = await get_kafka_lag()
        async with _lock:
            total = _sent
            errs = _errors
        logger.info(f"Stats — requests={total} errors={errs} kafka_lag={lag}")


async def main():
    global _running

    logger.info("=" * 60)
    logger.info("Scenario 02: Kafka Consumer Lag")
    logger.info("=" * 60)

    await wait_for_service(ORDER_SERVICE_URL, "order_service")
    await wait_for_service(INVENTORY_SERVICE_URL, "inventory_service")

    # Ensure consumer is running
    await set_inventory_consumer(paused=False)

    stats_task = asyncio.create_task(print_stats_loop())

    # Phase 1: normal baseline
    logger.info("--- Phase 1: Building baseline (10 req/s, 30s) ---")
    await traffic_generator(rate_per_second=10.0, duration_seconds=30.0, label="Phase1-Normal")

    # Phase 2: pause consumer while traffic runs
    logger.info("--- Phase 2: Pausing Kafka consumer — lag will accumulate ---")
    await set_inventory_consumer(paused=True)

    traffic_task = asyncio.create_task(
        traffic_generator(rate_per_second=20.0, duration_seconds=120.0, label="Phase2-BackgroundTraffic")
    )

    lag = await wait_for_lag(target_lag=1000, timeout_seconds=90.0, direction="above")
    logger.info(f"Lag reached {lag} — monitoring agent should have fired kafka_lag alert")

    # Hold for 15s so diagnosis agent can process
    await asyncio.sleep(15)

    # Phase 3: re-enable consumer, wait for drain
    logger.info("--- Phase 3: Re-enabling consumer — lag should drain ---")
    await set_inventory_consumer(paused=False)

    lag = await wait_for_lag(target_lag=50, timeout_seconds=60.0, direction="below")
    logger.info(f"Lag drained to {lag} — incident should be resolved")

    # Cancel background traffic
    traffic_task.cancel()
    _running = False
    stats_task.cancel()

    async with _lock:
        total = _sent
        errs = _errors
    logger.info("=" * 60)
    logger.info(f"Scenario 02 complete — {total} requests, {errs} errors")
    logger.info("Expected root cause: inventory_service kafka_lag (leaf node)")
    logger.info("Order and payment services should remain healthy")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
