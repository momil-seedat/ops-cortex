"""
Scenario 03 — Cascade Failure: Payment Service → Order Service

Phase 1: 30s normal traffic at 10 req/s to build baseline.
Phase 2: Set payment_service error rate to 0.9 (90% failures).
         order_service calls payment, starts failing, its own error rate rises.
         Both services now exceed alert threshold.
Phase 3: Hold 60s — diagnosis agent traces upstream, identifies payment_service
         as root cause (degraded first), order_service as downstream victim.
Recovery: Restore payment_service, verify metrics recover.

Expected diagnosis: root_cause=payment_service, fault_path=[payment_service, order_service].
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
logger = logging.getLogger("scenario_03")

_running = True
_sent = 0
_errors = 0
_lock = asyncio.Lock()


async def send_order(client: httpx.AsyncClient):
    global _sent, _errors
    try:
        resp = await client.post(
            f"{ORDER_SERVICE_URL}/order",
            json={"item": "premium_widget", "quantity": 1, "amount": 49.99},
            timeout=8.0,
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


async def set_payment_error_rate(error_rate: float):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{PAYMENT_SERVICE_URL}/control",
            json={"extra_error_rate": error_rate},
            timeout=5.0,
        )
        logger.info(f"Payment error_rate set to {error_rate} — HTTP {resp.status_code}")


async def get_service_health(url: str, name: str) -> dict:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{url}/health", timeout=3.0)
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "service": name,
                    "error_rate": data.get("error_rate", 0.0),
                    "request_count": data.get("request_count", 0),
                }
    except Exception:
        pass
    return {"service": name, "error_rate": -1, "request_count": 0}


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
        order_h = await get_service_health(ORDER_SERVICE_URL, "order")
        pay_h = await get_service_health(PAYMENT_SERVICE_URL, "payment")
        inv_h = await get_service_health(INVENTORY_SERVICE_URL, "inventory")
        logger.info(
            f"Health — order.err={order_h['error_rate']:.3f} "
            f"payment.err={pay_h['error_rate']:.3f} "
            f"inventory.err={inv_h['error_rate']:.3f}"
        )


async def reset_controls():
    async with httpx.AsyncClient() as client:
        for url, name in [
            (ORDER_SERVICE_URL, "order"),
            (PAYMENT_SERVICE_URL, "payment"),
            (INVENTORY_SERVICE_URL, "inventory"),
        ]:
            try:
                await client.post(
                    f"{url}/control",
                    json={"artificial_delay_ms": 0, "extra_error_rate": 0.0},
                    timeout=3.0,
                )
                logger.info(f"Reset controls on {name}")
            except Exception as e:
                logger.warning(f"Could not reset {name}: {e}")


async def main():
    global _running

    logger.info("=" * 60)
    logger.info("Scenario 03: Cascade Failure — Payment → Order")
    logger.info("=" * 60)

    await wait_for_service(ORDER_SERVICE_URL, "order_service")
    await wait_for_service(PAYMENT_SERVICE_URL, "payment_service")
    await wait_for_service(INVENTORY_SERVICE_URL, "inventory_service")

    await reset_controls()

    stats_task = asyncio.create_task(print_stats_loop())

    # Phase 1: baseline
    logger.info("--- Phase 1: Building baseline (10 req/s, 30s) ---")
    await traffic_generator(rate_per_second=10.0, duration_seconds=30.0, label="Phase1-Normal")

    # Phase 2: inject payment failure
    logger.info("--- Phase 2: Injecting 90% payment failure (60s) ---")
    await set_payment_error_rate(0.9)

    await traffic_generator(rate_per_second=15.0, duration_seconds=60.0, label="Phase2-PaymentFailing")

    order_h = await get_service_health(ORDER_SERVICE_URL, "order")
    pay_h = await get_service_health(PAYMENT_SERVICE_URL, "payment")
    logger.info(f"After injection — order.err={order_h['error_rate']:.3f} payment.err={pay_h['error_rate']:.3f}")

    # Phase 3: hold failure
    logger.info("--- Phase 3: Holding failure (60s) — diagnosis agent processing ---")
    await traffic_generator(rate_per_second=15.0, duration_seconds=60.0, label="Phase3-Hold")

    # Recovery
    logger.info("--- Recovery: Restoring payment service ---")
    await set_payment_error_rate(0.0)

    await asyncio.sleep(20)

    order_h = await get_service_health(ORDER_SERVICE_URL, "order")
    pay_h = await get_service_health(PAYMENT_SERVICE_URL, "payment")
    logger.info(f"After recovery — order.err={order_h['error_rate']:.3f} payment.err={pay_h['error_rate']:.3f}")

    _running = False
    stats_task.cancel()

    async with _lock:
        total = _sent
        errs = _errors
    logger.info("=" * 60)
    logger.info(f"Scenario 03 complete — {total} requests, {errs} errors ({errs/max(1,total)*100:.1f}%)")
    logger.info("Expected root cause: payment_service (degraded first)")
    logger.info("Expected victim: order_service (downstream)")
    logger.info("Expected fault_path: [payment_service, order_service]")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
