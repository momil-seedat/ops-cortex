"""
Scenario 05 — Thread-Blocked DB / Cascade to Payment

Fault: order_service DB connection pool is exhausted by slow blocking queries
(simulated via artificial_delay_ms holding each connection open). Once all 20
pool slots are held, new requests cannot acquire a connection and fail with a
DB error. order_service error_rate climbs. Because order calls payment on every
request, payment_service starts receiving 502s from order and its own
error_rate rises too — a real downstream cascade.

Why this avoids noise suppression:
  Only latency_p99 for order_service has accumulated noise fingerprints from
  previous runs. This scenario drives error_rate and db_connection_count —
  metrics with ZERO noise history — so alerts pass straight through to the
  diagnosis buffer without suppression.

Fault anatomy:
  Phase 1: 20s warm-up at normal rate — build baseline, verify clean state.
  Phase 2: Inject 900ms delay per request on order_service. With 10 req/s
           and pool_size=20, all 20 connections are held within ~2s.
           New requests fail to acquire → DB error → error_rate climbs on
           order_service. payment_service starts seeing 502s from order.
  Phase 3: Hold 90s — both error_rates peak, db_connection_count saturates.
           Diagnosis agent fires: root=order_service (DB exhaustion),
           payment_service is downstream victim.
  Phase 4: Reset delay → connections drain → both services recover.

Expected diagnosis:
  root_cause_service : order_service
  fault_path         : [order_service, payment_service]
  key signals        : db_connection_count near pool ceiling (20),
                       order error_rate > 0.20 (red),
                       payment error_rate rising as downstream victim,
                       latency_p99 stays high but error_rate is the trigger
"""
import asyncio
import time
import logging
import os
import sys

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

ORDER_SERVICE_URL     = os.getenv("ORDER_SERVICE_URL",     "http://localhost:8001")
PAYMENT_SERVICE_URL   = os.getenv("PAYMENT_SERVICE_URL",   "http://localhost:8002")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://localhost:8003")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scenario_05")

_running = True
_sent    = 0
_errors  = 0
_lock    = asyncio.Lock()

# Delay long enough to hold DB connections open and exhaust the pool.
# pool_size=20, rate=10 req/s → all slots filled in 2s at 2000ms delay.
BLOCK_DELAY_MS = 2000


async def send_order(client: httpx.AsyncClient) -> bool:
    global _sent, _errors
    try:
        resp = await client.post(
            f"{ORDER_SERVICE_URL}/order",
            json={"item": "bulk_widget", "quantity": 1, "amount": 9.99},
            timeout=15.0,
        )
        async with _lock:
            _sent += 1
            if resp.status_code >= 500:
                _errors += 1
        return resp.status_code == 200
    except Exception:
        async with _lock:
            _sent += 1
            _errors += 1
        return False


async def traffic_generator(rate_per_second: float, duration_seconds: float, label: str):
    interval  = 1.0 / rate_per_second
    end_time  = time.time() + duration_seconds
    dispatched = 0
    logger.info(f"[{label}] {rate_per_second} req/s for {duration_seconds}s")
    async with httpx.AsyncClient() as client:
        while time.time() < end_time and _running:
            asyncio.create_task(send_order(client))
            dispatched += 1
            await asyncio.sleep(interval)
    logger.info(f"[{label}] done — {dispatched} dispatched")


async def set_order_control(delay_ms: int, error_rate: float = 0.0):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{ORDER_SERVICE_URL}/control",
            json={"artificial_delay_ms": delay_ms, "extra_error_rate": error_rate},
            timeout=5.0,
        )
        logger.info(f"order_service control: delay={delay_ms}ms error_rate={error_rate} → {resp.status_code}")


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
                logger.info(f"Reset {name}")
            except Exception as e:
                logger.warning(f"Reset {name} failed: {e}")


async def print_stats_loop():
    while _running:
        await asyncio.sleep(10)
        async with _lock:
            total = _sent
            errs  = _errors
        try:
            async with httpx.AsyncClient() as client:
                oh = (await client.get(f"{ORDER_SERVICE_URL}/health",   timeout=3.0)).json()
                ph = (await client.get(f"{PAYMENT_SERVICE_URL}/health", timeout=3.0)).json()
            logger.info(
                f"Stats — sent={total} errors={errs} err_rate={errs/max(1,total):.3f} | "
                f"order.err={oh.get('error_rate',0):.3f} "
                f"payment.err={ph.get('error_rate',0):.3f} "
                f"order.db_conn={oh.get('db_connections_active',0)}"
            )
        except Exception:
            logger.info(f"Stats — sent={total} errors={errs}")


async def wait_for_service(url: str, label: str, retries: int = 30):
    async with httpx.AsyncClient() as client:
        for i in range(retries):
            try:
                r = await client.get(f"{url}/health", timeout=3.0)
                if r.status_code == 200:
                    logger.info(f"{label} is up")
                    return
            except Exception:
                pass
            logger.info(f"Waiting for {label}… ({i+1}/{retries})")
            await asyncio.sleep(2)
    raise RuntimeError(f"{label} not reachable")


async def main():
    global _running

    logger.info("=" * 60)
    logger.info("Scenario 05: Thread-Blocked DB → order + payment cascade")
    logger.info("Fault signal: error_rate + db_connection_count (clean noise buckets)")
    logger.info("=" * 60)

    await wait_for_service(ORDER_SERVICE_URL,     "order_service")
    await wait_for_service(PAYMENT_SERVICE_URL,   "payment_service")
    await wait_for_service(INVENTORY_SERVICE_URL, "inventory_service")

    await reset_controls()

    stats_task = asyncio.create_task(print_stats_loop())

    # Phase 1: warm-up — establish baseline, confirm clean error_rate
    logger.info("--- Phase 1: Warm-up (10 req/s, 20s) ---")
    await traffic_generator(10.0, 20.0, label="Phase1-Warmup")
    logger.info("Phase 1 complete")

    # Phase 2: inject DB block — 2000ms delay holds each connection open
    # pool_size=20 @ 10 req/s → pool exhausted in ~2s
    # requests start failing → error_rate on order climbs above red (0.20)
    # payment sees 502s from order → payment error_rate rises too
    logger.info(f"--- Phase 2: Injecting {BLOCK_DELAY_MS}ms DB block (10 req/s, 30s) ---")
    logger.info("    DB connections will saturate pool → order errors → payment cascade")
    await set_order_control(delay_ms=BLOCK_DELAY_MS)
    await traffic_generator(10.0, 30.0, label="Phase2-DBBlock")
    logger.info("Phase 2 complete — error_rate and db_connection_count should be red")

    # Phase 3: hold — give diagnosis agent time to see sustained breach
    logger.info("--- Phase 3: Hold fault (10 req/s, 90s) — waiting for diagnosis ---")
    await traffic_generator(10.0, 90.0, label="Phase3-Hold")
    logger.info("Phase 3 complete")

    # Phase 4: recovery
    logger.info("--- Phase 4: Recovery ---")
    await reset_controls()
    await traffic_generator(10.0, 20.0, label="Phase4-Recovery")

    _running = False
    stats_task.cancel()

    async with _lock:
        total = _sent
        errs  = _errors

    logger.info("=" * 60)
    logger.info(f"Scenario 05 complete — {total} requests, {errs} errors ({errs/max(1,total)*100:.1f}%)")
    logger.info("")
    logger.info("Expected diagnosis:")
    logger.info("  root_cause_service : order_service")
    logger.info("  fault_path         : [order_service, payment_service]")
    logger.info("  key signals        : db_connection_count near 20 (pool ceiling)")
    logger.info("                       order error_rate > 0.20 (red)")
    logger.info("                       payment error_rate rising (downstream)")
    logger.info("  NOT suppressed     : error_rate + db_connection_count have")
    logger.info("                       zero noise history at this hour/dow")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
