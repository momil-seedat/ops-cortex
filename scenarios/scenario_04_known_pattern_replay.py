"""
Scenario 04 — Known-Pattern Replay (ML-only diagnosis, no LLM)

Prerequisite: Scenario 01 must have run at least once so that the SQLite
incidents table already contains a resolved order_service/latency_p99
incident caused by DB connection pool exhaustion.

What this scenario does:
  Phase 1: 30s normal traffic at 10 req/s — builds / refreshes baseline.
  Phase 2: 60s surge to 150 req/s — same fault signature as Scenario 01:
           DB pool (size 20) exhausts, latency_p99 spikes, error rate climbs.
  Phase 3: 30s hold — diagnosis agent fires its batch flush.

Expected outcome:
  PatternMatcher finds ≥2 prior incidents for (order_service, latency_p99)
  with a matching alert value (±40%).  The diagnosis agent logs:
    "Pattern match (N prior occurrence(s)) — skipping LLM for order_service/latency_p99"
  and writes an incident whose llm_raw_response starts with "[ml-pattern-match:".
  No Anthropic / OpenAI API call is made.

Verification: after the run, check the incidents table:
  SELECT id, llm_raw_response, llm_confidence
  FROM incidents ORDER BY id DESC LIMIT 3;
  The newest row should have llm_raw_response like '[ml-pattern-match: ...'.
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
logger = logging.getLogger("scenario_04")

_running = True
_sent    = 0
_errors  = 0
_lock    = asyncio.Lock()


async def send_order(client: httpx.AsyncClient) -> bool:
    global _sent, _errors
    try:
        resp = await client.post(
            f"{ORDER_SERVICE_URL}/order",
            json={"item": "widget", "quantity": 1, "amount": 9.99},
            timeout=5.0,
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
    interval = 1.0 / rate_per_second
    end_time = time.time() + duration_seconds
    dispatched = 0
    logger.info(f"[{label}] {rate_per_second} req/s for {duration_seconds}s")
    async with httpx.AsyncClient() as client:
        while time.time() < end_time and _running:
            asyncio.create_task(send_order(client))
            dispatched += 1
            await asyncio.sleep(interval)
    logger.info(f"[{label}] done — {dispatched} requests dispatched")


async def print_stats_loop():
    while _running:
        await asyncio.sleep(10)
        async with _lock:
            total = _sent
            errs  = _errors
        logger.info(f"Stats — total={total} errors={errs} error_rate={errs/max(1,total):.3f}")


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


async def check_prerequisite():
    """Warn if no prior Scenario-01 incidents exist in SQLite."""
    try:
        import config
        from memory.sqlite_store import SQLiteStore
        store = SQLiteStore(os.getenv("SQLITE_DB_PATH", config.SQLITE_DB_PATH))
        incidents = store.get_recent_incidents_for_service("order_service", limit=10)
        surge_incidents = [
            i for i in incidents
            if i.get("metric") in ("latency_p99", "error_rate")
            and i.get("llm_root_cause")
        ]
        if len(surge_incidents) < 2:
            logger.warning(
                f"Only {len(surge_incidents)} prior diagnosed incident(s) found for order_service. "
                "PatternMatcher needs ≥2 matches to skip the LLM. "
                "Run scenario_01 at least twice before this scenario for a clean ML-only result."
            )
        else:
            logger.info(
                f"Prerequisite OK — {len(surge_incidents)} prior order_service incident(s) found. "
                "PatternMatcher should skip the LLM this run."
            )
    except Exception as e:
        logger.warning(f"Prerequisite check failed (non-fatal): {e}")


async def main():
    global _running

    logger.info("=" * 60)
    logger.info("Scenario 04: Known-Pattern Replay — ML diagnosis, no LLM")
    logger.info("=" * 60)

    await wait_for_service(ORDER_SERVICE_URL,     "order_service")
    await wait_for_service(PAYMENT_SERVICE_URL,   "payment_service")
    await wait_for_service(INVENTORY_SERVICE_URL, "inventory_service")

    await check_prerequisite()
    await reset_controls()

    stats_task = asyncio.create_task(print_stats_loop())

    # Phase 1: baseline
    logger.info("--- Phase 1: Normal traffic (10 req/s, 30s) ---")
    await traffic_generator(rate_per_second=10.0, duration_seconds=30.0, label="Phase1-Normal")
    logger.info("Phase 1 complete")

    # Phase 2: same surge as Scenario 01 — identical fault signature
    logger.info("--- Phase 2: Surge (150 req/s, 60s) — same fault as Scenario 01 ---")
    await traffic_generator(rate_per_second=150.0, duration_seconds=60.0, label="Phase2-Surge")
    logger.info("Phase 2 complete — latency_p99 should be spiking")

    # Phase 3: hold long enough for the diagnosis agent batch window to fire
    logger.info("--- Phase 3: Hold (150 req/s, 30s) — waiting for ML diagnosis ---")
    await traffic_generator(rate_per_second=150.0, duration_seconds=30.0, label="Phase3-Hold")

    _running = False
    stats_task.cancel()

    async with _lock:
        total = _sent
        errs  = _errors

    logger.info("=" * 60)
    logger.info(f"Scenario 04 complete — {total} requests, {errs} errors ({errs/max(1,total)*100:.1f}%)")
    logger.info("Expected: diagnosis agent log contains 'Pattern match' and 'skipping LLM'")
    logger.info("Verify:   SELECT llm_raw_response FROM incidents ORDER BY id DESC LIMIT 1;")
    logger.info("          Should start with '[ml-pattern-match:'")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
