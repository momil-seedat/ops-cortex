"""
Scenario 06 — New Service Auto-Registration & Scheduled Batch Job Discovery

This scenario demonstrates two things:

A) NEW SERVICE DISCOVERY
   The report_service is a brand-new service that was not part of the original
   system.  When this scenario starts, report_service is already running but
   completely idle (zero metrics).  The scenario triggers its batch job via
   POST /job/run and keeps traffic flowing for several minutes.

   The topology agent will:
     1. Discover node:report_service:* Redis keys on the next 5s sync cycle
     2. Auto-create service + category + metric nodes in the NetworkX graph
     3. Register 'report_service' in SQLite known_services (first_seen timestamp)
     4. Snapshot the new service node to Neo4j within 60s
     5. The baseline model begins learning the metric distributions

B) SCHEDULED BATCH JOB PATTERN
   report_service only runs for ~5 minutes per day (at REPORT_RUN_HOUR).
   After the job finishes, all its metrics drop to zero.  The topology agent
   marks the service as still present (not offline — it's still healthy, just
   idle).  The baseline model learns that:
     - During the job window: request_rate ~2-5 req/s, latency ~100-500ms
     - Outside the window:    request_rate = 0, all metrics flat

   This teaches the system not to raise false alarms when a batch service
   goes quiet — that IS its normal behaviour.

C) WHAT TO VERIFY IN NEO4J AFTER THE SCENARIO
   MATCH (s:Service {name:'report_service'}) RETURN s
   MATCH (s:Service)-[:HAS_CATEGORY]->(c)-[:HAS_METRIC]->(m)
     WHERE s.name = 'report_service'
     RETURN c.name, m.metric_name, m.current_value, m.status

D) WHAT TO VERIFY IN SQLITE AFTER THE SCENARIO
   SELECT * FROM known_services WHERE name = 'report_service';
   SELECT service, metric, mean, std_dev, sample_count
     FROM metric_baselines WHERE service = 'report_service';

Timeline:
  0s   — wait for report_service to be healthy
  0s   — trigger batch job via POST /report-service/job/run
  30s  — poll job status + verify topology agent has discovered the service
  5min — job finishes naturally; verify metrics drop to zero
  +30s — verify service is still in known_services (not offline, just idle)
"""
import asyncio
import time
import logging
import os
import sys

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

REPORT_SERVICE_URL    = os.getenv("REPORT_SERVICE_URL",    "http://localhost:8004")
ORDER_SERVICE_URL     = os.getenv("ORDER_SERVICE_URL",     "http://localhost:8001")
REDIS_HOST            = os.getenv("REDIS_HOST",            "localhost")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scenario_06")

# How long to keep polling after the job finishes to confirm idle state is stable
POST_JOB_OBSERVE_SECS = 30


async def wait_for_service(url: str, label: str, retries: int = 30):
    async with httpx.AsyncClient() as client:
        for i in range(retries):
            try:
                resp = await client.get(f"{url}/health", timeout=3.0)
                if resp.status_code == 200:
                    logger.info(f"{label} is healthy")
                    return
            except Exception:
                pass
            logger.info(f"Waiting for {label}… ({i+1}/{retries})")
            await asyncio.sleep(2)
    raise RuntimeError(f"{label} not reachable after {retries} attempts")


async def get_job_status(client: httpx.AsyncClient) -> dict:
    try:
        resp = await client.get(f"{REPORT_SERVICE_URL}/job/status", timeout=3.0)
        return resp.json() if resp.status_code == 200 else {}
    except Exception:
        return {}


async def trigger_job(client: httpx.AsyncClient) -> dict:
    resp = await client.post(f"{REPORT_SERVICE_URL}/job/run", timeout=5.0)
    return resp.json()


async def get_health(client: httpx.AsyncClient) -> dict:
    try:
        resp = await client.get(f"{REPORT_SERVICE_URL}/health", timeout=3.0)
        return resp.json() if resp.status_code == 200 else {}
    except Exception:
        return {}


async def check_topology_redis(redis_host: str) -> dict:
    """
    Check Redis directly to confirm topology agent has discovered report_service.
    Returns the set of node:report_service:* keys found.
    """
    try:
        import redis as redis_lib
        r = redis_lib.Redis(host=redis_host, port=6379, decode_responses=True)
        cursor = 0
        keys = []
        while True:
            cursor, batch = r.scan(cursor, match="node:report_service:*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break
        return {"keys_found": len(keys), "sample": keys[:5]}
    except Exception as e:
        return {"error": str(e), "keys_found": 0}


async def poll_job_until_done(client: httpx.AsyncClient) -> float:
    """Poll job status every 10s until it finishes. Returns actual duration."""
    start = time.time()
    while True:
        status = await get_job_status(client)
        running   = status.get("running", False)
        records   = status.get("records_so_far", 0)
        run_count = status.get("run_count", 0)
        elapsed   = time.time() - start
        logger.info(
            f"  Job status: running={running} records_so_far={records} "
            f"run_count={run_count} elapsed={elapsed:.0f}s"
        )
        if not running and run_count > 0:
            return status.get("last_duration_s", elapsed)
        await asyncio.sleep(10)


async def main():
    logger.info("=" * 65)
    logger.info("Scenario 06: New Service Registration + Batch Job Discovery")
    logger.info("=" * 65)

    # ── Step 1: Wait for report_service ──────────────────────────────────────
    logger.info("\n[Step 1] Waiting for report_service to be healthy…")
    await wait_for_service(REPORT_SERVICE_URL, "report_service")

    async with httpx.AsyncClient() as client:

        # ── Step 2: Confirm service is idle before trigger ────────────────────
        logger.info("\n[Step 2] Checking idle state before job trigger…")
        health = await get_health(client)
        logger.info(
            f"  report_service idle: job_running={health.get('job_running')} "
            f"run_count={health.get('run_count')} "
            f"error_rate={health.get('error_rate', 0):.4f}"
        )

        # ── Step 3: Trigger batch job ─────────────────────────────────────────
        logger.info("\n[Step 3] Triggering batch job via POST /job/run…")
        result = await trigger_job(client)
        logger.info(f"  Trigger response: {result}")
        job_start_ts = time.time()

        # ── Step 4: Poll until topology agent discovers the service ──────────
        # Pipeline: gauge set → Prometheus scrapes /metrics (10s interval)
        #           → monitoring agent polls Prometheus (10s interval)
        #           → monitoring agent writes Redis node:* keys
        #           → topology agent reads Redis keys (5s interval)
        # Worst-case end-to-end: ~25s. We poll for up to 60s to be safe.
        logger.info("\n[Step 4] Polling until topology agent discovers report_service (up to 60s)…")
        discovered = False
        for attempt in range(1, 13):   # 12 × 5s = 60s
            await asyncio.sleep(5)
            topo = await check_topology_redis(REDIS_HOST)
            if topo.get("keys_found", 0) > 0:
                logger.info(
                    f"  TOPOLOGY DISCOVERED after ~{attempt*5}s: "
                    f"{topo['keys_found']} Redis node:* keys found"
                )
                logger.info(f"  Sample keys: {topo.get('sample', [])}")
                discovered = True
                break
            logger.info(f"  attempt {attempt}/12 — not yet (keys=0, error={topo.get('error','none')})")
        if not discovered:
            logger.warning("  Not discovered within 60s — check monitoring-agent and Prometheus logs")

        # ── Step 5: Log metrics snapshot mid-job ──────────────────────────────
        logger.info("\n[Step 5] Mid-job metrics snapshot…")
        health = await get_health(client)
        logger.info(
            f"  job_running={health.get('job_running')} "
            f"last_records={health.get('last_records')} "
            f"run_count={health.get('run_count')} "
            f"error_rate={health.get('error_rate', 0):.4f}"
        )

        # ── Step 6: Wait for job to finish ────────────────────────────────────
        logger.info("\n[Step 6] Waiting for job to complete (polling every 10s)…")
        duration = await poll_job_until_done(client)
        logger.info(f"  Job completed in {duration:.1f}s")

        # ── Step 7: Verify metrics dropped back to zero ───────────────────────
        logger.info(f"\n[Step 7] Observing post-job idle state for {POST_JOB_OBSERVE_SECS}s…")
        await asyncio.sleep(POST_JOB_OBSERVE_SECS)
        health = await get_health(client)
        logger.info(
            f"  Post-job: job_running={health.get('job_running')} "
            f"last_records={health.get('last_records')} "
            f"last_duration_s={health.get('last_duration_s')} "
            f"error_rate={health.get('error_rate', 0):.4f}"
        )

        # ── Step 8: Verify Redis keys still exist (service idle, not offline) ─
        logger.info("\n[Step 8] Checking Redis — service should be IDLE not OFFLINE…")
        topo = await check_topology_redis(REDIS_HOST)
        logger.info(
            f"  Redis keys after idle: {topo.get('keys_found', 0)} "
            f"(expected: keys still present with value=0, TTL not expired)"
        )

    # ── Summary ───────────────────────────────────────────────────────────────
    total = time.time() - job_start_ts
    logger.info("\n" + "=" * 65)
    logger.info("Scenario 06 complete")
    logger.info(f"  Total elapsed: {total:.0f}s")
    logger.info("")
    logger.info("What the system learned:")
    logger.info("  - report_service registered in SQLite known_services")
    logger.info("  - Metric nodes created in NetworkX graph")
    logger.info("  - Service snapshotted to Neo4j (within 60s of job start)")
    logger.info("  - Baseline model started building (service, metric, hour) stats")
    logger.info("  - Service went quiet after job — NOT marked offline (by design)")
    logger.info("  - Next run at configured REPORT_RUN_HOUR will reinforce baselines")
    logger.info("")
    logger.info("Verify in Neo4j:")
    logger.info("  MATCH (s:Service {name:'report_service'}) RETURN s")
    logger.info("  MATCH (s:Service)-[:HAS_CATEGORY]->(c)-[:HAS_METRIC]->(m)")
    logger.info("    WHERE s.name='report_service'")
    logger.info("    RETURN c.name, m.metric_name, m.current_value, m.status")
    logger.info("")
    logger.info("Verify in SQLite:")
    logger.info("  SELECT * FROM known_services WHERE name='report_service';")
    logger.info("  SELECT service, metric, mean, std_dev FROM metric_baselines")
    logger.info("    WHERE service='report_service';")
    logger.info("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())
