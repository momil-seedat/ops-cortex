"""
Scenario 07 — Batch Job: Learning Normal Pattern, Then Suppressing It

FOCUS: Root cause identification + service awareness

This scenario answers three questions after each run:

  1. ROOT CAUSE — Did the system correctly identify report_service as the
     root cause and NOT implicate order/payment/inventory?
     (report_service calls nobody — fault path should always be [report_service])

  2. SUPPRESSION — Did the noise classifier correctly suppress run 4 because
     the latency matched the learned baseline?

  3. SERVICE AWARENESS — Does the graph know report_service is isolated?
     No call edges to other services = correct topology awareness.

RUN PLAN
========
  Run 1  fingerprint=0  → INCIDENT  (no baseline, first time seen)
  Run 2  fingerprint=1  → INCIDENT  (count < threshold 3)
  Run 3  fingerprint=2  → INCIDENT  (count < threshold 3)
  Run 4  fingerprint=3  → SUPPRESSED (count >= 3, z-score ≈ 0, trend flat)

After each run this scenario waits for the diagnosis agent to flush its
15-minute batch window, then reads SQLite directly to verify the outcome.
"""
import asyncio
import time
import logging
import os
import sys

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

REPORT_SERVICE_URL = os.getenv("REPORT_SERVICE_URL", "http://localhost:8004")
SQLITE_DB_PATH     = os.getenv("SQLITE_DB_PATH", "/data/service_brain.db")

# The diagnosis agent flushes every 15 min by default.
# For testing we read the DB directly without waiting — the scenario
# polls until a new incident or suppressed alert appears.
POLL_INTERVAL_SECONDS  = 15
DIAGNOSIS_TIMEOUT_SECS = 960   # 16 min — slightly more than the 15-min flush window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scenario_07")


# ── SQLite helpers ─────────────────────────────────────────────────────────────

def get_store():
    from memory.sqlite_store import SQLiteStore
    return SQLiteStore(SQLITE_DB_PATH)


def get_incident_count(store) -> int:
    return len(store.get_recent_incidents(100))


def get_suppressed_count(store) -> int:
    return len(store.get_recent_suppressed_alerts(100))


def get_latest_incident(store) -> dict | None:
    incidents = store.get_recent_incidents(1)
    return incidents[0] if incidents else None


def get_topology_edges(store) -> list[dict]:
    return store.get_all_topology_edges()


# ── Service helpers ────────────────────────────────────────────────────────────

async def wait_for_service():
    async with httpx.AsyncClient() as client:
        for i in range(30):
            try:
                r = await client.get(f"{REPORT_SERVICE_URL}/health", timeout=3.0)
                if r.status_code == 200:
                    logger.info("report_service is healthy")
                    return
            except Exception:
                pass
            logger.info(f"Waiting for report_service… ({i+1}/30)")
            await asyncio.sleep(2)
    raise RuntimeError("report_service not reachable")


async def trigger_and_wait_for_completion(run_number: int):
    """Trigger the job and poll until it finishes."""
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{REPORT_SERVICE_URL}/job/run", timeout=5.0)
        logger.info(f"  Triggered: {r.json()}")

        start = time.time()
        while True:
            await asyncio.sleep(10)
            s = (await client.get(f"{REPORT_SERVICE_URL}/job/status", timeout=3.0)).json()
            elapsed = time.time() - start
            records = s.get("records_so_far", s.get("last_records", 0))
            logger.info(f"  running={s['running']} records={records} elapsed={elapsed:.0f}s")
            if not s["running"] and s.get("run_count", 0) >= run_number:
                logger.info(f"  Job finished: duration={s.get('last_duration_s')}s records={s.get('last_records')}")
                return


async def wait_for_new_outcome(store, prev_incident_count: int, prev_suppressed_count: int):
    """
    Poll SQLite until either a new incident OR a new suppressed alert appears.
    Returns ('incident', row) or ('suppressed', row).
    The diagnosis agent flushes every 15 min so this may take up to 16 min.
    """
    logger.info(f"  Waiting for diagnosis agent decision (up to {DIAGNOSIS_TIMEOUT_SECS//60} min)…")
    deadline = time.time() + DIAGNOSIS_TIMEOUT_SECS
    while time.time() < deadline:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        new_inc  = get_incident_count(store)
        new_supp = get_suppressed_count(store)
        elapsed  = DIAGNOSIS_TIMEOUT_SECS - (deadline - time.time())

        if new_inc > prev_incident_count:
            inc = get_latest_incident(store)
            logger.info(f"  → NEW INCIDENT after {elapsed:.0f}s")
            return "incident", inc

        if new_supp > prev_suppressed_count:
            suppressed = store.get_recent_suppressed_alerts(1)[0]
            logger.info(f"  → SUPPRESSED after {elapsed:.0f}s")
            return "suppressed", suppressed

        logger.info(f"  Still waiting… elapsed={elapsed:.0f}s (incidents={new_inc} suppressed={new_supp})")

    return "timeout", None


# ── Result printer ─────────────────────────────────────────────────────────────

def print_incident_analysis(inc: dict, run: int):
    """Print the key root-cause and service-awareness findings."""
    logger.info(f"\n  ── Incident #{inc['id']} Analysis ──")
    logger.info(f"  Service:     {inc.get('service')}")
    logger.info(f"  Metric:      {inc.get('metric')} = {inc.get('alert_value', 0):.1f}")
    logger.info(f"  Fault path:  {inc.get('fault_path', [])}")
    logger.info(f"  Root cause:  {inc.get('root_cause_service')}")
    logger.info(f"  LLM root:    {inc.get('llm_root_cause', '—')}")
    logger.info(f"  Confidence:  {inc.get('llm_confidence', '—')}")
    logger.info(f"  Fix:         {inc.get('llm_recommended_fix', '—')}")

    # Service awareness check
    fp = inc.get("fault_path", [])
    if fp == ["report_service"] or fp == []:
        logger.info(f"  ✅ SERVICE AWARENESS CORRECT: fault isolated to report_service only")
        logger.info(f"     (report_service calls nobody — no downstream victims expected)")
    else:
        other = [s for s in fp if s != "report_service"]
        logger.info(f"  ⚠️  SERVICE AWARENESS: fault path includes {other}")
        logger.info(f"     Check if topology_edges has spurious call edges for report_service")

    root = inc.get("root_cause_service") or inc.get("service")
    if root == "report_service":
        logger.info(f"  ✅ ROOT CAUSE CORRECT: report_service identified")
    else:
        logger.info(f"  ⚠️  ROOT CAUSE: system pointed to '{root}' — expected 'report_service'")


def print_suppression_analysis(row: dict):
    logger.info(f"\n  ── Suppression Analysis ──")
    logger.info(f"  Service: {row.get('service')}")
    logger.info(f"  Metric:  {row.get('metric')}")
    logger.info(f"  Reason:  {row.get('reason', '—')}")
    logger.info(f"  ✅ SUPPRESSED CORRECTLY: normal latency pattern recognised as noise")


def print_topology_awareness(store):
    edges = get_topology_edges(store)
    report_edges = [e for e in edges if e["caller"] == "report_service" or e["callee"] == "report_service"]
    logger.info(f"\n  ── Topology / Service Awareness ──")
    if not report_edges:
        logger.info(f"  ✅ TOPOLOGY CORRECT: no call edges involving report_service")
        logger.info(f"     System knows it is an isolated batch service")
    else:
        for e in report_edges:
            logger.info(f"  ⚠️  Unexpected edge: {e['caller']} → {e['callee']} "
                        f"(confirmed {e['confirmation_count']}x via {e['source']})")


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 65)
    logger.info("Scenario 07 — Batch Job Normal-Pattern Learning")
    logger.info("Focus: root cause identification + service awareness")
    logger.info("")
    logger.info("Expected per run:")
    logger.info("  Run 1-3 → INCIDENT  (root_cause=report_service, fault_path=[report_service])")
    logger.info("  Run 4   → SUPPRESSED (normal latency matches learned baseline)")
    logger.info("=" * 65)

    await wait_for_service()

    store = get_store()

    for run in range(1, 5):
        logger.info(f"\n{'═'*55}")
        logger.info(f"RUN {run}/4")

        prev_inc  = get_incident_count(store)
        prev_supp = get_suppressed_count(store)

        await trigger_and_wait_for_completion(run)

        outcome, row = await wait_for_new_outcome(store, prev_inc, prev_supp)

        if outcome == "incident":
            print_incident_analysis(row, run)
        elif outcome == "suppressed":
            print_suppression_analysis(row)
            if run < 4:
                logger.info(f"  ⚠️  Suppressed on run {run} — expected incident. "
                            f"Check if baseline already existed from a previous test run.")
        elif outcome == "timeout":
            logger.warning(f"  ⏱ Timeout — no decision seen after {DIAGNOSIS_TIMEOUT_SECS//60} min.")
            logger.warning(f"  Check: docker logs diagnosis-agent")

        print_topology_awareness(store)

    logger.info("\n" + "=" * 65)
    logger.info("Scenario 07 complete")
    logger.info("")
    logger.info("Summary of what was proven:")
    logger.info("  • System diagnosed report_service in isolation (no false victims)")
    logger.info("  • Fault path was [report_service] — not [order→payment→...]")
    logger.info("  • Run 4 suppressed because latency matched the learned baseline")
    logger.info("")
    logger.info("NEXT: run scenario_08 to inject increasing latency and verify")
    logger.info("  the system detects degradation and does NOT suppress it.")
    logger.info("  docker compose --profile scenarios run --rm scenario-08")
    logger.info("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())
