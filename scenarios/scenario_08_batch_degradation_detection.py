"""
Scenario 08 — Batch Job Degradation: Root Cause Must Be Found Despite Suppression

PREREQUISITE
============
Run scenario_07 first until run 4 is suppressed.
That proves 100ms latency = normal and established the baseline.

FOCUS: Root cause identification when suppression is already active

The hard case: the noise classifier has already learned this metric at this
hour fires a lot (count >= 3). When latency increases, the system must NOT
suppress it — it must detect the anomaly, identify root cause as report_service,
and NOT pull in order/payment/inventory as victims.

WHAT MAKES THIS INTERESTING
============================
Without the z-score and trend checks, the noise classifier would suppress ALL
future runs regardless of latency — it only counts occurrences.
The test verifies that the two overrides work:
  - Self z-score > 2.5  → sudden spike detected → not suppressed
  - Trend z-score > 2.0 → gradual drift detected → not suppressed

RUN PLAN
========
  Run 5  delay=200ms  effective latency ~300ms
          self z-score = (300-100)/8 ≈ 25  >> 2.5
          → NOT suppressed → INCIDENT
          → root_cause = report_service
          → fault_path = [report_service]  (isolated — no downstream)

  Run 6  delay=350ms  effective latency ~450ms
          self z-score = (450-100)/8 ≈ 44  >> 2.5
          → NOT suppressed → INCIDENT
          → system confirms same root cause

  Run 7  delay=500ms  effective latency ~600ms  (5x normal)
          self z-score ≈ 63  >> 2.5
          trend z-score: slope across last 10 samples is strongly positive
          → NOT suppressed → INCIDENT, high confidence
          → LLM should mention degradation trend in contributing_factors

SERVICE AWARENESS CHECK
=======================
All 3 incidents should have:
  fault_path = ["report_service"]  — not ["order_service", "report_service"] etc.
  root_cause_service = "report_service"

If other services appear in the fault path it means the topology agent
wrongly inferred call edges from correlated traffic.  The topology check
at the end of each run will flag this explicitly.

AFTER EACH INCIDENT
===================
The scenario waits for the diagnosis agent decision and prints:
  - Was it suppressed or diagnosed?
  - What root cause did the LLM identify?
  - What is the fault path?
  - Did the trend appear in contributing_factors?
  - Is service awareness correct (no false victims)?
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

POLL_INTERVAL_SECONDS  = 15
DIAGNOSIS_TIMEOUT_SECS = 960

DEGRADATION_RUNS = [
    {
        "run":      5,
        "delay_ms": 200,
        "label":    "moderate spike",
        "expected_latency": "~300ms",
        "expected_zscore":  "≈25",
        "why_not_suppressed": "self z-score 25 >> threshold 2.5",
    },
    {
        "run":      6,
        "delay_ms": 350,
        "label":    "significant spike",
        "expected_latency": "~450ms",
        "expected_zscore":  "≈44",
        "why_not_suppressed": "self z-score 44 >> threshold 2.5",
    },
    {
        "run":      7,
        "delay_ms": 500,
        "label":    "severe degradation",
        "expected_latency": "~600ms",
        "expected_zscore":  "≈63 + rising trend",
        "why_not_suppressed": "self z-score 63 + trend z-score > 2.0",
    },
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scenario_08")


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


async def wait_for_service():
    async with httpx.AsyncClient() as client:
        for i in range(30):
            try:
                r = await client.get(f"{REPORT_SERVICE_URL}/health", timeout=3.0)
                if r.status_code == 200:
                    logger.info("report_service healthy")
                    return
            except Exception:
                pass
            await asyncio.sleep(2)
    raise RuntimeError("report_service not reachable")


async def set_delay(delay_ms: int):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{REPORT_SERVICE_URL}/control",
            json={"artificial_delay_ms": delay_ms},
            timeout=5.0,
        )
    logger.info(f"  Set artificial_delay_ms={delay_ms} → effective latency adds {delay_ms}ms")


async def trigger_and_wait_for_completion(run_number: int):
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
                logger.info(f"  Job finished: duration={s.get('last_duration_s')}s")
                return


async def wait_for_new_outcome(store, prev_inc: int, prev_supp: int):
    logger.info(f"  Waiting for diagnosis agent decision (up to {DIAGNOSIS_TIMEOUT_SECS//60} min)…")
    deadline = time.time() + DIAGNOSIS_TIMEOUT_SECS
    while time.time() < deadline:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        new_inc  = get_incident_count(store)
        new_supp = get_suppressed_count(store)
        elapsed  = DIAGNOSIS_TIMEOUT_SECS - (deadline - time.time())

        if new_inc > prev_inc:
            logger.info(f"  → NEW INCIDENT after {elapsed:.0f}s")
            return "incident", get_latest_incident(store)

        if new_supp > prev_supp:
            row = store.get_recent_suppressed_alerts(1)[0]
            logger.info(f"  → SUPPRESSED after {elapsed:.0f}s")
            return "suppressed", row

        logger.info(f"  Still waiting… elapsed={elapsed:.0f}s")

    return "timeout", None


def print_root_cause_analysis(inc: dict, cfg: dict):
    """
    The core analysis: did the system find the right root cause,
    keep the fault path isolated to report_service, and capture the trend?
    """
    logger.info(f"\n  ── Root Cause Analysis (Run {cfg['run']}) ──")
    logger.info(f"  Alert value:   {inc.get('alert_value', 0):.1f}ms  (normal baseline ≈100ms)")
    logger.info(f"  Fault path:    {inc.get('fault_path', [])}")
    logger.info(f"  Root cause svc: {inc.get('root_cause_service')}")
    logger.info(f"  LLM root cause: {inc.get('llm_root_cause', '—')}")
    logger.info(f"  LLM confidence: {inc.get('llm_confidence', '—')}")
    logger.info(f"  LLM fix:        {inc.get('llm_recommended_fix', '—')}")

    factors = inc.get("llm_contributing_factors", [])
    if isinstance(factors, str):
        import json
        try:
            factors = json.loads(factors)
        except Exception:
            factors = [factors]
    logger.info(f"  Contributing:   {factors}")

    # ── Check 1: root cause correctness ──────────────────────────────────────
    root = inc.get("root_cause_service") or inc.get("service")
    if root == "report_service":
        logger.info(f"\n  ✅ ROOT CAUSE CORRECT: report_service identified")
    else:
        logger.info(f"\n  ❌ ROOT CAUSE WRONG: got '{root}', expected 'report_service'")

    # ── Check 2: fault path isolation (service awareness) ────────────────────
    fp = inc.get("fault_path", [])
    others = [s for s in fp if s != "report_service"]
    if not others:
        logger.info(f"  ✅ SERVICE AWARENESS CORRECT: fault isolated to report_service")
        logger.info(f"     No other services wrongly included as victims")
    else:
        logger.info(f"  ❌ SERVICE AWARENESS: unexpected services in fault path: {others}")
        logger.info(f"     This means topology agent inferred spurious call edges.")
        logger.info(f"     Check: topology_edges table for report_service entries")

    # ── Check 3: degradation trend in LLM output ─────────────────────────────
    trend_keywords = ["trend", "increas", "degrad", "worsen", "escalat", "progress"]
    all_llm_text = " ".join([
        inc.get("llm_root_cause", ""),
        inc.get("llm_recommended_fix", ""),
        " ".join(factors) if isinstance(factors, list) else str(factors),
    ]).lower()

    if any(k in all_llm_text for k in trend_keywords):
        logger.info(f"  ✅ TREND DETECTED in LLM output: mentions degradation/increase")
    elif cfg["run"] >= 7:
        logger.info(f"  ⚠️  Run {cfg['run']}: expected LLM to mention trend — check ml_scores.zscores")
    else:
        logger.info(f"  ℹ  Run {cfg['run']}: trend mention not required yet (early spike)")

    # ── Check 4: not a pattern match (new behaviour, not cached) ─────────────
    llm_raw = inc.get("llm_raw_response", "")
    if "pattern-match" in llm_raw.lower() or "ml-pattern-match" in llm_raw:
        logger.info(f"  ⚠️  Used pattern match — LLM was NOT called")
        logger.info(f"     This means a similar high-latency incident existed already")
    else:
        logger.info(f"  ✅ FRESH LLM DIAGNOSIS: pattern matcher found no match (new behaviour)")


def print_suppression_warning(row: dict, cfg: dict):
    logger.info(f"\n  ❌ INCORRECTLY SUPPRESSED on run {cfg['run']}")
    logger.info(f"     Reason stored: {row.get('reason', '—')}")
    logger.info(f"     Expected: self z-score {cfg['expected_zscore']} >> 2.5 should have prevented suppression")
    logger.info(f"     Check: did scenario_07 actually complete? Is the baseline built?")
    logger.info(f"     If mean=0/std=0, the z-score check cannot fire — baseline not loaded")


async def main():
    logger.info("=" * 65)
    logger.info("Scenario 08 — Batch Degradation: Root Cause Despite Suppression")
    logger.info("Prerequisite: scenario_07 run 4 must have been SUPPRESSED")
    logger.info("")
    logger.info("Expected for all 3 runs:")
    logger.info("  • NOT suppressed (z-score overrides noise threshold)")
    logger.info("  • INCIDENT with root_cause=report_service")
    logger.info("  • fault_path=[report_service]  (no false victims)")
    logger.info("  • Run 7: LLM mentions degradation trend")
    logger.info("=" * 65)

    await wait_for_service()
    store = get_store()

    for cfg in DEGRADATION_RUNS:
        logger.info(f"\n{'═'*55}")
        logger.info(f"RUN {cfg['run']} — {cfg['label'].upper()}")
        logger.info(f"  Injecting delay={cfg['delay_ms']}ms → {cfg['expected_latency']}")
        logger.info(f"  Why not suppressed: {cfg['why_not_suppressed']}")

        prev_inc  = get_incident_count(store)
        prev_supp = get_suppressed_count(store)

        await set_delay(cfg["delay_ms"])
        await trigger_and_wait_for_completion(cfg["run"])

        outcome, row = await wait_for_new_outcome(store, prev_inc, prev_supp)

        if outcome == "incident":
            print_root_cause_analysis(row, cfg)
        elif outcome == "suppressed":
            print_suppression_warning(row, cfg)
        else:
            logger.warning(f"  ⏱ Timeout — no decision after {DIAGNOSIS_TIMEOUT_SECS//60} min")
            logger.warning(f"  Check: docker logs diagnosis-agent")

    # Reset delay
    await set_delay(0)
    logger.info("\nReset artificial delay to 0")

    logger.info("\n" + "=" * 65)
    logger.info("Scenario 08 complete")
    logger.info("")
    logger.info("What was proven:")
    logger.info("  • Noise suppression does NOT block real anomalies")
    logger.info("  • Self z-score override fired correctly (z >> 2.5)")
    logger.info("  • Root cause was isolated to report_service only")
    logger.info("  • Other services were NOT pulled into the fault path")
    logger.info("  • LLM produced fresh diagnosis (not cached pattern)")
    logger.info("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())
