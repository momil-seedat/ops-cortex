"""
Diagnosis agent — consumes the Redis alert stream, runs noise suppression,
and batches alerts into a single LLM call every LLM_BATCH_INTERVAL_SECONDS.

Alert flow:
  stream read → noise check (suppress or buffer)
  every 15 min → flush buffer
    → pattern_matcher checks past incidents first (no LLM cost for known faults)
    → if no high-confidence pattern match → one LLM call
    → one incident per unique (service, metric) pair in the batch
"""
import asyncio
import json
import logging
import os
import sys
import time

sys.path.insert(0, "/app")
import config
from memory.redis_store import RedisStore
from memory.sqlite_store import SQLiteStore
from memory.neo4j_store import Neo4jStore
from memory.graph_store import get_graph_store
from ml.noise_classifier import NoiseClassifier
from ml.baseline_model import BaselineModel
from ml.anomaly_detector import AnomalyDetector
from ml.communication_model import CommunicationModel
from ml.pattern_matcher import PatternMatcher
from agents.llm_analyzer import analyze as llm_analyze

NEO4J_URI      = os.getenv("NEO4J_URI",      config.NEO4J_URI)
NEO4J_USER     = os.getenv("NEO4J_USER",     config.NEO4J_USER)
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", config.NEO4J_PASSWORD)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("diagnosis_agent")

REDIS_HOST  = os.getenv("REDIS_HOST",   config.REDIS_HOST)
REDIS_PORT  = int(os.getenv("REDIS_PORT", str(config.REDIS_PORT)))
SQLITE_PATH = os.getenv("SQLITE_DB_PATH", config.SQLITE_DB_PATH)

CONSUMER_GROUP   = config.ALERT_STREAM_CONSUMER_GROUP
CONSUMER_NAME    = "diagnosis-01"
RECENT_ACTION_WINDOW_SECONDS = 1800   # 30 minutes

# One LLM call per this interval regardless of how many alerts arrive
LLM_BATCH_INTERVAL_SECONDS = int(os.getenv("LLM_BATCH_INTERVAL_SECONDS", "900"))  # 15 min

graph_store = get_graph_store()


# ── Context builders ───────────────────────────────────────────────────────────

def _build_timeseries_context(redis_store: RedisStore, services: list[str]) -> dict:
    """
    Build timeseries context from Redis for all discovered (service, category, metric)
    triples — no hardcoded category or metric lists.
    """
    ctx: dict = {}
    all_triples = redis_store.discover_node_keys()
    svc_set = set(services)
    for service, category, metric in all_triples:
        if service not in svc_set:
            continue
        entries = redis_store.get_timeseries(service, category, metric, count=config.REDIS_CONTEXT_WINDOW)
        if entries:
            ctx.setdefault(service, {})[f"{category}/{metric}"] = entries
    return ctx


def _build_actions_context(redis_store: RedisStore, services: list[str]) -> list[dict]:
    """Collect recent operational events for all known services."""
    actions = []
    for service in services:
        actions.extend(redis_store.get_recent_actions_window(service, RECENT_ACTION_WINDOW_SECONDS))
    actions.sort(key=lambda a: a["timestamp"], reverse=True)
    return actions


def _find_earliest_fault(service: str, context: dict) -> float:
    earliest = float("inf")
    for key, entries in context.get(service, {}).items():
        parts = key.split("/")
        if len(parts) != 2:
            continue
        metric = parts[1]
        thresholds = config.METRIC_THRESHOLDS.get(metric, {})
        # Use amber as the fault-detection floor so early-warning crossings
        # (e.g. db_connection_count approaching pool ceiling) are included in
        # the temporal ordering, not just fully-red breaches.
        amber = thresholds.get("amber", 0)
        if amber <= 0:
            continue
        for entry in entries:
            if entry.get("value", 0) >= amber:
                ts = entry.get("ts", time.time())
                if ts < earliest:
                    earliest = ts
    return earliest if earliest < float("inf") else time.time()


def _find_root_cause(faulted_service: str, context: dict) -> tuple[str, list[str]]:
    known = set(graph_store.get_all_services())
    all_services = set(graph_store.bfs_upstream(faulted_service)) | set(graph_store.bfs_downstream(faulted_service))
    all_services = [s for s in all_services if s in known] or [faulted_service]

    now = time.time()
    fault_times = {svc: _find_earliest_fault(svc, context) for svc in all_services}

    # Only include services that actually crossed a threshold (fault_time < now).
    # Services with no threshold breach return time.time() from _find_earliest_fault
    # and are innocent bystanders — don't include them in the fault path.
    actually_faulted = {svc: t for svc, t in fault_times.items() if t < now - 1}
    if not actually_faulted:
        actually_faulted = {faulted_service: fault_times.get(faulted_service, now)}

    root = min(actually_faulted, key=lambda s: actually_faulted[s])
    path = sorted(actually_faulted.keys(), key=lambda s: actually_faulted[s])
    return root, path


def _build_ml_context(service: str, baseline_model: BaselineModel,
                      anomaly_detector: AnomalyDetector, comm_model: CommunicationModel) -> dict:
    """Build ML context from graph-discovered categories/metrics — no hardcoded lists."""
    ml_ctx: dict = {"zscores": {}, "anomaly_score": 0.5, "call_pattern_zscores": {}}
    feature_dict = {}
    for category in graph_store.get_categories_for_service(service):
        for metric in graph_store.get_metrics_for_service_category(service, category):
            zs = baseline_model.get_zscore(service, metric, None)
            if zs is not None:
                ml_ctx["zscores"][f"{category}/{metric}"] = round(zs, 3)
                feature_dict[f"{category}_{metric}"] = zs
    if feature_dict:
        ml_ctx["anomaly_score"] = round(anomaly_detector.get_anomaly_score(service, feature_dict), 3)
    for neighbor in graph_store.get_all_neighbors(service):
        zs = comm_model.get_call_pattern_zscore(service, neighbor)
        if zs is not None:
            ml_ctx["call_pattern_zscores"][f"{service}->{neighbor}"] = round(zs, 3)
    return ml_ctx


# ── Alert buffer ───────────────────────────────────────────────────────────────
# Keyed by (service, metric) — keeps the entry with the highest value seen
# so the LLM always gets the worst reading from the batch window.

class AlertBuffer:
    def __init__(self):
        self._buf: dict[tuple, dict] = {}

    def add(self, alert: dict):
        key = (alert["service"], alert["metric"])
        existing = self._buf.get(key)
        if existing is None or alert["value"] > existing["value"]:
            self._buf[key] = alert

    def flush(self) -> list[dict]:
        alerts = list(self._buf.values())
        self._buf.clear()
        return alerts

    def __len__(self):
        return len(self._buf)


# ── Noise check (no LLM, instant) ─────────────────────────────────────────────

def _handle_noise(
    alert: dict,
    sqlite_store: SQLiteStore,
    noise_classifier: NoiseClassifier,
    baseline_model: BaselineModel,
) -> bool:
    """Returns True if suppressed."""
    service  = alert["service"]
    metric   = alert["metric"]
    value    = alert["value"]
    category = alert.get("category", "performance")
    ts       = alert.get("timestamp", time.time())
    hour     = int((ts % 86400) / 3600)

    is_noise, confidence, reason = noise_classifier.is_noise(
        service, metric, baseline_model=baseline_model, current_value=value
    )
    noise_classifier.record_alert(service, metric)
    if is_noise:
        logger.info(f"Suppressed [{confidence}]: {service}/{metric} — {reason}")
        sqlite_store.record_suppressed_alert(
            service=service, category=category, metric=metric,
            value=value, hour_bucket=hour,
            suppression_count=noise_classifier.get_count(service, metric),
            reason=reason,
        )
    return is_noise


# ── Batch LLM flush ────────────────────────────────────────────────────────────

def _flush_batch(
    alerts: list[dict],
    redis_store: RedisStore,
    sqlite_store: SQLiteStore,
    neo4j_store: Neo4jStore,
    baseline_model: BaselineModel,
    anomaly_detector: AnomalyDetector,
    comm_model: CommunicationModel,
    pattern_matcher: PatternMatcher,
):
    if not alerts:
        return

    logger.info(f"Batch flush: {len(alerts)} unique alert(s)")

    # Build unified context across all faulted services and their neighbors
    faulted_services = list({a["service"] for a in alerts})
    all_services = list(set(
        faulted_services
        + [n for svc in faulted_services for n in graph_store.get_all_neighbors(svc)]
        + graph_store.get_all_services()
    ))

    timeseries_ctx = _build_timeseries_context(redis_store, all_services)
    actions_ctx    = _build_actions_context(redis_store, all_services)

    recent_actions_summary = [
        f"{a['event_type']} on {a['service']} at {time.strftime('%H:%M:%S', time.localtime(a['timestamp']))}"
        for a in actions_ctx[:10]
    ]

    # One LLM call covering the whole batch
    # Use the highest-severity alert as the primary subject
    primary = max(alerts, key=lambda a: a["value"])
    primary_service  = primary["service"]
    primary_metric   = primary["metric"]
    primary_category = primary.get("category", "performance")

    root_cause, fault_path = _find_root_cause(primary_service, timeseries_ctx)
    ml_ctx = _build_ml_context(primary_service, baseline_model, anomaly_detector, comm_model)

    # Annotate each metric in the timeseries with threshold context so the LLM
    # can reason about whether a value is normal, amber, or red — and why.
    # db_connection_count=0 during a surge means pool exhaustion, not idle DB.
    annotated_timeseries = {}
    for svc, metrics in timeseries_ctx.items():
        annotated_timeseries[svc] = {}
        for key, entries in metrics.items():
            metric = key.split("/")[-1]
            thresholds = config.METRIC_THRESHOLDS.get(metric, {})
            annotation: dict = {"values": entries[-5:]}
            if thresholds.get("amber", 0) > 0:
                annotation["threshold_amber"] = thresholds["amber"]
                annotation["threshold_red"]   = thresholds["red"]
            if metric == "db_connection_count":
                annotation["pool_size"] = config.ORDER_DB_POOL_SIZE
                annotation["note"] = (
                    "A value at or near pool_size means pool is exhausted (connections queued/timing out). "
                    "A value of 0 after a surge often means requests failed before acquiring a connection."
                )
            annotated_timeseries[svc][key] = annotation

    # Query Neo4j for the last 3 topology snapshots taken before this alert.
    # This gives the LLM something no other source provides: how did each
    # service's health and anomaly score change over time leading up to the
    # fault, and did any new call edge appear just before it started?
    alert_ts = primary.get("timestamp", time.time())
    topology_history = neo4j_store.get_topology_context_for_diagnosis(
        alert_timestamp=alert_ts,
        faulted_services=all_services,
        lookback_snapshots=3,
    )
    if topology_history:
        logger.info(
            f"Neo4j topology history: {topology_history.get('snapshots_used', 0)} snapshots, "
            f"new edges: {len(topology_history.get('new_call_edges_since_last_snapshot', []))}"
        )

    context_for_llm = {
        "batch_summary": f"{len(alerts)} alert(s) in this 15-minute window",
        "all_faulted_services": faulted_services,
        "all_alerts": [
            {"service": a["service"], "metric": a["metric"],
             "value": a["value"], "severity": a.get("severity", "warning")}
            for a in alerts
        ],
        "primary_faulted_service": primary_service,
        "primary_faulted_metric":  primary_metric,
        "primary_alert_value":     primary["value"],
        "inferred_root_cause":     root_cause,
        "fault_path":              fault_path,
        "metric_timeseries": annotated_timeseries,
        "recent_actions": recent_actions_summary,
        "ml_scores":      ml_ctx,
        "graph_topology": {
            "upstream":   graph_store.get_upstream_neighbors(primary_service),
            "downstream": graph_store.get_downstream_neighbors(primary_service),
        },
        # Graph state history from Neo4j snapshots — shows degradation trajectory
        # and any new call edges that appeared just before the fault.
        # Empty dict if Neo4j has no snapshots yet (first run).
        "topology_history": topology_history,
    }

    # Check for a known recurring pattern before spending an LLM call.
    anomaly_score = ml_ctx.get("anomaly_score", 0.5)
    pattern_diagnosis, match_count = pattern_matcher.match(
        primary_service, primary_metric, primary["value"], anomaly_score
    )

    if pattern_diagnosis:
        logger.info(
            f"Pattern match ({match_count} prior occurrence(s)) — skipping LLM for "
            f"{primary_service}/{primary_metric}={primary['value']:.3f}"
        )
        llm_parsed = pattern_diagnosis
        llm_raw    = f"[ml-pattern-match: {match_count} prior occurrence(s), no LLM call]"
        diagnosis_tag = f"batch={len(alerts)} root={root_cause} pattern_match={match_count}"
    else:
        logger.info(f"No pattern match — calling LLM for {primary_service}/{primary_metric}")
        past_incidents = sqlite_store.get_recent_incidents_for_service(primary_service, limit=3)
        llm_parsed, llm_raw = llm_analyze(context_for_llm, past_incidents)
        diagnosis_tag = f"batch={len(alerts)} root={root_cause} path={' -> '.join(fault_path)}"

    incident_id = sqlite_store.record_incident(
        service=primary_service,
        category=primary_category,
        metric=primary_metric,
        alert_value=primary["value"],
        fault_path=fault_path,
        context_snapshot=timeseries_ctx,
        actions_context=actions_ctx,
        llm_root_cause=llm_parsed.get("root_cause", ""),
        llm_contributing_factors=llm_parsed.get("contributing_factors", []),
        llm_confidence=llm_parsed.get("confidence", ""),
        llm_recommended_fix=llm_parsed.get("recommended_fix", ""),
        llm_estimated_impact=llm_parsed.get("estimated_impact", ""),
        llm_raw_response=llm_raw,
        faulted_service=primary_service,
        alert_metric=primary_metric,
        diagnosis_summary=diagnosis_tag,
        root_cause_service=root_cause,
    )

    logger.info(
        f"Incident #{incident_id} (batch of {len(alerts)}): "
        f"{primary_service}/{primary_metric}={primary['value']:.3f} "
        f"root={root_cause} confidence={llm_parsed.get('confidence')}"
    )
    logger.info(f"  Root cause: {llm_parsed.get('root_cause')}")
    logger.info(f"  Fix: {llm_parsed.get('recommended_fix')}")


# ── Main loops ─────────────────────────────────────────────────────────────────

async def alert_consumer_loop(
    redis_store: RedisStore,
    sqlite_store: SQLiteStore,
    noise_classifier: NoiseClassifier,
    baseline_model: BaselineModel,
    buffer: AlertBuffer,
):
    """Continuously reads the alert stream and buffers non-noise alerts."""
    loop = asyncio.get_event_loop()
    logger.info(f"Consuming alert stream: group={CONSUMER_GROUP} consumer={CONSUMER_NAME}")

    while True:
        try:
            alerts = await loop.run_in_executor(
                None,
                lambda: redis_store.read_alert_stream(CONSUMER_GROUP, CONSUMER_NAME, count=10, block_ms=2000),
            )
            for alert in alerts:
                stream_id = alert.pop("stream_id", None)
                try:
                    if not _handle_noise(alert, sqlite_store, noise_classifier, baseline_model):
                        buffer.add(alert)
                        logger.debug(f"Buffered: {alert['service']}/{alert['metric']}={alert['value']:.3f} (buffer size={len(buffer)})")
                except Exception as e:
                    logger.error(f"Alert processing error: {e}")
                finally:
                    if stream_id:
                        redis_store.ack_alert(stream_id, CONSUMER_GROUP)
        except Exception as e:
            logger.error(f"Consumer loop error: {e}")
            await asyncio.sleep(2)


async def llm_flush_loop(
    redis_store: RedisStore,
    sqlite_store: SQLiteStore,
    neo4j_store: Neo4jStore,
    baseline_model: BaselineModel,
    anomaly_detector: AnomalyDetector,
    comm_model: CommunicationModel,
    pattern_matcher: PatternMatcher,
    buffer: AlertBuffer,
):
    """Fires diagnosis every LLM_BATCH_INTERVAL_SECONDS; skips LLM for known patterns."""
    logger.info(f"Flush loop: interval={LLM_BATCH_INTERVAL_SECONDS}s ({LLM_BATCH_INTERVAL_SECONDS//60} min)")
    while True:
        await asyncio.sleep(LLM_BATCH_INTERVAL_SECONDS)
        alerts = buffer.flush()
        if not alerts:
            logger.info("Flush: buffer empty — nothing to diagnose")
            continue
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: _flush_batch(alerts, redis_store, sqlite_store, neo4j_store,
                                     baseline_model, anomaly_detector, comm_model,
                                     pattern_matcher),
            )
        except Exception as e:
            logger.error(f"Flush error: {e}")


async def main():
    logger.info("Diagnosis agent starting…")

    redis_store = RedisStore(host=REDIS_HOST, port=REDIS_PORT)
    for attempt in range(30):
        if redis_store.ping():
            break
        logger.info(f"Waiting for Redis… attempt {attempt + 1}")
        await asyncio.sleep(2)
    else:
        logger.error("Redis not reachable")
        sys.exit(1)

    redis_store.ensure_alert_consumer_group(CONSUMER_GROUP)

    # Neo4j — used to read topology history for richer LLM context.
    # If unreachable, diagnosis still works — topology_history will be empty dict.
    neo4j_store = Neo4jStore(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    if not neo4j_store.ping():
        logger.warning("Neo4j not reachable — topology history will be omitted from LLM context")

    sqlite_store     = SQLiteStore(SQLITE_PATH)
    noise_classifier = NoiseClassifier(sqlite_store=sqlite_store)
    baseline_model   = BaselineModel(sqlite_store=sqlite_store)
    anomaly_detector = AnomalyDetector(
        saved_models_dir=os.getenv("ML_SAVED_MODELS_DIR", config.ML_SAVED_MODELS_DIR)
    )
    comm_model      = CommunicationModel(sqlite_store=sqlite_store, graph_store=graph_store)
    pattern_matcher = PatternMatcher(sqlite_store=sqlite_store, anomaly_detector=anomaly_detector)
    buffer          = AlertBuffer()

    logger.info(f"Diagnosis agent ready — pattern-match first, LLM fallback every {LLM_BATCH_INTERVAL_SECONDS//60} min")

    await asyncio.gather(
        alert_consumer_loop(redis_store, sqlite_store, noise_classifier, baseline_model, buffer),
        llm_flush_loop(redis_store, sqlite_store, neo4j_store, baseline_model, anomaly_detector,
                       comm_model, pattern_matcher, buffer),
    )


if __name__ == "__main__":
    asyncio.run(main())
