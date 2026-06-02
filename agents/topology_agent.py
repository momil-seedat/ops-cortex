"""
Topology agent — builds and maintains a live + persistent topology graph.

Memory tiers:
  Permanent (SQLite)
    known_services   — service lifecycle: active / offline / deprecated
    topology_edges   — confirmed caller→callee relationships with confidence score
    metric_baselines — every (service, metric) pair ever seen
  Hot (Redis)
    node:*           — live metric values written by monitoring agent, 2h TTL

Service lifecycle behaviour:
  New service      → first Redis keys appear → auto-registered in known_services
                     → graph nodes created → status 'active'
  Service offline  → absent from Redis (deployment, crash) → status 'offline'
                     → graph node stays in graph but marked offline
                     → permanent memory retained (we still know it exists)
  Service back up  → Redis keys reappear → status reset to 'active'
                     → graph node re-populated from Redis
  Deprecated       → absent from Redis for > DEPRECATION_THRESHOLD_DAYS
                     → status → 'deprecated' → all permanent memory purged
                     → removed from live graph entirely

No service names, categories, or metric names are hardcoded here.
"""
import asyncio
import logging
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, "/app")
import config
from memory.graph_store import get_graph_store
from memory.neo4j_store import Neo4jStore
from memory.sqlite_store import SQLiteStore
from memory.redis_store import RedisStore
from ml.baseline_model import BaselineModel
from ml.anomaly_detector import AnomalyDetector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("topology_agent")

REDIS_HOST     = os.getenv("REDIS_HOST",     config.REDIS_HOST)
REDIS_PORT     = int(os.getenv("REDIS_PORT", str(config.REDIS_PORT)))
NEO4J_URI      = os.getenv("NEO4J_URI",      config.NEO4J_URI)
NEO4J_USER     = os.getenv("NEO4J_USER",     config.NEO4J_USER)
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", config.NEO4J_PASSWORD)

ACTIVE_REQUEST_RATE_THRESHOLD = 0.1
CALL_CORRELATION_THRESHOLD    = 0.60
CORRELATION_WINDOW            = 20

# A service absent from Redis for this many days is considered deprecated
DEPRECATION_THRESHOLD_DAYS = int(os.getenv("DEPRECATION_THRESHOLD_DAYS", "7"))

# How often to run the deprecation check (seconds)
DEPRECATION_CHECK_INTERVAL = int(os.getenv("DEPRECATION_CHECK_INTERVAL", "3600"))  # 1h

graph_store = get_graph_store()


# ── Cold-start bootstrap ───────────────────────────────────────────────────────

def bootstrap_from_permanent_memory(sqlite_store: SQLiteStore):
    """
    Seed the in-process graph from SQLite before any Redis data arrives.

    Restores:
      1. Service + metric nodes (from metric_baselines) — nodes start with
         value=0/status='green'; Redis overwrites on first sync cycle.
         Services that were 'offline' at last shutdown are restored with status
         'offline' so the graph immediately reflects they were down.

      2. Call edges (from topology_edges) — replayed into the graph so
         upstream/downstream queries work before any traffic is observed.
    """
    known = sqlite_store.get_active_and_offline_services()
    if known:
        logger.info(f"Bootstrap: restoring {len(known)} services from permanent memory")
        for svc_row in known:
            service = svc_row["name"]
            known_metrics = sqlite_store.get_known_metrics_for_service(service)
            for row in known_metrics:
                metric   = row["metric"]
                category = config.METRIC_CATEGORY_MAP.get(metric, "performance")
                thresh   = config.METRIC_THRESHOLDS.get(metric, {"amber": 0.0, "red": 0.0})
                graph_store.update_metric(
                    service=service,
                    category=category,
                    metric=metric,
                    value=0.0,
                    status="green",
                    threshold_low=thresh["amber"],
                    threshold_high=thresh["red"],
                )

            # If this service was offline at last shutdown, reflect that immediately
            if svc_row["status"] == "offline":
                graph_store.set_service_offline(service, since=svc_row["offline_since"])
                logger.info(
                    f"Bootstrap: '{service}' restored as OFFLINE "
                    f"(last seen {_age_str(svc_row['last_seen_in_redis'])} ago)"
                )
            else:
                logger.debug(f"Bootstrap: '{service}' restored ({len(known_metrics)} metrics)")

    known_edges = sqlite_store.get_all_topology_edges()
    if known_edges:
        logger.info(f"Bootstrap: restoring {len(known_edges)} call edges from topology_edges")
        for edge in known_edges:
            graph_store.record_call(edge["caller"], edge["callee"],
                                    latency_ms=edge["avg_latency_ms"])

    if not known and not known_edges:
        logger.info("Bootstrap: no permanent memory found — first run")


def _age_str(ts: float) -> str:
    if not ts:
        return "unknown"
    secs = time.time() - ts
    if secs < 3600:
        return f"{int(secs/60)}m"
    if secs < 86400:
        return f"{int(secs/3600)}h"
    return f"{int(secs/86400)}d"


# ── Service lifecycle management ───────────────────────────────────────────────

def reconcile_service_lifecycle(
    sqlite_store: SQLiteStore,
    live_services: list[str],
):
    """
    Compare services currently seen in Redis against permanent memory and
    update the lifecycle state of every known service.

    Three transitions:
      live + not in permanent memory  → NEW: register in known_services + log
      live + was offline              → RECOVERED: status → active
      in permanent memory + not live  → OFFLINE: status → offline, graph node marked
    """
    live_set = set(live_services)

    # Register/refresh every live service
    for service in live_services:
        sqlite_store.mark_service_active(service)

    # Find services in permanent memory that are NOT currently live
    known = sqlite_store.get_active_and_offline_services()
    absent = [s["name"] for s in known if s["name"] not in live_set]

    if absent:
        sqlite_store.mark_services_offline(absent)
        for name in absent:
            graph_store.set_service_offline(name)


async def deprecation_loop(sqlite_store: SQLiteStore):
    """
    Periodically checks for services that have been offline for longer than
    DEPRECATION_THRESHOLD_DAYS and removes them from permanent memory + live graph.

    The threshold is intentionally long (default 7 days) because we want to
    distinguish a deployment window (hours) from a true retirement (days/weeks).
    """
    threshold_seconds = DEPRECATION_THRESHOLD_DAYS * 86400
    while True:
        await asyncio.sleep(DEPRECATION_CHECK_INTERVAL)
        try:
            known = sqlite_store.get_active_and_offline_services()
            now = time.time()
            for svc_row in known:
                if svc_row["status"] != "offline":
                    continue
                offline_since = svc_row["offline_since"] or svc_row["last_seen_in_redis"]
                absent_for = now - offline_since
                if absent_for >= threshold_seconds:
                    days = int(absent_for / 86400)
                    logger.info(
                        f"Service '{svc_row['name']}' absent for {days} days "
                        f"(threshold={DEPRECATION_THRESHOLD_DAYS}d) — deprecating"
                    )
                    purged = sqlite_store.deprecate_and_purge_service(svc_row["name"])
                    graph_store.remove_service(svc_row["name"])
                    logger.info(
                        f"'{svc_row['name']}' deprecated and removed from graph. "
                        f"Purged: {purged}"
                    )
        except Exception as e:
            logger.error(f"Deprecation loop error: {e}", exc_info=True)


# ── Call-graph inference ───────────────────────────────────────────────────────

def _collect_rate_vectors(redis_store: RedisStore, services: list[str]) -> dict[str, list[float]]:
    vectors: dict[str, list[float]] = {}
    for service in services:
        rates = redis_store.get_recent_request_rates(service, count=CORRELATION_WINDOW)
        if rates:
            vectors[service] = rates
    return vectors


def _pearson_correlation(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 3:
        return 0.0
    a, b = a[-n:], b[-n:]
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    num    = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
    den_a  = sum((x - mean_a) ** 2 for x in a) ** 0.5
    den_b  = sum((y - mean_b) ** 2 for y in b) ** 0.5
    if den_a == 0 or den_b == 0:
        return 0.0
    return num / (den_a * den_b)


def infer_call_graph(redis_store: RedisStore, sqlite_store: SQLiteStore, services: list[str]):
    """
    Infer directed call edges from OTel labels and traffic correlation.
    Persists every discovered edge to SQLite so it survives restarts.
    """
    # Signal 1: OTel upstream_service label (authoritative)
    all_keys = redis_store.discover_node_keys()
    for svc, category, metric in all_keys:
        if svc not in services:
            continue
        state = redis_store.get_metric_state(svc, category, metric)
        if not state:
            continue
        upstream = state.get("upstream_service")
        if upstream and upstream in services and upstream != svc:
            graph_store.record_call(upstream, svc, latency_ms=state.get("value", 0.0))
            sqlite_store.upsert_topology_edge(upstream, svc, source="otel",
                                              latency_ms=state.get("value", 0.0))

    # Signal 2: Pearson traffic correlation (statistical)
    vectors = _collect_rate_vectors(redis_store, services)
    active  = {
        svc: rates for svc, rates in vectors.items()
        if rates and sum(rates) / len(rates) > ACTIVE_REQUEST_RATE_THRESHOLD
    }
    if len(active) < 2:
        return

    svc_list = list(active.keys())
    for i in range(len(svc_list)):
        for j in range(i + 1, len(svc_list)):
            a, b = svc_list[i], svc_list[j]
            r = _pearson_correlation(active[a], active[b])
            if r >= CALL_CORRELATION_THRESHOLD:
                mean_a = sum(active[a]) / len(active[a])
                mean_b = sum(active[b]) / len(active[b])
                caller, callee = (a, b) if mean_a >= mean_b else (b, a)
                graph_store.record_call(caller, callee, latency_ms=0.0)
                sqlite_store.upsert_topology_edge(caller, callee, source="correlation")
                logger.debug(
                    f"Inferred call edge '{caller}'→'{callee}' "
                    f"(r={r:.2f}, rates {mean_a:.1f}/{mean_b:.1f})"
                )


# ── Graph sync from Redis ──────────────────────────────────────────────────────

def sync_graph_from_redis(
    redis_store: RedisStore,
    sqlite_store: SQLiteStore,
    baseline_model: BaselineModel,
) -> list[str]:
    """
    Discover all live (service, category, metric) triples from Redis node:* keys,
    push their values into the graph, reconcile lifecycle state, and return the
    list of currently live services.
    """
    triples = redis_store.discover_node_keys()

    by_service: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for service, category, metric in triples:
        by_service[service].append((category, metric))

    live_services = list(by_service.keys())

    # Update lifecycle for all services (new, recovered, offline)
    reconcile_service_lifecycle(sqlite_store, live_services)

    for service, cat_metrics in by_service.items():
        worst_zscore     = 0.0
        worst_metric_name = ""

        for category, metric in cat_metrics:
            state = redis_store.get_metric_state(service, category, metric)
            if state is None:
                continue
            value  = state["value"]
            status = state["status"]
            thresh = config.METRIC_THRESHOLDS.get(metric, {"amber": 0.0, "red": 0.0})

            graph_store.update_metric(
                service=service,
                category=category,
                metric=metric,
                value=value,
                status=status,
                threshold_low=thresh["amber"],
                threshold_high=thresh["red"],
            )

            baseline_model.record(service, metric, value)
            zscore = baseline_model.get_zscore(service, metric, value)
            if abs(zscore) > abs(worst_zscore):
                worst_zscore      = zscore
                worst_metric_name = metric

        for action_metric in ["last_deployment", "last_restart", "last_config_change", "last_scaling_event"]:
            matching = [
                a for a in redis_store.get_recent_actions(service, count=50)
                if a.get("event_type", "").replace(" ", "_") == action_metric
            ]
            ts_value = matching[0]["timestamp"] if matching else 0.0
            graph_store.update_metric(
                service=service, category="actions",
                metric=action_metric, value=ts_value, status="green",
            )

        graph_store.update_service_ml_attrs(
            service=service, anomaly_score=0.5,
            zscore_worst_metric=worst_metric_name,
        )

    graph_store.propagate_statuses()
    graph_store.mark_stale_edges()
    return live_services


def compute_and_write_anomaly_scores(
    anomaly_detector: AnomalyDetector,
    baseline_model: BaselineModel,
    services: list[str],
):
    for service in services:
        feature_dict: dict[str, float] = {}
        for cat in graph_store.get_categories_for_service(service):
            for metric in graph_store.get_metrics_for_service_category(service, cat):
                zs = baseline_model.get_zscore(service, metric, None)
                if zs is not None:
                    feature_dict[f"{cat}_{metric}"] = zs
        if feature_dict:
            score = anomaly_detector.get_anomaly_score(service, feature_dict)
            attrs = graph_store.get_service_attrs(service)
            graph_store.update_service_ml_attrs(
                service=service,
                anomaly_score=score,
                zscore_worst_metric=attrs.get("zscore_worst_metric", ""),
            )


# ── Main loops ─────────────────────────────────────────────────────────────────

async def sync_loop(
    redis_store: RedisStore,
    sqlite_store: SQLiteStore,
    baseline_model: BaselineModel,
    anomaly_detector: AnomalyDetector,
):
    while True:
        try:
            live = sync_graph_from_redis(redis_store, sqlite_store, baseline_model)
            if live:
                infer_call_graph(redis_store, sqlite_store, live)
                compute_and_write_anomaly_scores(anomaly_detector, baseline_model, live)
        except Exception as e:
            logger.error(f"Sync loop error: {e}", exc_info=True)
        await asyncio.sleep(config.TOPOLOGY_SYNC_INTERVAL_SECONDS)


async def neo4j_snapshot_loop(neo4j_store: Neo4jStore, sqlite_store: SQLiteStore):
    while True:
        await asyncio.sleep(config.TOPOLOGY_NEO4J_SNAPSHOT_INTERVAL_SECONDS)
        try:
            graph_data = graph_store.to_dict()
            if graph_data["node_count"] == 0:
                continue
            snapshot_id = neo4j_store.save_topology_snapshot(
                nodes=graph_data["nodes"],
                edges=graph_data["edges"],
                trigger="scheduled",
            )
            sqlite_store.record_graph_snapshot(
                node_count=graph_data["node_count"],
                edge_count=graph_data["edge_count"],
                trigger="scheduled",
                neo4j_snapshot_id=snapshot_id,
            )
            logger.info(f"Neo4j snapshot {snapshot_id}: {graph_data['node_count']} nodes")
        except Exception as e:
            logger.error(f"Neo4j snapshot error: {e}")


async def main():
    logger.info("Topology agent starting…")

    redis_store = RedisStore(host=REDIS_HOST, port=REDIS_PORT)
    for attempt in range(30):
        if redis_store.ping():
            break
        logger.info(f"Waiting for Redis… attempt {attempt + 1}")
        await asyncio.sleep(2)
    else:
        logger.error("Redis not reachable")
        sys.exit(1)

    neo4j_store = Neo4jStore(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    for attempt in range(30):
        if neo4j_store.ping():
            break
        logger.info(f"Waiting for Neo4j… attempt {attempt + 1}")
        await asyncio.sleep(3)
    else:
        logger.error("Neo4j not reachable")
        sys.exit(1)

    neo4j_store.create_indexes()
    sqlite_store = SQLiteStore(os.getenv("SQLITE_DB_PATH", config.SQLITE_DB_PATH))

    baseline_model   = BaselineModel(sqlite_store)
    anomaly_detector = AnomalyDetector(
        saved_models_dir=os.getenv("ML_SAVED_MODELS_DIR", config.ML_SAVED_MODELS_DIR)
    )

    bootstrap_from_permanent_memory(sqlite_store)

    logger.info(
        f"Topology agent ready — lifecycle tracking active "
        f"(deprecation threshold: {DEPRECATION_THRESHOLD_DAYS} days)"
    )
    await asyncio.gather(
        sync_loop(redis_store, sqlite_store, baseline_model, anomaly_detector),
        neo4j_snapshot_loop(neo4j_store, sqlite_store),
        deprecation_loop(sqlite_store),
    )


if __name__ == "__main__":
    asyncio.run(main())
