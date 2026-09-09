"""
Three-level directed topology graph — fully dynamic, zero hardcoding.

Level 1 — service nodes   discovered from Redis node:* keys
Level 2 — category nodes  discovered from Redis node:* keys (e.g. performance, database, kafka)
Level 3 — metric nodes    discovered from Redis node:* keys (any metric name)

Edges:  service --has_category--> category --has_metric--> metric
        service --calls--> service  (inferred from correlated traffic)

Status propagation:  metric → category → service  (worst-of-children)

Adding a new service or metric requires no code change: as soon as the monitoring
agent writes a node:{service}:{category}:{metric} key to Redis, the topology agent
discovers it on the next sync cycle and creates the corresponding graph nodes and
edges automatically.
"""
import threading
import time
import logging
from typing import Optional

import networkx as nx

logger = logging.getLogger("graph_store")

STATUS_ORDER = {"green": 0, "amber": 1, "red": 2}
SERVICE_STATUS_MAP = {"green": "healthy", "amber": "degraded", "red": "faulted"}

# Kept as a convenience export so other modules can reference the known default
# categories without importing config — but they are NOT used to gate discovery.
CATEGORIES = ["performance", "database", "kafka", "actions"]

CATEGORY_METRICS: dict[str, list[str]] = {
    "performance": ["error_rate", "latency_p99", "request_rate", "cpu_usage", "memory_usage"],
    "database":    ["db_connection_count", "db_query_time", "db_error_count"],
    "kafka":       ["consumer_lag", "producer_error_rate", "consumer_offset_delta"],
    "actions":     ["last_deployment", "last_restart", "last_config_change", "last_scaling_event"],
}

STALE_EDGE_SECONDS = 300


def _service_node_id(service: str) -> str:
    return service


def _category_node_id(service: str, category: str) -> str:
    return f"{service}:{category}"


def _metric_node_id(service: str, category: str, metric: str) -> str:
    return f"{service}:{category}:{metric}"


def _worst_status(statuses: list[str]) -> str:
    if not statuses:
        return "green"
    return max(statuses, key=lambda s: STATUS_ORDER.get(s, 0))


class GraphStore:
    def __init__(self):
        self._graph: nx.DiGraph = nx.DiGraph()
        self._lock = threading.RLock()
        # No skeleton — graph starts empty and grows from discovered Redis keys.

    # ── Node creation (idempotent) ─────────────────────────────────────────────

    def _ensure_service(self, service: str) -> str:
        svc_id = _service_node_id(service)
        with self._lock:
            if svc_id not in self._graph:
                logger.info(f"Topology: discovered new service '{service}'")
                self._graph.add_node(
                    svc_id,
                    node_type="service",
                    name=service,
                    status="healthy",
                    anomaly_score=0.5,
                    zscore_worst_metric="",
                    last_updated=time.time(),
                )
        return svc_id

    def _ensure_category(self, service: str, category: str) -> str:
        svc_id = self._ensure_service(service)
        cat_id = _category_node_id(service, category)
        with self._lock:
            if cat_id not in self._graph:
                logger.info(f"Topology: discovered new category '{service}/{category}'")
                self._graph.add_node(
                    cat_id,
                    node_type="category",
                    name=category,
                    parent_service=service,
                    status="green",
                    last_updated=time.time(),
                )
                self._graph.add_edge(svc_id, cat_id, edge_type="has_category")
        return cat_id

    def _ensure_metric(self, service: str, category: str, metric: str) -> str:
        cat_id = self._ensure_category(service, category)
        met_id = _metric_node_id(service, category, metric)
        with self._lock:
            if met_id not in self._graph:
                logger.info(f"Topology: discovered new metric '{service}/{category}/{metric}'")
                self._graph.add_node(
                    met_id,
                    node_type="metric",
                    metric_name=metric,
                    parent_category=category,
                    parent_service=service,
                    current_value=0.0,
                    status="green",
                    threshold_low=0.0,
                    threshold_high=0.0,
                    last_updated=time.time(),
                )
                self._graph.add_edge(cat_id, met_id, edge_type="has_metric")
        return met_id

    # ── Metric node updates ────────────────────────────────────────────────────

    def update_metric(
        self,
        service: str,
        category: str,
        metric: str,
        value: float,
        status: str,
        threshold_low: float = 0.0,
        threshold_high: float = 0.0,
    ):
        """Create nodes on first sight, then update values — no prior registration needed."""
        met_id = self._ensure_metric(service, category, metric)
        with self._lock:
            self._graph.nodes[met_id].update({
                "current_value": value,
                "status": status,
                "threshold_low": threshold_low,
                "threshold_high": threshold_high,
                "last_updated": time.time(),
            })

    # ── Status propagation (walks actual graph, no hardcoded service list) ─────

    def propagate_statuses(self):
        """Propagate worst status from metric → category → service."""
        with self._lock:
            service_ids = [
                n for n, d in self._graph.nodes(data=True)
                if d.get("node_type") == "service"
            ]
            for svc_id in service_ids:
                cat_statuses: list[str] = []
                for _, cat_id, edge_data in self._graph.out_edges(svc_id, data=True):
                    if edge_data.get("edge_type") != "has_category":
                        continue
                    met_statuses: list[str] = []
                    for _, met_id, me_data in self._graph.out_edges(cat_id, data=True):
                        if me_data.get("edge_type") == "has_metric":
                            met_statuses.append(
                                self._graph.nodes[met_id].get("status", "green")
                            )
                    worst_cat = _worst_status(met_statuses)
                    self._graph.nodes[cat_id]["status"] = worst_cat
                    self._graph.nodes[cat_id]["last_updated"] = time.time()
                    cat_statuses.append(worst_cat)

                # Don't overwrite an explicit 'offline' status set by lifecycle management
                if self._graph.nodes[svc_id].get("status") != "offline":
                    worst_svc = _worst_status(cat_statuses)
                    self._graph.nodes[svc_id]["status"] = SERVICE_STATUS_MAP.get(worst_svc, "healthy")
                    self._graph.nodes[svc_id]["last_updated"] = time.time()

    def set_service_offline(self, service: str, since: float = None):
        """
        Mark a service node as offline without removing it from the graph.
        All its metric/category children are preserved so the topology structure
        is intact when the service comes back up.
        """
        svc_id = _service_node_id(service)
        with self._lock:
            if svc_id not in self._graph:
                return
            self._graph.nodes[svc_id].update({
                "status": "offline",
                "offline_since": since or time.time(),
                "last_updated": time.time(),
            })

    def remove_service(self, service: str):
        """
        Remove a deprecated service and all its descendant nodes (category + metric)
        from the live graph entirely.  Call edges involving this service are also removed.
        """
        svc_id = _service_node_id(service)
        with self._lock:
            if svc_id not in self._graph:
                return
            # Collect all descendant node IDs before we start removing
            to_remove = [svc_id]
            for _, cat_id, d in list(self._graph.out_edges(svc_id, data=True)):
                if d.get("edge_type") == "has_category":
                    to_remove.append(cat_id)
                    for _, met_id, md in list(self._graph.out_edges(cat_id, data=True)):
                        if md.get("edge_type") == "has_metric":
                            to_remove.append(met_id)
            self._graph.remove_nodes_from(to_remove)
        logger.info(f"Graph: removed deprecated service '{service}' ({len(to_remove)} nodes)")

    def update_service_ml_attrs(self, service: str, anomaly_score: float, zscore_worst_metric: str):
        svc_id = _service_node_id(service)
        self._ensure_service(service)
        with self._lock:
            self._graph.nodes[svc_id].update({
                "anomaly_score": anomaly_score,
                "zscore_worst_metric": zscore_worst_metric,
                "last_updated": time.time(),
            })

    # ── Call-graph edges (service → service) ──────────────────────────────────

    def record_call(self, caller: str, callee: str, latency_ms: float):
        """Record a call edge between two services, creating service nodes if needed."""
        self._ensure_service(caller)
        self._ensure_service(callee)
        now = time.time()
        with self._lock:
            if self._graph.has_edge(caller, callee):
                edge = self._graph[caller][callee]
                if edge.get("edge_type") == "calls":
                    alpha = 0.1
                    edge["latency_ema"] = alpha * latency_ms + (1 - alpha) * edge["latency_ema"]
                    edge["call_count"] += 1
                    edge["last_seen"] = now
            else:
                self._graph.add_edge(
                    caller,
                    callee,
                    edge_type="calls",
                    call_count=1,
                    latency_ema=latency_ms,
                    last_seen=now,
                    stale=False,
                )
                logger.info(f"Topology: inferred call edge '{caller}' → '{callee}'")

    def mark_stale_edges(self):
        now = time.time()
        with self._lock:
            for u, v, data in self._graph.edges(data=True):
                if data.get("edge_type") == "calls":
                    data["stale"] = (now - data.get("last_seen", 0)) > STALE_EDGE_SECONDS

    # ── Neighbour queries (call-graph only) ───────────────────────────────────

    def get_upstream_neighbors(self, service: str) -> list[str]:
        with self._lock:
            return [
                u for u, v, d in self._graph.in_edges(service, data=True)
                if d.get("edge_type") == "calls"
            ]

    def get_downstream_neighbors(self, service: str) -> list[str]:
        with self._lock:
            return [
                v for u, v, d in self._graph.out_edges(service, data=True)
                if d.get("edge_type") == "calls"
            ]

    def get_all_neighbors(self, service: str) -> list[str]:
        up = self.get_upstream_neighbors(service)
        dn = self.get_downstream_neighbors(service)
        return list(set(up + dn))

    def bfs_upstream(self, fault_service: str) -> list[str]:
        with self._lock:
            visited, queue, seen = [], [fault_service], {fault_service}
            while queue:
                node = queue.pop(0)
                visited.append(node)
                for u, v, d in self._graph.in_edges(node, data=True):
                    if d.get("edge_type") == "calls" and u not in seen:
                        seen.add(u)
                        queue.append(u)
            return visited

    def bfs_downstream(self, service: str) -> list[str]:
        with self._lock:
            visited, queue, seen = [], [service], {service}
            while queue:
                node = queue.pop(0)
                visited.append(node)
                for u, v, d in self._graph.out_edges(node, data=True):
                    if d.get("edge_type") == "calls" and v not in seen:
                        seen.add(v)
                        queue.append(v)
            return visited

    # ── Read helpers ──────────────────────────────────────────────────────────

    def get_node_attributes(self, node_id: str) -> dict:
        with self._lock:
            return dict(self._graph.nodes[node_id]) if node_id in self._graph else {}

    def get_service_attrs(self, service: str) -> dict:
        return self.get_node_attributes(_service_node_id(service))

    def get_metric_attrs(self, service: str, category: str, metric: str) -> dict:
        return self.get_node_attributes(_metric_node_id(service, category, metric))

    def get_all_services(self) -> list[str]:
        with self._lock:
            return [
                d["name"] for _, d in self._graph.nodes(data=True)
                if d.get("node_type") == "service"
            ]

    def get_categories_for_service(self, service: str) -> list[str]:
        svc_id = _service_node_id(service)
        with self._lock:
            return [
                self._graph.nodes[cat_id]["name"]
                for _, cat_id, d in self._graph.out_edges(svc_id, data=True)
                if d.get("edge_type") == "has_category"
            ]

    def get_metrics_for_service_category(self, service: str, category: str) -> list[str]:
        cat_id = _category_node_id(service, category)
        with self._lock:
            return [
                self._graph.nodes[met_id]["metric_name"]
                for _, met_id, d in self._graph.out_edges(cat_id, data=True)
                if d.get("edge_type") == "has_metric"
            ]

    def get_all_nodes(self) -> list[dict]:
        with self._lock:
            return [{"id": n, **dict(self._graph.nodes[n])} for n in self._graph.nodes()]

    def get_all_edges(self) -> list[dict]:
        with self._lock:
            return [{"source": u, "target": v, **dict(d)} for u, v, d in self._graph.edges(data=True)]

    def get_calls_edges(self) -> list[dict]:
        with self._lock:
            return [
                {"source": u, "target": v, **dict(d)}
                for u, v, d in self._graph.edges(data=True)
                if d.get("edge_type") == "calls"
            ]

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "nodes": self.get_all_nodes(),
                "edges": self.get_all_edges(),
                "node_count": self._graph.number_of_nodes(),
                "edge_count": self._graph.number_of_edges(),
            }

    def get_graph(self) -> nx.DiGraph:
        with self._lock:
            return self._graph.copy()

    def node_count(self) -> int:
        with self._lock:
            return self._graph.number_of_nodes()

    def edge_count(self) -> int:
        with self._lock:
            return self._graph.number_of_edges()


_global_graph_store: Optional[GraphStore] = None


def get_graph_store() -> GraphStore:
    global _global_graph_store
    if _global_graph_store is None:
        _global_graph_store = GraphStore()
    return _global_graph_store
