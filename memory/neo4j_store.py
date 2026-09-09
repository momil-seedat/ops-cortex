import time
import uuid
import logging
from typing import Optional

from neo4j import GraphDatabase

logger = logging.getLogger("neo4j_store")


class Neo4jStore:
    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def save_topology_snapshot(self, nodes: list[dict], edges: list[dict], trigger: str = "scheduled") -> str:
        snapshot_id = str(uuid.uuid4())
        ts = time.time()
        with self._driver.session() as session:
            for node in nodes:
                node_type = node.get("node_type", "service")
                node_id   = node.get("id", "")
                if node_type == "service":
                    session.run(
                        """
                        MERGE (n:Service {id: $id, snapshot_id: $sid})
                        SET n.name        = $name,
                            n.status      = $status,
                            n.anomaly_score = $anomaly_score,
                            n.zscore_worst_metric = $zscore_worst,
                            n.last_updated = $last_updated,
                            n.snapshot_ts  = $snapshot_ts,
                            n.trigger      = $trigger
                        """,
                        id=node_id, sid=snapshot_id,
                        name=node.get("name", node_id),
                        status=node.get("status", "healthy"),
                        anomaly_score=float(node.get("anomaly_score", 0.5)),
                        zscore_worst=str(node.get("zscore_worst_metric", "")),
                        last_updated=float(node.get("last_updated", ts)),
                        snapshot_ts=ts, trigger=trigger,
                    )
                elif node_type == "category":
                    session.run(
                        """
                        MERGE (n:Category {id: $id, snapshot_id: $sid})
                        SET n.name           = $name,
                            n.parent_service = $parent,
                            n.status         = $status,
                            n.last_updated   = $last_updated,
                            n.snapshot_ts    = $snapshot_ts
                        """,
                        id=node_id, sid=snapshot_id,
                        name=node.get("name", ""),
                        parent=node.get("parent_service", ""),
                        status=node.get("status", "green"),
                        last_updated=float(node.get("last_updated", ts)),
                        snapshot_ts=ts,
                    )
                elif node_type == "metric":
                    session.run(
                        """
                        MERGE (n:Metric {id: $id, snapshot_id: $sid})
                        SET n.metric_name     = $metric_name,
                            n.current_value   = $current_value,
                            n.status          = $status,
                            n.threshold_low   = $threshold_low,
                            n.threshold_high  = $threshold_high,
                            n.parent_category = $parent_category,
                            n.parent_service  = $parent_service,
                            n.last_updated    = $last_updated,
                            n.snapshot_ts     = $snapshot_ts
                        """,
                        id=node_id, sid=snapshot_id,
                        metric_name=node.get("metric_name", ""),
                        current_value=float(node.get("current_value", 0.0)),
                        status=node.get("status", "green"),
                        threshold_low=float(node.get("threshold_low", 0.0)),
                        threshold_high=float(node.get("threshold_high", 0.0)),
                        parent_category=node.get("parent_category", ""),
                        parent_service=node.get("parent_service", ""),
                        last_updated=float(node.get("last_updated", ts)),
                        snapshot_ts=ts,
                    )

            for edge in edges:
                edge_type = edge.get("edge_type", "calls")
                src = edge.get("source", "")
                dst = edge.get("target", "")
                if edge_type == "calls":
                    session.run(
                        """
                        MATCH (a {id: $src, snapshot_id: $sid})
                        MATCH (b {id: $dst, snapshot_id: $sid})
                        MERGE (a)-[r:CALLS {snapshot_id: $sid}]->(b)
                        SET r.call_count  = $call_count,
                            r.latency_ema = $latency_ema,
                            r.last_seen   = $last_seen,
                            r.stale       = $stale
                        """,
                        sid=snapshot_id, src=src, dst=dst,
                        call_count=int(edge.get("call_count", 0)),
                        latency_ema=float(edge.get("latency_ema", 0.0)),
                        last_seen=float(edge.get("last_seen", ts)),
                        stale=bool(edge.get("stale", False)),
                    )
                elif edge_type == "has_category":
                    session.run(
                        """
                        MATCH (a {id: $src, snapshot_id: $sid})
                        MATCH (b {id: $dst, snapshot_id: $sid})
                        MERGE (a)-[r:HAS_CATEGORY {snapshot_id: $sid}]->(b)
                        """,
                        sid=snapshot_id, src=src, dst=dst,
                    )
                elif edge_type == "has_metric":
                    session.run(
                        """
                        MATCH (a {id: $src, snapshot_id: $sid})
                        MATCH (b {id: $dst, snapshot_id: $sid})
                        MERGE (a)-[r:HAS_METRIC {snapshot_id: $sid}]->(b)
                        """,
                        sid=snapshot_id, src=src, dst=dst,
                    )

        logger.info(f"Saved snapshot {snapshot_id}: {len(nodes)} nodes, {len(edges)} edges")
        return snapshot_id

    def get_snapshot(self, snapshot_id: str) -> dict:
        with self._driver.session() as session:
            nodes_result = session.run(
                "MATCH (n {snapshot_id: $sid}) RETURN n", sid=snapshot_id
            )
            nodes = [dict(r["n"]) for r in nodes_result]
            edges_result = session.run(
                """
                MATCH (a {snapshot_id: $sid})-[r]->(b {snapshot_id: $sid})
                RETURN a.id AS source, b.id AS target, type(r) AS rel_type, r
                """,
                sid=snapshot_id,
            )
            edges = [
                {"source": r["source"], "target": r["target"],
                 "edge_type": r["rel_type"].lower(), **dict(r["r"])}
                for r in edges_result
            ]
        return {"snapshot_id": snapshot_id, "nodes": nodes, "edges": edges}

    def get_snapshots_near_time(self, target_ts: float, window_seconds: float = 120) -> list[dict]:
        with self._driver.session() as session:
            result = session.run(
                """
                MATCH (s:Service)
                WHERE s.snapshot_ts >= $from_ts AND s.snapshot_ts <= $to_ts
                RETURN DISTINCT s.snapshot_id AS snapshot_id, s.snapshot_ts AS snapshot_ts
                ORDER BY s.snapshot_ts DESC
                """,
                from_ts=target_ts - window_seconds,
                to_ts=target_ts + window_seconds,
            )
            return [{"snapshot_id": r["snapshot_id"], "snapshot_ts": r["snapshot_ts"]} for r in result]

    def get_service_history(self, service_name: str, limit: int = 10) -> list[dict]:
        with self._driver.session() as session:
            result = session.run(
                "MATCH (s:Service {name: $name}) RETURN s ORDER BY s.snapshot_ts DESC LIMIT $limit",
                name=service_name, limit=limit,
            )
            return [dict(r["s"]) for r in result]

    def create_indexes(self):
        with self._driver.session() as session:
            stmts = [
                "CREATE INDEX service_id IF NOT EXISTS FOR (n:Service) ON (n.id)",
                "CREATE INDEX service_snapshot IF NOT EXISTS FOR (n:Service) ON (n.snapshot_id)",
                "CREATE INDEX service_ts IF NOT EXISTS FOR (n:Service) ON (n.snapshot_ts)",
                "CREATE INDEX category_id IF NOT EXISTS FOR (n:Category) ON (n.id)",
                "CREATE INDEX metric_id IF NOT EXISTS FOR (n:Metric) ON (n.id)",
            ]
            for stmt in stmts:
                try:
                    session.run(stmt)
                except Exception as e:
                    logger.warning(f"Index creation: {e}")

    def get_topology_context_for_diagnosis(
        self,
        alert_timestamp: float,
        faulted_services: list[str],
        lookback_snapshots: int = 3,
    ) -> dict:
        """
        Read the last `lookback_snapshots` graph snapshots taken before the alert
        fired and extract three signals the LLM can use for root-cause reasoning:

          1. status_progression  — per service: was it healthy → degraded → faulted
                                   or did it jump straight to faulted?
          2. anomaly_trend       — per service: is the anomaly_score rising or flat?
          3. new_call_edges      — any CALLS edge that only appeared in the most
                                   recent snapshot (not present two snapshots ago)
                                   — a new dependency may have introduced the fault

        Returns a compact dict safe to embed in the LLM JSON context.
        Returns {} silently if Neo4j is unreachable or has no snapshots yet.
        """
        try:
            with self._driver.session() as session:
                # Find the N most recent snapshots taken at or before the alert time
                result = session.run(
                    """
                    MATCH (s:Service)
                    WHERE s.snapshot_ts <= $alert_ts
                    RETURN DISTINCT s.snapshot_id AS sid, s.snapshot_ts AS ts
                    ORDER BY s.snapshot_ts DESC
                    LIMIT $limit
                    """,
                    alert_ts=alert_timestamp,
                    limit=lookback_snapshots,
                )
                snapshot_rows = list(result)
                if not snapshot_rows:
                    return {}

                # Order oldest → newest so we can read progression left to right
                snapshots = list(reversed([
                    {"sid": r["sid"], "ts": r["ts"]} for r in snapshot_rows
                ]))

                svc_set = set(faulted_services)

                # ── Signal 1 & 2: status and anomaly_score per service per snapshot ──
                status_progression: dict[str, list[str]] = {}
                anomaly_trend:      dict[str, list[float]] = {}

                for snap in snapshots:
                    srv_result = session.run(
                        """
                        MATCH (s:Service {snapshot_id: $sid})
                        RETURN s.name AS name, s.status AS status,
                               s.anomaly_score AS score
                        """,
                        sid=snap["sid"],
                    )
                    for row in srv_result:
                        name = row["name"]
                        # Include faulted services + their direct neighbours
                        if name not in svc_set:
                            continue
                        status_progression.setdefault(name, []).append(
                            row["status"] or "unknown"
                        )
                        anomaly_trend.setdefault(name, []).append(
                            round(float(row["score"] or 0.5), 3)
                        )

                # ── Signal 3: new call edges in the most recent snapshot ────────────
                new_edges: list[dict] = []
                if len(snapshots) >= 2:
                    latest_sid  = snapshots[-1]["sid"]
                    earlier_sid = snapshots[-2]["sid"]

                    # Edges in latest snapshot
                    latest_edges = session.run(
                        """
                        MATCH (a:Service {snapshot_id: $sid})-[r:CALLS]->(b:Service {snapshot_id: $sid})
                        RETURN a.name AS caller, b.name AS callee,
                               r.call_count AS calls, r.latency_ema AS latency
                        """,
                        sid=latest_sid,
                    )
                    latest_pairs = {
                        (r["caller"], r["callee"]): {
                            "call_count": r["calls"],
                            "latency_ms": round(float(r["latency"] or 0), 1),
                        }
                        for r in latest_edges
                    }

                    # Edges in earlier snapshot
                    earlier_edges = session.run(
                        """
                        MATCH (a:Service {snapshot_id: $sid})-[r:CALLS]->(b:Service {snapshot_id: $sid})
                        RETURN a.name AS caller, b.name AS callee
                        """,
                        sid=earlier_sid,
                    )
                    earlier_pairs = {(r["caller"], r["callee"]) for r in earlier_edges}

                    for (caller, callee), attrs in latest_pairs.items():
                        if (caller, callee) not in earlier_pairs:
                            new_edges.append({
                                "caller": caller,
                                "callee": callee,
                                **attrs,
                                "note": "edge not present in previous snapshot",
                            })

                return {
                    "snapshots_used":     len(snapshots),
                    "oldest_snapshot_age_min": round((alert_timestamp - snapshots[0]["ts"]) / 60, 1),
                    "status_progression": status_progression,
                    "anomaly_score_trend": anomaly_trend,
                    "new_call_edges_since_last_snapshot": new_edges,
                }

        except Exception as e:
            logger.warning(f"Neo4j topology context query failed: {e}")
            return {}

    def ping(self) -> bool:
        try:
            with self._driver.session() as session:
                session.run("RETURN 1")
            return True
        except Exception:
            return False
