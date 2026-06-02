"""
SQLite persistence for OpsCortex.

Tables:
  incidents                      — one row per diagnosed alert
  suppressed_alerts              — noise suppression log
  graph_snapshots                — Neo4j snapshot registry
  metric_baselines               — baseline_model statistics (permanent — survives restart)
  noise_fingerprints             — noise_classifier fingerprint registry
  service_communication_patterns — communication_model call statistics
  topology_edges                 — confirmed caller→callee relationships (permanent topology memory)
  known_services                 — service lifecycle registry (active / offline / deprecated)
  model_training_log             — model_trainer retraining history

Service lifecycle (known_services):
  - A service is written here the first time its Redis node:* keys appear.
  - Every sync cycle refreshes last_seen_in_redis for services that are live.
  - If a service disappears from Redis (deployment, crash) its graph node is marked
    "offline" immediately; it is NOT removed — permanent memory remembers it exists.
  - A periodic maintenance loop promotes a service from "offline" to "deprecated"
    once it has been absent from Redis for longer than DEPRECATION_THRESHOLD_DAYS.
    Deprecated services are pruned from permanent memory and removed from the graph.

topology_edges is the permanent brain for service relationships.  The topology agent
reads it on startup to seed the in-process graph before any Redis data arrives.
"""
import json
import time
import logging
from typing import Optional

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean, Text,
    Index, UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session

logger = logging.getLogger("sqlite_store")

Base = declarative_base()


class Incident(Base):
    __tablename__ = "incidents"
    id                      = Column(Integer, primary_key=True, autoincrement=True)
    timestamp               = Column(Float, nullable=False)
    service                 = Column(String(100), nullable=False)
    category                = Column(String(100), nullable=False)
    metric                  = Column(String(100), nullable=False)
    alert_value             = Column(Float, nullable=False)
    fault_path              = Column(Text)   # JSON list
    context_snapshot        = Column(Text)   # JSON dict
    actions_context         = Column(Text)   # JSON list of recent action events
    llm_root_cause          = Column(Text)
    llm_contributing_factors= Column(Text)   # JSON list
    llm_confidence          = Column(String(20))
    llm_recommended_fix     = Column(Text)
    llm_estimated_impact    = Column(Text)
    llm_raw_response        = Column(Text)
    resolved                = Column(Boolean, default=False)
    resolved_at             = Column(Float, nullable=True)
    # kept for backward compat
    faulted_service         = Column(String(100))
    alert_metric            = Column(String(100))
    diagnosis_summary       = Column(Text)
    root_cause_service      = Column(String(100))


class SuppressedAlert(Base):
    __tablename__ = "suppressed_alerts"
    id               = Column(Integer, primary_key=True, autoincrement=True)
    timestamp        = Column(Float, nullable=False)
    service          = Column(String(100), nullable=False)
    category         = Column(String(100), nullable=False)
    metric           = Column(String(100), nullable=False)
    value            = Column(Float, nullable=False, default=0.0)
    hour_bucket      = Column(Integer, nullable=False)
    suppression_count= Column(Integer, nullable=False, default=1)
    reason           = Column(Text)


class GraphSnapshot(Base):
    __tablename__ = "graph_snapshots"
    id               = Column(Integer, primary_key=True, autoincrement=True)
    timestamp        = Column(Float, nullable=False)
    node_count       = Column(Integer, nullable=False)
    edge_count       = Column(Integer, nullable=False)
    faulted_service  = Column(String(100), nullable=True)
    neo4j_snapshot_id= Column(String(100))
    trigger          = Column(String(50), nullable=False, default="scheduled")


class MetricBaseline(Base):
    __tablename__ = "metric_baselines"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    service     = Column(String(100), nullable=False)
    metric      = Column(String(100), nullable=False)
    hour_of_day = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    mean        = Column(Float, nullable=False)
    std         = Column(Float, nullable=False)
    sample_count= Column(Integer, nullable=False, default=0)
    last_updated= Column(Float, nullable=False)
    __table_args__ = (
        UniqueConstraint("service", "metric", "hour_of_day", "day_of_week"),
    )


class NoiseFingerprint(Base):
    __tablename__ = "noise_fingerprints"
    id               = Column(Integer, primary_key=True, autoincrement=True)
    service          = Column(String(100), nullable=False)
    metric           = Column(String(100), nullable=False)
    hour_of_day      = Column(Integer, nullable=False)
    day_of_week      = Column(Integer, nullable=False)
    occurrence_count = Column(Integer, nullable=False, default=0)
    last_seen        = Column(Float, nullable=False)
    __table_args__ = (
        UniqueConstraint("service", "metric", "hour_of_day", "day_of_week"),
    )


class ServiceCommunicationPattern(Base):
    __tablename__ = "service_communication_patterns"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    caller          = Column(String(100), nullable=False)
    callee          = Column(String(100), nullable=False)
    hour_of_day     = Column(Integer, nullable=False)
    call_count_mean = Column(Float, nullable=False, default=0.0)
    call_count_std  = Column(Float, nullable=False, default=0.0)
    latency_mean    = Column(Float, nullable=False, default=0.0)
    latency_std     = Column(Float, nullable=False, default=0.0)
    sample_count    = Column(Integer, nullable=False, default=0)
    last_updated    = Column(Float, nullable=False)
    __table_args__ = (
        UniqueConstraint("caller", "callee", "hour_of_day"),
    )


class TopologyEdge(Base):
    """
    Permanent record of a confirmed caller→callee service relationship.

    confirmation_count grows every time the topology agent re-observes the edge
    (via OTel label or traffic correlation).  source is how we first learned about
    it: 'otel' (direct trace label) or 'correlation' (Pearson rate correlation).
    Higher confirmation_count = higher confidence the edge is real.
    """
    __tablename__ = "topology_edges"
    id                 = Column(Integer, primary_key=True, autoincrement=True)
    caller             = Column(String(100), nullable=False)
    callee             = Column(String(100), nullable=False)
    source             = Column(String(50), nullable=False, default="correlation")
    confirmation_count = Column(Integer, nullable=False, default=1)
    first_seen         = Column(Float, nullable=False)
    last_seen          = Column(Float, nullable=False)
    avg_latency_ms     = Column(Float, nullable=False, default=0.0)
    __table_args__ = (UniqueConstraint("caller", "callee"),)


class KnownService(Base):
    """
    Single source of truth for service lifecycle.

    status values:
      'active'     — seen in Redis within the last sync cycle
      'offline'    — was active, now absent from Redis (deployment, crash, etc.)
                     Graph node kept but marked offline.  Permanent memory retained.
      'deprecated' — absent from Redis for > DEPRECATION_THRESHOLD_DAYS days.
                     Pruned from permanent memory and removed from the live graph.
    """
    __tablename__ = "known_services"
    id                  = Column(Integer, primary_key=True, autoincrement=True)
    name                = Column(String(100), nullable=False, unique=True)
    status              = Column(String(20), nullable=False, default="active")
    first_seen          = Column(Float, nullable=False)
    last_seen_in_redis  = Column(Float, nullable=False)
    offline_since       = Column(Float, nullable=True)   # set when status → offline
    deprecated_at       = Column(Float, nullable=True)   # set when status → deprecated


class ModelTrainingLog(Base):
    __tablename__ = "model_training_log"
    id               = Column(Integer, primary_key=True, autoincrement=True)
    timestamp        = Column(Float, nullable=False)
    service          = Column(String(100), nullable=False)
    sample_count     = Column(Integer, nullable=False)
    training_duration= Column(Float, nullable=False)
    model_path       = Column(String(500))


class SQLiteStore:
    def __init__(self, db_path: str = "/data/service_brain.db"):
        self._engine = create_engine(
            f"sqlite:///{db_path}",
            connect_args={"check_same_thread": False},
        )
        Base.metadata.create_all(self._engine)
        self._Session = sessionmaker(bind=self._engine)

    def _session(self) -> Session:
        return self._Session()

    # ── Incidents ─────────────────────────────────────────────────────────────

    def record_incident(
        self,
        service: str,
        category: str,
        metric: str,
        alert_value: float,
        fault_path: list,
        context_snapshot: dict,
        actions_context: list,
        llm_root_cause: str = "",
        llm_contributing_factors: Optional[list] = None,
        llm_confidence: str = "",
        llm_recommended_fix: str = "",
        llm_estimated_impact: str = "",
        llm_raw_response: str = "",
        # backward compat params
        faulted_service: Optional[str] = None,
        alert_metric: Optional[str] = None,
        diagnosis_summary: Optional[str] = None,
        root_cause_service: Optional[str] = None,
    ) -> int:
        session = self._session()
        try:
            inc = Incident(
                timestamp=time.time(),
                service=service,
                category=category,
                metric=metric,
                alert_value=alert_value,
                fault_path=json.dumps(fault_path),
                context_snapshot=json.dumps(context_snapshot),
                actions_context=json.dumps(actions_context),
                llm_root_cause=llm_root_cause,
                llm_contributing_factors=json.dumps(llm_contributing_factors or []),
                llm_confidence=llm_confidence,
                llm_recommended_fix=llm_recommended_fix,
                llm_estimated_impact=llm_estimated_impact,
                llm_raw_response=llm_raw_response,
                resolved=False,
                faulted_service=faulted_service or service,
                alert_metric=alert_metric or metric,
                diagnosis_summary=diagnosis_summary or "",
                root_cause_service=root_cause_service or service,
            )
            session.add(inc)
            session.commit()
            return inc.id
        finally:
            session.close()

    def resolve_incident(self, incident_id: int):
        session = self._session()
        try:
            inc = session.query(Incident).filter_by(id=incident_id).first()
            if inc:
                inc.resolved = True
                inc.resolved_at = time.time()
                session.commit()
        finally:
            session.close()

    def get_recent_incidents(self, limit: int = 10) -> list[dict]:
        session = self._session()
        try:
            rows = session.query(Incident).order_by(Incident.timestamp.desc()).limit(limit).all()
            return [self._incident_to_dict(r) for r in rows]
        finally:
            session.close()

    def get_last_incident(self) -> Optional[dict]:
        session = self._session()
        try:
            row = session.query(Incident).order_by(Incident.timestamp.desc()).first()
            return self._incident_to_dict(row) if row else None
        finally:
            session.close()

    def get_unresolved_incidents(self) -> list[dict]:
        session = self._session()
        try:
            rows = session.query(Incident).filter_by(resolved=False).order_by(Incident.timestamp.desc()).all()
            return [self._incident_to_dict(r) for r in rows]
        finally:
            session.close()

    def get_recent_incidents_for_service(self, service: str, limit: int = 3) -> list[dict]:
        session = self._session()
        try:
            rows = (
                session.query(Incident)
                .filter(Incident.service == service)
                .order_by(Incident.timestamp.desc())
                .limit(limit)
                .all()
            )
            return [self._incident_to_dict(r) for r in rows]
        finally:
            session.close()

    def _incident_to_dict(self, r: Incident) -> dict:
        return {
            "id": r.id,
            "timestamp": r.timestamp,
            "service": r.service or r.faulted_service,
            "category": r.category or "",
            "metric": r.metric or r.alert_metric,
            "alert_value": r.alert_value,
            "fault_path": json.loads(r.fault_path) if r.fault_path else [],
            "context_snapshot": json.loads(r.context_snapshot) if r.context_snapshot else {},
            "actions_context": json.loads(r.actions_context) if r.actions_context else [],
            "llm_root_cause": r.llm_root_cause or "",
            "llm_contributing_factors": json.loads(r.llm_contributing_factors) if r.llm_contributing_factors else [],
            "llm_confidence": r.llm_confidence or "",
            "llm_recommended_fix": r.llm_recommended_fix or "",
            "llm_estimated_impact": r.llm_estimated_impact or "",
            "llm_raw_response": r.llm_raw_response or "",
            "resolved": r.resolved,
            "resolved_at": r.resolved_at,
            # backward compat
            "faulted_service": r.faulted_service or r.service,
            "alert_metric": r.alert_metric or r.metric,
            "diagnosis_summary": r.diagnosis_summary or "",
            "root_cause_service": r.root_cause_service or r.service,
        }

    # ── Suppressed alerts ─────────────────────────────────────────────────────

    def record_suppressed_alert(
        self,
        service: str,
        category: str,
        metric: str,
        value: float,
        hour_bucket: int,
        suppression_count: int,
        reason: str,
        # backward compat
        hour_of_day: Optional[int] = None,
        suppression_reason: Optional[str] = None,
    ) -> int:
        session = self._session()
        try:
            sa = SuppressedAlert(
                timestamp=time.time(),
                service=service,
                category=category,
                metric=metric,
                value=value,
                hour_bucket=hour_bucket if hour_bucket is not None else (hour_of_day or 0),
                suppression_count=suppression_count,
                reason=reason or suppression_reason or "",
            )
            session.add(sa)
            session.commit()
            return sa.id
        finally:
            session.close()

    def get_recent_suppressed_alerts(self, limit: int = 10) -> list[dict]:
        session = self._session()
        try:
            rows = session.query(SuppressedAlert).order_by(SuppressedAlert.timestamp.desc()).limit(limit).all()
            return [
                {
                    "id": r.id,
                    "timestamp": r.timestamp,
                    "service": r.service,
                    "category": r.category,
                    "metric": r.metric,
                    "value": r.value,
                    "hour_bucket": r.hour_bucket,
                    "suppression_count": r.suppression_count,
                    "reason": r.reason,
                    # compat keys
                    "hour_of_day": r.hour_bucket,
                    "suppression_reason": r.reason,
                }
                for r in rows
            ]
        finally:
            session.close()

    # ── Graph snapshots ───────────────────────────────────────────────────────

    def record_graph_snapshot(
        self,
        node_count: int,
        edge_count: int,
        trigger: str = "scheduled",
        neo4j_snapshot_id: Optional[str] = None,
        faulted_service: Optional[str] = None,
    ) -> int:
        session = self._session()
        try:
            snap = GraphSnapshot(
                timestamp=time.time(),
                node_count=node_count,
                edge_count=edge_count,
                trigger=trigger,
                neo4j_snapshot_id=neo4j_snapshot_id,
                faulted_service=faulted_service,
            )
            session.add(snap)
            session.commit()
            return snap.id
        finally:
            session.close()

    # ── Metric baselines ──────────────────────────────────────────────────────

    def upsert_metric_baseline(
        self,
        service: str,
        metric: str,
        hour_of_day: int,
        day_of_week: int,
        mean: float,
        std: float,
        sample_count: int,
    ):
        session = self._session()
        try:
            row = session.query(MetricBaseline).filter_by(
                service=service, metric=metric,
                hour_of_day=hour_of_day, day_of_week=day_of_week,
            ).first()
            if row:
                row.mean = mean
                row.std = std
                row.sample_count = sample_count
                row.last_updated = time.time()
            else:
                session.add(MetricBaseline(
                    service=service, metric=metric,
                    hour_of_day=hour_of_day, day_of_week=day_of_week,
                    mean=mean, std=std, sample_count=sample_count,
                    last_updated=time.time(),
                ))
            session.commit()
        finally:
            session.close()

    def get_metric_baseline(
        self, service: str, metric: str, hour_of_day: int, day_of_week: int
    ) -> Optional[dict]:
        session = self._session()
        try:
            row = session.query(MetricBaseline).filter_by(
                service=service, metric=metric,
                hour_of_day=hour_of_day, day_of_week=day_of_week,
            ).first()
            if row:
                return {"mean": row.mean, "std": row.std, "sample_count": row.sample_count}
            return None
        finally:
            session.close()

    def get_all_baselines_for_service(self, service: str) -> list[dict]:
        session = self._session()
        try:
            rows = session.query(MetricBaseline).filter_by(service=service).all()
            return [
                {"metric": r.metric, "hour_of_day": r.hour_of_day, "day_of_week": r.day_of_week,
                 "mean": r.mean, "std": r.std, "sample_count": r.sample_count}
                for r in rows
            ]
        finally:
            session.close()

    # ── Noise fingerprints ────────────────────────────────────────────────────

    def upsert_noise_fingerprint(
        self,
        service: str,
        metric: str,
        hour_of_day: int,
        day_of_week: int,
        occurrence_count: int,
    ):
        session = self._session()
        try:
            row = session.query(NoiseFingerprint).filter_by(
                service=service, metric=metric,
                hour_of_day=hour_of_day, day_of_week=day_of_week,
            ).first()
            if row:
                row.occurrence_count = occurrence_count
                row.last_seen = time.time()
            else:
                session.add(NoiseFingerprint(
                    service=service, metric=metric,
                    hour_of_day=hour_of_day, day_of_week=day_of_week,
                    occurrence_count=occurrence_count,
                    last_seen=time.time(),
                ))
            session.commit()
        finally:
            session.close()

    def get_noise_fingerprint(
        self, service: str, metric: str, hour_of_day: int, day_of_week: int
    ) -> Optional[dict]:
        session = self._session()
        try:
            row = session.query(NoiseFingerprint).filter_by(
                service=service, metric=metric,
                hour_of_day=hour_of_day, day_of_week=day_of_week,
            ).first()
            if row:
                return {"occurrence_count": row.occurrence_count, "last_seen": row.last_seen}
            return None
        finally:
            session.close()

    # ── Communication patterns ────────────────────────────────────────────────

    def upsert_communication_pattern(
        self,
        caller: str,
        callee: str,
        hour_of_day: int,
        call_count_mean: float,
        call_count_std: float,
        latency_mean: float,
        latency_std: float,
        sample_count: int,
    ):
        session = self._session()
        try:
            row = session.query(ServiceCommunicationPattern).filter_by(
                caller=caller, callee=callee, hour_of_day=hour_of_day,
            ).first()
            if row:
                row.call_count_mean = call_count_mean
                row.call_count_std = call_count_std
                row.latency_mean = latency_mean
                row.latency_std = latency_std
                row.sample_count = sample_count
                row.last_updated = time.time()
            else:
                session.add(ServiceCommunicationPattern(
                    caller=caller, callee=callee, hour_of_day=hour_of_day,
                    call_count_mean=call_count_mean, call_count_std=call_count_std,
                    latency_mean=latency_mean, latency_std=latency_std,
                    sample_count=sample_count, last_updated=time.time(),
                ))
            session.commit()
        finally:
            session.close()

    def get_communication_pattern(self, caller: str, callee: str, hour_of_day: int) -> Optional[dict]:
        session = self._session()
        try:
            row = session.query(ServiceCommunicationPattern).filter_by(
                caller=caller, callee=callee, hour_of_day=hour_of_day,
            ).first()
            if row:
                return {
                    "call_count_mean": row.call_count_mean,
                    "call_count_std": row.call_count_std,
                    "latency_mean": row.latency_mean,
                    "latency_std": row.latency_std,
                    "sample_count": row.sample_count,
                }
            return None
        finally:
            session.close()

    # ── Service lifecycle registry ────────────────────────────────────────────

    def mark_service_active(self, service: str):
        """
        Called every sync cycle for each service seen in Redis.
        Creates the row on first sight; updates last_seen_in_redis and resets
        status to 'active' if the service was previously offline.
        """
        now = time.time()
        session = self._session()
        try:
            row = session.query(KnownService).filter_by(name=service).first()
            if row:
                prev_status = row.status
                row.last_seen_in_redis = now
                row.status = "active"
                row.offline_since = None
                if prev_status != "active":
                    logger.info(f"Service '{service}' is back online (was {prev_status})")
            else:
                logger.info(f"Service '{service}' seen for the first time — registering in permanent memory")
                session.add(KnownService(
                    name=service,
                    status="active",
                    first_seen=now,
                    last_seen_in_redis=now,
                ))
            session.commit()
        finally:
            session.close()

    def mark_services_offline(self, absent_services: list[str]):
        """
        Called when services that exist in permanent memory are no longer seen in
        Redis (deployment gap, crash, etc.).  Sets status to 'offline' and records
        offline_since if this is the first cycle they've been absent.
        Does NOT remove them — permanent memory retains knowledge of their existence.
        """
        if not absent_services:
            return
        now = time.time()
        session = self._session()
        try:
            for name in absent_services:
                row = session.query(KnownService).filter_by(name=name).first()
                if row and row.status == "active":
                    row.status = "offline"
                    row.offline_since = now
                    logger.info(f"Service '{name}' went offline — marking in permanent memory (not removing)")
            session.commit()
        finally:
            session.close()

    def get_all_known_services(self) -> list[dict]:
        """Return all services in permanent memory (active, offline, and deprecated)."""
        session = self._session()
        try:
            rows = session.query(KnownService).order_by(KnownService.name).all()
            return [
                {
                    "name": r.name,
                    "status": r.status,
                    "first_seen": r.first_seen,
                    "last_seen_in_redis": r.last_seen_in_redis,
                    "offline_since": r.offline_since,
                    "deprecated_at": r.deprecated_at,
                }
                for r in rows
            ]
        finally:
            session.close()

    def get_active_and_offline_services(self) -> list[dict]:
        """Return services that are known but NOT yet deprecated."""
        session = self._session()
        try:
            rows = (
                session.query(KnownService)
                .filter(KnownService.status.in_(["active", "offline"]))
                .all()
            )
            return [
                {
                    "name": r.name,
                    "status": r.status,
                    "last_seen_in_redis": r.last_seen_in_redis,
                    "offline_since": r.offline_since,
                }
                for r in rows
            ]
        finally:
            session.close()

    def deprecate_and_purge_service(self, service: str) -> dict:
        """
        Mark a service as deprecated and remove all its permanent memory:
          - known_services row status → 'deprecated'
          - metric_baselines rows deleted
          - noise_fingerprints rows deleted
          - topology_edges rows where caller or callee = service deleted
          - service_communication_patterns rows deleted

        The incidents table is NOT touched — historical incident records are kept
        for audit purposes even after a service is retired.

        Returns a summary dict of what was purged.
        """
        now = time.time()
        session = self._session()
        purged = {"baselines": 0, "noise_fps": 0, "edges": 0, "comm_patterns": 0}
        try:
            row = session.query(KnownService).filter_by(name=service).first()
            if row:
                row.status = "deprecated"
                row.deprecated_at = now

            purged["baselines"] = session.query(MetricBaseline).filter_by(service=service).delete()
            purged["noise_fps"] = session.query(NoiseFingerprint).filter_by(service=service).delete()
            purged["edges"] = (
                session.query(TopologyEdge)
                .filter(
                    (TopologyEdge.caller == service) | (TopologyEdge.callee == service)
                )
                .delete(synchronize_session=False)
            )
            purged["comm_patterns"] = (
                session.query(ServiceCommunicationPattern)
                .filter(
                    (ServiceCommunicationPattern.caller == service) |
                    (ServiceCommunicationPattern.callee == service)
                )
                .delete(synchronize_session=False)
            )
            session.commit()
            logger.info(
                f"Deprecated '{service}': purged baselines={purged['baselines']} "
                f"noise_fps={purged['noise_fps']} edges={purged['edges']} "
                f"comm_patterns={purged['comm_patterns']}"
            )
        finally:
            session.close()
        return purged

    # ── Topology edges (permanent brain memory) ───────────────────────────────

    def upsert_topology_edge(
        self,
        caller: str,
        callee: str,
        source: str = "correlation",
        latency_ms: float = 0.0,
    ):
        """
        Record or reinforce a caller→callee edge.  On first sight creates the row;
        on subsequent calls increments confirmation_count and updates last_seen/latency.
        """
        now = time.time()
        session = self._session()
        try:
            row = session.query(TopologyEdge).filter_by(caller=caller, callee=callee).first()
            if row:
                row.confirmation_count += 1
                row.last_seen = now
                # EMA update on latency (only when a real measurement is provided)
                if latency_ms > 0:
                    alpha = 0.1
                    row.avg_latency_ms = alpha * latency_ms + (1 - alpha) * row.avg_latency_ms
                # Prefer 'otel' source label over 'correlation' once we have direct evidence
                if source == "otel":
                    row.source = "otel"
            else:
                session.add(TopologyEdge(
                    caller=caller,
                    callee=callee,
                    source=source,
                    confirmation_count=1,
                    first_seen=now,
                    last_seen=now,
                    avg_latency_ms=latency_ms,
                ))
            session.commit()
        finally:
            session.close()

    def get_all_topology_edges(self) -> list[dict]:
        """Return all known edges sorted by confidence (confirmation_count desc)."""
        session = self._session()
        try:
            rows = (
                session.query(TopologyEdge)
                .order_by(TopologyEdge.confirmation_count.desc())
                .all()
            )
            return [
                {
                    "caller": r.caller,
                    "callee": r.callee,
                    "source": r.source,
                    "confirmation_count": r.confirmation_count,
                    "first_seen": r.first_seen,
                    "last_seen": r.last_seen,
                    "avg_latency_ms": r.avg_latency_ms,
                }
                for r in rows
            ]
        finally:
            session.close()

    def get_known_services_from_baselines(self) -> list[str]:
        """
        Return all service names that have ever had a metric baseline recorded.
        Used by the topology agent to pre-populate service nodes on cold start
        before any Redis data has arrived.
        """
        session = self._session()
        try:
            rows = session.query(MetricBaseline.service).distinct().all()
            return [r[0] for r in rows]
        finally:
            session.close()

    def get_known_metrics_for_service(self, service: str) -> list[dict]:
        """
        Return all (metric, hour_of_day) pairs ever seen for a service.
        The topology agent uses this to pre-create metric nodes on cold start.
        """
        session = self._session()
        try:
            rows = (
                session.query(MetricBaseline.metric, MetricBaseline.hour_of_day)
                .filter_by(service=service)
                .distinct()
                .all()
            )
            return [{"metric": r[0], "hour_of_day": r[1]} for r in rows]
        finally:
            session.close()

    # ── Model training log ────────────────────────────────────────────────────

    def record_model_training(
        self,
        service: str,
        sample_count: int,
        training_duration: float,
        model_path: str = "",
    ) -> int:
        session = self._session()
        try:
            log = ModelTrainingLog(
                timestamp=time.time(),
                service=service,
                sample_count=sample_count,
                training_duration=training_duration,
                model_path=model_path,
            )
            session.add(log)
            session.commit()
            return log.id
        finally:
            session.close()
