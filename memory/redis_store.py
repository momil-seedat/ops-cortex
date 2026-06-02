"""
Redis working memory for OpsCortex.

Key schema:
  node:{service}:{category}:{metric}          HASH   – current state (value, status, ts, thresholds)
  timeseries:{service}:{category}:{metric}    LIST   – JSON entries, capped at 300
  actions:{service}                           STREAM – operational events
  stream:alerts                               STREAM – Alertmanager / monitoring-agent alerts
  last_alert:{service}:{metric}              STRING – cooldown timestamp
  service:status                             HASH   – quick service-level status lookup
"""
import json
import time
import logging
from typing import Optional

import redis

logger = logging.getLogger("redis_store")

TIMESERIES_MAX = 300
METRIC_TTL = 7200          # 2 h — longer than 300 × 10s = 3000s
ALERT_STREAM = "stream:alerts"
ALERT_CONSUMER_GROUP = "diagnosis-agents"


class RedisStore:
    def __init__(self, host: str = "redis", port: int = 6379):
        self._client = redis.Redis(host=host, port=port, decode_responses=True)

    # ── Current state hash ────────────────────────────────────────────────────

    def set_metric_state(
        self,
        service: str,
        category: str,
        metric: str,
        value: float,
        status: str,
        threshold_low: float = 0.0,
        threshold_high: float = 0.0,
        timestamp: Optional[float] = None,
    ):
        ts = timestamp or time.time()
        key = f"node:{service}:{category}:{metric}"
        self._client.hset(key, mapping={
            "value": str(value),
            "status": status,
            "timestamp": str(ts),
            "threshold_low": str(threshold_low),
            "threshold_high": str(threshold_high),
        })
        self._client.expire(key, METRIC_TTL)

    def get_metric_state(self, service: str, category: str, metric: str) -> Optional[dict]:
        key = f"node:{service}:{category}:{metric}"
        data = self._client.hgetall(key)
        if not data:
            return None
        return {
            "value": float(data.get("value", 0.0)),
            "status": data.get("status", "green"),
            "timestamp": float(data.get("timestamp", 0.0)),
            "threshold_low": float(data.get("threshold_low", 0.0)),
            "threshold_high": float(data.get("threshold_high", 0.0)),
        }

    # ── Time series list ──────────────────────────────────────────────────────

    def push_timeseries(
        self,
        service: str,
        category: str,
        metric: str,
        value: float,
        timestamp: Optional[float] = None,
    ):
        ts = timestamp or time.time()
        key = f"timeseries:{service}:{category}:{metric}"
        entry = json.dumps({"ts": ts, "value": value})
        pipe = self._client.pipeline()
        pipe.rpush(key, entry)
        pipe.ltrim(key, -TIMESERIES_MAX, -1)
        pipe.expire(key, METRIC_TTL)
        pipe.execute()

    def get_timeseries(
        self,
        service: str,
        category: str,
        metric: str,
        count: int = 50,
    ) -> list[dict]:
        key = f"timeseries:{service}:{category}:{metric}"
        raw = self._client.lrange(key, -count, -1)
        return [json.loads(r) for r in raw]

    # ── Legacy metric access (push_metric / get_metrics) kept for compat ──────

    def push_metric(self, service: str, metric_name: str, value: float, timestamp: Optional[float] = None):
        """Compatibility shim — stores under timeseries:{service}:_raw:{metric_name}."""
        ts = timestamp or time.time()
        key = f"metric:{service}:{metric_name}"
        entry = json.dumps({"ts": ts, "value": value})
        pipe = self._client.pipeline()
        pipe.rpush(key, entry)
        pipe.ltrim(key, -TIMESERIES_MAX, -1)
        pipe.expire(key, METRIC_TTL)
        pipe.execute()

    def get_metrics(self, service: str, metric_name: str, count: int = 50) -> list[dict]:
        key = f"metric:{service}:{metric_name}"
        raw = self._client.lrange(key, -count, -1)
        return [json.loads(r) for r in raw]

    def get_latest_metric(self, service: str, metric_name: str) -> Optional[dict]:
        key = f"metric:{service}:{metric_name}"
        raw = self._client.lindex(key, -1)
        return json.loads(raw) if raw else None

    # ── Actions stream ────────────────────────────────────────────────────────

    def append_action(
        self,
        service: str,
        event_type: str,
        triggered_by: str = "system",
        details: str = "",
    ) -> str:
        key = f"actions:{service}"
        entry_id = self._client.xadd(key, {
            "event_type": event_type,
            "timestamp": str(time.time()),
            "triggered_by": triggered_by,
            "details": details,
        })
        return entry_id

    def get_recent_actions(self, service: str, count: int = 50) -> list[dict]:
        key = f"actions:{service}"
        try:
            entries = self._client.xrevrange(key, count=count)
            result = []
            for entry_id, fields in entries:
                result.append({
                    "id": entry_id,
                    "service": service,
                    "event_type": fields.get("event_type", ""),
                    "timestamp": float(fields.get("timestamp", 0)),
                    "triggered_by": fields.get("triggered_by", ""),
                    "details": fields.get("details", ""),
                })
            return result
        except Exception:
            return []

    def get_recent_actions_window(self, service: str, window_seconds: int = 1800) -> list[dict]:
        cutoff = time.time() - window_seconds
        actions = self.get_recent_actions(service, count=200)
        return [a for a in actions if a["timestamp"] >= cutoff]

    # ── Alert stream ──────────────────────────────────────────────────────────

    def ensure_alert_consumer_group(self, group: str = ALERT_CONSUMER_GROUP):
        try:
            self._client.xgroup_create(ALERT_STREAM, group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def publish_alert_stream(
        self,
        service: str,
        category: str,
        metric: str,
        value: float,
        threshold: float,
        severity: str,
        timestamp: Optional[float] = None,
    ) -> str:
        ts = timestamp or time.time()
        entry_id = self._client.xadd(ALERT_STREAM, {
            "service": service,
            "category": category,
            "metric": metric,
            "value": str(value),
            "threshold": str(threshold),
            "severity": severity,
            "timestamp": str(ts),
        })
        logger.info(f"Alert stream: {service}/{category}/{metric}={value:.3f} [{severity}]")
        return entry_id

    def read_alert_stream(
        self,
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 1000,
    ) -> list[dict]:
        try:
            raw = self._client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={ALERT_STREAM: ">"},
                count=count,
                block=block_ms,
            )
            if not raw:
                return []
            alerts = []
            for stream_name, entries in raw:
                for entry_id, fields in entries:
                    alerts.append({
                        "stream_id": entry_id,
                        "service": fields.get("service", ""),
                        "category": fields.get("category", ""),
                        "metric": fields.get("metric", ""),
                        "value": float(fields.get("value", 0.0)),
                        "threshold": float(fields.get("threshold", 0.0)),
                        "severity": fields.get("severity", "warning"),
                        "timestamp": float(fields.get("timestamp", time.time())),
                    })
            return alerts
        except Exception as e:
            logger.warning(f"Alert stream read error: {e}")
            return []

    def ack_alert(self, entry_id: str, group: str = ALERT_CONSUMER_GROUP):
        try:
            self._client.xack(ALERT_STREAM, group, entry_id)
        except Exception as e:
            logger.warning(f"Alert ack error: {e}")

    # ── Legacy pub/sub alert (kept for monitoring_agent compat) ───────────────

    ALERT_PUBSUB_CHANNEL = "alerts"

    def publish_alert(self, service: str, metric: str, value: float, threshold: float, message: str):
        alert = {
            "service": service,
            "metric": metric,
            "value": value,
            "threshold": threshold,
            "message": message,
            "timestamp": time.time(),
        }
        self._client.publish(self.ALERT_PUBSUB_CHANNEL, json.dumps(alert))

    def subscribe_alerts(self):
        pubsub = self._client.pubsub()
        pubsub.subscribe(self.ALERT_PUBSUB_CHANNEL)
        return pubsub

    # ── Alert cooldown ────────────────────────────────────────────────────────

    def set_last_alert_time(self, service: str, metric: str):
        self._client.set(f"last_alert:{service}:{metric}", time.time(), ex=3600)

    def get_last_alert_time(self, service: str, metric: str) -> Optional[float]:
        val = self._client.get(f"last_alert:{service}:{metric}")
        return float(val) if val else None

    # ── Service status hash ───────────────────────────────────────────────────

    def set_service_status(self, service: str, status: str):
        self._client.hset("service:status", service, status)
        self._client.expire("service:status", METRIC_TTL)

    def get_service_status(self, service: str) -> str:
        return self._client.hget("service:status", service) or "unknown"

    # ── Context helper ────────────────────────────────────────────────────────

    def get_context_for_service(self, service: str, metrics: list[str], count: int = 50) -> dict:
        ctx = {}
        for metric in metrics:
            ctx[metric] = self.get_metrics(service, metric, count)
        return ctx

    # ── Discovery helpers (used by topology agent) ────────────────────────────

    def discover_node_keys(self) -> list[tuple[str, str, str]]:
        """
        Scan all node:{service}:{category}:{metric} keys and return the unique
        (service, category, metric) triples that currently exist in Redis.
        No hardcoded service/category/metric lists — everything is inferred from
        what monitoring agents have actually written.
        """
        found: list[tuple[str, str, str]] = []
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match="node:*:*:*", count=200)
            for key in keys:
                parts = key.split(":", 3)
                if len(parts) == 4:
                    _, service, category, metric = parts
                    found.append((service, category, metric))
            if cursor == 0:
                break
        return found

    def discover_services(self) -> list[str]:
        """Return distinct service names seen in node:* keys."""
        return list({svc for svc, _, _ in self.discover_node_keys()})

    def discover_categories_for_service(self, service: str) -> list[str]:
        """Return distinct category names for a given service."""
        return list({cat for svc, cat, _ in self.discover_node_keys() if svc == service})

    def discover_metrics_for_service_category(self, service: str, category: str) -> list[str]:
        """Return distinct metric names for a given (service, category) pair."""
        return [m for svc, cat, m in self.discover_node_keys() if svc == service and cat == category]

    def get_recent_request_rates(self, service: str, count: int = 5) -> list[float]:
        """Return the last `count` request_rate values for a service across any category."""
        cursor = 0
        key = None
        while True:
            cursor, keys = self._client.scan(
                cursor, match=f"timeseries:{service}:*:request_rate", count=50
            )
            if keys:
                key = keys[0]
                break
            if cursor == 0:
                break
        if not key:
            return []
        raw = self._client.lrange(key, -count, -1)
        result = []
        for r in raw:
            try:
                result.append(float(json.loads(r).get("value", 0.0)))
            except Exception:
                pass
        return result

    # ── Misc ──────────────────────────────────────────────────────────────────

    def ping(self) -> bool:
        try:
            return self._client.ping()
        except Exception:
            return False

    def get_client(self):
        return self._client
