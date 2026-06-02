import os

# ── Service URLs ───────────────────────────────────────────────────────────────
ORDER_SERVICE_URL     = os.getenv("ORDER_SERVICE_URL",     "http://order-service:8001")
PAYMENT_SERVICE_URL   = os.getenv("PAYMENT_SERVICE_URL",   "http://payment-service:8002")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://inventory-service:8003")

ORDER_SERVICE_PORT    = int(os.getenv("ORDER_SERVICE_PORT",    "8001"))
PAYMENT_SERVICE_PORT  = int(os.getenv("PAYMENT_SERVICE_PORT",  "8002"))
INVENTORY_SERVICE_PORT= int(os.getenv("INVENTORY_SERVICE_PORT","8003"))

# ── Infrastructure URLs ────────────────────────────────────────────────────────
REDIS_URL    = os.getenv("REDIS_URL",    "redis://redis:6379")
REDIS_HOST   = os.getenv("REDIS_HOST",  "redis")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))

NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://neo4j:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")

PROMETHEUS_URL       = os.getenv("PROMETHEUS_URL",       "http://prometheus:9090")
OTEL_COLLECTOR_URL   = os.getenv("OTEL_COLLECTOR_URL",   "http://otel-collector:4318")
OTEL_EXPORTER_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "/data/service_brain.db")

# ── LLM ────────────────────────────────────────────────────────────────────────
LLM_PROVIDER     = os.getenv("LLM_PROVIDER",     "claude")   # "claude", "openai", or "ollama"
LLM_MODEL        = os.getenv("LLM_MODEL",        "claude-sonnet-4-6")
ANTHROPIC_API_KEY= os.getenv("ANTHROPIC_API_KEY","")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY",   "")
OLLAMA_BASE_URL  = os.getenv("OLLAMA_BASE_URL",  "http://localhost:11434")

# ── Redis ──────────────────────────────────────────────────────────────────────
REDIS_TIMESERIES_MAX   = 300          # capped list length (50 min @ 10s)
REDIS_METRIC_TTL_SECONDS = 7200       # 2h TTL on all metric keys
REDIS_CONTEXT_WINDOW   = 50           # entries pulled for diagnosis context
ALERT_STREAM_CONSUMER_GROUP = os.getenv("ALERT_STREAM_CONSUMER_GROUP", "diagnosis-agents")

# ── Polling intervals ──────────────────────────────────────────────────────────
MONITORING_PROMETHEUS_INTERVAL_SECONDS = 10
MONITORING_OTEL_INTERVAL_SECONDS       = 5
TOPOLOGY_SYNC_INTERVAL_SECONDS         = 5
TOPOLOGY_NEO4J_SNAPSHOT_INTERVAL_SECONDS = 60
TOPOLOGY_EDGE_STALE_SECONDS            = 300

# ── ML ─────────────────────────────────────────────────────────────────────────
ISOLATION_FOREST_CONTAMINATION = float(os.getenv("ISOLATION_FOREST_CONTAMINATION", "0.05"))
ISOLATION_FOREST_N_ESTIMATORS  = int(os.getenv("ISOLATION_FOREST_N_ESTIMATORS",   "100"))
NOISE_SUPPRESSION_THRESHOLD    = int(os.getenv("NOISE_SUPPRESSION_THRESHOLD",      "3"))
NOISE_SUPPRESSION_WINDOW_HOURS = int(os.getenv("NOISE_SUPPRESSION_WINDOW_HOURS",   "1"))
ML_SAVED_MODELS_DIR            = os.getenv("ML_SAVED_MODELS_DIR", "/data/ml_models")

# ── Service names ──────────────────────────────────────────────────────────────
SERVICES = ["order_service", "payment_service", "inventory_service"]
SERVICE_NAME_MAP = {
    "order_service":     ORDER_SERVICE_URL,
    "payment_service":   PAYMENT_SERVICE_URL,
    "inventory_service": INVENTORY_SERVICE_URL,
}

# ── Prometheus metric names ────────────────────────────────────────────────────
PROM_ERROR_RATE_METRIC       = "service_error_rate"
PROM_LATENCY_P99_METRIC      = "service_request_duration_p99"
PROM_REQUEST_RATE_METRIC     = "service_request_rate"
PROM_DB_CONNECTIONS_METRIC   = "service_db_connections_active"
PROM_KAFKA_LAG_METRIC        = "service_kafka_consumer_lag"
PROM_RECORDS_PROCESSED_METRIC= "service_records_processed"
PROM_BATCH_JOB_RUNNING_METRIC= "service_batch_job_running"

# ── Graph category → Prometheus metric mapping ────────────────────────────────
# Used by monitoring agent to write into the correct category bucket.
# This is a hint only — the topology agent discovers categories from Redis keys
# and will use this map to assign a sensible category when first seen.
METRIC_CATEGORY_MAP: dict[str, str] = {
    "error_rate":            "performance",
    "latency_p99":           "performance",
    "request_rate":          "performance",
    "cpu_usage":             "performance",
    "memory_usage":          "performance",
    "db_connection_count":   "database",
    "db_query_time":         "database",
    "db_error_count":        "database",
    "consumer_lag":          "kafka",
    "producer_error_rate":   "kafka",
    "consumer_offset_delta": "kafka",
    "last_deployment":       "actions",
    "last_restart":          "actions",
    "last_config_change":    "actions",
    "last_scaling_event":    "actions",
    # batch-service metrics
    "records_processed":     "batch",
    "batch_job_running":     "batch",
    "batch_job_duration_secs":"batch",
}

# Prometheus metric name → graph metric name
PROM_TO_GRAPH_METRIC: dict[str, str] = {
    PROM_ERROR_RATE_METRIC:        "error_rate",
    PROM_LATENCY_P99_METRIC:       "latency_p99",
    PROM_REQUEST_RATE_METRIC:      "request_rate",
    PROM_DB_CONNECTIONS_METRIC:    "db_connection_count",
    PROM_KAFKA_LAG_METRIC:         "consumer_lag",
    PROM_RECORDS_PROCESSED_METRIC: "records_processed",
    PROM_BATCH_JOB_RUNNING_METRIC: "batch_job_running",
}

# ── Per-metric thresholds (amber and red) ─────────────────────────────────────
# Format: metric_name -> {"amber": value, "red": value}
METRIC_THRESHOLDS: dict[str, dict[str, float]] = {
    "error_rate": {
        "amber": float(os.getenv("THRESH_ERROR_RATE_AMBER", "0.05")),
        "red":   float(os.getenv("THRESH_ERROR_RATE_RED",   "0.20")),
    },
    "latency_p99": {
        "amber": float(os.getenv("THRESH_LATENCY_AMBER", "300")),
        "red":   float(os.getenv("THRESH_LATENCY_RED",   "800")),
    },
    "request_rate": {
        "amber": float(os.getenv("THRESH_REQUEST_RATE_AMBER", "80")),
        "red":   float(os.getenv("THRESH_REQUEST_RATE_RED",   "130")),
    },
    "cpu_usage": {
        "amber": float(os.getenv("THRESH_CPU_AMBER", "70")),
        "red":   float(os.getenv("THRESH_CPU_RED",   "90")),
    },
    "memory_usage": {
        "amber": float(os.getenv("THRESH_MEM_AMBER", "75")),
        "red":   float(os.getenv("THRESH_MEM_RED",   "90")),
    },
    "db_connection_count": {
        "amber": float(os.getenv("THRESH_DB_CONN_AMBER", "14")),
        "red":   float(os.getenv("THRESH_DB_CONN_RED",   "18")),
    },
    "db_query_time": {
        "amber": float(os.getenv("THRESH_DB_QUERY_AMBER", "200")),
        "red":   float(os.getenv("THRESH_DB_QUERY_RED",   "500")),
    },
    "db_error_count": {
        "amber": float(os.getenv("THRESH_DB_ERR_AMBER", "5")),
        "red":   float(os.getenv("THRESH_DB_ERR_RED",   "20")),
    },
    "consumer_lag": {
        "amber": float(os.getenv("THRESH_LAG_AMBER", "500")),
        "red":   float(os.getenv("THRESH_LAG_RED",   "1000")),
    },
    "producer_error_rate": {
        "amber": float(os.getenv("THRESH_PROD_ERR_AMBER", "0.02")),
        "red":   float(os.getenv("THRESH_PROD_ERR_RED",   "0.10")),
    },
    "consumer_offset_delta": {
        "amber": float(os.getenv("THRESH_OFFSET_AMBER", "200")),
        "red":   float(os.getenv("THRESH_OFFSET_RED",   "500")),
    },
    # action metrics have no numeric thresholds — always green unless missing
    "last_deployment":    {"amber": 0.0, "red": 0.0},
    "last_restart":       {"amber": 0.0, "red": 0.0},
    "last_config_change": {"amber": 0.0, "red": 0.0},
    "last_scaling_event": {"amber": 0.0, "red": 0.0},
    # batch service metrics
    # records_processed: amber if job stalls at < 100 records, red < 10
    "records_processed":       {"amber": 0.0, "red": 0.0},  # thresholds N/A — just track value
    # batch_job_running: 0=idle 1=running — no threshold (informational)
    "batch_job_running":       {"amber": 0.0, "red": 0.0},
    "batch_job_duration_secs": {"amber": 0.0, "red": 0.0},
}


def compute_metric_status(metric: str, value: float) -> str:
    """Return 'green', 'amber', or 'red' given a metric name and value."""
    thresholds = METRIC_THRESHOLDS.get(metric)
    if not thresholds:
        return "green"
    if value >= thresholds["red"] and thresholds["red"] > 0:
        return "red"
    if value >= thresholds["amber"] and thresholds["amber"] > 0:
        return "amber"
    return "green"


# ── Legacy flat thresholds (kept for backward compat) ─────────────────────────
THRESHOLD_ERROR_RATE        = METRIC_THRESHOLDS["error_rate"]["amber"]
THRESHOLD_LATENCY_P99_MS    = METRIC_THRESHOLDS["latency_p99"]["amber"]
THRESHOLD_REQUEST_RATE_SPIKE= METRIC_THRESHOLDS["request_rate"]["red"]
THRESHOLD_DB_CONNECTIONS    = int(METRIC_THRESHOLDS["db_connection_count"]["red"])
THRESHOLD_KAFKA_LAG         = int(METRIC_THRESHOLDS["consumer_lag"]["red"])

# ── Service DB pool ────────────────────────────────────────────────────────────
ORDER_DB_POOL_SIZE = int(os.getenv("ORDER_DB_POOL_SIZE", "20"))

# ── Dashboard ──────────────────────────────────────────────────────────────────
DASHBOARD_REFRESH_INTERVAL_SECONDS = 5
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8501"))

# ── Kafka (mocked) ─────────────────────────────────────────────────────────────
KAFKA_TOPIC_INVENTORY = "inventory-events"
