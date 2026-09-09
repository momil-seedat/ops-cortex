"""
Reset all SQLite tables and Redis keys so you can run scenarios from a clean
slate without restarting any containers.

What gets cleared:
  SQLite:
    incidents               — all LLM diagnoses
    suppressed_alerts       — noise suppression log
    noise_fingerprints      — alert occurrence counts (resets suppression thresholds)
    metric_baselines        — learned mean/std per metric (resets z-score history)
    topology_edges          — learned call relationships
    known_services          — service lifecycle registry
    graph_snapshots         — Neo4j snapshot registry
    service_communication_patterns
    model_training_log

  Redis:
    node:*                  — current metric state hashes
    timeseries:*            — metric time-series lists
    stream:alerts           — alert stream (recreated empty)
    last_alert:*            — alert cooldown timestamps
    metric:*                — legacy metric keys

  ML models:
    /data/ml_models/*.pkl   — Isolation Forest files (optional, see --keep-models)

Usage:
  python -m scenarios.reset_for_testing
  python -m scenarios.reset_for_testing --keep-models   # keep trained .pkl files

After running this, restart diagnosis-agent and topology-agent so their
in-memory state (baseline model samples, noise classifier counts, graph nodes)
is also cleared:

  docker compose restart diagnosis-agent topology-agent
"""
import os
import sys
import glob
import logging
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reset")

REDIS_HOST   = os.getenv("REDIS_HOST",   "localhost")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))
SQLITE_PATH  = os.getenv("SQLITE_DB_PATH", "/data/service_brain.db")
ML_MODELS_DIR= os.getenv("ML_SAVED_MODELS_DIR", "/data/ml_models")


def reset_sqlite():
    from memory.sqlite_store import SQLiteStore
    from sqlalchemy import text

    store = SQLiteStore(SQLITE_PATH)
    session = store._Session()
    tables = [
        "incidents",
        "suppressed_alerts",
        "noise_fingerprints",
        "metric_baselines",
        "topology_edges",
        "known_services",
        "graph_snapshots",
        "service_communication_patterns",
        "model_training_log",
    ]
    try:
        for table in tables:
            result = session.execute(text(f"DELETE FROM {table}"))
            logger.info(f"  SQLite {table}: {result.rowcount} rows deleted")
        session.commit()
    finally:
        session.close()


def reset_redis():
    import redis as redis_lib
    r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    patterns = ["node:*", "timeseries:*", "last_alert:*", "metric:*",
                "service:status", "actions:*"]
    total = 0
    for pattern in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=pattern, count=200)
            if keys:
                r.delete(*keys)
                total += len(keys)
            if cursor == 0:
                break
    logger.info(f"  Redis: {total} keys deleted")

    # Delete and recreate the alert stream so the consumer group starts fresh
    r.delete("stream:alerts")
    r.xadd("stream:alerts", {"_init": "reset"})
    try:
        r.xgroup_create("stream:alerts", "diagnosis-agents", id="0")
    except Exception:
        pass
    logger.info("  Redis: stream:alerts recreated")


def reset_ml_models():
    pattern = os.path.join(ML_MODELS_DIR, "*.pkl")
    files = glob.glob(pattern)
    for f in files:
        os.remove(f)
        logger.info(f"  Deleted model: {f}")
    if not files:
        logger.info("  No .pkl model files found")


def main():
    parser = argparse.ArgumentParser(description="Reset all OpsCortex state for clean testing")
    parser.add_argument("--keep-models", action="store_true",
                        help="Keep trained Isolation Forest .pkl files")
    args = parser.parse_args()

    logger.info("=" * 55)
    logger.info("OpsCortex — Reset for Testing")
    logger.info("=" * 55)

    logger.info("\nClearing SQLite tables…")
    try:
        reset_sqlite()
    except Exception as e:
        logger.error(f"  SQLite reset failed: {e}")

    logger.info("\nClearing Redis keys…")
    try:
        reset_redis()
    except Exception as e:
        logger.error(f"  Redis reset failed: {e}")

    if not args.keep_models:
        logger.info("\nDeleting ML model files…")
        try:
            reset_ml_models()
        except Exception as e:
            logger.error(f"  ML model reset failed: {e}")
    else:
        logger.info("\nSkipping ML model files (--keep-models)")

    logger.info("\n" + "=" * 55)
    logger.info("Reset complete.")
    logger.info("")
    logger.info("IMPORTANT: restart agents to clear in-memory state:")
    logger.info("  docker compose restart diagnosis-agent topology-agent")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
