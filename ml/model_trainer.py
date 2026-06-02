"""
Model trainer — scheduled background task that retrains Isolation Forest
models every 24 hours using the last 7 days of Redis time series data.
Uses APScheduler for the cron schedule.
"""
import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, "/app")
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("model_trainer")

RETRAIN_INTERVAL_HOURS = 24
LOOKBACK_DAYS = 7
REDIS_HOST = os.getenv("REDIS_HOST",  config.REDIS_HOST)
REDIS_PORT = int(os.getenv("REDIS_PORT", str(config.REDIS_PORT)))


def _collect_training_data(redis_store, service: str) -> list[list[float]]:
    """Pull time-series data for a service and build feature vectors."""
    from memory.graph_store import CATEGORIES, CATEGORY_METRICS
    # Collect all metric series
    series: dict[str, list[dict]] = {}
    for category in CATEGORIES:
        for metric in CATEGORY_METRICS[category]:
            entries = redis_store.get_timeseries(service, category, metric, count=300)
            if entries:
                series[f"{category}/{metric}"] = entries

    if not series:
        return []

    # Align by timestamp: build a matrix row per 10s tick
    all_keys = list(series.keys())
    # Use the first series as the time axis
    ref_series = next(iter(series.values()))
    vectors = []
    for i, ref_entry in enumerate(ref_series):
        vec = []
        for key in all_keys:
            s = series[key]
            # Pick the closest entry by index
            idx = min(i, len(s) - 1)
            vec.append(s[idx].get("value", 0.0))
        vectors.append(vec)

    return vectors


def retrain_all(redis_store, sqlite_store, anomaly_detector):
    from ml.anomaly_detector import AnomalyDetector
    for service in config.SERVICES:
        try:
            logger.info(f"Retraining model for {service}…")
            data = _collect_training_data(redis_store, service)
            if len(data) < 20:
                logger.info(f"Not enough data for {service} ({len(data)} samples) — skipping")
                continue
            anomaly_detector.retrain_from_data(service, data, sqlite_store=sqlite_store)
            logger.info(f"Model retrained for {service}: {len(data)} samples")
        except Exception as e:
            logger.error(f"Retrain failed for {service}: {e}")


async def training_loop(redis_store, sqlite_store, anomaly_detector):
    while True:
        await asyncio.sleep(RETRAIN_INTERVAL_HOURS * 3600)
        logger.info("Scheduled retraining starting…")
        try:
            retrain_all(redis_store, sqlite_store, anomaly_detector)
        except Exception as e:
            logger.error(f"Scheduled retraining error: {e}")


async def main():
    from memory.redis_store import RedisStore
    from memory.sqlite_store import SQLiteStore
    from ml.anomaly_detector import AnomalyDetector

    redis_store = RedisStore(host=REDIS_HOST, port=REDIS_PORT)
    for attempt in range(30):
        if redis_store.ping():
            break
        logger.info(f"Waiting for Redis… {attempt + 1}")
        await asyncio.sleep(2)
    else:
        logger.error("Redis not reachable")
        sys.exit(1)

    sqlite_store     = SQLiteStore(os.getenv("SQLITE_DB_PATH", config.SQLITE_DB_PATH))
    anomaly_detector = AnomalyDetector(
        saved_models_dir=os.getenv("ML_SAVED_MODELS_DIR", config.ML_SAVED_MODELS_DIR)
    )

    # Run an initial training immediately if no models exist
    for service in config.SERVICES:
        if not anomaly_detector.has_model(service):
            logger.info(f"No model for {service} — attempting initial train")
            data = _collect_training_data(redis_store, service)
            if len(data) >= 20:
                anomaly_detector.retrain_from_data(service, data, sqlite_store=sqlite_store)

    logger.info(f"Model trainer ready — will retrain every {RETRAIN_INTERVAL_HOURS}h")
    await training_loop(redis_store, sqlite_store, anomaly_detector)


if __name__ == "__main__":
    asyncio.run(main())
