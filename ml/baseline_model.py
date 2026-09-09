"""
Baseline model — maintains rolling mean and std per (service, metric, hour_of_day, day_of_week).
Persists statistics to SQLite so they survive restarts.
"""
import time
import logging
from collections import defaultdict
from typing import Optional

import numpy as np

logger = logging.getLogger("baseline_model")

MIN_SAMPLES = 10


class BaselineModel:
    def __init__(self, sqlite_store=None):
        self._sqlite = sqlite_store
        # (service, metric, hour, dow) -> list of float values
        self._samples: dict[tuple, list[float]] = defaultdict(list)
        # (service, metric, hour, dow) -> (mean, std)
        self._stats:   dict[tuple, tuple[float, float]] = {}
        if self._sqlite:
            self._load_from_db()

    def _load_from_db(self):
        try:
            # Load baselines for every service ever seen — not just the original 3.
            # This ensures report_service (and any future service) gets its baselines
            # restored on restart so the noise classifier works correctly from run 1.
            services = self._sqlite.get_known_services_from_baselines()
            for service in services:
                rows = self._sqlite.get_all_baselines_for_service(service)
                for row in rows:
                    key = (service, row["metric"], row["hour_of_day"], row["day_of_week"])
                    self._stats[key] = (row["mean"], max(row["std"], 1e-6))
            if services:
                logger.info(f"Baseline model loaded stats for {len(services)} services from DB")
        except Exception as e:
            logger.warning(f"Baseline load from DB failed: {e}")

    def _bucket(self, ts: Optional[float] = None) -> tuple[int, int]:
        t = ts or time.time()
        import datetime
        dt = datetime.datetime.fromtimestamp(t)
        return dt.hour, dt.weekday()

    def record(self, service: str, metric: str, value: float, timestamp: Optional[float] = None):
        hour, dow = self._bucket(timestamp)
        key = (service, metric, hour, dow)
        self._samples[key].append(value)
        samples = self._samples[key]
        # Keep last 500 samples per bucket
        if len(samples) > 500:
            self._samples[key] = samples[-500:]
            samples = self._samples[key]
        if len(samples) >= MIN_SAMPLES:
            arr = np.array(samples)
            mean = float(arr.mean())
            std  = max(float(arr.std()), 1e-6)
            self._stats[key] = (mean, std)
            # Persist every 50 new samples to avoid excessive writes
            if len(samples) % 50 == 0 and self._sqlite:
                try:
                    self._sqlite.upsert_metric_baseline(
                        service=service,
                        metric=metric,
                        hour_of_day=hour,
                        day_of_week=dow,
                        mean=mean,
                        std=std,
                        sample_count=len(samples),
                    )
                except Exception as e:
                    logger.warning(f"Baseline DB write failed: {e}")

    def get_zscore(self, service: str, metric: str, value: Optional[float], timestamp: Optional[float] = None) -> float:
        """Return z-score for value. Returns 0.0 if no baseline yet."""
        hour, dow = self._bucket(timestamp)
        key = (service, metric, hour, dow)
        if key not in self._stats:
            # Try DB
            if self._sqlite:
                try:
                    row = self._sqlite.get_metric_baseline(service, metric, hour, dow)
                    if row:
                        self._stats[key] = (row["mean"], max(row["std"], 1e-6))
                except Exception:
                    pass
        if key not in self._stats:
            return 0.0
        mean, std = self._stats[key]
        if value is None:
            return 0.0
        return (value - mean) / std

    def get_baseline(self, service: str, metric: str, timestamp: Optional[float] = None) -> Optional[tuple[float, float]]:
        hour, dow = self._bucket(timestamp)
        return self._stats.get((service, metric, hour, dow))

    def get_trend_zscore(
        self,
        service: str,
        metric: str,
        window: int = 10,
        timestamp: Optional[float] = None,
    ) -> float:
        """
        Detect gradual degradation that a plain z-score misses.

        A plain z-score compares the current value against a rolling mean.
        If latency increases slowly run-over-run, the mean keeps rising to
        follow it and the z-score stays low — the baseline chases the trend
        and never fires.

        This method fits a linear regression slope over the last `window`
        samples in the current time bucket and normalises it:

            trend_z = slope / std_of_residuals

        A positive trend_z means values are consistently rising.
        A negative trend_z means they are consistently falling.

        Interpretation:
          |trend_z| < 1.5  → flat, no trend
          |trend_z| > 2.0  → meaningful trend (worth flagging)
          |trend_z| > 3.0  → strong persistent degradation

        Returns 0.0 if fewer than `window` samples exist (not enough history).
        """
        hour, dow = self._bucket(timestamp)
        key = (service, metric, hour, dow)
        samples = self._samples.get(key, [])
        if len(samples) < window:
            return 0.0

        recent = np.array(samples[-window:], dtype=float)
        x = np.arange(len(recent), dtype=float)

        # Fit y = mx + b
        slope, intercept = np.polyfit(x, recent, 1)

        # Residuals around the fit line
        fitted    = slope * x + intercept
        residuals = recent - fitted
        res_std   = float(np.std(residuals))

        if res_std < 1e-6:
            # All values are identical — perfectly flat, no trend
            return 0.0

        # Normalise slope by residual spread so the result is dimensionless
        # and comparable across metrics with very different scales.
        # One "unit" of trend_z = the slope equals one residual std per sample.
        return float(slope / res_std)
