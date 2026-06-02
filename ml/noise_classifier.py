"""
Noise classifier -- fingerprint registry backed by SQLite.
Suppresses alerts where the same (service, metric, hour, dow) combination
has fired more than THRESHOLD times, UNLESS:
  1. The current value has a high self z-score (statistically anomalous vs baseline)
  2. Any peer service currently has a z-score > 2 (correlated incident)
"""
import time
import logging
from collections import defaultdict
from typing import Optional

logger = logging.getLogger("noise_classifier")

SUPPRESSION_THRESHOLD = 3    # occurrences in the current hour+dow bucket


class NoiseClassifier:
    def __init__(
        self,
        sqlite_store=None,
        threshold: int = SUPPRESSION_THRESHOLD,
    ):
        self._sqlite    = sqlite_store
        self._threshold = threshold
        # In-memory counts: (service, metric, hour, dow) -> count
        self._counts: dict[tuple, int] = defaultdict(int)
        if self._sqlite:
            self._load_from_db()

    def _load_from_db(self):
        pass  # fingerprints are loaded on-demand per lookup

    def _bucket(self) -> tuple[int, int]:
        t = time.time()
        import datetime
        dt = datetime.datetime.fromtimestamp(t)
        return dt.hour, dt.weekday()

    def _db_count(self, service: str, metric: str, hour: int, dow: int) -> int:
        if not self._sqlite:
            return 0
        try:
            row = self._sqlite.get_noise_fingerprint(service, metric, hour, dow)
            return row["occurrence_count"] if row else 0
        except Exception:
            return 0

    def _db_persist(self, service: str, metric: str, hour: int, dow: int, count: int):
        if not self._sqlite:
            return
        try:
            self._sqlite.upsert_noise_fingerprint(service, metric, hour, dow, count)
        except Exception as e:
            logger.warning(f"Noise fingerprint DB write failed: {e}")

    def is_noise(
        self,
        service: str,
        metric: str,
        baseline_model=None,
        current_value: Optional[float] = None,
    ) -> tuple[bool, str, str]:
        """
        Returns (is_noise, confidence, reason).
        confidence: 'low' | 'medium' | 'high'
        """
        hour, dow = self._bucket()
        key = (service, metric, hour, dow)
        mem_count = self._counts.get(key, 0)
        db_count  = self._db_count(service, metric, hour, dow)
        count = max(mem_count, db_count)

        if count < self._threshold:
            return False, "low", ""

        if baseline_model is not None:
            from memory.graph_store import CATEGORY_METRICS, CATEGORIES

            # Self z-score: current value vs rolling mean.
            # Catches sudden spikes — e.g. latency jumps from 100ms to 500ms.
            if current_value is not None:
                self_zs = baseline_model.get_zscore(service, metric, current_value)
                if abs(self_zs) > 2.5:
                    reason = (
                        f"Would suppress {service}/{metric} (count={count}) "
                        f"but self z-score={self_zs:.1f} — sudden anomaly, not suppressing"
                    )
                    logger.info(reason)
                    return False, "low", reason

            # Trend z-score: is the metric gradually increasing run-over-run?
            # A plain z-score misses gradual degradation because the rolling mean
            # chases the drift. The trend z-score fits a linear slope over the
            # last 10 samples — if it is consistently rising, this is a real
            # degradation trend even if each individual value looks "normal".
            trend_zs = baseline_model.get_trend_zscore(service, metric)
            if trend_zs > 2.0:
                reason = (
                    f"Would suppress {service}/{metric} (count={count}) "
                    f"but trend z-score={trend_zs:.1f} — gradual degradation detected, not suppressing"
                )
                logger.info(reason)
                return False, "low", reason

            # Cross-service z-score: if any peer service is also anomalous,
            # this may be a correlated incident -- do not suppress.
            from memory.graph_store import get_graph_store
            for other_service in get_graph_store().get_all_services():
                if other_service == service:
                    continue
                for category in CATEGORIES:
                    for met in CATEGORY_METRICS[category]:
                        zs = baseline_model.get_zscore(other_service, met, None)
                        if abs(zs) > 2.0:
                            reason = (
                                f"Would suppress {service}/{metric} (count={count}) "
                                f"but {other_service}/{met} has z-score={zs:.1f} -- not suppressing"
                            )
                            logger.debug(reason)
                            return False, "medium", reason

        reason = (
            f"Alert ({service}, {metric}, hour={hour}, dow={dow}) "
            f"has fired {count} times -- suppressed as noise"
        )
        confidence = "high" if count >= self._threshold * 2 else "medium"
        return True, confidence, reason

    def record_alert(self, service: str, metric: str):
        hour, dow = self._bucket()
        key = (service, metric, hour, dow)
        self._counts[key] += 1
        self._db_persist(service, metric, hour, dow, self._counts[key])

    def get_count(self, service: str, metric: str) -> int:
        hour, dow = self._bucket()
        key = (service, metric, hour, dow)
        return max(self._counts.get(key, 0), self._db_count(service, metric, hour, dow))

    # Kept for backward compat with old diagnosis_agent call signature
    def should_suppress(self, service: str, metric: str) -> tuple[bool, str]:
        is_noise, _confidence, reason = self.is_noise(service, metric)
        return is_noise, reason
