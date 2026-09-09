import time
import logging
from collections import defaultdict
from typing import Optional

import numpy as np

logger = logging.getLogger("traffic_model")

HOURS_IN_DAY = 24
MIN_SAMPLES_FOR_MODEL = 10


class TrafficModel:
    """
    Maintains a per-service, per-hour-of-day baseline of request rate.
    Used to detect whether current traffic is anomalous relative to the
    expected pattern for this hour.
    """

    def __init__(self):
        # service -> hour -> list of observed request rates
        self._hourly_samples: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
        # service -> hour -> (mean, std)
        self._baselines: dict[str, dict[int, tuple[float, float]]] = defaultdict(dict)

    def record(self, service: str, request_rate: float, timestamp: Optional[float] = None):
        ts = timestamp or time.time()
        hour = int((ts % 86400) / 3600)
        self._hourly_samples[service][hour].append(request_rate)
        # Refit whenever we have enough samples
        samples = self._hourly_samples[service][hour]
        if len(samples) >= MIN_SAMPLES_FOR_MODEL:
            arr = np.array(samples[-200:])  # use last 200 samples to stay current
            self._baselines[service][hour] = (float(arr.mean()), float(arr.std()) + 1e-6)

    def get_baseline(self, service: str, hour: Optional[int] = None) -> Optional[tuple[float, float]]:
        if hour is None:
            hour = int((time.time() % 86400) / 3600)
        return self._baselines[service].get(hour)

    def is_anomalous(self, service: str, request_rate: float, z_threshold: float = 3.0) -> tuple[bool, float]:
        hour = int((time.time() % 86400) / 3600)
        baseline = self._baselines[service].get(hour)
        if baseline is None:
            return False, 0.0
        mean, std = baseline
        z_score = abs(request_rate - mean) / std
        return z_score > z_threshold, float(z_score)

    def expected_rate(self, service: str) -> Optional[float]:
        hour = int((time.time() % 86400) / 3600)
        baseline = self._baselines[service].get(hour)
        return baseline[0] if baseline else None

    def summary(self) -> dict:
        return {
            service: {
                str(hour): {"mean": float(mean), "std": float(std)}
                for hour, (mean, std) in hours.items()
            }
            for service, hours in self._baselines.items()
        }
