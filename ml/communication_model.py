"""
Communication model — tracks call count and latency statistics per service pair
per hour of day, persisted in SQLite.  Exposes get_call_pattern_zscore(caller, callee).
"""
import time
import logging
from collections import defaultdict
from typing import Optional

import numpy as np

logger = logging.getLogger("communication_model")

MIN_SAMPLES = 5


class CommunicationModel:
    def __init__(self, sqlite_store=None, graph_store=None):
        self._sqlite = sqlite_store
        self._graph  = graph_store
        # (caller, callee, hour) -> list of (call_count_delta, latency_ms)
        self._samples: dict[tuple, list[tuple[float, float]]] = defaultdict(list)
        # (caller, callee, hour) -> (call_count_mean, call_count_std, latency_mean, latency_std)
        self._stats:   dict[tuple, tuple] = {}
        if self._sqlite:
            self._load_from_db()

    def _hour(self) -> int:
        import datetime
        return datetime.datetime.now().hour

    def _load_from_db(self):
        pass  # loaded on-demand per zscore lookup

    def _get_db_stats(self, caller: str, callee: str, hour: int) -> Optional[tuple]:
        if not self._sqlite:
            return None
        try:
            row = self._sqlite.get_communication_pattern(caller, callee, hour)
            if row:
                return (
                    row["call_count_mean"], row["call_count_std"],
                    row["latency_mean"],    row["latency_std"],
                )
        except Exception:
            pass
        return None

    def record(self, caller: str, callee: str, call_count_delta: float, latency_ms: float):
        hour = self._hour()
        key  = (caller, callee, hour)
        self._samples[key].append((call_count_delta, latency_ms))
        if len(self._samples[key]) > 500:
            self._samples[key] = self._samples[key][-500:]

        samples = self._samples[key]
        if len(samples) >= MIN_SAMPLES:
            arr = np.array(samples)
            cc_mean = float(arr[:, 0].mean())
            cc_std  = max(float(arr[:, 0].std()), 1e-6)
            lat_mean = float(arr[:, 1].mean())
            lat_std  = max(float(arr[:, 1].std()), 1e-6)
            self._stats[key] = (cc_mean, cc_std, lat_mean, lat_std)

            if len(samples) % 20 == 0 and self._sqlite:
                try:
                    self._sqlite.upsert_communication_pattern(
                        caller=caller,
                        callee=callee,
                        hour_of_day=hour,
                        call_count_mean=cc_mean,
                        call_count_std=cc_std,
                        latency_mean=lat_mean,
                        latency_std=lat_std,
                        sample_count=len(samples),
                    )
                except Exception as e:
                    logger.warning(f"Comm pattern DB write failed: {e}")

    def record_from_graph(self):
        """Pull current edge data from graph_store and record."""
        if not self._graph:
            return
        for edge in self._graph.get_calls_edges():
            caller = edge.get("source")
            callee = edge.get("target")
            if caller and callee:
                latency = edge.get("latency_ema", 0.0)
                calls   = edge.get("call_count", 0)
                self.record(caller, callee, float(calls), latency)

    def get_call_pattern_zscore(self, caller: str, callee: str) -> Optional[float]:
        """
        Return a combined z-score of how abnormal the current call rate
        and latency is for this pair at the current hour.
        None if no baseline exists.
        """
        hour = self._hour()
        key  = (caller, callee, hour)
        stats = self._stats.get(key) or self._get_db_stats(caller, callee, hour)
        if not stats:
            return None

        cc_mean, cc_std, lat_mean, lat_std = stats

        # Get current values from graph store
        current_calls   = 0.0
        current_latency = 0.0
        if self._graph:
            edge_attrs = self._graph.get_edge_attributes(caller, callee)
            current_calls   = float(edge_attrs.get("call_count", 0))
            current_latency = float(edge_attrs.get("latency_ema", 0))

        cc_z  = (current_calls   - cc_mean)  / cc_std
        lat_z = (current_latency - lat_mean) / lat_std
        return float((abs(cc_z) + abs(lat_z)) / 2.0)
