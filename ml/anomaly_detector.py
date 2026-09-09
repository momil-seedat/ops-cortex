"""
Anomaly detector — one Isolation Forest per service.
Models are saved/loaded from disk so they survive restarts.
Returns a [0, 1] anomaly score; 0.5 is neutral (no model yet).
"""
import logging
import os
import pickle
from typing import Optional

import numpy as np
from sklearn.ensemble import IsolationForest

logger = logging.getLogger("anomaly_detector")

MIN_SAMPLES_TO_FIT = 20
REFIT_EVERY_N = 50


class AnomalyDetector:
    def __init__(
        self,
        saved_models_dir: str = "/data/ml_models",
        contamination: float = 0.05,
        n_estimators: int = 100,
    ):
        self._dir = saved_models_dir
        self._contamination = contamination
        self._n_estimators  = n_estimators
        # service -> IsolationForest
        self._models: dict[str, IsolationForest] = {}
        # service -> list[list[float]] — in-memory training buffer
        self._buffer: dict[str, list[list[float]]] = {}
        os.makedirs(self._dir, exist_ok=True)
        self._load_all()

    def _model_path(self, service: str) -> str:
        return os.path.join(self._dir, f"{service}_isolation_forest.pkl")

    def _load_all(self):
        import config
        for service in config.SERVICES:
            path = self._model_path(service)
            if os.path.exists(path):
                try:
                    with open(path, "rb") as f:
                        model = pickle.load(f)
                    # Validate feature count matches current feature space
                    expected = model.n_features_in_
                    if expected != self._expected_features():
                        logger.warning(
                            f"Stale model for {service}: expected {self._expected_features()} features "
                            f"but model has {expected} — deleting and will retrain"
                        )
                        os.remove(path)
                        continue
                    self._models[service] = model
                    logger.info(f"Loaded model for {service} from {path}")
                except Exception as e:
                    logger.warning(f"Failed to load model for {service}: {e}")

    def _expected_features(self) -> int:
        from memory.graph_store import CATEGORIES, CATEGORY_METRICS
        return sum(len(CATEGORY_METRICS[c]) for c in CATEGORIES)

    def _save(self, service: str):
        path = self._model_path(service)
        try:
            with open(path, "wb") as f:
                pickle.dump(self._models[service], f)
        except Exception as e:
            logger.warning(f"Failed to save model for {service}: {e}")

    def _fit(self, service: str, data: list[list[float]]):
        X = np.array(data[-1000:])
        model = IsolationForest(
            contamination=self._contamination,
            n_estimators=self._n_estimators,
            random_state=42,
        )
        model.fit(X)
        self._models[service] = model
        self._save(service)
        logger.info(f"Refitted anomaly model for {service} on {len(X)} samples")

    def record(self, service: str, feature_dict: dict[str, float]):
        """Add a feature vector to the buffer and refit when threshold reached."""
        vec = list(feature_dict.values())
        if not vec:
            return
        if service not in self._buffer:
            self._buffer[service] = []
        self._buffer[service].append(vec)
        n = len(self._buffer[service])
        if n >= MIN_SAMPLES_TO_FIT and n % REFIT_EVERY_N == 0:
            self._fit(service, self._buffer[service])

    def get_anomaly_score(self, service: str, feature_dict: dict[str, float]) -> float:
        """
        Return anomaly score in [0, 1].
        0 = very normal, 1 = very anomalous.
        0.5 = neutral (no model yet).
        """
        if service not in self._models:
            return 0.5
        vec = list(feature_dict.values())
        if not vec:
            return 0.5
        try:
            raw_score = self._models[service].score_samples([vec])[0]
            # score_samples returns negative values; more negative = more anomalous
            # Normalise to [0, 1]: typical range is roughly [-0.6, 0.1]
            normalised = float(1.0 - (raw_score + 0.6) / 0.7)
            return max(0.0, min(1.0, normalised))
        except Exception as e:
            logger.warning(f"Anomaly score error for {service}: {e}")
            return 0.5

    def retrain_from_data(self, service: str, data: list[list[float]], sqlite_store=None):
        """Called by model_trainer for scheduled retraining."""
        import time
        start = time.time()
        self._fit(service, data)
        duration = time.time() - start
        if sqlite_store:
            try:
                sqlite_store.record_model_training(
                    service=service,
                    sample_count=len(data),
                    training_duration=duration,
                    model_path=self._model_path(service),
                )
            except Exception:
                pass

    def has_model(self, service: str) -> bool:
        return service in self._models

    def training_samples(self, service: str) -> int:
        return len(self._buffer.get(service, []))
