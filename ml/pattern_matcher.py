"""
Pattern matcher — detects recurring incidents without calling the LLM.

Logic:
  For each alert in the batch, look up recent resolved incidents for the same
  (service, metric) pair.  If we find N or more matches whose alert_value was
  within VALUE_TOLERANCE of the current value AND whose anomaly_score at the
  time was above ANOMALY_SCORE_MIN, we consider the pattern "known" and return
  a pre-built diagnosis dict instead of calling the LLM.

  Confidence levels:
    - "high"   : ≥ MATCH_THRESHOLD_HIGH matches
    - "medium" : ≥ MATCH_THRESHOLD_MED  matches
  Only "high" skips the LLM; "medium" still calls it but includes pattern hints.
"""
import logging
import time
from typing import Optional

logger = logging.getLogger("pattern_matcher")

MATCH_THRESHOLD_HIGH = 2       # ≥2 past incidents → skip LLM
MATCH_THRESHOLD_MED  = 1       # ≥1 past incidents → hint LLM
VALUE_TOLERANCE      = 0.40    # ±40% of alert value counts as a match
MAX_AGE_SECONDS      = 86400 * 30  # only consider incidents from the last 30 days
LOOK_BACK_LIMIT      = 10      # max past incidents to scan per (service, metric)


def _value_matches(past_value: float, current_value: float) -> bool:
    if current_value == 0:
        return past_value == 0
    return abs(past_value - current_value) / abs(current_value) <= VALUE_TOLERANCE


def _build_diagnosis_from_past(past: dict, current_value: float, match_count: int) -> dict:
    """Reconstruct a diagnosis dict from a previously resolved incident."""
    return {
        "root_cause": past.get("llm_root_cause") or past.get("diagnosis_summary") or
                      f"{past['service']} {past['metric']} — recurring pattern",
        "contributing_factors": past.get("llm_contributing_factors") or [],
        "confidence": "high",
        "recommended_fix": past.get("llm_recommended_fix") or
                           "Apply the same remediation as the previous occurrence.",
        "estimated_impact": (
            f"Known recurring pattern ({match_count} prior occurrence(s)). "
            + (past.get("llm_estimated_impact") or "")
        ),
    }


class PatternMatcher:
    """
    Wraps SQLiteStore.get_recent_incidents_for_service to detect recurring
    patterns and decide whether the LLM call can be skipped.
    """

    def __init__(self, sqlite_store, anomaly_detector=None):
        self._sqlite   = sqlite_store
        self._detector = anomaly_detector

    def match(
        self,
        service: str,
        metric: str,
        current_value: float,
        anomaly_score: float = 0.5,
    ) -> tuple[Optional[dict], int]:
        """
        Returns (diagnosis_dict_or_None, match_count).

        If match_count >= MATCH_THRESHOLD_HIGH, diagnosis_dict is populated and
        the caller can skip the LLM.  Otherwise diagnosis_dict is None.
        """
        try:
            past_incidents = self._sqlite.get_recent_incidents_for_service(
                service, limit=LOOK_BACK_LIMIT
            )
        except Exception as e:
            logger.warning(f"PatternMatcher DB read failed: {e}")
            return None, 0

        cutoff = time.time() - MAX_AGE_SECONDS
        matches = [
            inc for inc in past_incidents
            if inc.get("metric") == metric
            and inc.get("timestamp", 0) >= cutoff
            and _value_matches(inc.get("alert_value", 0.0), current_value)
            and inc.get("llm_root_cause")  # only use incidents that were actually diagnosed
        ]

        count = len(matches)
        if count == 0:
            return None, 0

        best = max(matches, key=lambda i: i.get("timestamp", 0))
        logger.info(
            f"PatternMatcher: {service}/{metric}={current_value:.2f} — "
            f"{count} historical match(es), best from ts={best.get('timestamp'):.0f}"
        )

        if count >= MATCH_THRESHOLD_HIGH:
            diagnosis = _build_diagnosis_from_past(best, current_value, count)
            return diagnosis, count

        return None, count
