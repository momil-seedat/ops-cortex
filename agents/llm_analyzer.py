"""
LLM integration for root cause analysis.

Single entry point: analyze(context) → dict with structured diagnosis fields.
Supports Anthropic Claude, OpenAI, and Ollama (local).
"""
import json
import logging
import os
import sys

sys.path.insert(0, "/app")
import config

logger = logging.getLogger("llm_analyzer")

_SYSTEM_PROMPT = """You are an expert site reliability engineer performing root cause analysis on a \
distributed system incident. You have access to pre-computed signals from an ML brain layer — use \
them as your primary evidence, not just the raw timeseries numbers.

CONTEXT FIELDS AND HOW TO USE THEM
====================================

inferred_root_cause
  The service whose metrics crossed a threshold FIRST in the timeseries, determined by graph BFS
  + temporal ordering. This is your strongest structural signal for root cause. Trust it unless
  topology_history or call_pattern_zscores contradict it.

fault_path
  Services ordered by time of first threshold crossing. First = root cause, rest = downstream
  victims. A fault_path of ["payment_service", "order_service"] means payment degraded first
  and order failed because of it.

ml_scores.zscores
  Per-metric z-scores: how many standard deviations from the historical mean for THIS service
  at THIS hour of day on THIS day of week. A z-score of 14 means the value is 14 standard
  deviations above normal — extremely anomalous. A z-score near 0 means the value is typical.
  Use these to identify WHICH metric is most abnormal, not just which crossed a threshold.

ml_scores.anomaly_score
  Isolation Forest score [0,1] for the COMBINED metric profile of the primary service.
  0.5 = no model yet (neutral). Above 0.7 = the full combination of metrics is collectively
  unusual even if individual thresholds were not crossed. Above 0.85 = very strong signal.

ml_scores.call_pattern_zscores
  Z-score of call volume and latency between service pairs at this hour. A high score on
  "order_service->payment_service" means order is sending abnormally many calls to payment
  OR the latency is abnormal compared to the historical norm for this time of day.
  Distinguishes "payment is broken" from "order is hammering payment".

topology_history.status_progression
  How each service's health status changed across the last 3 graph snapshots taken before the
  alert. ["healthy","degraded","faulted"] confirms gradual degradation. ["healthy","faulted"]
  means sudden failure. Compare services: whichever degraded earliest in the sequence is the
  root cause regardless of who triggered the alert.

topology_history.anomaly_score_trend
  Isolation Forest score across 3 snapshots. [0.51, 0.71, 0.89] = gradual worsening over
  ~3 minutes = resource leak or slow degradation. [0.51, 0.52, 0.91] = sudden jump = crash
  or deployment. Include this in contributing_factors when the trend is notable.

topology_history.new_call_edges_since_last_snapshot
  Call edges that appeared for the first time in the most recent snapshot. A new dependency
  appearing just before an incident is a strong indicator of a recently deployed change
  introducing a new failure mode. Always mention this in contributing_factors if present.

metric_timeseries
  Raw time series with threshold annotations. Use to confirm the temporal narrative:
  did the metric spike suddenly or drift gradually? Does it match a known pattern?
  Check threshold_amber and threshold_red annotations to understand severity context.

recent_actions
  Deployments, restarts, config changes in the last 30 minutes. Always check whether a
  recent action on the root cause service preceded the fault. If yes, mention it.

REASONING APPROACH
==================
1. Start with inferred_root_cause and fault_path — these are pre-computed from graph topology
   and temporal ordering. They are usually correct.
2. Confirm or challenge using topology_history.status_progression — does the degradation
   sequence match the fault_path order?
3. Check ml_scores.zscores to identify the specific metric driving the anomaly.
4. Check call_pattern_zscores — is there an upstream service causing abnormal load?
5. Look at anomaly_score_trend — gradual rise vs sudden jump tells you the failure mode.
6. Check recent_actions — deployment or config change on the root cause service?
7. Check new_call_edges — new dependency introduced just before incident?

CONFIDENCE RULES
================
- high:   inferred_root_cause confirmed by status_progression AND z-score clearly elevated
- medium: inferred_root_cause plausible but status_progression ambiguous or z-scores moderate
- low:    contradictions between signals, or insufficient data (anomaly_score=0.5, zscores=0)

OUTPUT FORMAT
=============
Respond ONLY with a valid JSON object — no markdown, no explanation outside the JSON:
{
  "root_cause": "<one sentence: which service, which metric, what the ML signals show>",
  "contributing_factors": [
    "<reference specific z-scores, anomaly scores, or topology history observations>",
    "<mention call pattern anomalies, trends, or new edges if present>",
    "<mention recent actions if relevant>"
  ],
  "confidence": "<low|medium|high>",
  "recommended_fix": "<specific actionable step based on the identified root cause>",
  "estimated_impact": "<which services affected and what user-facing behaviour is degraded>"
}"""


def _build_prompt(context: dict, past_incidents: list[dict]) -> str:
    """
    Structure the prompt so the LLM sees the ML brain signals first — in plain
    readable form — before the raw JSON blob. This prevents the LLM from ignoring
    the pre-computed signals and falling back to generic timeseries pattern-matching.
    """
    ml  = context.get("ml_scores", {})
    topo = context.get("topology_history", {})
    lines = []

    # ── Section 1: Brain signals summary (plain text, most important first) ──
    lines.append("=== ML BRAIN SIGNALS (pre-computed — use as primary evidence) ===\n")

    lines.append(f"Primary faulted service : {context.get('primary_faulted_service', '?')}")
    lines.append(f"Primary faulted metric  : {context.get('primary_faulted_metric', '?')} "
                 f"= {context.get('primary_alert_value', '?')}")
    lines.append(f"Inferred root cause     : {context.get('inferred_root_cause', '?')}  "
                 f"← graph BFS + temporal ordering")
    lines.append(f"Fault path (time order) : {context.get('fault_path', [])}  "
                 f"← first = root cause, rest = downstream victims\n")

    # Anomaly score
    anomaly_score = ml.get("anomaly_score", 0.5)
    if anomaly_score >= 0.85:
        score_note = "VERY HIGH — full metric profile highly unusual"
    elif anomaly_score >= 0.70:
        score_note = "HIGH — collectively anomalous even if individual thresholds borderline"
    elif anomaly_score == 0.5:
        score_note = "NEUTRAL — no model trained yet"
    else:
        score_note = "LOW — metric profile within normal range"
    lines.append(f"Isolation Forest score  : {anomaly_score:.3f}  ({score_note})")

    # Top z-scores
    zscores = ml.get("zscores", {})
    if zscores:
        top_z = sorted(zscores.items(), key=lambda x: abs(x[1]), reverse=True)[:5]
        lines.append("Top metric z-scores     :")
        for metric_key, z in top_z:
            direction = "above" if z > 0 else "below"
            lines.append(f"  {metric_key:45s}  z={z:+.2f}  ({abs(z):.1f}σ {direction} normal for this hour)")
    else:
        lines.append("Top metric z-scores     : none (baseline not yet built)")

    # Call pattern z-scores
    call_zscores = ml.get("call_pattern_zscores", {})
    if call_zscores:
        lines.append("Call pattern z-scores   :")
        for pair, z in sorted(call_zscores.items(), key=lambda x: abs(x[1]), reverse=True):
            lines.append(f"  {pair:45s}  z={z:+.2f}  (abnormal call volume/latency for this hour)")
    lines.append("")

    # Topology history from Neo4j
    if topo:
        lines.append(f"Neo4j snapshots used    : {topo.get('snapshots_used', 0)} "
                     f"(oldest was {topo.get('oldest_snapshot_age_min', '?')} min before alert)")

        status_prog = topo.get("status_progression", {})
        if status_prog:
            lines.append("Status progression      :")
            for svc, history in status_prog.items():
                arrow = " → ".join(history)
                lines.append(f"  {svc:30s}  {arrow}")

        score_trend = topo.get("anomaly_score_trend", {})
        if score_trend:
            lines.append("Anomaly score trend     :")
            for svc, scores in score_trend.items():
                trend_dir = "rising" if len(scores) >= 2 and scores[-1] > scores[0] else "stable"
                lines.append(f"  {svc:30s}  {scores}  ({trend_dir})")

        new_edges = topo.get("new_call_edges_since_last_snapshot", [])
        if new_edges:
            lines.append("NEW call edges (just appeared before incident) ← high suspicion:")
            for e in new_edges:
                lines.append(f"  {e['caller']} → {e['callee']}  calls={e.get('call_count',0)}")
        lines.append("")

    # Recent actions
    recent_actions = context.get("recent_actions", [])
    if recent_actions:
        lines.append("Recent operational actions (last 30 min):")
        for a in recent_actions[:5]:
            lines.append(f"  {a}")
        lines.append("")

    lines.append("=== FULL INCIDENT CONTEXT (raw JSON for detail) ===\n")
    # Exclude topology_history and ml_scores from the JSON since we've shown them above
    slim_context = {k: v for k, v in context.items()
                    if k not in ("topology_history", "ml_scores", "recent_actions")}
    lines.append(json.dumps(slim_context, indent=2, default=str))

    # Past incidents for few-shot
    if past_incidents:
        lines.append("\n=== SIMILAR PAST INCIDENTS (for pattern recognition) ===")
        for i, inc in enumerate(past_incidents, 1):
            lines.append(f"\n--- Past Incident {i} ---")
            lines.append(f"Service: {inc.get('service', inc.get('faulted_service', '?'))}")
            lines.append(f"Metric:  {inc.get('metric', inc.get('alert_metric', '?'))}")
            if inc.get("llm_root_cause"):
                lines.append(f"Root cause: {inc['llm_root_cause']}")
            if inc.get("llm_contributing_factors"):
                factors = inc["llm_contributing_factors"]
                if isinstance(factors, str):
                    try:
                        factors = json.loads(factors)
                    except Exception:
                        pass
                lines.append(f"Contributing factors: {factors}")
            if inc.get("llm_recommended_fix"):
                lines.append(f"Fix applied: {inc['llm_recommended_fix']}")

    lines.append("\nProvide your diagnosis as a JSON object now:")
    return "\n".join(lines)


def _call_claude(prompt: str) -> str:
    import anthropic
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    response = client.messages.create(
        model=config.LLM_MODEL,
        max_tokens=1024,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


def _call_openai(prompt: str) -> str:
    import openai
    client = openai.OpenAI(api_key=config.OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        temperature=0.2,
        response_format={"type": "json_object"},
    )
    return response.choices[0].message.content


def _call_ollama(prompt: str) -> str:
    import urllib.request
    payload = json.dumps({
        "model": config.LLM_MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "stream": False,
        "format": "json",
    }).encode()
    req = urllib.request.Request(
        f"{config.OLLAMA_BASE_URL}/api/chat",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read())
    return data["message"]["content"]


def _strip_markdown_fence(text: str) -> str:
    """Remove ```json ... ``` or ``` ... ``` wrappers Claude sometimes adds."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        # drop first line (```json or ```) and last line (```)
        inner = lines[1:] if lines[-1].strip() == "```" else lines[1:]
        if inner and inner[-1].strip() == "```":
            inner = inner[:-1]
        text = "\n".join(inner).strip()
    return text


def _parse_response(raw: str) -> dict:
    try:
        parsed = json.loads(_strip_markdown_fence(raw))
        return {
            "root_cause":           str(parsed.get("root_cause", "")),
            "contributing_factors": parsed.get("contributing_factors", []),
            "confidence":           str(parsed.get("confidence", "low")),
            "recommended_fix":      str(parsed.get("recommended_fix", "")),
            "estimated_impact":     str(parsed.get("estimated_impact", "")),
        }
    except Exception as e:
        logger.warning(f"LLM response parse error: {e} — raw={raw[:200]}")
        return {
            "root_cause": raw[:500] if raw else "parse error",
            "contributing_factors": [],
            "confidence": "low",
            "recommended_fix": "",
            "estimated_impact": "",
        }


def analyze(context: dict, past_incidents: list[dict] | None = None) -> tuple[dict, str]:
    """
    Call the LLM with the incident context and return (parsed_dict, raw_response).
    Falls back to a stub dict if LLM is unavailable.
    """
    if past_incidents is None:
        past_incidents = []

    prompt = _build_prompt(context, past_incidents)
    raw = ""

    try:
        if config.LLM_PROVIDER == "claude":
            raw = _call_claude(prompt)
        elif config.LLM_PROVIDER == "openai":
            raw = _call_openai(prompt)
        else:
            raw = _call_ollama(prompt)
        parsed = _parse_response(raw)
        logger.info(f"LLM analysis complete: confidence={parsed['confidence']}")
        return parsed, raw
    except Exception as e:
        logger.warning(f"LLM call failed ({config.LLM_PROVIDER}): {e}")
        service = context.get("faulted_service", "unknown")
        metric  = context.get("faulted_metric",  "unknown")
        stub = {
            "root_cause":           f"LLM unavailable — manual review required for {service}/{metric}",
            "contributing_factors": [],
            "confidence":           "low",
            "recommended_fix":      "Check service logs and metric history manually.",
            "estimated_impact":     "Unknown",
        }
        return stub, raw
