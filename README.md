# OpsCortex — Agentic AIOps for Distributed Microservices

OpsCortex is a research project that builds a **multi-agent, LLM-assisted operations intelligence system** for microservice environments. Three specialized agents continuously observe a simulated e-commerce backend, detect anomalies, trace cascade failures, and produce root-cause diagnoses — all without human intervention.

---

## Table of Contents

1. [How to Run](#1-how-to-run)
2. [Research Summary](#2-research-summary)
3. [Full Architecture](#3-full-architecture)
4. [Layer-by-Layer Breakdown](#4-layer-by-layer-breakdown)
   - [Layer 0 — Application Services](#layer-0--application-services)
   - [Layer 1 — Observability Pipeline](#layer-1--observability-pipeline)
   - [Layer 2 — Memory System](#layer-2--memory-system)
   - [Layer 3 — ML Models](#layer-3--ml-models)
   - [Layer 4 — Agents](#layer-4--agents)
   - [Layer 5 — LLM Reasoning](#layer-5--llm-reasoning)
   - [Layer 6 — Dashboard](#layer-6--dashboard)
5. [Memory Architecture](#5-memory-architecture)
   - [Tier 1 — Redis: Hot Working Memory](#tier-1--redis-hot-working-memory)
   - [Tier 2 — NetworkX: Live In-Process Graph](#tier-2--networkx-live-in-process-graph)
   - [Tier 3 — SQLite: Permanent Brain](#tier-3--sqlite-permanent-brain)
   - [Tier 4 — Neo4j: Graph Snapshot History](#tier-4--neo4j-graph-snapshot-history)
6. [Failure Scenarios](#6-failure-scenarios)
7. [Commands & Data Inspection](#7-commands--data-inspection)
   - [Neo4j Graph Queries](#neo4j-graph-queries)
   - [Redis Working Memory](#redis-working-memory)
   - [SQLite Incident History](#sqlite-incident-history)
   - [Prometheus Metrics](#prometheus-metrics)
   - [Service Control API](#service-control-api)
8. [Configuration](#8-configuration)
9. [LLM Provider Setup](#9-llm-provider-setup)
10. [Project Structure](#project-structure)

---

## 1. How to Run

### Prerequisites

- Docker + Docker Compose v2
- An Anthropic API key (or OpenAI key / local Ollama instance)
- 4 GB RAM minimum (Neo4j + Redis + 3 services + 3 agents + dashboard)

### Step 1 — Configure environment

```bash
# Edit .env and add your API key:
nano .env
```

`.env` fields:
```
LLM_PROVIDER=claude          # claude | openai | ollama
LLM_MODEL=claude-haiku-4-5-20251001
ANTHROPIC_API_KEY=sk-ant-...
```

### Step 2 — Start the full stack

```bash
docker compose up --build -d
```

Wait ~60 seconds for all health checks to pass:

```bash
docker compose ps    # all containers should show "healthy" or "running"
```

### Step 3 — Open the dashboard

```
http://localhost:8501
```

### Step 4 — Run a failure scenario

```bash
# Cascade failure (Payment → Order)
docker compose --profile scenarios run --rm scenario-03

# Request surge
docker compose --profile scenarios run --rm scenario-01

# Kafka lag
docker compose --profile scenarios run --rm scenario-02

# Internal DB bottleneck
docker compose --profile scenarios run --rm scenario-05
```

### Step 5 — Watch agents in real time

```bash
# Monitoring agent (alert publishing)
docker logs -f monitoring-agent

# Topology agent (graph syncs + Neo4j snapshots)
docker logs -f topology-agent

# Diagnosis agent (LLM calls + incident writes)
docker logs -f diagnosis-agent
```

### Stop everything

```bash
docker compose down -v    # -v removes volumes (clears all stored data)
docker compose down       # keeps volumes (preserves incidents, Neo4j, SQLite)
```

---

## 2. Research Summary

| Property | Value |
|---|---|
| Domain | AIOps / Autonomous incident management |
| System type | Multi-agent, event-driven, self-learning |
| Memory tiers | Redis (hot) → NetworkX (live graph) → Neo4j (snapshots) → SQLite (permanent brain) |
| Brain layer | 6 ML models — baseline, anomaly, noise, trend, pattern, communication |
| Root cause method | Graph BFS on call edges + temporal ordering of first threshold crossing |
| Suppression logic | Count-based + self z-score override + trend z-score override |
| Service lifecycle | Auto-discovers, tracks offline vs deprecated, purges after 7 days absent |
| LLM backends | Claude (Anthropic), OpenAI GPT, Ollama (local) |
| Scenarios | 8 scenarios including suppression learning + degradation detection |
| Observability | OpenTelemetry traces + Prometheus — zero hardcoded service names |

---

## 3. Full Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           OpsCortex System                              │
│                                                                         │
│  ┌──────────────┐  ┌────────────────┐  ┌─────────────────┐  ┌───────┐  │
│  │order-service │  │payment-service │  │inventory-service│  │report │  │
│  │  :8001       │  │   :8002        │  │     :8003       │  │:8004  │  │
│  └──────┬───────┘  └───────┬────────┘  └────────┬────────┘  └───┬───┘  │
│         │                  │                    │               │      │
│         └──────────────────┴────────────────────┴───────────────┘      │
│                            │ OTLP traces + /metrics                    │
│                     ┌──────▼───────┐                                   │
│                     │otel-collector│ :4317/:4318                       │
│                     └──────┬───────┘                                   │
│                            │ scrape every 10s                          │
│                     ┌──────▼───────┐                                   │
│                     │  prometheus  │ :9090                             │
│                     └──────┬───────┘                                   │
│                            │ poll every 10s — any service label        │
│  ┌─────────────────────────▼─────────────────────────────────────────┐ │
│  │                    monitoring-agent :8080                         │ │
│  │  discover services → compute amber/red → publish alert stream    │ │
│  └─────────────────────────┬─────────────────────────────────────────┘ │
│                            │ node:* keys + stream:alerts               │
│  ┌─────────────────────────▼─────────────────────────────────────────┐ │
│  │                       Redis :6379                                 │ │
│  │  node:{svc}:{cat}:{metric}           HASH  — current state (2h)  │ │
│  │  timeseries:{svc}:{cat}:{metric}     LIST  — 300-entry window    │ │
│  │  stream:alerts                       STREAM — fan-out             │ │
│  └──────────┬───────────────────────────────────┬────────────────────┘ │
│             │                                   │                      │
│  ┌──────────▼───────────┐          ┌────────────▼──────────────────┐   │
│  │    topology-agent    │          │       diagnosis-agent          │   │
│  │                      │          │                                │   │
│  │  discover Redis keys │          │  consume stream:alerts         │   │
│  │  → build graph       │          │  → noise suppress              │   │
│  │  → lifecycle track   │          │  → pattern match (no LLM)     │   │
│  │  → ML brain layer    │          │  → LLM root-cause analysis     │   │
│  │  → snapshot Neo4j    │          │  → write incidents to SQLite   │   │
│  └──────────┬───────────┘          └────────────────────────────────┘   │
│             │                                                           │
│  ┌──────────▼──────────────────────────────────────────────────────┐   │
│  │                  THE BRAIN — Permanent Memory                   │   │
│  │                                                                  │   │
│  │  SQLite (/data/service_brain.db)                                 │   │
│  │  ├── known_services      lifecycle: active/offline/deprecated   │   │
│  │  ├── topology_edges      confirmed call edges + confidence       │   │
│  │  ├── metric_baselines    mean/std per (svc, metric, hour, dow)  │   │
│  │  ├── noise_fingerprints  suppression counts per time bucket      │   │
│  │  ├── communication_patterns  call volume/latency statistics     │   │
│  │  └── incidents           full LLM diagnosis history             │   │
│  │                                                                  │   │
│  │  Neo4j (:7474)  — topology snapshots with full graph history    │   │
│  │  ML models (/data/ml_models/*.pkl)  — Isolation Forests         │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                 dashboard :8501 (Streamlit)                      │   │
│  │  live topology (all discovered services) · incidents ·           │   │
│  │  noise log · history · per-service timeseries                   │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Layer-by-Layer Breakdown

### Layer 0 — Application Services

| Service | Port | Type | Responsibilities |
|---|---|---|---|
| `order-service` | 8001 | Always-on API | Accepts orders, calls payment + inventory downstream |
| `payment-service` | 8002 | Always-on API | Processes payments; fault-injection `/control` endpoint |
| `inventory-service` | 8003 | Always-on API | Manages stock; emits Kafka-like consumer lag events |
| `report-service` | 8004 | Scheduled batch | Runs a 5-minute data pipeline job once daily (02:00); idles silently otherwise |

Each service emits **OpenTelemetry traces** and exposes a **Prometheus `/metrics`** endpoint. The monitoring agent accepts any service label — adding a new service to Prometheus is all that is needed for it to appear in the system.

### Layer 1 — Observability Pipeline

```
Services → OTEL Collector (4317/4318) → Prometheus (9090) → Monitoring Agent
```

- **Monitoring Agent** polls Prometheus every 10 s for every metric in `PROMETHEUS_POLLS`
- Accepts any `service` label it finds — no hardcoded service list
- Writes `node:{service}:{category}:{metric}` HASH keys to Redis for every discovered service
- Publishes `stream:alerts` when any metric crosses amber/red thresholds (60 s cooldown)
- Also receives **Alertmanager webhooks** on port 8080

### Layer 2 — Memory System

See [Section 5 — Memory Architecture](#5-memory-architecture) for the full diagram.

| Tier | Technology | Lifetime | Purpose |
|---|---|---|---|
| Hot state | Redis HASH `node:*` | 2 h TTL | Current metric values, status, thresholds |
| Time series | Redis LIST `timeseries:*` | 2 h TTL | 300-entry (50 min) sliding window per metric |
| Alert bus | Redis STREAM `stream:alerts` | Consumed | Fan-out alert delivery to all agent consumers |
| Live graph | NetworkX DiGraph (in-process) | Process lifetime | 3-level directed topology graph |
| Topology snapshots | Neo4j | Permanent | Full graph history queryable via Cypher |
| Permanent brain | SQLite | Permanent | Incidents, baselines, call edges, lifecycle, ML logs |

### Layer 3 — The Brain (ML Models)

The brain is what separates OpsCortex from a simple threshold alerting system. It builds understanding over time from two sources working together: **Redis** (what is happening right now) and **SQLite** (what has happened historically). Every ML decision uses both.

```
Every 5s — Redis node:* keys          Permanent — SQLite tables
  current metric values            +    metric_baselines (learned norms)
  current service statuses              topology_edges   (call relationships)
  last 300 timeseries entries           noise_fingerprints (structural noise)
         │                              incidents (past diagnoses)
         └──────────┬──────────────────────────┘
                    ▼
           ┌─────────────────────────────────────────────┐
           │            ML Brain Layer                   │
           │                                             │
           │  1. BaselineModel    — z-score per metric   │
           │  2. BaselineModel    — trend z-score        │
           │  3. AnomalyDetector  — Isolation Forest     │
           │  4. NoiseClassifier  — suppress or pass     │
           │  5. PatternMatcher   — LLM or cache hit     │
           │  6. CommunicationModel — call pair norms    │
           └─────────────────────────────────────────────┘
```

#### 1 — Baseline Model: Self Z-Score (`ml/baseline_model.py`)

Tracks **rolling mean and standard deviation** per `(service, metric, hour_of_day, day_of_week)`. Persisted to `metric_baselines` in SQLite every 50 samples. Loaded on startup so the model never starts blind.

The key insight is **time-bucketing**. A batch service like `report_service` has completely different normal behaviour at 02:00 (running, ~100ms latency) versus 14:00 (idle, 0ms). Without bucketing, the mean would average both and every run would look anomalous.

```
report_service / latency_p99 / hour=2 / dow=6
  Stored after 3 runs:  mean=98ms, std=4ms

Run 4 at 105ms → z = (105 - 98) / 4 = +1.75  ← normal variation, suppress
Run 4 at 450ms → z = (450 - 98) / 4 = +88.0  ← huge spike, DO NOT suppress
```

The z-score is passed to the Noise Classifier as an override — if `|z| > 2.5` the alert is never suppressed regardless of how many times it has fired before.

#### 2 — Baseline Model: Trend Z-Score (`ml/baseline_model.get_trend_zscore`)

A plain z-score **cannot detect gradual degradation** because the rolling mean chases the trend:

```
Run 1: 95ms   mean=95    z_of_next = high
Run 2: 110ms  mean=102   z ≈ 1.3   ← already adapting
Run 3: 125ms  mean=110   z ≈ 1.2
Run 4: 140ms  mean=117   z ≈ 1.2   ← mean keeps following — never fires
Run 5: 155ms  mean=125   z ≈ 1.2
```

The trend z-score fits a **linear regression** over the last 10 samples and normalises the slope by the standard deviation of the residuals:

```
trend_z = slope_per_sample / std_of_residuals_around_fit_line
```

- `trend_z < 1.5` → flat, no consistent direction → normal
- `trend_z > 2.0` → consistent upward drift → real degradation
- `trend_z > 3.0` → strong persistent slope → high confidence incident

This fires alongside the self z-score as a second override in the Noise Classifier. Even if each individual value looks "not that different from last time", a consistent rising slope over 10 observations is treated as a real degradation signal.

#### 3 — Anomaly Detector: Isolation Forest (`ml/anomaly_detector.py`)

One **Isolation Forest** per service, trained on the **complete metric feature vector** — not individual metrics. Saved as `.pkl` files, reloaded on startup, retrained every 24 hours.

Input: a dict of z-scores for every metric the service has:
```
{"performance_error_rate": 0.3, "performance_latency_p99": 1.2,
 "database_db_connection_count": 0.1, "batch_batch_job_running": 0.0}
```

Output: anomaly score `[0, 1]`. `0.5` = neutral (no model). `> 0.7` = collectively unusual.

This catches the case where no single metric crosses a threshold but the combination of metrics has never appeared during normal operation — a subtle multi-dimensional anomaly that threshold checks miss entirely.

#### 4 — Noise Classifier (`ml/noise_classifier.py`)

Controls whether an alert becomes an incident or gets silently dropped. Uses the `noise_fingerprints` SQLite table which counts occurrences per `(service, metric, hour_of_day, day_of_week)`.

**The decision logic:**

```
Alert arrives for (report_service, latency_p99, hour=2, dow=6)

Step 1: Check fingerprint count
  count = noise_fingerprints[(report_service, latency_p99, 2, 6)]
  if count < 3:
    → pass through (too few observations to suppress)

Step 2: count >= 3 — suppression check runs
  Override A — self z-score:
    z = baseline_model.get_zscore(report_service, latency_p99, current_value)
    if |z| > 2.5:  → NOT suppressed (sudden spike)

  Override B — trend z-score:
    trend_z = baseline_model.get_trend_zscore(report_service, latency_p99)
    if trend_z > 2.0:  → NOT suppressed (gradual degradation)

  Override C — peer anomaly:
    for every other service in graph:
      if any metric has |z| > 2.0:  → NOT suppressed (correlated incident)

  All overrides passed:
    → SUPPRESSED as structural noise
```

**The practical result for `report_service`:**

| Run | Count | Value | Self z | Trend z | Decision |
|-----|-------|-------|--------|---------|----------|
| 1 | 0 | 95ms | — | — | **INCIDENT** (below threshold) |
| 2 | 1 | 102ms | — | — | **INCIDENT** |
| 3 | 2 | 98ms | — | — | **INCIDENT** |
| 4 | 3 | 100ms | 0.4 | 0.1 | **SUPPRESSED** ✅ normal |
| 5 | 4 | 310ms | 25.0 | — | **INCIDENT** ✅ spike caught |
| 6 | 5 | 420ms | 44.0 | 2.4 | **INCIDENT** ✅ trend caught |

#### 5 — Pattern Matcher (`ml/pattern_matcher.py`)

Before the LLM is called, scans the last 30 days of resolved `incidents` in SQLite for the same `(service, metric)` pair with alert value within ±40%.

- **≥2 prior matches** → reconstructs diagnosis from the best past incident. LLM is not called — zero cost, instant.
- **1 prior match** → passes it as a hint to the LLM prompt.
- **No match** → full LLM call.

Over time the system becomes cheaper: known fault patterns never call the LLM again. A new type of fault (e.g. `report_service` latency at 400ms — never seen before) bypasses the pattern matcher and gets a fresh LLM diagnosis.

#### 6 — Communication Model (`ml/communication_model.py`)

Tracks **call volume and latency per service pair per hour of day**. Persisted to `service_communication_patterns` in SQLite.

Reads call edges from NetworkX (EMA-smoothed call count and latency per edge) and computes:
```
combined_z = (|call_count_z| + |latency_z|) / 2
```

Tells the LLM which inter-service relationships are behaving abnormally *for this time of day* — distinguishing "payment_service is slow because it is broken" from "order_service is hammering payment_service with 3x its normal call volume."

#### How the brain identifies root cause

The six models feed into a two-step root cause algorithm:

**Step 1 — Graph BFS (topology agent, NetworkX)**

Starting from the faulted service, walks `CALLS` edges in both directions to find all connected services. These call edges were learned over time from OTel labels and Pearson traffic correlation, stored in `topology_edges` SQLite.

```
order_service is faulted → BFS finds: [payment_service, inventory_service, order_service]
```

**Step 2 — Temporal ordering (Redis timeseries)**

For each candidate service, scans its Redis `timeseries:*` lists to find the **earliest timestamp** where any metric crossed amber. Whichever service crossed a threshold first is the root cause. The others are victims.

```
payment_service  first amber at 14:03:10  ← ROOT CAUSE
order_service    first amber at 14:03:47  ← VICTIM (37 seconds later)
inventory_service  no threshold crossed   ← INNOCENT BYSTANDER
```

The LLM receives this pre-computed `inferred_root_cause` and `fault_path` as the **first thing it reads** — structured in plain text before the raw JSON. The system prompt explicitly instructs it to use `inferred_root_cause` as its primary signal and to confirm or challenge it using `topology_history.status_progression`. It is not guessing — it is explaining with specific evidence from z-scores, anomaly trends, and topology changes why the brain's determination is correct, and what to do about it.

See [Layer 5 — LLM Reasoning](#layer-5--llm-reasoning-agentsllm_analyzerpy) for the full prompt structure and output format.

### Layer 4 — Agents

Three always-on agent containers plus a background trainer:

#### Monitoring Agent (`agents/monitoring_agent.py`)
- Polls Prometheus every 10 s; accepts **any service** in the metric labels — no allowlist
- Computes amber/red status per metric against configurable thresholds
- Writes `node:{svc}:{cat}:{metric}` HASH + `timeseries:*` LIST to Redis
- Publishes `stream:alerts` on threshold breaches

#### Topology Agent (`agents/topology_agent.py`)
- **Cold start:** reads `known_services`, `metric_baselines`, and `topology_edges` from SQLite — graph is pre-populated before any Redis data arrives
- **Every 5 s:** scans `node:*` Redis keys → discovers new services/categories/metrics → updates NetworkX graph
- **Service lifecycle:** reconciles live Redis services against permanent memory every cycle
  - New service in Redis → registered in `known_services`, log "first time seen"
  - Service absent from Redis → marked `offline` in graph + SQLite (not deleted)
  - Service absent > `DEPRECATION_THRESHOLD_DAYS` (default 7 days) → deprecated, purged from permanent memory and graph
- **Call-graph inference:** two signals — OTel `upstream_service` label (authoritative) and Pearson correlation of request_rate time-series (statistical). Every confirmed edge written to `topology_edges` with growing `confirmation_count`
- **Every 60 s:** snapshots full graph to Neo4j

#### Diagnosis Agent (`agents/diagnosis_agent.py`)
- Consumes `stream:alerts` from Redis (consumer group, at-least-once delivery)
- Runs **noise suppression** via `NoiseClassifier` — suppressed alerts logged to SQLite
- Buffers surviving alerts; every 15 min flushes the batch:
  1. `PatternMatcher` — if ≥2 prior matching incidents: reuse diagnosis, skip LLM
  2. Builds rich context: 50-entry time-series, graph topology, fault path (BFS on call edges), recent actions, ML scores
  3. **LLM call** — structured JSON response with `root_cause`, `contributing_factors`, `confidence`, `recommended_fix`, `estimated_impact`
  4. Writes incident to SQLite

### Layer 5 — LLM Reasoning (`agents/llm_analyzer.py`)

The LLM does not re-derive the root cause from scratch — the ML brain has already done that work. The LLM's job is to **explain, confirm, and add remediation context** to what the brain already determined.

#### What the LLM receives

The prompt is structured in two sections. Brain signals come first in plain readable form so the LLM sees them before the raw data:

```
=== ML BRAIN SIGNALS (pre-computed — use as primary evidence) ===

Primary faulted service : order_service
Primary faulted metric  : error_rate = 0.24
Inferred root cause     : payment_service  ← graph BFS + temporal ordering
Fault path (time order) : ['payment_service', 'order_service']

Isolation Forest score  : 0.87  (VERY HIGH — full metric profile highly unusual)
Top metric z-scores     :
  performance/error_rate                         z=+14.30  (14.3σ above normal for this hour)
  performance/latency_p99                        z=+8.10   (8.1σ above normal for this hour)
  database/db_connection_count                   z=+3.20   (3.2σ above normal for this hour)

Call pattern z-scores   :
  order_service->payment_service                 z=+3.20   (abnormal call volume/latency)

Neo4j snapshots used    : 3 (oldest was 4.2 min before alert)
Status progression      :
  payment_service              healthy → degraded → faulted
  order_service                healthy → healthy  → degraded

Anomaly score trend     :
  payment_service              [0.51, 0.71, 0.89]  (rising)

Recent operational actions:
  deployment on payment_service at 14:32:01

=== FULL INCIDENT CONTEXT (raw JSON for detail) ===
{ ... metric_timeseries, graph_topology, all_alerts ... }
```

#### What each brain field instructs the LLM to do

| Field | Instruction to LLM |
|---|---|
| `inferred_root_cause` | Trust this as primary signal — computed from graph BFS + who crossed amber first |
| `fault_path` | First = root cause, rest = downstream victims — use to scope impact |
| `ml_scores.zscores` | Use to identify the specific metric driving anomaly — z=14 is much stronger than z=1.2 |
| `ml_scores.anomaly_score` | Above 0.7 = collectively unusual even if no individual threshold crossed |
| `ml_scores.call_pattern_zscores` | High score = abnormal inter-service traffic — distinguishes broken service from overloaded one |
| `topology_history.status_progression` | Whichever service degraded earliest across snapshots = root cause confirmation |
| `topology_history.anomaly_score_trend` | Rising trend = slow leak/degradation; sudden jump = crash or deployment |
| `topology_history.new_call_edges` | New dependency just before incident = high suspicion for the cause |
| `recent_actions` | Deployment or config change on root cause service = likely trigger |

#### Confidence rules built into the prompt

- **high** — `inferred_root_cause` confirmed by `status_progression` AND z-scores clearly elevated
- **medium** — root cause plausible but `status_progression` ambiguous or z-scores moderate
- **low** — contradictions between signals, or insufficient data (`anomaly_score=0.5`, `zscores=0`)

#### What the LLM produces

```json
{
  "root_cause": "payment_service error_rate spiked to 14.3σ above its Tuesday-afternoon
                 baseline, confirmed by status_progression showing payment degraded 37s
                 before order_service crossed amber",
  "contributing_factors": [
    "Isolation Forest anomaly_score=0.87 — full metric profile of payment_service
     has never looked like this during normal operation",
    "call_pattern_zscore order→payment = 3.2 — order_service is sending normal call
     volume but payment latency is 3σ above the hourly norm, confirming payment is
     the broken side not the caller",
    "Deployment on payment_service at 14:32:01, 4 minutes before first amber crossing"
  ],
  "confidence": "high",
  "recommended_fix": "Roll back payment_service deployment from 14:32 or check payment
                      processor connectivity — error_rate at 14.3σ above baseline
                      indicates systemic failure not traffic spike",
  "estimated_impact": "order_service downstream victim — all order creation failing
                       until payment_service recovers"
}
```

The output is stored directly to SQLite `incidents` table and displayed in the dashboard. The `contributing_factors` reference specific z-scores and topology history observations rather than generic descriptions.

Backends: **Claude** (default, `claude-haiku-4-5-20251001`), **OpenAI** (GPT-4o), **Ollama** (local).

### Layer 6 — Dashboard (`dashboard/app.py`) — Port 8501

Fully discovery-driven — reads services from Redis keys at runtime, not from a hardcoded list.

| Panel | Content |
|---|---|
| Live Topology | PyVis graph with dynamically-positioned service nodes; call edges from `topology_edges`; offline services shown in grey |
| Active Incident | LLM diagnosis with fault path, contributing factors, recommended fix, one-click resolve |
| Noise Suppression Log | Alerts suppressed by the noise classifier with reason |
| Incident History | Last 10 incidents with root cause, confidence, fault path |
| Per-Service Timeseries | One tab per discovered service; all discovered metrics; includes batch metrics for `report_service` |

---

## 5. Memory Architecture

OpsCortex uses four distinct memory tiers. Each has a different lifetime and serves a different part of the system. Understanding what lives where is essential to understanding how the brain works.

---

### Tier 1 — Redis: Hot Working Memory

**Lifetime:** 2 hour TTL per key. Lost on restart. Expires if the monitoring agent stops writing.

Redis is the system's present tense. It holds what is happening right now.

```
node:{service}:{category}:{metric}         HASH
  Fields: value, status, threshold_low, threshold_high, timestamp
  Example: node:order_service:performance:error_rate
             value=0.24, status=red, threshold_low=0.05, threshold_high=0.20

timeseries:{service}:{category}:{metric}   LIST (capped at 300 entries)
  Each entry: {ts: 1780268645, value: 0.24}
  Covers ~50 minutes at 10-second poll interval
  Read by: LLM context builder, baseline model training

stream:alerts                              STREAM
  Consumer group: diagnosis-agents
  Each entry: service, category, metric, value, threshold, severity, timestamp
  Written by: monitoring-agent on threshold breach
  Read by: diagnosis-agent (at-least-once delivery)

actions:{service}                          STREAM
  Each entry: event_type, timestamp, triggered_by, details
  Examples: last_deployment, last_restart, last_config_change
  Read by: topology-agent, LLM context builder

last_alert:{service}:{metric}              STRING
  Value: epoch timestamp of last alert for this pair
  TTL: 1 hour — enforces 60-second alert cooldown
```

**Who writes to Redis:**
- Monitoring agent → `node:*`, `timeseries:*`, `stream:alerts`, `last_alert:*`
- Services themselves → `actions:*` (operational events)

**Who reads from Redis:**
- Topology agent → `node:*` every 5s to sync graph
- Diagnosis agent → `stream:alerts` (consumer group)
- Baseline model → `timeseries:*` for training samples
- LLM context builder → `timeseries:*` for the 50-entry context window

---

### Tier 2 — NetworkX: Live In-Process Graph

**Lifetime:** Process lifetime only. Lost on restart — immediately rebuilt from SQLite.

NetworkX holds the live topology graph in memory inside the topology-agent process. It is the fast working representation of which services exist, what state they are in, and how they connect.

```
3-level directed graph:

  Level 1 — Service nodes
    order_service   status=faulted  anomaly_score=0.87  zscore_worst=error_rate
    payment_service status=offline  offline_since=1780268000
    report_service  status=healthy  anomaly_score=0.51

  Level 2 — Category nodes (per service)
    order_service:performance  status=red
    order_service:database     status=amber
    order_service:kafka        status=green
    order_service:actions      status=green

  Level 3 — Metric nodes (per category)
    order_service:performance:error_rate     value=0.24  status=red
    order_service:performance:latency_p99    value=420   status=amber
    order_service:database:db_connection_count value=17  status=amber

  CALLS edges (service → service)
    order_service → payment_service   call_count=422  latency_ema=52ms  stale=False
    order_service → inventory_service call_count=163  latency_ema=31ms  stale=False
```

**Status propagation runs every 5s:**
```
metric status (green/amber/red)
    → worst of all metrics = category status
    → worst of all categories = service status (healthy/degraded/faulted)
    → but never overwrites 'offline' set by lifecycle management
```

**Used by:**
- Root cause algorithm: BFS on CALLS edges to find connected services
- Diagnosis agent: `get_upstream_neighbors()`, `get_downstream_neighbors()`
- Dashboard: renders the live topology graph
- Communication model: reads call count and latency EMA per edge

---

### Tier 3 — SQLite: Permanent Brain

**Lifetime:** Permanent. Survives restarts, container rebuilds, and system outages.

SQLite is the system's long-term memory — everything it has ever learned. The topology agent reads it on cold start to reconstruct the graph before any Redis data arrives.

#### `known_services` — service lifecycle registry

```
name               status    first_seen   last_seen_in_redis  offline_since
order_service      active    1780267846   1780318212          NULL
payment_service    active    1780267846   1780318212          NULL
report_service     active    1780268707   1780318212          NULL
old_auth_service   offline   1780000000   1780100000          1780100001
```

Written by: topology-agent every 5s (`mark_service_active`, `mark_services_offline`)
Read by: topology-agent on cold start (`get_active_and_offline_services`)
Purpose: topology agent knows all services even when Redis is empty

When `status=offline` and `offline_since` is more than 7 days ago → `deprecate_and_purge_service()` removes all related rows and deletes the service from the live graph.

#### `topology_edges` — permanent call relationships

```
caller            callee             source        confirmation_count  avg_latency_ms
order_service     payment_service    correlation   422                 52.1
order_service     inventory_service  correlation   163                 31.4
inventory_service payment_service    correlation   408                 48.7
```

Written by: topology-agent whenever a call edge is observed (OTel label or Pearson correlation)
Read by: topology-agent on cold start to pre-populate CALLS edges
Key field: `confirmation_count` — grows every sync cycle. After hundreds of observations the system is highly confident the edge is real, not a fluke of traffic correlation.
`source=otel` edges are more authoritative than `source=correlation` edges.

#### `metric_baselines` — what normal looks like per time window

```
service         metric        hour_of_day  day_of_week  mean    std      sample_count
order_service   error_rate    14           2            0.018   0.004    450
order_service   error_rate    14           1            0.021   0.005    380
report_service  latency_p99   2            6            98.3    4.1      12
report_service  latency_p99   2            0            102.1   5.2      8
report_service  request_rate  14           6            0.0     0.001    600
```

Written by: baseline model every 50 samples (`upsert_metric_baseline`)
Read by: baseline model on cold start; noise classifier z-score override check

This is why the system understands that `report_service/request_rate=3.0` at 02:00 on Sunday is normal, but the same value at 14:00 on Wednesday is a crisis. The hour+day-of-week bucket makes the context time-aware.

Also powers the **trend z-score**: the baseline model reads the in-memory `_samples` list (which is rebuilt from Redis timeseries on every topology-agent sync) and fits a linear regression. The slope divided by residual standard deviation gives a dimensionless trend signal that detects gradual degradation the simple z-score misses.

#### `noise_fingerprints` — structural noise registry

```
service         metric        hour_of_day  day_of_week  occurrence_count  last_seen
report_service  latency_p99   2            6            4                 1780318000
inventory_service consumer_lag 2           0            9                 1780315000
```

Written by: noise classifier on every alert (`record_alert` → `upsert_noise_fingerprint`)
Read by: noise classifier to decide whether to suppress

Once `occurrence_count >= 3` for a bucket, the suppress check runs. The self z-score and trend z-score overrides can still un-suppress it if the value is genuinely anomalous.

#### `incidents` — full LLM diagnosis history

```
id  timestamp   service       metric       alert_value  root_cause_service  llm_confidence
1   1780267900  order_service error_rate   0.24         payment_service     high
2   1780268800  report_service latency_p99 310.0        report_service      medium
```

Plus full fields: `fault_path` (JSON list), `llm_root_cause` (text), `llm_contributing_factors` (JSON list), `llm_recommended_fix`, `llm_estimated_impact`, `context_snapshot` (the full JSON sent to the LLM).

Written by: diagnosis agent after every LLM call or pattern match
Read by: pattern matcher (scans last 30 days for matching `service+metric+value`)

The pattern matcher's ±40% value tolerance means `latency=310ms` matches `latency=290ms` but does NOT match `latency=100ms` — so a healthy normal run and an anomalous run are correctly treated as different patterns.

#### Other SQLite tables

| Table | Written by | Purpose |
|---|---|---|
| `suppressed_alerts` | Diagnosis agent | Log of every noise-suppressed alert with reason |
| `service_communication_patterns` | Communication model | Call volume/latency mean+std per service pair per hour |
| `graph_snapshots` | Topology agent | Registry of Neo4j snapshot IDs and timestamps |
| `model_training_log` | Model trainer | History of every Isolation Forest retraining event |

---

### Tier 4 — Neo4j: Graph Snapshot History

**Lifetime:** Permanent. Write-only by agents. Read by humans and the diagnosis agent.

Neo4j stores a full copy of the NetworkX graph every 60 seconds — Service nodes, Category nodes, Metric nodes, HAS_CATEGORY edges, HAS_METRIC edges, and CALLS edges — all tagged with `snapshot_id` and `snapshot_ts`.

**Written by:** topology-agent (`save_topology_snapshot`) every 60s
**Read by:** diagnosis-agent at diagnosis time (`get_topology_context_for_diagnosis`)

When the diagnosis agent is about to call the LLM, it queries the last 3 Neo4j snapshots taken before the alert timestamp and extracts:

```
status_progression:  payment_service: ["healthy", "degraded", "faulted"]
                     order_service:   ["healthy", "healthy",  "degraded"]
→ LLM sees: payment_service degraded first — it is the root cause

anomaly_score_trend: payment_service: [0.51, 0.71, 0.89]
→ LLM sees: gradual score rise = slow degradation, not sudden crash

new_call_edges:      [{caller: "report_service", callee: "order_service",
                       note: "edge not present in previous snapshot"}]
→ LLM sees: new dependency appeared just before incident
```

No other part of the system reads Neo4j programmatically. It exists so the LLM can reason about **how the system changed over time** — something Redis (which only shows the present) and SQLite (which shows aggregated statistics) cannot provide.

---

### How the four tiers work together on a cold start

```
System restarts
      │
      ▼
1. Read SQLite known_services
   → graph nodes created for every active/offline service
   → offline services immediately shown as grey in dashboard

2. Read SQLite topology_edges
   → CALLS edges recreated in NetworkX
   → BFS root-cause algorithm works before any traffic observed

3. Read SQLite metric_baselines
   → baseline model stats loaded into memory
   → z-score and trend-z checks work from first alert

4. ML models loaded from /data/ml_models/*.pkl
   → Isolation Forest scoring works immediately, no retraining needed

5. Redis node:* keys arrive (5-10 seconds after startup)
   → graph nodes populated with live values
   → status propagation runs
   → baseline model starts recording new samples

6. stream:alerts begins flowing
   → noise classifier checks against loaded fingerprints
   → pattern matcher scans SQLite incidents
   → LLM called for unknown faults
   → Neo4j consulted for topology change history
```

---

### What the brain learns over time

| After | What is known |
|---|---|
| **First run** | Services exist, basic metrics. No baselines. Every alert is an incident. |
| **1 day** | Hourly norms established for all active metrics. First Isolation Forest trained. Normal batch jobs start being suppressed. |
| **1 week** | All 168 hour×day buckets populated. Noise fingerprints stable. Pattern matcher starts catching recurring faults. Call edges confirmed hundreds of times. |
| **1 month** | System rarely calls the LLM for known fault types. High confidence on all root-cause decisions. Isolation Forest robust to seasonal variation. |

### Service lifecycle in permanent memory

```
Redis keys appear for a new service
         │
         ▼
  known_services: ACTIVE
  first_seen = now
  Baseline model starts collecting samples
  Isolation Forest will train after 20 samples
         │
         │  Redis keys stop appearing (deployment / crash)
         ▼
  known_services: OFFLINE
  offline_since = now
  Graph node status → "offline" (grey in dashboard)
  All metric structure preserved
  Permanent memory retained — system still knows it exists
         │
    ┌────┴────────────────────────────────┐
    │ Redis keys return                   │ absent > 7 days
    ▼                                     ▼
  status → ACTIVE                   status → DEPRECATED
  graph re-populated                 metric_baselines deleted
  offline_since cleared              noise_fingerprints deleted
                                     topology_edges deleted
                                     service_communication_patterns deleted
                                     known_services.deprecated_at = now
                                     incidents KEPT (audit history)
```

---

## 6. Failure Scenarios

Run any scenario on demand using Docker Compose profiles:

| Scenario | File | What it injects | Expected diagnosis |
|---|---|---|---|
| 01 — Request Surge | `scenario_01_request_surge.py` | 150 req/s spike → error rate + latency breach | `order_service` overloaded |
| 02 — Kafka Lag | `scenario_02_kafka_lag.py` | Slow consumer, lag builds to 2000+ | `inventory_service` Kafka consumer lag |
| 03 — Cascade Failure | `scenario_03_cascade_failure.py` | Payment service 90% errors → order service fails downstream | Root cause: `payment_service`; victim: `order_service` |
| 04 — Known Pattern Replay | `scenario_04_known_pattern_replay.py` | Replays a previously seen fault; pattern matcher should skip LLM | Pattern-matched diagnosis (no LLM call) |
| 05 — Internal Bottleneck | `scenario_05_internal_bottleneck.py` | DB connection pool exhaustion | `order_service` DB bottleneck |
| 06 — New Service Registration | `scenario_06_new_service_registration.py` | Triggers `report_service` batch job; verifies topology auto-discovery | Service registered in permanent memory; Neo4j node created; baseline model starts learning |
| 07 — Batch Suppression Learning | `scenario_07_batch_suppression_learning.py` | Runs batch job 4 times at normal latency; verifies runs 1-3 produce incidents and run 4 is suppressed | Root cause = `report_service` isolated; fault path = `[report_service]`; run 4 suppressed as noise |
| 08 — Batch Degradation Detection | `scenario_08_batch_degradation_detection.py` | After suppression learned, injects 200→500ms latency across 3 runs | All 3 produce incidents despite suppression being active; self z-score override fires; LLM identifies degradation trend |

**Run order for testing the full suppress→detect lifecycle:**

```bash
# 1. Clean slate
docker compose --profile scenarios run --rm reset-for-testing
docker compose restart diagnosis-agent topology-agent

# 2. Learn normal pattern (runs 1-3 incident, run 4 suppressed)
docker compose --profile scenarios run --rm scenario-07

# 3. Inject degradation (all 3 runs produce incidents despite suppression)
docker compose --profile scenarios run --rm scenario-08
```

---

## 7. Commands & Data Inspection

### Neo4j Graph Queries

Open the Neo4j browser at **http://localhost:7474**  
Login: `neo4j` / `password123`

#### View all service nodes and their current status
```cypher
MATCH (s:Service) RETURN s.name, s.status, s.anomaly_score ORDER BY s.snapshot_ts DESC LIMIT 10
```

#### View the full topology for the latest snapshot
```cypher
MATCH (s:Service)-[:HAS_CATEGORY]->(c:Category)-[:HAS_METRIC]->(m:Metric)
RETURN s.name AS service, c.name AS category, m.metric_name AS metric,
       m.current_value AS value, m.status AS status
ORDER BY s.name, c.name, m.metric_name
```

#### View service call graph
```cypher
MATCH (a:Service)-[r:CALLS]->(b:Service)
RETURN a.name AS caller, b.name AS callee, r.call_count, r.avg_latency_ms
```

#### Find all red/faulted metrics
```cypher
MATCH (m:Metric) WHERE m.status = 'red'
RETURN m.parent_service, m.parent_category, m.metric_name,
       m.current_value, m.threshold_high
ORDER BY m.snapshot_ts DESC
```

#### List all topology snapshots (most recent first)
```cypher
MATCH (s:Service) RETURN DISTINCT s.snapshot_id, s.snapshot_ts, s.trigger
ORDER BY s.snapshot_ts DESC LIMIT 20
```

#### Compare two snapshots
```cypher
MATCH (s:Service) WHERE s.snapshot_id IN ['<id1>', '<id2>']
RETURN s.snapshot_id, s.name, s.status, s.anomaly_score
ORDER BY s.snapshot_id, s.name
```

#### Count nodes per snapshot
```cypher
MATCH (n) WHERE n.snapshot_id IS NOT NULL
RETURN n.snapshot_id, labels(n)[0] AS type, COUNT(n) AS count
ORDER BY n.snapshot_id DESC
```

### Redis Working Memory

Connect directly:
```bash
docker exec -it redis redis-cli
```

#### View current metric state for a service
```
HGETALL node:order_service:performance:error_rate
HGETALL node:payment_service:performance:latency_p99
HGETALL node:inventory_service:kafka:consumer_lag
```

#### View time series (last 10 entries)
```
LRANGE timeseries:order_service:performance:error_rate 0 9
LRANGE timeseries:payment_service:performance:latency_p99 0 9
```

#### View the alert stream (last 5 alerts)
```
XREVRANGE stream:alerts + - COUNT 5
```

#### Check alert cooldown timestamps
```
KEYS last_alert:*
GET last_alert:order_service:error_rate
```

#### List all metric state keys
```
KEYS node:*
```

#### Check service-level status summary
```
HGETALL service:status
```

#### Monitor live writes in real time
```
MONITOR
```

### SQLite Incident History

```bash
docker exec -it diagnosis-agent sqlite3 /data/service_brain.db
```

#### View all unresolved incidents
```sql
SELECT id, datetime(timestamp,'unixepoch'), service, metric,
       alert_value, llm_root_cause, llm_confidence
FROM incidents WHERE resolved = 0
ORDER BY timestamp DESC;
```

#### View full LLM diagnosis for latest incident
```sql
SELECT id, service, metric, llm_root_cause,
       llm_contributing_factors, llm_confidence,
       llm_recommended_fix, llm_estimated_impact
FROM incidents ORDER BY timestamp DESC LIMIT 1;
```

#### View incident history with fault paths
```sql
SELECT id, datetime(timestamp,'unixepoch'), service, metric,
       fault_path, llm_root_cause, resolved
FROM incidents ORDER BY timestamp DESC LIMIT 10;
```

#### View noise suppression log
```sql
SELECT id, datetime(timestamp,'unixepoch'), service, metric, reason
FROM suppressed_alerts ORDER BY timestamp DESC LIMIT 20;
```

#### View metric baselines (z-score stats)
```sql
SELECT service, metric, hour_of_day, sample_count, mean, std_dev
FROM metric_baselines ORDER BY service, metric;
```

#### View model training history
```sql
SELECT model_name, datetime(trained_at,'unixepoch'), samples_used, status
FROM model_training_log ORDER BY trained_at DESC;
```

#### Export incidents to CSV
```bash
docker exec diagnosis-agent sqlite3 -csv /data/service_brain.db \
  "SELECT * FROM incidents" > incidents.csv
```

### Prometheus Metrics

Open **http://localhost:9090** for the Prometheus UI.

Useful PromQL queries:

```promql
# Current error rates per service
service_error_rate

# P99 latency per service
service_request_duration_p99

# Request rate
service_request_rate

# DB connections
service_db_connections_active

# Kafka consumer lag
service_kafka_consumer_lag

# Rate of increase in error rate (last 2 min)
rate(service_error_rate[2m])

# Services with error rate above 20%
service_error_rate > 0.20
```

### Service Control API

Inject faults manually without running a full scenario:

```bash
# Inject 50% error rate into payment service
curl -X POST http://localhost:8002/control \
  -H "Content-Type: application/json" \
  -d '{"extra_error_rate": 0.5}'

# Add 200ms artificial delay to order service
curl -X POST http://localhost:8001/control \
  -H "Content-Type: application/json" \
  -d '{"artificial_delay_ms": 200}'

# Reset all controls on all services
curl -X POST http://localhost:8001/control -H "Content-Type: application/json" -d '{"artificial_delay_ms": 0, "extra_error_rate": 0.0}'
curl -X POST http://localhost:8002/control -H "Content-Type: application/json" -d '{"artificial_delay_ms": 0, "extra_error_rate": 0.0}'
curl -X POST http://localhost:8003/control -H "Content-Type: application/json" -d '{"artificial_delay_ms": 0, "extra_error_rate": 0.0}'

# Check service health
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health

# Trigger an Alertmanager-style webhook manually
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -d '{"alerts": [{"labels": {"service": "order_service", "metric": "error_rate", "severity": "critical"}, "annotations": {"value": "0.35", "threshold": "0.20"}}]}'
```

---

## 8. Configuration


All thresholds and intervals are in `config.py` and can be overridden via environment variables:

| Variable | Default | Description |
|---|---|---|
| `THRESH_ERROR_RATE_AMBER` | `0.05` | Error rate amber threshold |
| `THRESH_ERROR_RATE_RED` | `0.20` | Error rate red threshold |
| `THRESH_LATENCY_AMBER` | `300` | P99 latency amber (ms) |
| `THRESH_LATENCY_RED` | `800` | P99 latency red (ms) |
| `THRESH_LAG_RED` | `1000` | Kafka consumer lag red |
| `THRESH_DB_CONN_RED` | `18` | DB connections red |
| `LLM_BATCH_INTERVAL_SECONDS` | `900` | LLM call frequency (seconds) |
| `NOISE_SUPPRESSION_THRESHOLD` | `3` | Alerts before noise suppression kicks in |
| `ISOLATION_FOREST_CONTAMINATION` | `0.05` | Anomaly detector sensitivity |

---

## 9. LLM Provider Setup

### Claude (default)

```env
LLM_PROVIDER=claude
LLM_MODEL=claude-haiku-4-5-20251001
ANTHROPIC_API_KEY=sk-ant-...
```

Get key: https://console.anthropic.com

### OpenAI

```env
LLM_PROVIDER=openai
LLM_MODEL=gpt-4o-mini
OPENAI_API_KEY=sk-...
```

### Ollama (local, no API key needed)

```bash
# Install Ollama and pull a model locally first
ollama pull llama3
```

```env
LLM_PROVIDER=ollama
LLM_MODEL=llama3
OLLAMA_BASE_URL=http://host.docker.internal:11434
```

---

## Project Structure

```

├── agents/
│   ├── monitoring_agent.py    # Prometheus poller — discovers any service label
│   ├── topology_agent.py      # Graph sync, lifecycle tracking, Neo4j snapshot
│   ├── diagnosis_agent.py     # Alert consumer + ML brain + LLM orchestrator
│   ├── llm_analyzer.py        # Claude / OpenAI / Ollama interface
│   └── Dockerfile
├── memory/
│   ├── redis_store.py         # Hot state + time series + alert bus + discovery helpers
│   ├── graph_store.py         # In-process 3-level NetworkX graph (fully dynamic)
│   ├── neo4j_store.py         # Persistent graph snapshots
│   └── sqlite_store.py        # Permanent brain: lifecycle, edges, baselines, incidents
├── ml/
│   ├── anomaly_detector.py    # Isolation Forest per service — multi-metric anomaly scoring
│   ├── baseline_model.py      # Hourly z-score baselines — what is normal per time bucket
│   ├── noise_classifier.py    # Noise suppression — structural vs real faults
│   ├── pattern_matcher.py     # Known-fault detection — skip LLM on recurring patterns
│   ├── communication_model.py # Inter-service call volume/latency baselines
│   └── model_trainer.py       # 24h retraining loop
├── services/
│   ├── order_service/         # FastAPI (port 8001) — always-on API
│   ├── payment_service/       # FastAPI (port 8002) — always-on API
│   ├── inventory_service/     # FastAPI (port 8003) — always-on API with Kafka sim
│   └── report_service/        # FastAPI (port 8004) — scheduled batch, runs once daily
├── scenarios/
│   ├── scenario_01_request_surge.py
│   ├── scenario_02_kafka_lag.py
│   ├── scenario_03_cascade_failure.py
│   ├── scenario_04_known_pattern_replay.py
│   ├── scenario_05_internal_bottleneck.py
│   └── scenario_06_new_service_registration.py  # auto-discovery + batch job
├── dashboard/
│   └── app.py                 # Streamlit — fully discovery-driven, no hardcoded services
├── config/
│   ├── prometheus.yml         # Scrape targets — add new service here + it auto-appears
│   └── otel-collector.yml
├── config.py                  # Thresholds, intervals, metric→category map
├── docker-compose.yml
├── requirements.txt
└── .env.example
```
