"""
OpsCortex dashboard — Streamlit, auto-refreshes every 5 seconds.

Panel 1  — Live topology graph (PyVis, 3-level: service → category → metric)
Panel 2  — Active incident with LLM diagnosis and resolve button
Panel 3  — Noise suppression log
Panel 4  — Incident history (last 10 resolved)
Panel 5  — Per-service metric timeseries (tabs, one per discovered service)

All service/category/metric lists are discovered from Redis node:* keys at
runtime — no hardcoded service names anywhere.  New services appear automatically
as soon as the monitoring agent writes their first Redis keys.
"""
import json
import math
import os
import sys
import time
import tempfile
from datetime import datetime

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
from pyvis.network import Network

sys.path.insert(0, "/app")
import config
from memory.redis_store import RedisStore
from memory.sqlite_store import SQLiteStore

st.set_page_config(
    page_title="OpsCortex",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

REDIS_HOST     = os.getenv("REDIS_HOST",    config.REDIS_HOST)
REDIS_PORT     = int(os.getenv("REDIS_PORT", str(config.REDIS_PORT)))
SQLITE_PATH    = os.getenv("SQLITE_DB_PATH", config.SQLITE_DB_PATH)
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL",  config.PROMETHEUS_URL)
REFRESH        = config.DASHBOARD_REFRESH_INTERVAL_SECONDS

NODE_COLORS = {
    "green":    "#2ecc71",
    "amber":    "#f39c12",
    "red":      "#e74c3c",
    "healthy":  "#2ecc71",
    "degraded": "#f39c12",
    "faulted":  "#e74c3c",
    "offline":  "#7f8c8d",
    "unknown":  "#95a5a6",
}
STATUS_ICONS = {
    "healthy":  "🟢",
    "degraded": "🟡",
    "faulted":  "🔴",
    "offline":  "⚫",
    "unknown":  "⚪",
}


@st.cache_resource
def get_redis() -> RedisStore:
    return RedisStore(host=REDIS_HOST, port=REDIS_PORT)


@st.cache_resource
def get_sqlite() -> SQLiteStore:
    return SQLiteStore(SQLITE_PATH)


def ts_str(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts).strftime("%H:%M:%S")
    except Exception:
        return str(ts)


# ── Service discovery ──────────────────────────────────────────────────────────

def discover_services(redis_store: RedisStore) -> list[str]:
    """
    Return all service names currently visible in Redis node:* keys.
    Falls back to SQLite known_services if Redis is empty (e.g. cold start).
    """
    services = redis_store.discover_services()
    if not services:
        # Redis empty — fall back to permanent memory so the graph still shows
        try:
            sqlite = get_sqlite()
            rows = sqlite.get_active_and_offline_services()
            services = [r["name"] for r in rows]
        except Exception:
            pass
    return sorted(services)


def get_service_lifecycle(sqlite_store: SQLiteStore) -> dict[str, str]:
    """Return {service_name: status} from permanent memory."""
    try:
        rows = sqlite_store.get_all_known_services()
        return {r["name"]: r["status"] for r in rows}
    except Exception:
        return {}


# ── Prometheus helpers ─────────────────────────────────────────────────────────

def prom_query(metric: str, services: list[str]) -> dict[str, float]:
    """Query Prometheus for a metric and return {service: value} for known services."""
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": metric},
            timeout=3,
        )
        out = {}
        svc_set = set(services)
        for r in resp.json().get("data", {}).get("result", []):
            svc = r["metric"].get("service", r["metric"].get("job", ""))
            if svc in svc_set:
                out[svc] = float(r["value"][1])
        return out
    except Exception:
        return {}


def get_live_metrics(services: list[str]) -> dict[str, dict]:
    """
    Fetch live Prometheus metrics for every discovered service.
    Handles any service regardless of type — batch services will have
    records_processed and batch_job_running instead of kafka_lag etc.
    """
    if not services:
        return {}

    error_rates  = prom_query(config.PROM_ERROR_RATE_METRIC,       services)
    latencies    = prom_query(config.PROM_LATENCY_P99_METRIC,      services)
    req_rates    = prom_query(config.PROM_REQUEST_RATE_METRIC,      services)
    db_conns     = prom_query(config.PROM_DB_CONNECTIONS_METRIC,    services)
    kafka_lags   = prom_query(config.PROM_KAFKA_LAG_METRIC,         services)
    records      = prom_query(config.PROM_RECORDS_PROCESSED_METRIC, services)
    job_running  = prom_query(config.PROM_BATCH_JOB_RUNNING_METRIC, services)

    result = {}
    for svc in services:
        err = error_rates.get(svc, 0.0)
        lat = latencies.get(svc, 0.0)
        lag = kafka_lags.get(svc, 0.0)
        req = req_rates.get(svc, 0.0)
        dbc = db_conns.get(svc, 0.0)
        rec = records.get(svc, 0.0)
        job = job_running.get(svc, 0.0)

        if err > 0.3 or lag > config.THRESHOLD_KAFKA_LAG:
            status = "faulted"
        elif (err > config.THRESHOLD_ERROR_RATE
              or lat > config.THRESHOLD_LATENCY_P99_MS
              or lag > config.THRESHOLD_KAFKA_LAG * 0.5
              or dbc > config.THRESHOLD_DB_CONNECTIONS):
            status = "degraded"
        else:
            status = "healthy"

        result[svc] = {
            "status":           status,
            "error_rate":       err,
            "latency_p99":      lat,
            "req_rate":         req,
            "db_conns":         dbc,
            "kafka_lag":        lag,
            "records_processed": rec,
            "batch_job_running": job,
        }
    return result


# ── Dynamic layout engine ──────────────────────────────────────────────────────

def _service_positions(services: list[str]) -> dict[str, tuple[int, int]]:
    """
    Place service nodes on a circle so any number of services renders cleanly.
    With 3 services this produces the same triangle as the old hardcoded layout.
    """
    n = len(services)
    if n == 0:
        return {}
    cx, cy, r = 500, 350, 250
    positions = {}
    for i, svc in enumerate(services):
        angle = (2 * math.pi * i / n) - math.pi / 2   # start at top
        x = int(cx + r * math.cos(angle))
        y = int(cy + r * math.sin(angle))
        positions[svc] = (x, y)
    return positions


def _category_offsets(n_categories: int) -> list[tuple[int, int]]:
    """Spread category nodes evenly around their parent service node."""
    offsets = []
    for i in range(n_categories):
        angle = (2 * math.pi * i / max(n_categories, 1))
        offsets.append((int(70 * math.cos(angle)), int(70 * math.sin(angle))))
    return offsets


# ── PyVis graph builder ────────────────────────────────────────────────────────

def build_pyvis_graph(
    redis_store: RedisStore,
    services: list[str],
    live_metrics: dict,
    lifecycle: dict[str, str],
) -> str:
    net = Network(
        height="540px", width="100%",
        bgcolor="#1e1e2e", font_color="white", directed=True,
    )
    net.set_options("""
    {
      "physics": {"enabled": false},
      "edges": {"arrows": {"to": {"enabled": true, "scaleFactor": 0.5}}, "smooth": false},
      "interaction": {"hover": true, "tooltipDelay": 100}
    }
    """)

    service_pos = _service_positions(services)

    for service in services:
        sx, sy = service_pos[service]

        # Determine service display status — prefer lifecycle status for offline services
        perm_status = lifecycle.get(service, "unknown")
        if perm_status == "offline":
            svc_display_status = "offline"
        else:
            svc_display_status = live_metrics.get(service, {}).get("status", "unknown")

        svc_color = NODE_COLORS.get(svc_display_status, NODE_COLORS["unknown"])
        err = live_metrics.get(service, {}).get("error_rate", 0.0)
        req = live_metrics.get(service, {}).get("req_rate", 0.0)
        lag = int(live_metrics.get(service, {}).get("kafka_lag", 0))
        rec = int(live_metrics.get(service, {}).get("records_processed", 0))
        job = live_metrics.get(service, {}).get("batch_job_running", 0.0)

        tooltip = (
            f"<b>{service}</b><br>Status: {svc_display_status}<br>"
            f"Error rate: {err:.3f}<br>Req/s: {req:.1f}"
        )
        if lag > 0:
            tooltip += f"<br>Kafka lag: {lag}"
        if job > 0 or rec > 0:
            tooltip += f"<br>Job running: {'yes' if job else 'no'}<br>Records: {rec}"
        if perm_status == "offline":
            tooltip += "<br><i>⚫ Currently offline</i>"

        net.add_node(
            service,
            label=service.replace("_service", ""),
            title=tooltip,
            color={"background": svc_color, "border": "#ffffff"},
            size=35,
            x=sx, y=sy,
            font={"size": 14, "bold": True},
            shape="circle",
        )

        # Discover categories for this service from Redis
        categories = redis_store.discover_categories_for_service(service)
        if not categories:
            # Service known from permanent memory but Redis keys expired — use config hint
            categories = list({
                config.METRIC_CATEGORY_MAP.get(m, "performance")
                for m in (config.CATEGORY_METRICS if hasattr(config, "CATEGORY_METRICS") else {})
            }) or ["performance"]

        offsets = _category_offsets(len(categories))
        for cat_idx, category in enumerate(sorted(categories)):
            dx, dy = offsets[cat_idx]
            cx_pos = sx + dx * 2
            cy_pos = sy + dy * 2
            cat_id = f"{service}:{category}"

            # Category status from Redis
            cat_worst = "green"
            metrics_in_cat = redis_store.discover_metrics_for_service_category(service, category)
            for metric in metrics_in_cat:
                state = redis_store.get_metric_state(service, category, metric)
                if state:
                    s = state.get("status", "green")
                    if s == "red":
                        cat_worst = "red"
                    elif s == "amber" and cat_worst != "red":
                        cat_worst = "amber"

            if perm_status == "offline":
                cat_color = NODE_COLORS["offline"]
            else:
                cat_color = NODE_COLORS.get(cat_worst, NODE_COLORS["green"])

            net.add_node(
                cat_id,
                label=category,
                title=f"{service} / {category}",
                color={"background": cat_color, "border": "#888888"},
                size=18,
                x=cx_pos, y=cy_pos,
                font={"size": 10},
                shape="dot",
            )
            net.add_edge(service, cat_id, color="#444466", width=1)

            for met_idx, metric in enumerate(metrics_in_cat):
                met_id = f"{service}:{category}:{metric}"
                n_met = len(metrics_in_cat)
                angle = (met_idx / max(n_met - 1, 1)) * math.pi - math.pi / 2
                mr = 55
                # Flip arc direction based on which side of centre the category is
                flip = 1 if dx >= 0 else -1
                mx = cx_pos + mr * math.cos(angle) * flip
                my = cy_pos + mr * math.sin(angle)

                state = redis_store.get_metric_state(service, category, metric)
                if perm_status == "offline":
                    met_status = "offline"
                    met_value  = 0.0
                    met_color  = NODE_COLORS["offline"]
                else:
                    met_status = state.get("status", "green") if state else "green"
                    met_value  = state.get("value",  0.0)     if state else 0.0
                    met_color  = NODE_COLORS.get(met_status, NODE_COLORS["green"])

                vstr = f"{met_value:.3f}" if met_value < 1000 else f"{int(met_value)}"
                net.add_node(
                    met_id,
                    label=f"{metric.replace('_',' ')[:12]}\n{vstr}",
                    title=f"<b>{metric}</b><br>Value: {vstr}<br>Status: {met_status}",
                    color={"background": met_color, "border": "#555555"},
                    size=9,
                    x=mx, y=my,
                    font={"size": 7},
                    shape="dot",
                )
                net.add_edge(cat_id, met_id, color="#333355", width=0.7)

    # Call-graph edges — read from topology_edges permanent memory
    try:
        sqlite = get_sqlite()
        edges = sqlite.get_all_topology_edges()
        svc_set = set(services)
        for edge in edges:
            caller, callee = edge["caller"], edge["callee"]
            if caller in svc_set and callee in svc_set:
                dst_status = live_metrics.get(callee, {}).get("status", "unknown")
                ec = {"faulted": "#e74c3c", "degraded": "#f39c12"}.get(dst_status, "#555599")
                conf = edge["confirmation_count"]
                net.add_edge(
                    caller, callee,
                    color=ec, width=max(1, min(4, conf // 5)),
                    dashes=False, arrows="to",
                    title=f"{caller} → {callee}<br>confirmed {conf}x via {edge['source']}",
                )
    except Exception:
        pass

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html", mode="w") as f:
        net.save_graph(f.name)
        return open(f.name).read()


# ── Main app ───────────────────────────────────────────────────────────────────

def main():
    st.title("🧠 OpsCortex — Topology-Aware Operational Memory")
    st.caption(f"Auto-refresh every {REFRESH}s | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    redis_store  = get_redis()
    sqlite_store = get_sqlite()
    redis_ok     = redis_store.ping()

    # Discover everything from Redis (+ SQLite fallback for offline services)
    services     = discover_services(redis_store)
    lifecycle    = get_service_lifecycle(sqlite_store)
    live_metrics = get_live_metrics(services)

    # ── Sidebar ────────────────────────────────────────────────────────────────
    st.sidebar.subheader("System Status")
    st.sidebar.write(f"Redis: {'🟢 Connected' if redis_ok else '🔴 Disconnected'}")
    try:
        sqlite_store.get_recent_incidents(limit=1)
        st.sidebar.write("SQLite: 🟢 Connected")
    except Exception:
        st.sidebar.write("SQLite: 🔴 Error")

    st.sidebar.markdown("---")
    st.sidebar.subheader(f"Services ({len(services)} discovered)")
    for svc in services:
        m = live_metrics.get(svc, {})
        perm_status = lifecycle.get(svc, "unknown")
        display_status = "offline" if perm_status == "offline" else m.get("status", "unknown")
        icon = STATUS_ICONS.get(display_status, "⚪")
        label = svc.replace("_service", "")
        if display_status == "offline":
            st.sidebar.markdown(f"{icon} **{label}** — *offline*")
        else:
            st.sidebar.markdown(
                f"{icon} **{label}** — err={m.get('error_rate',0):.3f} "
                f"lat={m.get('latency_p99',0):.0f}ms"
            )

    # Show permanent-memory services that are NOT in Redis (offline/deprecated)
    perm_only = [s for s in lifecycle if s not in set(services)]
    if perm_only:
        st.sidebar.markdown("---")
        st.sidebar.caption("Known but not in Redis:")
        for s in perm_only:
            status = lifecycle[s]
            icon = "⚫" if status == "offline" else "🗑️"
            st.sidebar.markdown(f"{icon} {s.replace('_service','')} ({status})")

    try:
        unresolved = sqlite_store.get_unresolved_incidents()
        st.sidebar.markdown("---")
        st.sidebar.metric("Active Incidents", len(unresolved))
    except Exception:
        pass

    # ── Panel 1: Topology graph ────────────────────────────────────────────────
    st.subheader("Panel 1 — Live Topology Graph")
    if not services:
        st.warning("No services discovered yet — waiting for monitoring agent to write Redis keys")
    elif redis_ok:
        try:
            html = build_pyvis_graph(redis_store, services, live_metrics, lifecycle)
            components.html(html, height=560, scrolling=False)
        except Exception as e:
            st.warning(f"Graph render error: {e}")
    else:
        st.warning("Redis not connected — graph unavailable")

    col1, col2 = st.columns([1, 1])

    # ── Panel 2: Active incident ───────────────────────────────────────────────
    with col1:
        st.subheader("Panel 2 — Active Incident")
        try:
            unresolved = sqlite_store.get_unresolved_incidents()
            incident   = unresolved[0] if unresolved else None
        except Exception:
            incident = None

        if incident:
            svc    = incident.get("service", incident.get("faulted_service", "?"))
            metric = incident.get("metric",  incident.get("alert_metric",    "?"))
            val    = incident.get("alert_value", 0.0)
            fp     = incident.get("fault_path", [])
            conf   = incident.get("llm_confidence", "?")

            conf_color = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(conf, "⚪")
            st.error(f"🔥 **Incident #{incident['id']}** — {svc} / {metric} = {val:.4f}")

            c1, c2 = st.columns(2)
            c1.write(f"**Category:** {incident.get('category', '?')}")
            c1.write(f"**Time:** {ts_str(incident['timestamp'])}")
            c2.write(f"**Confidence:** {conf_color} {conf}")
            c2.write(f"**Fault path:** {' → '.join(fp) if fp else '—'}")

            if incident.get("llm_root_cause"):
                st.write(f"**Root cause:** {incident['llm_root_cause']}")
            if incident.get("llm_contributing_factors"):
                factors = incident["llm_contributing_factors"]
                if isinstance(factors, str):
                    try:
                        factors = json.loads(factors)
                    except Exception:
                        factors = [factors]
                st.write("**Contributing factors:**")
                for f in factors:
                    st.write(f"  • {f}")
            if incident.get("llm_recommended_fix"):
                st.info(f"💡 **Recommended fix:** {incident['llm_recommended_fix']}")
            if incident.get("llm_estimated_impact"):
                st.write(f"**Estimated impact:** {incident['llm_estimated_impact']}")

            if st.button(f"✅ Mark #{incident['id']} as Resolved"):
                sqlite_store.resolve_incident(incident["id"])
                st.success("Marked as resolved")
                st.rerun()
        else:
            st.success("No active incidents")

    # ── Panel 3: Noise suppression log ────────────────────────────────────────
    with col2:
        st.subheader("Panel 3 — Noise Suppression Log")
        try:
            suppressed = sqlite_store.get_recent_suppressed_alerts(limit=10)
        except Exception:
            suppressed = []
        if suppressed:
            df = pd.DataFrame(suppressed)
            df["time"] = df["timestamp"].apply(ts_str)
            df["value"] = df.get("value", pd.Series([0.0]*len(df))).apply(lambda v: f"{v:.3f}")
            cols = [c for c in ["time","service","metric","value","suppression_count","reason"] if c in df.columns]
            st.dataframe(df[cols], use_container_width=True, height=280)
        else:
            st.info("No suppressed alerts yet")

    # ── Panel 4: Incident history ──────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Panel 4 — Incident History (Last 10)")
    try:
        incidents = sqlite_store.get_recent_incidents(limit=10)
    except Exception:
        incidents = []

    if incidents:
        rows = []
        for inc in incidents:
            fp = inc.get("fault_path", [])
            rows.append({
                "ID":         inc["id"],
                "Time":       ts_str(inc["timestamp"]),
                "Service":    inc.get("service", inc.get("faulted_service", "?")),
                "Metric":     inc.get("metric",  inc.get("alert_metric",    "?")),
                "Root Cause": (inc.get("llm_root_cause") or inc.get("root_cause_service") or "?")[:80],
                "Confidence": inc.get("llm_confidence", "?"),
                "Fault Path": " → ".join(fp) if fp else "—",
                "Resolved":   "✅" if inc["resolved"] else "🔥",
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, height=350)

        for inc in incidents[:3]:
            svc    = inc.get("service", inc.get("faulted_service", "?"))
            metric = inc.get("metric",  inc.get("alert_metric",    "?"))
            with st.expander(f"#{inc['id']} — {svc}/{metric} @ {ts_str(inc['timestamp'])}"):
                if inc.get("llm_root_cause"):
                    st.write(f"**Root cause:** {inc['llm_root_cause']}")
                if inc.get("llm_recommended_fix"):
                    st.write(f"**Fix:** {inc['llm_recommended_fix']}")
                if inc.get("llm_raw_response"):
                    st.caption("Raw LLM response")
                    st.code(inc["llm_raw_response"][:2000])
                if not inc["resolved"]:
                    if st.button(f"Resolve #{inc['id']}", key=f"resolve_{inc['id']}"):
                        sqlite_store.resolve_incident(inc["id"])
                        st.rerun()
    else:
        st.info("No incidents yet — run a scenario to generate data")

    # ── Panel 5: Per-service timeseries ───────────────────────────────────────
    st.markdown("---")
    st.subheader("Panel 5 — Metric Timeseries")

    if not services:
        st.info("No services discovered yet")
    else:
        tabs = st.tabs([s.replace("_service", "") for s in services])
        for i, svc in enumerate(services):
            with tabs[i]:
                m = live_metrics.get(svc, {})
                perm_status = lifecycle.get(svc, "unknown")

                if perm_status == "offline":
                    st.warning(f"⚫ {svc} is currently **offline** (known from permanent memory)")

                # Discover all metrics for this service from Redis
                cats_and_metrics: list[tuple[str, str]] = []
                if redis_ok:
                    for cat in redis_store.discover_categories_for_service(svc):
                        for met in redis_store.discover_metrics_for_service_category(svc, cat):
                            cats_and_metrics.append((cat, met))

                # Live metric summary — show all discovered metrics in columns of 4
                if cats_and_metrics:
                    col_vals = []
                    for cat, met in cats_and_metrics:
                        state = redis_store.get_metric_state(svc, cat, met)
                        val   = state["value"] if state else 0.0
                        col_vals.append((f"{cat}/{met}", val))

                    n_cols = 4
                    for row_start in range(0, len(col_vals), n_cols):
                        row_items = col_vals[row_start:row_start + n_cols]
                        cols = st.columns(len(row_items))
                        for ci, (label, val) in enumerate(row_items):
                            disp = f"{val:.3f}" if val < 1000 else f"{int(val)}"
                            cols[ci].metric(label.split("/")[-1].replace("_", " "), disp)

                # Timeseries charts for key metrics
                if redis_ok:
                    chart_metrics = [
                        (config.PROM_ERROR_RATE_METRIC,    "Error Rate"),
                        (config.PROM_LATENCY_P99_METRIC,   "Latency p99 ms"),
                        (config.PROM_KAFKA_LAG_METRIC,     "Kafka Lag"),
                        (config.PROM_RECORDS_PROCESSED_METRIC, "Records Processed"),
                    ]
                    for prom_name, label in chart_metrics:
                        entries = redis_store.get_metrics(svc, prom_name, 50)
                        if entries:
                            df = pd.DataFrame(entries)
                            df["time"] = pd.to_datetime(df["ts"], unit="s")
                            st.caption(label)
                            st.line_chart(df.set_index("time")["value"], height=90)

    time.sleep(REFRESH)
    st.rerun()


if __name__ == "__main__":
    main()
