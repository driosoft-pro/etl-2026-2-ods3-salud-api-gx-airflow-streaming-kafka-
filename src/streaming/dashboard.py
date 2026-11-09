"""Real-time dashboard for Health Colombia (ODS 3) — section 2.7.

Streamlit application that renders the streaming metrics maintained by the
Kafka consumer (src/streaming/consumer.py). The live region auto-refreshes
every ``REFRESH_SECONDS`` via ``st.fragment(run_every=...)`` so metric changes
become visible shortly after the consumer processes new events.

Difference vs the batch analytical dashboard (Superset, port 8088):
    * Batch    -> historical KPIs (R1-R7) over the dimensional DW, refreshed
                  after each Airflow DAG run.
    * This one -> low-latency operational metrics fed by Kafka events,
                  refreshed every few seconds without re-running the ETL.

Run:
    streamlit run src/streaming/dashboard.py --server.port 8501
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from src.streaming import store

REFRESH_SECONDS = int(os.getenv("STREAMING_DASHBOARD_REFRESH", "3"))
HEARTBEAT_TIMEOUT_S = int(os.getenv("STREAMING_HEARTBEAT_TIMEOUT", "15"))

st.set_page_config(
    page_title="Salud Colombia · Tiempo Real",
    page_icon="⚡",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def _connection():
    conn = store.get_connection()
    store.ensure_schema(conn)
    return conn


def _fmt(value, decimals: int = 0) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def _load_all():
    conn = _connection()
    metrics = {m["metric_name"]: m for m in store.fetch_metrics(conn)}
    events = pd.DataFrame(store.fetch_recent_events(conn, limit=30))
    invalid = pd.DataFrame(store.fetch_recent_invalid(conn, limit=8))
    heartbeat = store.fetch_heartbeat(conn)
    series_aff = pd.DataFrame(store.fetch_metric_series(conn, "affiliates_by_department", limit=80))
    series_cap = pd.DataFrame(store.fetch_metric_series(conn, "capacity_by_department", limit=80))
    return metrics, events, invalid, heartbeat, series_aff, series_cap


def _metric_value(metrics, name):
    row = metrics.get(name)
    return None if row is None else row.get("metric_value")


# ---------------------------------------------------------------------------
# Static header
# ---------------------------------------------------------------------------
st.title("⚡ Salud Colombia — Dashboard de Tiempo Real (Streaming)")
st.caption(
    "Métricas de bajo latencia actualizadas por el consumidor Kafka a partir de "
    "los eventos publicados desde el Data Warehouse. "
    "Dashboard **distinto** al analítico histórico de Superset (puerto 8088), "
    "que se refresca después de cada corrida del ETL batch."
)

if REFRESH_SECONDS <= 0:
    REFRESH_SECONDS = 3


# ---------------------------------------------------------------------------
# Live region (auto-refreshed)
# ---------------------------------------------------------------------------
@st.fragment(run_every=timedelta(seconds=REFRESH_SECONDS))
def render_live():
    try:
        metrics, events, invalid, heartbeat, series_aff, series_cap = _load_all()
    except Exception as e:
        st.error(f"No se pudo conectar a PostgreSQL: {e}")
        return

    # --- Consumer liveness banner ---
    col_status = st.columns([3, 1])
    with col_status[0]:
        if heartbeat is None:
            st.warning("🔄 Sin heartbeat: el consumidor Kafka **nunca se ha iniciado**.")
        else:
            age = float(heartbeat.get("seconds_since") or 0)
            status = str(heartbeat.get("status") or "unknown")
            if status == "running" and age <= HEARTBEAT_TIMEOUT_S:
                st.success(
                    f"✅ Consumidor **{heartbeat['consumer_id']}** activo — "
                    f"último heartbeat hace {age:.0f}s "
                    f"({int(heartbeat.get('events_processed') or 0):,} eventos procesados, "
                    f"{int(heartbeat.get('invalid_events') or 0):,} inválidos)."
                )
            else:
                st.error(
                    f"⛔ Consumidor **{heartbeat.get('consumer_id')}** "
                    f"{'detenido' if status == 'stopped' else 'sin heartbeat'} "
                    f"desde hace {age:.0f}s (> {HEARTBEAT_TIMEOUT_S}s). "
                    "Los eventos publicados mientras tanto quedan encolados en Kafka y "
                    "se procesan al reanudarlo (offsets no confirmados)."
                )
    with col_status[1]:
        st.caption(
            f"Auto-refresh: {REFRESH_SECONDS}s\n\n"
            f"Hora UTC: {datetime.now(timezone.utc).strftime('%H:%M:%S')}"
        )

    # --- KPI row (streaming metrics) ---
    st.markdown("#### Métricas streaming (≥3 requeridas)")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Eventos procesados", _fmt(_metric_value(metrics, "events_processed_total")))
    k2.metric("Eventos inválidos", _fmt(_metric_value(metrics, "invalid_events_total")))
    k3.metric("Alertas de umbral", _fmt(_metric_value(metrics, "threshold_alerts_total")))
    k4.metric("Eventos / minuto", _fmt(_metric_value(metrics, "events_per_minute")))

    g1, g2, g3 = st.columns(3)
    aff = _metric_value(metrics, "affiliates_latest_total")
    g1.metric("Afiliados observados (suma por depto.)", _fmt(aff),
              help="SUM(último valor por departamento)")
    net = _metric_value(metrics, "affiliates_net_delta")
    g2.metric("Delta neto de afiliados", _fmt(net),
              delta=_fmt(net))
    g3.metric("Capacidad instalada observada", _fmt(_metric_value(metrics, "capacity_latest_total")),
              help="SUM(último valor por departamento)")

    # --- Trend charts ---
    st.markdown("#### Tendencia de eventos (últimos valores recibidos)")
    c1, c2 = st.columns(2)
    with c1:
        if not series_aff.empty:
            chart = series_aff.copy()
            chart["hora"] = pd.to_datetime(chart["received_at"]).dt.strftime("%H:%M:%S")
            st.line_chart(chart.set_index("hora")[["value"]],
                          height=220, use_container_width=True)
        else:
            st.info("Aún no se han recibido eventos de `affiliates_by_department`.")
    with c2:
        if not series_cap.empty:
            chart = series_cap.copy()
            chart["hora"] = pd.to_datetime(chart["received_at"]).dt.strftime("%H:%M:%S")
            st.line_chart(chart.set_index("hora")[["value"]],
                          height=220, use_container_width=True)
        else:
            st.info("Aún no se han recibido eventos de `capacity_by_department`.")

    # --- Event feed + invalid (dead-letter) feed ---
    st.markdown("#### Flujo de eventos")
    f1, f2 = st.columns([3, 2])
    with f1:
        if events.empty:
            st.info("Sin eventos procesados todavía.")
        else:
            events = events.copy()
            events["simulation"] = events["simulation"].map({True: "🧪 sí", False: "no"})
            events["delta"] = events["delta"].apply(
                lambda d: "—" if d is None else f"{float(d):,.0f}")
            events["value"] = events["value"].apply(
                lambda v: "—" if v is None else f"{float(v):,.0f}")
            events = events.rename(columns={
                "event_type": "tipo", "event_key": "clave",
                "metric_name": "métrica", "value": "valor", "delta": "delta",
                "unit": "unidad", "simulation": "simulación",
                "received_at": "recibido (UTC)",
            })
            st.dataframe(events[["tipo", "clave", "métrica", "valor", "delta",
                                 "unidad", "simulación", "recibido (UTC)"]],
                         use_container_width=True, height=300, hide_index=True)
    with f2:
        if invalid.empty:
            st.success("Sin eventos inválidos (contrato cumplido).")
        else:
            inv = invalid.copy()
            inv["errors"] = inv["errors"].apply(
                lambda e: "; ".join(e) if isinstance(e, list) else str(e))
            inv["raw_payload"] = inv["raw_payload"].astype(str).str.slice(0, 80)
            inv = inv.rename(columns={
                "invalid_id": "#", "reason": "motivo", "errors": "violaciones",
                "raw_payload": "payload crudo", "received_at": "recibido (UTC)",
            })
            st.dataframe(inv[["#", "motivo", "violaciones", "payload crudo",
                              "recibido (UTC)"]],
                         use_container_width=True, height=300, hide_index=True)


render_live()

# --- Context / distinction note ---
st.markdown(
    f"""
---
**¿Batch vs tiempo real?**

| | Dashboard analítico (Superset :8088) | Este dashboard (:8501) |
|---|---|---|
| **Fuente** | Tablas dimensionales del DW (PostgreSQL) | Tabla `streaming.metric_state` (eventos Kafka) |
| **Latencia** | Minutos/horas (tras cada corrida de Airflow) | Segundos (tras cada evento consumido) |
| **Pregunta** | *Histórica*: cobertura, brechas, R1–R7 | *Operativa*: ¿qué está cambiando ahora? |
| **Actualización** | Refresh manual / por lote | Auto-refresh cada {REFRESH_SECONDS}s |
"""
)
