"""Granularidad & Calidad del DW - Salud Colombia ODS 3.

Dashboard de auditoría del modelo dimensional: grano por tabla, lotes de
carga (audit.etl_batch_log + batch_id), corridas ETL y reglas DQ-01..DQ-12.
"""

from __future__ import annotations

from common import (
    SupersetClient, ensure_database, ensure_dataset, ensure_chart,
    build_layout, build_filters, apply_dashboard, simple_metric,
    _spec, _big, _hbar, _table,
)

G = "granularity"

GRANULARITY_TITLE = "Granularidad & Calidad del DW - Salud Colombia ODS 3"
GRANULARITY_SLUG = "ods3-granularity"

GRANULARITY_CSS = """
/* --- Banner intro --- */
#MARKDOWN-r0c0 .dashboard-markdown {
    padding: 6px 12px !important;
    background: rgba(128, 128, 128, 0.08) !important;
    border: 1px solid rgba(128, 128, 128, 0.30) !important;
    border-left: 4px solid #0EA5E9 !important;
    border-radius: 6px !important;
}
#MARKDOWN-r0c0 p { margin: 0 !important; font-size: 13.5px !important; }
#MARKDOWN-r0c0 a { color: #0284C7 !important; font-weight: 600 !important; text-decoration: none !important; }

/* --- Tarjetas KPI --- */
.dashboard-component-chart-holder:has(#CHART-r1c0),
.dashboard-component-chart-holder:has(#CHART-r1c1),
.dashboard-component-chart-holder:has(#CHART-r1c2),
.dashboard-component-chart-holder:has(#CHART-r1c3) {
    border-radius: 10px !important;
    box-sizing: border-box !important;
    transition: transform 0.2s ease, box-shadow 0.2s ease !important;
}
.dashboard-component-chart-holder:has(#CHART-r1c0) { border-top: 4px solid #0EA5E9 !important; box-shadow: 0 4px 12px rgba(14, 165, 233, 0.15) !important; }
.dashboard-component-chart-holder:has(#CHART-r1c1) { border-top: 4px solid #10B981 !important; box-shadow: 0 4px 12px rgba(16, 185, 129, 0.15) !important; }
.dashboard-component-chart-holder:has(#CHART-r1c2) { border-top: 4px solid #F59E0B !important; box-shadow: 0 4px 12px rgba(245, 158, 11, 0.15) !important; }
.dashboard-component-chart-holder:has(#CHART-r1c3) { border-top: 4px solid #8B5CF6 !important; box-shadow: 0 4px 12px rgba(139, 92, 246, 0.15) !important; }
.dashboard-component-chart-holder:has(#CHART-r1c0):hover,
.dashboard-component-chart-holder:has(#CHART-r1c1):hover,
.dashboard-component-chart-holder:has(#CHART-r1c2):hover,
.dashboard-component-chart-holder:has(#CHART-r1c3):hover { transform: translateY(-2px) !important; }

/* --- Transparencia de contenido interno de KPIs --- */
.dashboard-component-chart-holder .chart-slice,
.dashboard-component-chart-holder .dashboard-chart,
.dashboard-component-chart-holder .slice_container { background: transparent !important; border: none !important; box-shadow: none !important; border-radius: 0 !important; }
"""

GRANULARITY_INLINE_QUERIES: dict[str, str] = {
    "gran_table_grain": (
        "SELECT 1 AS sort_order, 'Dimension' AS layer, 'dim_time' AS table_name, "
        "'one quarter (year x quarter)' AS grain, count(*)::bigint AS row_count FROM dim_time "
        "UNION ALL SELECT 2, 'Dimension', 'dim_geografia', 'one municipality (DANE code + API enrichment)', count(*) FROM dim_geografia "
        "UNION ALL SELECT 3, 'Dimension', 'dim_regime', 'one SGSSS regime (CONTRIBUTORY/SUBSIDIZED/...)', count(*) FROM dim_regime "
        "UNION ALL SELECT 4, 'Dimension', 'dim_facility', 'one healthcare facility (IPS, provider x geography)', count(*) FROM dim_facility "
        "UNION ALL SELECT 5, 'Dimension', 'dim_capacity_type', 'one capacity type (group x description)', count(*) FROM dim_capacity_type "
        "UNION ALL SELECT 6, 'Fact', 'fact_affiliates', 'quarter x municipality x regime (Q2 2022 snapshot)', count(*) FROM fact_affiliates "
        "UNION ALL SELECT 7, 'Fact', 'fact_facility_capacity', 'facility x capacity type x quarter (Q4 2022 stock)', count(*) FROM fact_facility_capacity "
        "UNION ALL SELECT 8, 'Audit', 'audit.etl_run', 'one ETL run (CLI or Airflow)', (SELECT count(*) FROM audit.etl_run) "
        "UNION ALL SELECT 9, 'Audit', 'audit.dq_check_result', 'one DQ rule result per run', (SELECT count(*) FROM audit.dq_check_result) "
        "UNION ALL SELECT 10, 'Audit', 'audit.etl_batch_log', 'one warehouse batch load', (SELECT count(*) FROM audit.etl_batch_log)"
    ),
    "gran_affiliate_funnel": (
        "SELECT 1 AS step, 'Affiliate fact rows' AS stage, count(*)::bigint AS row_count FROM fact_affiliates "
        "UNION ALL SELECT 2, 'Municipalities with affiliates', count(DISTINCT sk_geografia)::bigint FROM fact_affiliates "
        "UNION ALL SELECT 3, 'Regimes represented', count(DISTINCT sk_regime)::bigint FROM fact_affiliates "
        "UNION ALL SELECT 4, 'Departments covered', count(DISTINCT g.codigo_dane_depto)::bigint "
        "FROM fact_affiliates f JOIN dim_geografia g ON g.sk_geografia = f.sk_geografia"
    ),
    "gran_capacity_funnel": (
        "SELECT 1 AS step, 'Capacity fact rows' AS stage, count(*)::bigint AS row_count FROM fact_facility_capacity "
        "UNION ALL SELECT 2, 'Distinct facilities (IPS)', count(DISTINCT sk_facility)::bigint FROM fact_facility_capacity "
        "UNION ALL SELECT 3, 'Capacity types', count(DISTINCT sk_capacity_type)::bigint FROM fact_facility_capacity "
        "UNION ALL SELECT 4, 'Total installed capacity units', COALESCE(SUM(capacity_amount), 0)::bigint FROM fact_facility_capacity"
    ),
    "gran_batch_provenance": (
        "SELECT b.batch_id, COALESCE(b.dag_id, 'manual') AS dag_id, "
        "COALESCE(b.run_id::text, '-') AS run_id, b.status, b.strategy, "
        "COALESCE((b.target_rows_after->>'fact_affiliates')::bigint, 0) AS rows_fact_affiliates, "
        "COALESCE((b.target_rows_after->>'fact_facility_capacity')::bigint, 0) AS rows_fact_facility_capacity, "
        "COALESCE(b.duration_seconds, 0) AS duration_seconds, b.started_at "
        "FROM audit.etl_batch_log b ORDER BY b.started_at DESC LIMIT 10"
    ),
    "gran_batch_latest_rows": (
        "SELECT "
        "COALESCE((SELECT MAX(batch_id) FROM audit.etl_batch_log), 0)::bigint AS latest_batch_id, "
        "COALESCE((SELECT SUM(e.value::bigint) FROM audit.etl_batch_log b, "
        "  jsonb_each_text(b.target_rows_after) e WHERE b.batch_id = (SELECT MAX(batch_id) FROM audit.etl_batch_log) "
        "  AND e.key LIKE 'fact_%' AND e.value ~ '^[0-9]+$'), 0)::bigint AS latest_fact_rows, "
        "COALESCE((SELECT COUNT(*) FROM audit.etl_batch_log), 0)::bigint AS total_batches"
    ),
    "gran_dq_by_gate": (
        "SELECT gate, status, count(*)::bigint AS rule_count "
        "FROM audit.dq_check_result WHERE run_id = (SELECT MAX(run_id) FROM audit.etl_run) "
        "GROUP BY gate, status"
    ),
    "gran_dq_latest_detail": (
        "SELECT rule_id, gate, layer, severity, status, observed_value, threshold, "
        "to_char(checked_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS checked_at "
        "FROM audit.dq_check_result WHERE run_id = (SELECT MAX(run_id) FROM audit.etl_run) "
        "ORDER BY rule_id"
    ),
    "gran_run_history": (
        "SELECT run_id, COALESCE(dag_id, 'CLI') AS dag_id, run_type, status, "
        "stage_reached, dq_rules_passed || '/' || dq_rules_total AS dq_rules, "
        "COALESCE(ROUND(duration_seconds, 1), 0) AS duration_seconds, "
        "to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS started_at "
        "FROM audit.etl_run ORDER BY started_at DESC LIMIT 10"
    ),
}

GRANULARITY_DATASET_SPECS: dict[str, dict[str, str]] = {
    "gran_table_grain": {"row_count": "SUM(row_count)"},
    "gran_affiliate_funnel": {"row_count": "SUM(row_count)"},
    "gran_capacity_funnel": {"row_count": "SUM(row_count)"},
    "gran_batch_latest_rows": {
        "latest_batch_id": "MAX(latest_batch_id)",
        "latest_fact_rows": "MAX(latest_fact_rows)",
        "total_batches": "MAX(total_batches)",
    },
}

GRANULARITY_CHARTS: list[dict] = [
    _spec(G, "gran_batch_latest_rows", "Lotes Cargados", 
          "Total de lotes de carga registrados en audit.etl_batch_log.",
          _big(simple_metric("total_batches", "MAX", "Lotes"),
               "Lotes de Carga al DW", ",d",
               color={"r": 14, "g": 165, "b": 233, "a": 1})),
    _spec(G, "gran_batch_latest_rows", "Último batch_id",
          "Identificador del lote más reciente estampado en dim_* y fact_*.",
          _big(simple_metric("latest_batch_id", "MAX", "Último batch_id"),
               "Último Lote (batch_id)", ",d",
               color={"r": 16, "g": 185, "b": 129, "a": 1})),
    _spec(G, "gran_batch_latest_rows", "Filas Hechos (último lote)",
          "Filas de fact_affiliates + fact_facility_capacity del último lote.",
          _big(simple_metric("latest_fact_rows", "MAX", "Filas hechos"),
               "Filas en Hechos (último lote)", ",d",
               color={"r": 245, "g": 158, "b": 11, "a": 1})),
    _spec(G, "v_r6_demographic_coverage", "Cobertura Media %",
          "Cobertura promedio de afiliación SGSSS vs población oficial (R6).",
          _big(simple_metric("cobertura_afiliacion_pct", "AVG", "Cobertura media (%)", "NUMERIC"),
               "Cobertura Media de Afiliación", ".1f",
               color={"r": 139, "g": 92, "b": 246, "a": 1})),

    _spec(G, "gran_table_grain", "Granularidad · Filas y grano por tabla",
          "Una fila por tabla del DW: capa, grano declarado y volumen actual.",
          [_table(["sort_order", "layer", "table_name", "grain", "row_count"], 10,
                  order=[["sort_order", True]])]),
    _spec(G, "gran_affiliate_funnel", "Granularidad · De afiliados a cobertura",
          "Hecho de afiliados: filas -> municipios -> regímenes -> departamentos cubiertos.",
          _hbar("stage", simple_metric("row_count", "SUM", "Filas"), 10, ",d")
          + [_table(["step", "stage", "row_count"], 10, order=[["step", True]])]),
    _spec(G, "gran_capacity_funnel", "Granularidad · De capacidad a IPS",
          "Hecho de capacidad: filas -> IPS distintos -> tipos de capacidad -> unidades instaladas.",
          _hbar("stage", simple_metric("row_count", "SUM", "Filas"), 10, ",d")
          + [_table(["step", "stage", "row_count"], 10, order=[["step", True]])]),

    _spec(G, "gran_dq_by_gate", "Calidad · Reglas DQ por gate (última corrida)",
          "Resultado PASS/FAIL de las 12 reglas DQ agrupadas por gate de la última corrida.",
          [("echarts_timeseries_bar", {"x_axis": "gate",
                                       "metrics": [simple_metric("rule_count", "SUM", "Reglas")],
                                       "groupby": ["status"], "stack": True,
                                       "show_legend": True, "y_axis_format": ",d",
                                       "row_limit": 20}),
           ("pie", {"groupby": ["status"],
                    "metric": simple_metric("rule_count", "SUM", "Reglas"),
                    "metrics": [simple_metric("rule_count", "SUM", "Reglas")],
                    "donut": True, "show_legend": True, "label_type": "key_percent",
                    "row_limit": 10}),
           _table(["gate", "status", "rule_count"], 20)]),
    _spec(G, "gran_dq_latest_detail", "Calidad · Detalle de reglas DQ (última corrida)",
          "Detalle por regla DQ-01..DQ-12: capa, severidad, valor observado y umbral.",
          [_table(["rule_id", "gate", "layer", "severity", "status",
                   "observed_value", "threshold", "checked_at"], 20)]),

    _spec(G, "gran_batch_provenance", "Auditoría · Últimos lotes de carga (batch_id)",
          "Procedencia de las filas del DW: cada lote con su corrida, estado y conteos por hecho.",
          [_table(["batch_id", "dag_id", "run_id", "status", "strategy",
                   "rows_fact_affiliates", "rows_fact_facility_capacity",
                   "duration_seconds", "started_at"], 10)]),
    _spec(G, "gran_run_history", "Auditoría · Historial de corridas ETL",
          "Últimas corridas del pipeline (CLI o Airflow) con fase alcanzada y reglas DQ.",
          [_table(["run_id", "dag_id", "run_type", "status", "stage_reached",
                   "dq_rules", "duration_seconds", "started_at"], 10)]),
]

GRANULARITY_LAYOUT: list = [
    [("md", "gran_intro", 12, 4)],
    [("chart", "Lotes Cargados", 3, 17),
     ("chart", "Último batch_id", 3, 17),
     ("chart", "Filas Hechos (último lote)", 3, 17),
     ("chart", "Cobertura Media %", 3, 17)],
    "Granularidad del Data Warehouse: modelado dimensional y grano por tabla",
    [("chart", "Granularidad · Filas y grano por tabla", 12, 34)],
    [("chart", "Granularidad · De afiliados a cobertura", 6, 42),
     ("chart", "Granularidad · De capacidad a IPS", 6, 42)],
    "Calidad de Datos: reglas DQ-01..DQ-12 y trazabilidad de lotes ETL",
    [("chart", "Calidad · Reglas DQ por gate (última corrida)", 6, 40),
     ("chart", "Calidad · Detalle de reglas DQ (última corrida)", 6, 40)],
    [("chart", "Auditoría · Últimos lotes de carga (batch_id)", 12, 28)],
    [("chart", "Auditoría · Historial de corridas ETL", 12, 28)],
]

GRANULARITY_MARKDOWN: dict[str, str] = {
    "gran_intro": (
        "**Granularidad & Calidad del DW - Salud Colombia ODS 3** &nbsp;|&nbsp; "
        "Auditoría del modelado dimensional Kimball y trazabilidad ETL (Airflow → PostgreSQL `salud_colombia`, esquema `audit`) &nbsp;·&nbsp; "
        "[Ver Dashboard Ejecutivo (R1–R7) →](/superset/dashboard/ods3-salud-colombia/)"
    ),
}

GRANULARITY_FILTERS: list[dict] = [
    {"name": "Capa", "column": "layer", "description": "Capa del modelo: Dimension, Fact o Audit"},
    {"name": "Estado DQ", "column": "status", "description": "Estado de reglas DQ y lotes: PASS/FAIL o RUNNING/SUCCESS/FAILED"},
]


def build(client: SupersetClient) -> int:
    database_id = ensure_database(client)

    used: dict[str, int] = {}
    cols: dict[int, set[str]] = {}

    for name, sql in GRANULARITY_INLINE_QUERIES.items():
        ds_id, columns = ensure_dataset(client, database_id, name, sql=sql)
        used[name] = ds_id
        cols[ds_id] = columns

    ds_id, columns = ensure_dataset(client, database_id, "v_r6_demographic_coverage")
    used["v_r6_demographic_coverage"] = ds_id
    cols[ds_id] = columns

    dash = client.ensure("/api/v1/dashboard/", "slug", GRANULARITY_SLUG, {
        "dashboard_title": GRANULARITY_TITLE, "slug": GRANULARITY_SLUG,
        "published": True, "json_metadata": "{}",
    })
    dash_id = int(dash["id"])

    chart_ids: dict[str, int] = {}
    chart_dataset: dict[int, int] = {}
    dash_chart_ids: list[int] = []
    for spec in GRANULARITY_CHARTS:
        cid = ensure_chart(client, spec, used[spec["dataset"]], dash_id)
        chart_ids[spec["slice_name"]] = cid
        chart_dataset[cid] = used[spec["dataset"]]
        dash_chart_ids.append(cid)

    layout = build_layout(GRANULARITY_TITLE, GRANULARITY_LAYOUT, chart_ids,
                          GRANULARITY_MARKDOWN)
    filters = build_filters(chart_dataset, cols, GRANULARITY_FILTERS, dash_chart_ids)
    apply_dashboard(client, dash_id, GRANULARITY_TITLE, GRANULARITY_SLUG,
                    layout, filters, GRANULARITY_CSS)
    return dash_id


def main() -> None:
    client = SupersetClient()
    client.login()
    dash_id = build(client)
    print(f"[dashboard] '{GRANULARITY_TITLE}' listo (id={dash_id}): "
          f"{client.base_url}/superset/dashboard/{GRANULARITY_SLUG}/")


if __name__ == "__main__":
    main()
