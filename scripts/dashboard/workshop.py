"""Dashboard Ejecutivo R1-R7 - Salud Colombia ODS 3.

Cobertura de afiliados SGSSS, capacidad hospitalaria territorial y
disparidades por régimen/Región (vistas analíticas del DW `salud_colombia`).
"""

from __future__ import annotations

from common import (
    SupersetClient, ensure_database, ensure_dataset, ensure_chart,
    build_layout, build_filters, apply_dashboard, simple_metric,
    _spec, _big, _table,
)


def _bar(x: str, metric, limit: int, fmt: str = ",.0f",
         groupby: list[str] | None = None, stacked: bool = False,
         show_value: bool = True, orientation: str = "vertical") -> list:
    form = {
        "x_axis": x, "metrics": [metric], "orientation": orientation,
        "show_value": show_value, "y_axis_format": fmt, "row_limit": limit,
        "color_scheme": "supersetColors", "stack": "Stack" if stacked else "None",
        "x_axis_sort": (metric["label"] if isinstance(metric, dict) else metric),
        "x_axis_sort_asc": False,
    }
    if groupby:
        form["groupby"] = groupby
    return [("echarts_timeseries_bar", form)]


def _pie(groupby: list[str], metric, donut: bool = True, limit: int = 10) -> list:
    return [("pie", {
        "groupby": groupby,
        "metric": metric,
        "metrics": [metric],
        "donut": donut,
        "show_legend": True,
        "label_type": "key_percent",
        "row_limit": limit,
    })]


W = "workshop"

WORKSHOP_TITLE = "ODS 3 - Salud Colombia: Disparidades y Cobertura Territorial (R1–R7)"
WORKSHOP_SLUG = "ods3-salud-colombia"

WORKSHOP_CSS = """
/* --- Banner intro --- */
#MARKDOWN-r0c0 .dashboard-markdown {
    padding: 6px 12px !important;
    background: rgba(128, 128, 128, 0.08) !important;
    border: 1px solid rgba(128, 128, 128, 0.30) !important;
    border-left: 4px solid #0284C7 !important;
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
.dashboard-component-chart-holder:has(#CHART-r1c0) { border-top: 4px solid #0284C7 !important; box-shadow: 0 4px 12px rgba(2, 132, 199, 0.15) !important; }
.dashboard-component-chart-holder:has(#CHART-r1c1) { border-top: 4px solid #F59E0B !important; box-shadow: 0 4px 12px rgba(245, 158, 11, 0.15) !important; }
.dashboard-component-chart-holder:has(#CHART-r1c2) { border-top: 4px solid #10B981 !important; box-shadow: 0 4px 12px rgba(16, 185, 129, 0.15) !important; }
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

WORKSHOP_CHARTS: list[dict] = [
    _spec(W, "v_affiliates_summary", "Total Afiliados SGSSS",
          "Total acumulado de afiliados al SGSSS en el snapshot Q2 2022.",
          _big(simple_metric("numero_afiliados", "SUM", "Afiliados SGSSS"),
               "Afiliados al Sistema", ",d",
               color={"r": 2, "g": 132, "b": 199, "a": 1})),
    _spec(W, "v_r7_territorial_capacity", "Departamentos",
          "Departamentos con IPS habilitadas en el snapshot Q4 2022.",
          _big(simple_metric("total_ips_habilitadas", "SUM", "IPS habilitadas"),
               "Departamentos con IPS", ",d",
               color={"r": 245, "g": 158, "b": 11, "a": 1})),
    _spec(W, "v_r6_demographic_coverage", "Cobertura Media %",
          "Cobertura promedio de afiliación vs población oficial API Colombia.",
          _big(simple_metric("cobertura_afiliacion_pct", "AVG", "Cobertura media (%)", "NUMERIC"),
               "Cobertura Media de Afiliación", ".1f",
               color={"r": 16, "g": 185, "b": 129, "a": 1})),
    _spec(W, "v_r7_territorial_capacity", "Camas por 1000 hab (prom)",
          "Promedio de camas hospitalarias por cada 1000 habitantes.",
          _big(simple_metric("camas_por_1000_hab", "AVG", "Camas/1000 hab", "NUMERIC"),
               "Camas por 1000 Habitantes", ".2f",
               color={"r": 139, "g": 92, "b": 246, "a": 1})),

    _spec(W, "v_affiliates_summary", "R1 - Afiliados por Regimen",
          "Distribución de afiliados SGSSS por régimen de cotización (Contributivo/Subsidiado).",
          _bar("regime", simple_metric("numero_afiliados", "SUM", "Afiliados"), 10,
               orientation="horizontal")),

    _spec(W, "v_affiliates_summary", "R2 - Top 10 Departamentos por Afiliados",
          "Departamentos con mayor número de afiliados al SGSSS.",
          _bar("departamento", simple_metric("numero_afiliados", "SUM", "Afiliados"), 10)),

    _spec(W, "v_facility_summary", "R3 - Establecimientos por Naturaleza",
          "Distribución de establecimientos (IPS) por tipo de naturaleza jurídica.",
          _bar("nature", simple_metric("capacity_amount", "COUNT", "Establecimientos"), 20)),

    _spec(W, "v_r7_territorial_capacity", "R4 - Camas por 1000 Habitantes",
          "Top departamentos por camas hospitalarias cada 1000 habitantes.",
          _bar("departamento", simple_metric("camas_por_1000_hab", "MAX",
                                             "Camas / 1000 hab", "NUMERIC"), 10, ",.3f")),

    _spec(W, "v_affiliates_summary", "R5 - Afiliados por Region y Regimen",
          "Afiliados SGSSS por Región del país y régimen (comparación Contributivo vs Subsidiado).",
          _bar("region", simple_metric("numero_afiliados", "SUM", "Afiliados"), 50,
               groupby=["regime"], stacked=True, show_value=False)),

    # R6 Visuals
    _spec(W, "v_r6_demographic_coverage", "R6 - Cobertura de Afiliacion por Departamento (%)",
          "Porcentaje de afiliación SGSSS vs población censal oficial (API Colombia) por departamento.",
          _bar("departamento", simple_metric("cobertura_afiliacion_pct", "MAX",
                                             "Cobertura (%)", "NUMERIC"), 35,
               fmt=",.1f", orientation="horizontal")),

    _spec(W, "v_r6_demographic_coverage", "R6 - Distribucion por Categoria de Cobertura",
          "Proporción de departamentos según su categoría de cobertura frente a la meta universal.",
          _pie(["categoria_cobertura"], simple_metric("departamento", "COUNT", "Departamentos", "VARCHAR"))),

    _spec(W, "v_r6_demographic_coverage", "R6 - Cobertura Demografica por Departamento",
          "Cobertura de afiliación SGSSS vs población oficial: brecha y categoría por departamento.",
          [_table(["departamento", "region", "poblacion_oficial_api",
                   "total_afiliados_sgsss", "cobertura_afiliacion_pct",
                   "brecha_poblacional", "categoria_cobertura"], 50)]),

    # R7 Visuals
    _spec(W, "v_r7_territorial_capacity", "R7 - Camas por 1000 Habitantes (vs Estandar OMS)",
          "Densidad de camas hospitalarias cada 1000 habitantes frente al estándar OMS (2.5) por departamento.",
          _bar("departamento", simple_metric("camas_por_1000_hab", "MAX",
                                             "Camas / 1000 hab", "NUMERIC"), 35,
               fmt=",.2f", orientation="horizontal")),

    _spec(W, "v_r7_territorial_capacity", "R7 - Distribucion por Nivel de Alerta Hospitalaria",
          "Proporción de departamentos según nivel de alerta hospitalaria frente a la OMS.",
          _pie(["nivel_alerta_hospitalaria"], simple_metric("departamento", "COUNT", "Departamentos", "VARCHAR"))),

    _spec(W, "v_r7_territorial_capacity", "R7 - Capacidad Territorial por Departamento",
          "Capacidad instalada, camas/1000 hab y nivel de alerta hospitalaria por departamento.",
          [_table(["departamento", "region", "total_ips_habilitadas",
                   "total_camas", "capacidad_total_instalada",
                   "camas_por_1000_hab", "capacidad_por_1000_km2",
                   "nivel_alerta_hospitalaria"], 50)]),
]

WORKSHOP_LAYOUT: list = [
    [("md", "workshop_intro", 12, 4)],
    [("chart", "Total Afiliados SGSSS", 3, 17),
     ("chart", "Departamentos", 3, 17),
     ("chart", "Cobertura Media %", 3, 17),
     ("chart", "Camas por 1000 hab (prom)", 3, 17)],
    "R1 & R2: Afiliados SGSSS por régimen y territorio",
    [("chart", "R1 - Afiliados por Regimen", 4, 42),
     ("chart", "R2 - Top 10 Departamentos por Afiliados", 8, 42)],
    "R3 & R4: Capacidad de oferta hospitalaria (IPS y camas)",
    [("chart", "R3 - Establecimientos por Naturaleza", 6, 42),
     ("chart", "R4 - Camas por 1000 Habitantes", 6, 42)],
    "R5: Disparidad territorial de afiliación por Región y régimen",
    [("chart", "R5 - Afiliados por Region y Regimen", 12, 44)],
    "R6: Cobertura demográfica y brecha de aseguramiento (SGSSS vs API Colombia)",
    [("chart", "R6 - Cobertura de Afiliacion por Departamento (%)", 8, 52),
     ("chart", "R6 - Distribucion por Categoria de Cobertura", 4, 52)],
    [("chart", "R6 - Cobertura Demografica por Departamento", 12, 38)],
    "R7: Capacidad hospitalaria territorial y alerta OMS (REPS vs API Colombia)",
    [("chart", "R7 - Camas por 1000 Habitantes (vs Estandar OMS)", 8, 52),
     ("chart", "R7 - Distribucion por Nivel de Alerta Hospitalaria", 4, 52)],
    [("chart", "R7 - Capacidad Territorial por Departamento", 12, 38)],
]

WORKSHOP_MARKDOWN: dict[str, str] = {
    "workshop_intro": (
        "**ODS 3 - Salud Colombia: Disparidades y Cobertura Territorial (R1–R7)** &nbsp;|&nbsp; "
        "Pipeline batch Airflow → PostgreSQL `salud_colombia` → Superset &nbsp;·&nbsp; "
        "[Ver Granularidad & Calidad del DW →](/superset/dashboard/ods3-granularity/)"
    ),
}

WORKSHOP_FILTERS: list[dict] = [
    {"name": "Regimen", "column": "regime",
     "description": "Régimen de cotización al SGSSS (Contributivo, Subsidiado, Especial...)",
     "default": None},
    {"name": "Region", "column": "region", "description": "Región del país (Caribbean, Andean, Pacific...)"}, 
    {"name": "Departamento", "column": "departamento", "description": "Departamento DANE"},
    {"name": "Categoria de Cobertura", "column": "categoria_cobertura",
     "description": "Categoría R6: Déficit de Cobertura, Meta Universal, Alta Concentración"},
    {"name": "Nivel de Alerta Hospitalaria", "column": "nivel_alerta_hospitalaria",
     "description": "Categoría R7: Déficit Crítico, Moderada, Alta"},
]


def build(client: SupersetClient) -> int:
    database_id = ensure_database(client)

    used: dict[str, int] = {}
    cols: dict[int, set[str]] = {}
    for view in ("v_affiliates_summary", "v_facility_summary",
                 "v_r6_demographic_coverage", "v_r7_territorial_capacity"):
        ds_id, columns = ensure_dataset(client, database_id, view)
        used[view] = ds_id
        cols[ds_id] = columns

    dash = client.ensure("/api/v1/dashboard/", "slug", WORKSHOP_SLUG, {
        "dashboard_title": WORKSHOP_TITLE, "slug": WORKSHOP_SLUG,
        "published": True, "json_metadata": "{}",
    })
    dash_id = int(dash["id"])

    chart_ids: dict[str, int] = {}
    chart_dataset: dict[int, int] = {}
    dash_chart_ids: list[int] = []
    for spec in WORKSHOP_CHARTS:
        cid = ensure_chart(client, spec, used[spec["dataset"]], dash_id)
        chart_ids[spec["slice_name"]] = cid
        chart_dataset[cid] = used[spec["dataset"]]
        dash_chart_ids.append(cid)

    layout = build_layout(WORKSHOP_TITLE, WORKSHOP_LAYOUT, chart_ids, WORKSHOP_MARKDOWN)
    filters = build_filters(chart_dataset, cols, WORKSHOP_FILTERS, dash_chart_ids)
    apply_dashboard(client, dash_id, WORKSHOP_TITLE, WORKSHOP_SLUG,
                    layout, filters, WORKSHOP_CSS)
    return dash_id


def main() -> None:
    client = SupersetClient()
    client.login()
    dash_id = build(client)
    print(f"[dashboard] '{WORKSHOP_TITLE}' listo (id={dash_id}): "
          f"{client.base_url}/superset/dashboard/{WORKSHOP_SLUG}/")


if __name__ == "__main__":
    main()
