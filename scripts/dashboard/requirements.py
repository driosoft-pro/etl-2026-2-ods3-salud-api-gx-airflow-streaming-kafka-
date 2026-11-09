"""Dashboard explorador de los 7 requerimientos R1–R7 con tabs y filtros nativos.

Una pestaña (TAB) por requerimiento: descripción + SQL del requerimiento,
chart principal y tabla de detalle. Filtros nativos compartidos (régimen,
región, departamento, categoría de cobertura, nivel de alerta).
"""

from __future__ import annotations

import os

from common import (
    SupersetClient, ensure_database, ensure_dataset, ensure_chart,
    build_filters, apply_dashboard, simple_metric, _spec, _big, _table,
)

W = "requirements"

REQ_TITLE = "ODS 3 · Requerimientos R1–R7 (Explorador con Filtros)"
REQ_SLUG = "ods3-requerimientos"

REQ_CSS = """
/* --- Banner de tabs --- */
.superset-dashboard-component-tab .dashboard-component-chart-holder .chart-slice {
    border-radius: 8px !important;
}
/* --- Tarjetas de charts en tabs --- */
.dashboard-component-chart-holder:has([id^="CHART-t"]) {
    border: 1px solid rgba(128, 128, 128, 0.20) !important;
    border-radius: 8px !important;
    box-sizing: border-box !important;
}
.dashboard-component-chart-holder .chart-slice,
.dashboard-component-chart-holder .dashboard-chart,
.dashboard-component-chart-holder .slice_container {
    border: none !important; box-shadow: none !important; border-radius: 0 !important;
}
/* --- SQL en los markdown de tab --- */
[class^="MARKDOWN-t"] .dashboard-markdown pre,
.dashboard-markdown pre {
    background: rgba(2, 132, 199, 0.06) !important;
    border: 1px solid rgba(2, 132, 199, 0.25) !important;
    border-radius: 6px !important;
    padding: 8px 10px !important;
    font-size: 12px !important;
}
"""

# ---------------------------------------------------------------- charts


def _agg_table(groupby: list[str], metrics: list, limit: int = 50) -> tuple:
    form = {"query_mode": "aggregate", "groupby": groupby, "metrics": metrics,
            "row_limit": limit, "order_desc": True, "include_search": True,
            "table_timestamp_format": "smart_date", "table_format": "flexible"}
    return ("table", form)


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


M_AFF = lambda: simple_metric("numero_afiliados", "SUM", "Afiliados")
M_BEDS = lambda: simple_metric("camas_por_1000_hab", "MAX", "Camas / 1000 hab", "NUMERIC")


# ------------------------------------------------------------- mapa territorial
# Dataset virtual (no requiere tocar sql/init.sql: funciona en BDs ya creadas).
# Formato "largo": una fila por (departamento x requerimiento); el filtro nativo
# "Requerimiento (mapa)" elige el indicador R1-R7 que pintan los mapas.
# lat/lon = centroide aproximado del departamento; iso = ISO 3166-2:CO.
MAP_DATASET = "v_map_requirements"

_DEPTS = [  # (DANE, ISO, lat, lon)
    ("05", "CO-ANT", 6.90, -75.50), ("08", "CO-ATL", 10.70, -74.90),
    ("11", "CO-DC", 4.65, -74.10), ("13", "CO-BOL", 8.90, -74.50),
    ("15", "CO-BOY", 5.70, -73.00), ("17", "CO-CAL", 5.30, -75.30),
    ("18", "CO-CAQ", 1.00, -74.00), ("19", "CO-CAU", 2.40, -76.80),
    ("20", "CO-CES", 9.50, -73.50), ("23", "CO-COR", 8.40, -75.80),
    ("25", "CO-CUN", 4.90, -74.40), ("27", "CO-CHO", 5.70, -76.70),
    ("41", "CO-HUI", 2.60, -75.60), ("44", "CO-LAG", 11.40, -72.50),
    ("47", "CO-MAG", 10.20, -74.20), ("50", "CO-MET", 3.90, -73.00),
    ("52", "CO-NAR", 1.30, -77.60), ("54", "CO-NSA", 8.00, -72.90),
    ("63", "CO-QUI", 4.50, -75.70), ("66", "CO-RIS", 5.20, -75.90),
    ("68", "CO-SAN", 6.70, -73.20), ("70", "CO-SUC", 9.00, -75.10),
    ("73", "CO-TOL", 4.00, -75.20), ("76", "CO-VAC", 3.80, -76.60),
    ("81", "CO-ARA", 6.60, -71.00), ("85", "CO-CAS", 5.30, -71.70),
    ("86", "CO-PUT", 0.40, -76.40), ("88", "CO-SAP", 12.55, -81.70),
    ("91", "CO-AMA", -1.50, -71.50), ("94", "CO-GUA", 3.00, -68.50),
    ("95", "CO-GUV", 2.00, -72.30), ("97", "CO-VAU", 0.60, -70.50),
    ("99", "CO-VID", 4.70, -69.50),
]
_COORDS_VALUES = ",\n    ".join(
    f"('{d}', '{iso}', {lat}, {lon})" for d, iso, lat, lon in _DEPTS)

MAP_SQL = f"""
WITH coords(codigo_dane_depto, iso, lat, lon) AS (VALUES
    {_COORDS_VALUES}
),
cov AS (SELECT * FROM v_r6_demographic_coverage),
cap AS (SELECT * FROM v_r7_territorial_capacity),
reg AS (
    SELECT region, SUM(total_afiliados_sgsss) AS afiliados_region
    FROM cov GROUP BY region
),
ind AS (
    SELECT c.codigo_dane_depto, 'Población (API Colombia)' AS requerimiento,
           c.poblacion_oficial_api::numeric AS valor FROM cov c
    UNION ALL SELECT codigo_dane_depto, 'R1 · Afiliados Contributivo',
           afiliados_contributivo::numeric FROM cov
    UNION ALL SELECT codigo_dane_depto, 'R1 · Afiliados Subsidiado',
           afiliados_subsidiado::numeric FROM cov
    UNION ALL SELECT codigo_dane_depto, 'R1 · Afiliados Régimen Especial/Otros',
           afiliados_otros::numeric FROM cov
    UNION ALL SELECT codigo_dane_depto, 'R2 · Afiliados totales SGSSS',
           total_afiliados_sgsss::numeric FROM cov
    UNION ALL SELECT codigo_dane_depto, 'R3 · Establecimientos (IPS) habilitados',
           total_ips_habilitadas::numeric FROM cap
    UNION ALL SELECT codigo_dane_depto, 'R4 · Camas por 1000 habitantes',
           camas_por_1000_hab FROM cap
    UNION ALL SELECT c.codigo_dane_depto, 'R5 · Afiliados de la región',
           r.afiliados_region::numeric FROM cov c JOIN reg r USING (region)
    UNION ALL SELECT codigo_dane_depto, 'R6 · Cobertura de afiliación (%)',
           cobertura_afiliacion_pct FROM cov
    UNION ALL SELECT codigo_dane_depto, 'R7 · Capacidad instalada por 1000 km²',
           capacidad_por_1000_km2 FROM cap
)
SELECT
    k.iso, k.lat, k.lon,
    g.departamento, g.region,
    g.poblacion_oficial_api AS poblacion,
    ind.requerimiento,
    COALESCE(ind.valor, 0) AS valor,
    COALESCE(ind.valor, 0)
        / NULLIF(MAX(ind.valor) OVER (PARTITION BY ind.requerimiento), 0) * 100
        AS valor_norm,
    cov.categoria_cobertura,
    cap.nivel_alerta_hospitalaria
FROM ind
JOIN coords k USING (codigo_dane_depto)
JOIN (SELECT codigo_dane_depto, departamento, region, poblacion_oficial_api
      FROM cov) g USING (codigo_dane_depto)
LEFT JOIN cov USING (codigo_dane_depto)
LEFT JOIN cap USING (codigo_dane_depto)
"""

MAP_DEFAULT_REQ = "Población (API Colombia)"
M_VALOR = lambda: simple_metric("valor", "SUM", "Valor del indicador", "NUMERIC")
# Tamaño relativo (0-100 = % del máximo del indicador): los valores crudos van de
# decenas a millones y saturaban el radio, dejando todas las burbujas iguales.
M_SIZE = lambda: simple_metric("valor_norm", "MAX", "Tamaño relativo (% del máximo)", "NUMERIC")
MAP_VIEWPORT = {"longitude": -73.0, "latitude": 4.6, "zoom": 4.6,
                "bearing": 0, "pitch": 0}

MAPBOX_STYLE = (
    "mapbox://styles/mapbox/light-v9"
    if os.getenv("MAPBOX_API_KEY", "").strip()
    else "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"
)

MAP_CHARTS: list[dict] = [
    _spec(W, MAP_DATASET, "Req · Mapa — Coroplético por Departamento",
          "Mapa de Colombia: el color de cada departamento representa el indicador "
          "elegido en el filtro 'Requerimiento (mapa)' (población, R1–R7).",
          [("country_map", {
              "select_country": "colombia", "entity": "iso", "metric": M_VALOR(),
              "linear_color_scheme": "blue_white_yellow", "number_format": "SMART_NUMBER",
              "row_limit": 100})]),
    _spec(W, MAP_DATASET, "Req · Mapa — Marcadores por Departamento",
          "Marca (burbuja) en cada departamento; el tamaño representa el indicador "
          "elegido en el filtro 'Requerimiento (mapa)'. Mapa base libre (sin token requerido).",
          [("deck_scatter", {
              "spatial": {"type": "latlong", "lonCol": "lon", "latCol": "lat"},
              "point_radius_fixed": {"type": "metric", "value": M_SIZE()},
              "multiplier": 0.4, "point_unit": "pixels", "min_radius": 6,
              "max_radius": 45, "row_limit": 100,
              "groupby": ["departamento", "requerimiento"],
              "metrics": [M_VALOR()], 
                          #M_SIZE()],
              # Mapa base libre CARTO Positron o Mapbox Light si hay token
              "mapbox_style": MAPBOX_STYLE,
              "tooltip_contents": [
                  {"item_type": "column", "column_name": "departamento"},
                  {"item_type": "column", "column_name": "requerimiento"},
                  {"item_type": "metric", "metric_name": "Valor del indicador"},
                  {"item_type": "metric", "metric_name": "Tamaño relativo (% del máximo)"}],
              "viewport": MAP_VIEWPORT, "color_picker": {"r": 2, "g": 132, "b": 199, "a": 0.8},
              "autozoom": False, "js_columns": [], "adhoc_filters": []})]),
    _spec(W, MAP_DATASET, "Req · Mapa — Detalle por Departamento",
          "Valores del indicador seleccionado por departamento (tabla del mapa).",
          [_table(["departamento", "region", "requerimiento", "poblacion", "valor",
                   "categoria_cobertura", "nivel_alerta_hospitalaria"], 50)]),
]

REQ_CHARTS: list[dict] = [
    # --- R1
    _spec(W, "v_affiliates_summary", "Req · R1 — Afiliados por Régimen",
          "R1: distribución de afiliados SGSSS por régimen de cotización.",
          _bar("regime", M_AFF(), 10, orientation="horizontal")),
    _spec(W, "v_affiliates_summary", "Req · R1 — Detalle por Régimen",
          "R1: totales de afiliados por régimen (tabla).",
          [_agg_table(["regime"], [M_AFF()], 20)]),

    # --- R2
    _spec(W, "v_affiliates_summary", "Req · R2 — Top 10 Departamentos",
          "R2: departamentos con mayor número de afiliados al SGSSS.",
          _bar("departamento", M_AFF(), 10)),
    _spec(W, "v_affiliates_summary", "Req · R2 — Detalle Top 10",
          "R2: totales de afiliados por departamento (top 10).",
          [_agg_table(["departamento"], [M_AFF()], 10)]),

    # --- R3
    _spec(W, "v_facility_summary", "Req · R3 — Establecimientos por Naturaleza",
          "R3: establecimientos (IPS) por tipo de naturaleza jurídica.",
          _bar("nature", simple_metric("capacity_amount", "COUNT", "Establecimientos"), 20)),
    _spec(W, "v_facility_summary", "Req · R3 — Detalle por Naturaleza",
          "R3: conteo de establecimientos por naturaleza (tabla).",
          [_agg_table(["nature"], [simple_metric("capacity_amount", "COUNT", "Establecimientos")], 20)]),

    # --- R4
    _spec(W, "v_r7_territorial_capacity", "Req · R4 — Camas por 1000 Habitantes",
          "R4: departamentos con más camas cada 1000 habitantes.",
          _bar("departamento", M_BEDS(), 10, ",.3f")),
    _spec(W, "v_r7_territorial_capacity", "Req · R4 — Detalle Camas/1000",
          "R4: camas por 1000 habitantes por departamento (top 10).",
          [_agg_table(["departamento"], [M_BEDS()], 10)]),

    # --- R5
    _spec(W, "v_affiliates_summary", "Req · R5 — Afiliados por Región y Régimen",
          "R5: afiliados por Región del país y régimen (apilado).",
          _bar("region", M_AFF(), 50, groupby=["regime"], stacked=True, show_value=False)),
    _spec(W, "v_affiliates_summary", "Req · R5 — Detalle Región × Régimen",
          "R5: totales de afiliados por región y régimen (tabla).",
          [_agg_table(["region", "regime"], [M_AFF()], 50)]),

    # --- R6
    _spec(W, "v_r6_demographic_coverage", "Req · R6 — Cobertura (%) por Departamento",
          "R6: porcentaje de afiliación SGSSS vs población censal oficial por departamento.",
          _bar("departamento", simple_metric("cobertura_afiliacion_pct", "MAX",
                                             "Cobertura (%)", "NUMERIC"), 35,
               fmt=",.1f", orientation="horizontal")),
    _spec(W, "v_r6_demographic_coverage", "Req · R6 — Categoría de Cobertura",
          "R6: proporción de departamentos por categoría frente a la meta universal.",
          _pie(["categoria_cobertura"], simple_metric("departamento", "COUNT", "Departamentos", "VARCHAR"))),
    _spec(W, "v_r6_demographic_coverage", "Req · R6 — Cobertura Demográfica",
          "R6: cobertura de afiliación vs población oficial por departamento.",
          [_table(["departamento", "region", "poblacion_oficial_api",
                   "total_afiliados_sgsss", "cobertura_afiliacion_pct",
                   "brecha_poblacional", "categoria_cobertura"], 50)]),

    # --- R7
    _spec(W, "v_r7_territorial_capacity", "Req · R7 — Camas por 1000 hab (vs OMS)",
          "R7: densidad de camas hospitalarias cada 1000 habitantes frente al estándar OMS (2.5).",
          _bar("departamento", simple_metric("camas_por_1000_hab", "MAX",
                                             "Camas / 1000 hab", "NUMERIC"), 35,
               fmt=",.2f", orientation="horizontal")),
    _spec(W, "v_r7_territorial_capacity", "Req · R7 — Nivel de Alerta Hospitalaria",
          "R7: distribución de departamentos según nivel de alerta hospitalaria.",
          _pie(["nivel_alerta_hospitalaria"], simple_metric("departamento", "COUNT", "Departamentos", "VARCHAR"))),
    _spec(W, "v_r7_territorial_capacity", "Req · R7 — Capacidad Territorial",
          "R7: capacidad instalada, camas/1000 hab y nivel de alerta por departamento.",
          [_table(["departamento", "region", "total_ips_habilitadas",
                   "total_camas", "capacidad_total_instalada",
                   "camas_por_1000_hab", "capacidad_por_1000_km2",
                   "nivel_alerta_hospitalaria"], 50)]),
]

# ------------------------------------------------- tabs (una pestaña por R)

REQ_MARKDOWN: dict[str, str] = {
    "tab_map": (
        "### 🗺️ Mapa territorial · Población e indicadores R1–R7\n"
        "Elige el indicador en el filtro **Requerimiento (mapa)** (población, R1–R7) y "
        "combínalo con Región, Departamento, Categoría de Cobertura o Nivel de Alerta: "
        "el mapa coroplético y los marcadores se actualizan.\n\n"
        "> Nota: el tamaño de cada burbuja es relativo (% del máximo del indicador). La geometría base de Superset no separa Bogotá D.C. de "
        "Cundinamarca, por eso Bogotá solo aparece en los marcadores.\n\n"
        "```sql\n"
        "SELECT departamento, requerimiento, SUM(valor) AS valor\n"
        "FROM v_map_requirements  -- dataset virtual sobre v_r6 / v_r7\n"
        "GROUP BY 1, 2;\n"
        "```"
    ),
    "tab_r1": (
        "### R1 · Afiliados por Régimen\n"
        "Distribución de los afiliados al SGSSS según el régimen de cotización "
        "(Contributivo, Subsidiado, Especial, Individuales).\n\n"
        "```sql\n"
        "SELECT regime, SUM(numero_afiliados) AS afiliados\n"
        "FROM v_affiliates_summary GROUP BY regime ORDER BY 2 DESC;\n"
        "```"
    ),
    "tab_r2": (
        "### R2 · Top 10 Departamentos por Afiliados\n"
        "Departamentos con mayor volumen de afiliados al SGSSS (concentración territorial "
        "de la cobertura).\n\n"
        "```sql\n"
        "SELECT departamento, SUM(numero_afiliados) AS afiliados\n"
        "FROM v_affiliates_summary GROUP BY departamento\n"
        "ORDER BY 2 DESC LIMIT 10;\n"
        "```"
    ),
    "tab_r3": (
        "### R3 · Establecimientos por Naturaleza\n"
        "Distribución de los establecimientos (IPS habilitados) por tipo de naturaleza "
        "jurídica (Pública, Privada, Mixta...).\n\n"
        "```sql\n"
        "SELECT nature, COUNT(capacity_amount) AS establecimientos\n"
        "FROM v_facility_summary GROUP BY nature ORDER BY 2 DESC;\n"
        "```"
    ),
    "tab_r4": (
        "### R4 · Camas por 1000 Habitantes\n"
        "Top departamentos por densidad de camas hospitalarias cada 1000 habitantes "
        "(capacidad de respuesta del sistema).\n\n"
        "```sql\n"
        "SELECT departamento, MAX(camas_por_1000_hab) AS camas_por_1000\n"
        "FROM v_r7_territorial_capacity\n"
        "GROUP BY departamento ORDER BY 2 DESC LIMIT 10;\n"
        "```"
    ),
    "tab_r5": (
        "### R5 · Afiliados por Región y Régimen\n"
        "Comparación de la afiliación entre Regiones del país y régimen "
        "(Contributivo vs Subsidiado) para evidenciar disparidades territoriales.\n\n"
        "```sql\n"
        "SELECT region, regime, SUM(numero_afiliados) AS afiliados\n"
        "FROM v_affiliates_summary GROUP BY region, regime\n"
        "ORDER BY region, 3 DESC;\n"
        "```"
    ),
    "tab_r6": (
        "### R6 · Cobertura Demográfica por Departamento\n"
        "Cobertura de afiliación SGSSS vs población oficial API Colombia: brecha "
        "poblacional y categoría de cobertura por departamento.\n\n"
        "```sql\n"
        "SELECT * FROM v_r6_demographic_coverage\n"
        "ORDER BY cobertura_afiliacion_pct ASC;\n"
        "```"
    ),
    "tab_r7": (
        "### R7 · Capacidad Territorial por Departamento\n"
        "Capacidad instalada (IPS, camas, salas), camas/1000 hab, densidad territorial "
        "e indicador de alerta hospitalaria por departamento.\n\n"
        "```sql\n"
        "SELECT * FROM v_r7_territorial_capacity\n"
        "ORDER BY camas_por_1000_hab ASC;\n"
        "```"
    ),
}

REQ_TABS: list[tuple[str, list]] = [
    ("🗺️ Mapa Territorial", [
        [("md", "tab_map", 12, 26)],
        [("chart", "Req · Mapa — Coroplético por Departamento", 6, 70),
         ("chart", "Req · Mapa — Marcadores por Departamento", 6, 70)],
        [("chart", "Req · Mapa — Detalle por Departamento", 12, 44)],
    ]),
    ("R1 · Afiliados por Régimen", [
        [("md", "tab_r1", 12, 26)],
        [("chart", "Req · R1 — Afiliados por Régimen", 6, 46),
         ("chart", "Req · R1 — Detalle por Régimen", 6, 46)],
    ]),
    ("R2 · Top 10 Departamentos", [
        [("md", "tab_r2", 12, 26)],
        [("chart", "Req · R2 — Top 10 Departamentos", 7, 44),
         ("chart", "Req · R2 — Detalle Top 10", 5, 44)],
    ]),
    ("R3 · Establecimientos por Naturaleza", [
        [("md", "tab_r3", 12, 26)],
        [("chart", "Req · R3 — Establecimientos por Naturaleza", 6, 44),
         ("chart", "Req · R3 — Detalle por Naturaleza", 6, 44)],
    ]),
    ("R4 · Camas por 1000 Habitantes", [
        [("md", "tab_r4", 12, 26)],
        [("chart", "Req · R4 — Camas por 1000 Habitantes", 7, 44),
         ("chart", "Req · R4 — Detalle Camas/1000", 5, 44)],
    ]),
    ("R5 · Afiliados por Región y Régimen", [
        [("md", "tab_r5", 12, 26)],
        [("chart", "Req · R5 — Afiliados por Región y Régimen", 12, 46)],
        [("chart", "Req · R5 — Detalle Región × Régimen", 12, 44)],
    ]),
    ("R6 · Cobertura Demográfica", [
        [("md", "tab_r6", 12, 26)],
        [("chart", "Req · R6 — Cobertura (%) por Departamento", 7, 52),
         ("chart", "Req · R6 — Categoría de Cobertura", 5, 52)],
        [("chart", "Req · R6 — Cobertura Demográfica", 12, 44)],
    ]),
    ("R7 · Capacidad Territorial", [
        [("md", "tab_r7", 12, 26)],
        [("chart", "Req · R7 — Camas por 1000 hab (vs OMS)", 7, 52),
         ("chart", "Req · R7 — Nivel de Alerta Hospitalaria", 5, 52)],
        [("chart", "Req · R7 — Capacidad Territorial", 12, 44)],
    ]),
]

REQ_FILTERS: list[dict] = [
    {"name": "Requerimiento (mapa)", "column": "requerimiento", "multi": False,
     "default": [MAP_DEFAULT_REQ], "required": True,
     "description": "Indicador que pintan los mapas: población y R1–R7"},
    {"name": "Régimen", "column": "regime",
     "description": "Régimen de cotización (Contributivo, Subsidiado, Especial, Individual)"},
    {"name": "Región", "column": "region", "description": "Región del país (Caribbean, Andean, Pacific...)"},
    {"name": "Departamento", "column": "departamento", "description": "Departamento DANE"},
    {"name": "Categoría de Cobertura", "column": "categoria_cobertura",
     "description": "R6: Déficit de Cobertura, Meta Universal, Alta Concentración"},
    {"name": "Nivel de Alerta Hospitalaria", "column": "nivel_alerta_hospitalaria",
     "description": "R7: Déficit Crítico, Moderada, Alta"},
]


def build_tabs_layout(title: str, tabs: list[tuple[str, list]],
                      chart_ids: dict[str, int], markdown: dict[str, str]) -> dict:
    layout: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["TABS_ID"],
                    "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER",
                      "meta": {"text": title}},
    }
    tab_ids = []
    for ti, (tab_title, rows) in enumerate(tabs):
        tab_id = f"TAB-t{ti}"
        tab_ids.append(tab_id)
        layout[tab_id] = {"type": "TAB", "id": tab_id, "children": [],
                          "parents": ["ROOT_ID", "GRID_ID", "TABS_ID"],
                          "meta": {"text": tab_title,
                                   "background": "BACKGROUND_TRANSPARENT"}}
        for ri, row in enumerate(rows):
            rid = f"ROW-t{ti}r{ri}"
            layout[rid] = {"type": "ROW", "id": rid, "children": [],
                           "parents": ["ROOT_ID", "GRID_ID", "TABS_ID", tab_id],
                           "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            layout[tab_id]["children"].append(rid)
            for mi, (kind, ref, width, height) in enumerate(row):
                cid = f"CHART-t{ti}r{ri}c{mi}"
                parents = ["ROOT_ID", "GRID_ID", "TABS_ID", tab_id, rid]
                if kind == "chart":
                    meta = {"width": width, "height": height,
                            "chartId": chart_ids[ref], "sliceName": ref}
                    ctype = "CHART"
                else:
                    meta = {"width": width, "height": height,
                            "code": markdown[ref]}
                    ctype = "MARKDOWN"
                layout[cid] = {"type": ctype, "id": cid, "children": [],
                               "parents": parents, "meta": meta}
                layout[rid]["children"].append(cid)
    layout["TABS_ID"] = {"type": "TABS", "id": "TABS_ID", "children": tab_ids,
                         "parents": ["ROOT_ID", "GRID_ID"]}
    return layout


def build(client: SupersetClient) -> int:
    database_id = ensure_database(client)

    used: dict[str, int] = {}
    cols: dict[int, set[str]] = {}
    for view in ("v_affiliates_summary", "v_facility_summary",
                 "v_r6_demographic_coverage", "v_r7_territorial_capacity"):
        ds_id, columns = ensure_dataset(client, database_id, view)
        used[view] = ds_id
        cols[ds_id] = columns

    ds_id, columns = ensure_dataset(client, database_id, MAP_DATASET, sql=MAP_SQL)
    used[MAP_DATASET] = ds_id
    cols[ds_id] = columns

    dash = client.ensure("/api/v1/dashboard/", "slug", REQ_SLUG, {
        "dashboard_title": REQ_TITLE, "slug": REQ_SLUG,
        "published": True, "json_metadata": "{}",
    })
    dash_id = int(dash["id"])

    chart_ids: dict[str, int] = {}
    chart_dataset: dict[int, int] = {}
    dash_chart_ids: list[int] = []
    for spec in MAP_CHARTS + REQ_CHARTS:
        cid = ensure_chart(client, spec, used[spec["dataset"]], dash_id)
        chart_ids[spec["slice_name"]] = cid
        chart_dataset[cid] = used[spec["dataset"]]
        dash_chart_ids.append(cid)

    layout = build_tabs_layout(REQ_TITLE, REQ_TABS, chart_ids, REQ_MARKDOWN)
    filters = build_filters(chart_dataset, cols, REQ_FILTERS, dash_chart_ids)
    apply_dashboard(client, dash_id, REQ_TITLE, REQ_SLUG,
                    layout, filters, REQ_CSS)
    return dash_id


def main() -> None:
    client = SupersetClient()
    client.login()
    dash_id = build(client)
    print(f"[dashboard] '{REQ_TITLE}' listo (id={dash_id}): "
          f"{client.base_url}/superset/dashboard/{REQ_SLUG}/")


if __name__ == "__main__":
    main()
