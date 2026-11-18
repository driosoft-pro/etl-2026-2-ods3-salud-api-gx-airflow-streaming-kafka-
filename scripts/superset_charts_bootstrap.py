#!/usr/bin/env python3
"""Create R1-R7 charts and assemble the ODS3 dashboard layout via Superset API."""

import json
import os
import sys
import urllib.error
import urllib.request
from http.cookiejar import CookieJar

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088").rstrip("/")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
DASHBOARD_SLUG = "ods3-salud-colombia"

cj = CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def api(method, path, payload=None, token=None, csrf=None):
    url = f"{SUPERSET_URL}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if csrf:
        req.add_header("X-CSRFToken", csrf)
    try:
        with opener.open(req, timeout=60) as resp:
            body = resp.read().decode()
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        try:
            return e.code, json.loads(err)
        except Exception:
            return e.code, {"error": err}
    except Exception as exc:
        return 0, {"error": str(exc)}


def login():
    status, res = api("POST", "/api/v1/security/login", {
        "username": ADMIN_USER, "password": ADMIN_PASS,
        "provider": "db", "refresh": True,
    })
    if status != 200 or "access_token" not in res:
        raise RuntimeError(f"Login failed: {status} {res}")
    token = res["access_token"]
    status, res = api("GET", "/api/v1/security/csrf_token/", token=token)
    csrf = res.get("result", "")
    return token, csrf


def simple_metric(col, agg="SUM", label=None):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col, "type": "BIGINT", "uuid": None, "is_dttm": False},
        "aggregate": agg,
        "label": label or f"{agg}({col})",
    }


def echarts_bar(x, metric, limit, fmt=",.0f", groupby=None, stacked=False,
                show_value=True):
    form = {
        "time_range": "No filter",
        "adhoc_filters": [],
        "x_axis": x,
        "metrics": [metric],
        "orientation": "vertical",
        "show_value": show_value,
        "y_axis_format": fmt,
        "row_limit": limit,
        "color_scheme": "supersetColors",
        "stack": "Stack" if stacked else "None",
        "x_axis_sort": (metric["label"] if isinstance(metric, dict) else metric),
        "x_axis_sort_asc": False,
    }
    if groupby:
        form["groupby"] = groupby
    return "echarts_timeseries_bar", form


def list_objects(resource, token):
    objects = []
    page = 0
    while True:
        status, res = api(
            "GET", f"/api/v1/{resource}/?q=(page:{page},page_size:100)", token=token
        )
        if status != 200:
            raise RuntimeError(f"Could not list {resource}: {status} {res}")
        batch = res.get("result", [])
        objects.extend(batch)
        if not batch or len(objects) >= res.get("count", len(objects)):
            return objects
        page += 1


def ensure_facility_dataset(token, csrf, datasets):
    """R3 uses one row per facility, not the capacity view's repeated rows."""
    facility = next((d for d in datasets if d["table_name"] == "dim_facility"), None)
    if facility:
        return facility["id"]
    reference = next(d for d in datasets if d["table_name"] == "fact_facility_capacity")
    payload = {
        "database": reference["database"]["id"],
        "schema": reference.get("schema") or "public",
        "table_name": "dim_facility",
    }
    status, res = api("POST", "/api/v1/dataset/", payload, token=token, csrf=csrf)
    if status not in (200, 201) or "id" not in res:
        raise RuntimeError(f"Could not register facility dataset: {status} {res}")
    print(f"  dataset 'dim_facility' registered (id={res['id']})")
    return res["id"]


def ensure_chart(token, csrf, name, ds_id, viz_type, params, dashboard_id):
    existing = next(
        (chart for chart in list_objects("chart", token) if chart["slice_name"] == name),
        None,
    )
    chart_params = dict(params, datasource=f"{ds_id}__table", viz_type=viz_type)
    if existing:
        chart_params["slice_id"] = existing["id"]
    dashboards = {dashboard_id}
    if existing:
        dashboards.update(d["id"] for d in existing.get("dashboards", []))
    payload = {
        "slice_name": name,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(chart_params),
        "dashboards": sorted(dashboards),
    }
    path = f"/api/v1/chart/{existing['id']}" if existing else "/api/v1/chart/"
    status, res = api("PUT" if existing else "POST", path, payload, token=token, csrf=csrf)
    if status not in (200, 201):
        raise RuntimeError(f"Could not save chart '{name}': {status} {res}")
    chart_id = existing["id"] if existing else res["id"]
    print(f"  chart '{name}' saved (id={chart_id}, dashboard={dashboard_id})")
    return chart_id


def main():
    print(f"Bootstrapping charts at {SUPERSET_URL}...")
    token, csrf = login()

    dashboard = next(
        (d for d in list_objects("dashboard", token) if d.get("slug") == DASHBOARD_SLUG),
        None,
    )
    if dashboard is None:
        raise RuntimeError("Project dashboard missing. Run superset_bootstrap.py first.")
    dashboard_id = dashboard["id"]
    registered_datasets = list_objects("dataset", token)
    datasets = {d["table_name"]: d["id"] for d in registered_datasets}
    ds_affiliates = datasets["v_affiliates_summary"]
    ds_facility = ensure_facility_dataset(token, csrf, registered_datasets)
    ds_r6 = datasets["v_r6_demographic_coverage"]
    ds_r7 = datasets["v_r7_territorial_capacity"]

    base_table = {
        "query_mode": "raw",
        "time_range": "No filter",
        "adhoc_filters": [],
        "row_limit": 10000,
        "order_desc": True,
        "show_cell_bars": True,
        "table_timestamp_format": "smart_date",
        "include_search": True,
        "table_format": "flexible",
        "color_pn": True,
    }

    charts = []

    # R1: Afiliados por regimen (barras horizontales eCharts)
    viz, params = echarts_bar("regime", simple_metric("numero_afiliados"), 10)
    params["orientation"] = "horizontal"
    cid = ensure_chart(token, csrf, "R1 - Afiliados por Regimen", ds_affiliates, viz, params, dashboard_id)
    if cid:
        charts.append(cid)

    # R2: Top 10 departamentos (bar)
    viz, params = echarts_bar("departamento", simple_metric("numero_afiliados"), 10)
    cid = ensure_chart(token, csrf, "R2 - Top 10 Departamentos por Afiliados", ds_affiliates, viz, params, dashboard_id)
    if cid:
        charts.append(cid)

    # R3: Establecimientos por naturaleza (bar)
    viz, params = echarts_bar("nature", simple_metric("sk_facility", agg="COUNT_DISTINCT", label="Establecimientos"), 20)
    cid = ensure_chart(token, csrf, "R3 - Establecimientos por Naturaleza", ds_facility, viz, params, dashboard_id)
    if cid:
        charts.append(cid)

    # R4: Camas por 1000 hab - top 10 departamentos (bar)
    viz, params = echarts_bar(
        "departamento",
        {
            "expressionType": "SIMPLE",
            "column": {"column_name": "camas_por_1000_hab", "type": "NUMERIC", "uuid": None, "is_dttm": False},
            "aggregate": "MAX",
            "label": "Camas / 1000 hab",
        },
        10, ",.3f")
    cid = ensure_chart(token, csrf, "R4 - Camas por 1000 Habitantes", ds_r7, viz, params, dashboard_id)
    if cid:
        charts.append(cid)

    # R5: Afiliados por region y regimen (barra apilada por región y régimen)
    viz, params = echarts_bar(
        "region", simple_metric("numero_afiliados"), 50,
        groupby=["regime"], stacked=True, show_value=False)
    cid = ensure_chart(token, csrf, "R5 - Afiliados por Region y Regimen", ds_affiliates, viz, params, dashboard_id)
    if cid:
        charts.append(cid)

    # R6: Cobertura demografica (table)
    cid = ensure_chart(token, csrf, "R6 - Cobertura Demografica por Departamento", ds_r6, "table", dict(base_table, **{
        "all_columns": [
            "departamento", "region", "poblacion_oficial_api",
            "total_afiliados_sgsss", "cobertura_afiliacion_pct",
            "brecha_poblacional", "categoria_cobertura",
        ],
    }), dashboard_id)
    if cid:
        charts.append(cid)

    # R7: Capacidad territorial (table)
    cid = ensure_chart(token, csrf, "R7 - Capacidad Territorial por Departamento", ds_r7, "table", dict(base_table, **{
        "all_columns": [
            "departamento", "region", "total_ips_habilitadas",
            "total_camas", "capacidad_total_instalada",
            "camas_por_1000_hab", "capacidad_por_1000_km2",
            "nivel_alerta_hospitalaria",
        ],
    }), dashboard_id)
    if cid:
        charts.append(cid)

    if not charts:
        print("No charts created; aborting dashboard layout.")
        sys.exit(1)

    # Build dashboard position_json
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID",
                      "meta": {"text": "ODS 3 - Salud Colombia: Disparidades y Cobertura Territorial (R1-R7)"}},
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }

    row_children = []
    for i, chart_id in enumerate(charts):
        chart_key = f"CHART-{chart_id}"
        row_id = f"ROW-{i // 2 if i < 4 else i - 2}"
        position[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "meta": {
                "chartId": chart_id,
                "sliceName": f"Chart {chart_id}",
                "width": 6 if i < 4 else 12,
                "height": 45,
                "sliceNameOverride": None,
            },
            "parents": ["ROOT_ID", "GRID_ID", row_id],
        }
        if row_id not in row_children:
            row_children.append(row_id)
            position[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [],
                "meta": {"background": "transparent"},
                "parents": ["ROOT_ID", "GRID_ID"],
            }
        position[row_id]["children"].append(chart_key)

    position["GRID_ID"]["children"] = row_children

    # Fetch chart names to set sliceName properly
    for chart_id in charts:
        status, res = api("GET", f"/api/v1/chart/{chart_id}", token=token)
        if status == 200:
            name = res.get("result", {}).get("slice_name", f"Chart {chart_id}")
            position[f"CHART-{chart_id}"]["meta"]["sliceName"] = name

    # Update dashboard
    print(f"Updating project dashboard {dashboard_id}...")

    payload = {
        "published": True,
        "position_json": json.dumps(position),
    }
    status, res = api("PUT", f"/api/v1/dashboard/{dashboard_id}", payload, token=token, csrf=csrf)
    print(f"Dashboard update: {status} {str(res)[:200]}")

    if status == 200:
        print("DONE: dashboard populated with charts.")
    else:
        print("WARN: dashboard update may have failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
