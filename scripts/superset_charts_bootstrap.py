#!/usr/bin/env python3
"""Create R1-R7 charts and assemble the ODS3 dashboard layout via Superset API."""

import json
import os
import re
import sys
import urllib.error
import urllib.request
from http.cookiejar import CookieJar

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088").rstrip("/")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

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


def ensure_chart(token, csrf, name, ds_id, viz_type, params):
    status, res = api(
        "GET",
        f"/api/v1/chart/?q=(filters:!((col:slice_name,opr:eq,value:{name})))",
        token=token,
    )
    if status == 200 and res.get("result"):
        cid = res["result"][0]["id"]
        print(f"  chart '{name}' exists (id={cid})")
        return cid
    payload = {
        "slice_name": name,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(params),
    }
    status, res = api("POST", "/api/v1/chart/", payload, token=token, csrf=csrf)
    if status in (200, 201) and "id" in res:
        print(f"  chart '{name}' created (id={res['id']})")
        return res["id"]
    print(f"  FAILED chart '{name}': {status} {res}")
    return 0


def main():
    print(f"Bootstrapping charts at {SUPERSET_URL}...")
    token, csrf = login()

    # Dataset ids (from previous bootstrap, verified)
    ds_affiliates = 1   # v_affiliates_summary
    ds_facility = 2     # v_facility_summary
    ds_r6 = 3           # v_r6_demographic_coverage
    ds_r7 = 4           # v_r7_territorial_capacity

    base_table = {
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
    cid = ensure_chart(token, csrf, "R1 - Afiliados por Regimen", ds_affiliates, viz, params)
    if cid:
        charts.append(cid)

    # R2: Top 10 departamentos (bar)
    viz, params = echarts_bar("departamento", simple_metric("numero_afiliados"), 10)
    cid = ensure_chart(token, csrf, "R2 - Top 10 Departamentos por Afiliados", ds_affiliates, viz, params)
    if cid:
        charts.append(cid)

    # R3: Establecimientos por naturaleza (bar)
    viz, params = echarts_bar("nature", simple_metric("capacity_amount", agg="COUNT", label="Establecimientos"), 20)
    cid = ensure_chart(token, csrf, "R3 - Establecimientos por Naturaleza", ds_facility, viz, params)
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
    cid = ensure_chart(token, csrf, "R4 - Camas por 1000 Habitantes", ds_r7, viz, params)
    if cid:
        charts.append(cid)

    # R5: Afiliados por region y regimen (barra apilada por región y régimen)
    viz, params = echarts_bar(
        "region", simple_metric("numero_afiliados"), 50,
        groupby=["regime"], stacked=True, show_value=False)
    cid = ensure_chart(token, csrf, "R5 - Afiliados por Region y Regimen", ds_affiliates, viz, params)
    if cid:
        charts.append(cid)

    # R6: Cobertura demografica (table)
    cid = ensure_chart(token, csrf, "R6 - Cobertura Demografica por Departamento", ds_r6, "table", dict(base_table, **{
        "all_columns": [
            "departamento", "region", "poblacion_oficial_api",
            "total_afiliados_sgsss", "cobertura_afiliacion_pct",
            "brecha_poblacional", "categoria_cobertura",
        ],
    }))
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
    }))
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
            "parents": ["ROOT_ID", "GRID_ID", f"ROW-{(i // 2)}"],
        }
        row_id = f"ROW-{(i // 2)}"
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
    status, res = api("GET", "/api/v1/dashboard/1", token=token)
    print(f"Dashboard 1 fetch: {status}")

    payload = {
        "published": True,
        "position_json": json.dumps(position),
    }
    status, res = api("PUT", "/api/v1/dashboard/1", payload, token=token, csrf=csrf)
    print(f"Dashboard update: {status} {str(res)[:200]}")

    if status == 200:
        print("DONE: dashboard populated with charts.")
    else:
        print("WARN: dashboard update may have failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
