"""Helpers compartidos: cliente Superset + especificaciones de charts/layout."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from http.cookiejar import CookieJar

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088").rstrip("/")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
# URI del DW desde el entorno (sin credenciales hardcodeadas en el repo).
DW_URI = os.getenv("SUPERSET_DW_SQLALCHEMY_URI") or (
    "postgresql+psycopg2://{user}:{password}@{host}:5432/{db}".format(
        user=os.getenv("DB_USER", "etl_user"),
        password=os.getenv("POSTGRES_PASSWORD")
        or os.getenv("DB_PASSWORD")
        or "change_me_2026",
        host=os.getenv("DB_HOST", "postgres"),
        db=os.getenv("DB_NAME", "salud_colombia"),
    )
)

OPTIONAL_META_KEYS = ("cross_filters_enabled", "filter_bar_orientation")

LABEL_COLORS = {
    "CONTRIBUTORY": "#0284C7", "SUBSIDIZED": "#F59E0B", "OTHERS": "#64748B",
    "PASS": "#10B981", "FAIL": "#EF4444", "RUNNING": "#F59E0B",
    "SUCCESS": "#10B981", "FAILED": "#EF4444", "BLOCKED_QUALITY": "#8B5CF6",
    "Déficit de Cobertura": "#EF4444", "Meta Universal Adecuada": "#10B981",
    "Alta Concentración / Movilidad": "#F59E0B", "Sin Dato Demográfico": "#94A3B8",
    "Déficit Hospitalario Crítico": "#EF4444", "Capacidad Hospitalaria Moderada": "#F59E0B",
    "Capacidad Hospitalaria Alta": "#10B981", "Sin Dato Base": "#94A3B8",
}


class SupersetClient:
    def __init__(self, base_url: str = SUPERSET_URL):
        self.base_url = base_url
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(CookieJar()))
        self.token = None
        self.csrf = None

    def call(self, method: str, path: str, payload: dict | None = None):
        url = f"{self.base_url}{path}"
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        if self.csrf:
            req.add_header("X-CSRFToken", self.csrf)
        try:
            with self.opener.open(req, timeout=60) as resp:
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

    def login(self) -> None:
        status, res = self.call("POST", "/api/v1/security/login", {
            "username": ADMIN_USER, "password": ADMIN_PASS,
            "provider": "db", "refresh": True,
        })
        if status != 200 or "access_token" not in res:
            raise RuntimeError(f"Login failed: {status} {res}")
        self.token = res["access_token"]
        status, res = self.call("GET", "/api/v1/security/csrf_token/")
        self.csrf = res.get("result", "")

    def list(self, endpoint: str, key: str) -> dict:
        items = {}
        page, size = 0, 200
        while True:
            sep = "&" if "?" in endpoint else "?"
            status, res = self.call(
                "GET", f"{endpoint}{sep}q=(page:{page},page_size:{size})")
            if status != 200:
                break
            batch = res.get("result") or []
            for item in batch:
                items[item.get(key)] = item
            if len(batch) < size:
                break
            page += 1
        return items

    def ensure(self, endpoint: str, name_key: str, name: str,
               payload: dict, update_payload: dict | None = None) -> dict:
        existing = self.list(endpoint, name_key).get(name)
        body = update_payload or payload
        if existing:
            self.call("PUT", f"{endpoint}{existing['id']}", body)
            return existing
        status, res = self.call("POST", endpoint, payload)
        if status not in (200, 201) or "id" not in res:
            raise RuntimeError(f"ensure {name} failed: {status} {res}")
        return res


def ensure_database(client: SupersetClient) -> int:
    record = client.ensure("/api/v1/database/", "database_name", "Salud Colombia DW", {
        "database_name": "Salud Colombia DW",
        "sqlalchemy_uri": DW_URI,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
    })
    return int(record["id"])


def ensure_dataset(client: SupersetClient, database_id: int, table_name: str,
                   schema: str = "public", sql: str | None = None) -> tuple[int, set[str]]:
    existing = client.list("/api/v1/dataset/", "table_name").get(table_name)
    if existing:
        ds_id = int(existing["id"])
    else:
        payload = {"database": database_id, "table_name": table_name}
        if sql:
            payload["sql"] = sql
        else:
            payload["schema"] = schema
        status, res = client.call("POST", "/api/v1/dataset/", payload)
        if status not in (200, 201) or "id" not in res:
            raise RuntimeError(f"dataset {table_name} failed: {status} {res}")
        ds_id = int(res["id"])
    if sql:
        client.call("PUT", f"/api/v1/dataset/{ds_id}", {
            "database_id": database_id, "table_name": table_name, "sql": sql})
        client.call("PUT", f"/api/v1/dataset/{ds_id}/refresh")
    status, res = client.call("GET", f"/api/v1/dataset/{ds_id}")
    columns = set()
    for col in ((res.get("result") or {}).get("columns") or []):
        columns.add(col.get("column_name") or col.get("name"))
    return ds_id, columns


def ensure_chart(client: SupersetClient, spec: dict, dataset_id: int,
                 dashboard_id: int) -> int:
    description = spec.get("description") or "Generado por scripts/dashboard/"
    candidates = spec["candidates"]
    # preservar vínculos a otros dashboards (charts compartidos)
    dashboards = [dashboard_id]
    existing = client.list("/api/v1/chart/", "slice_name").get(spec["slice_name"])
    if existing:
        st, res = client.call("GET", f"/api/v1/chart/{existing['id']}")
        if st == 200:
            current = (res.get("result") or {}).get("dashboards") or []
            current_ids = [d["id"] if isinstance(d, dict) else d for d in current]
            dashboards = sorted(set(current_ids + [dashboard_id]))
    last = None
    for viz_type, form in candidates:
        saved = dict(form)
        if "order_by_cols" in saved:
            saved["order_by_cols"] = [json.dumps(list(pair)) for pair in saved["order_by_cols"]]
        form_data = {"adhoc_filters": [], "time_range": "No filter", **saved,
                     "viz_type": viz_type, "datasource": f"{dataset_id}__table"}
        payload = {
            "slice_name": spec["slice_name"],
            "description": description,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": viz_type,
            "params": json.dumps(form_data),
            "query_context": None,
            "dashboards": dashboards,
        }
        record = client.ensure("/api/v1/chart/", "slice_name", spec["slice_name"],
                               payload, update_payload=payload)
        status, out = client.call("POST", "/api/v1/chart/data", {
            "datasource": {"id": dataset_id, "type": "table"},
            "queries": [_validation_query(form)],
            "form_data": form_data,
            "result_format": "json",
            "result_type": "full",
        })
        if status == 200 and not out.get("error"):
            return int(record["id"])
        last = f"{viz_type}: {status} {json.dumps(out)[:200]}"
    raise RuntimeError(f"chart '{spec['slice_name']}' no ejecutó: {last}")


def _validation_query(form: dict) -> dict:
    metrics = list(form.get("metrics") or ([form["metric"]] if form.get("metric") else []))
    metrics = [m for m in metrics if isinstance(m, dict)]
    if "all_columns" in form:
        groupby = list(form["all_columns"])
        metrics = []
    else:
        groupby = [form[k] for k in ("x_axis", "all_columns_x", "all_columns_y", "entity") if form.get(k)]
        spatial = form.get("spatial") or {}
        groupby += [spatial[k] for k in ("lonCol", "latCol") if spatial.get(k)]
        radius = form.get("point_radius_fixed") or {}
        if radius.get("type") == "metric" and isinstance(radius.get("value"), dict):
            metrics = metrics + [radius["value"]]
        extra = form.get("groupby") or []
        groupby += [extra] if isinstance(extra, str) else list(extra)
        groupby += list(form.get("columns") or [])
    query = {
        "groupby": list(dict.fromkeys(groupby)),
        "metrics": metrics,
        "row_limit": int(form.get("row_limit", 1000)),
        "extras": {"time_grain_sqla": None},
        "applied_time_extras": {},
        "is_timeseries": False,
    }
    filters = [{"col": f["subject"], "op": f["operator"], "val": f["comparator"]}
               for f in form.get("adhoc_filters", []) if f.get("expressionType") == "SIMPLE"]
    if filters:
        query["filters"] = filters
    for column, is_ascending in form.get("order_by_cols", []):
        query.setdefault("orderby", []).append([column, bool(is_ascending)])
    return query


def build_layout(title: str, rows: list, chart_ids: dict[str, int],
                 markdown: dict[str, str]) -> dict:
    layout: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
    }
    grid = layout["GRID_ID"]["children"]
    for n, item in enumerate(rows):
        if isinstance(item, str):
            hid = f"HEADER-sec{n}"
            layout[hid] = {"type": "HEADER", "id": hid, "children": [],
                           "parents": ["ROOT_ID", "GRID_ID"],
                           "meta": {"text": item, "headerSize": "MEDIUM_HEADER",
                                    "background": "BACKGROUND_TRANSPARENT"}}
            grid.append(hid)
            continue
        rid = f"ROW-r{n}"
        layout[rid] = {"type": "ROW", "id": rid, "children": [],
                       "parents": ["ROOT_ID", "GRID_ID"],
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        grid.append(rid)
        for m, (kind, ref, width, height) in enumerate(item):
            parents = ["ROOT_ID", "GRID_ID", rid]
            if kind == "chart":
                cid = f"CHART-r{n}c{m}"
                meta = {"width": width, "height": height,
                        "chartId": chart_ids[ref], "sliceName": ref}
                ctype = "CHART"
            else:
                cid = f"MARKDOWN-r{n}c{m}"
                meta = {"width": width, "height": height, "code": markdown[ref]}
                ctype = "MARKDOWN"
            layout[cid] = {"type": ctype, "id": cid, "children": [], "parents": parents, "meta": meta}
            layout[rid]["children"].append(cid)
    return layout


def build_filters(chart_dataset: dict[int, int], dataset_cols: dict[int, set[str]],
                  filter_defs: list[dict], dash_chart_ids: list[int]) -> list[dict]:
    filters = []
    for spec in filter_defs:
        col = spec["column"]
        scoped = [c for c in dash_chart_ids if col in dataset_cols.get(chart_dataset[c], set())]
        if not scoped:
            continue
        default = spec.get("default")
        mask = {"extraFormData": {}, "filterState": {}, "ownState": {}}
        if default:
            mask = {"extraFormData": {"filters": [{"col": col, "op": "IN", "val": default}]},
                    "filterState": {"value": default}, "ownState": {}}
        target_datasets = list(dict.fromkeys(chart_dataset[c] for c in scoped))
        filters.append({
            "id": f"NATIVE_FILTER-{spec['column']}",
            "type": "NATIVE_FILTER",
            "name": spec["name"],
            "description": spec.get("description", ""),
            "filterType": "filter_select",
            "targets": [{"datasetId": ds_id, "column": {"name": col}} for ds_id in target_datasets],
            "controlValues": {
                "multiSelect": spec.get("multi", True),
                "enableEmptyFilter": bool(spec.get("required", False)),
                "defaultToFirstItem": False,
                "inverseSelection": False,
                "searchAllOptions": False,
            },
            "defaultDataMask": mask,
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": [c for c in dash_chart_ids if c not in scoped]},
            "chartsInScope": scoped,
            "tabsInScope": [],
        })
    return filters


def apply_dashboard(client: SupersetClient, dash_id: int, title: str, slug: str,
                    layout: dict, filters: list[dict], css: str) -> None:
    status, res = client.call("GET", f"/api/v1/dashboard/{dash_id}")
    existing = {}
    if status == 200:
        existing = json.loads((res.get("result") or {}).get("json_metadata") or "{}")
    existing.update({
        "label_colors": LABEL_COLORS,
        "color_scheme": "",
        "refresh_frequency": 0,
        "cross_filters_enabled": True,
        "filter_bar_orientation": "HORIZONTAL",
        "native_filter_configuration": filters,
    })
    payload = {
        "dashboard_title": title,
        "slug": slug,
        "published": True,
        "position_json": json.dumps(layout),
        "json_metadata": json.dumps(
            {k: v for k, v in existing.items() if k not in OPTIONAL_META_KEYS or v is not None}),
        "css": css,
    }
    last = None
    for _ in range(3):
        status, res = client.call("PUT", f"/api/v1/dashboard/{dash_id}", payload)
        if status == 200:
            return
        last = f"{status} {json.dumps(res)[:200]}"
    raise RuntimeError(f"dashboard '{title}' update failed: {last}")


def simple_metric(col: str, agg: str = "SUM", label: str | None = None,
                  col_type: str = "BIGINT") -> dict:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col, "type": col_type, "uuid": None, "is_dttm": False},
        "aggregate": agg,
        "label": label or f"{agg}({col})",
    }


def custom_metric(sql: str, label: str) -> dict:
    return {"expressionType": "CUSTOM_SQL", "sqlExpression": sql, "label": label}


def _spec(dashboard: str, dataset: str, name: str, description: str, candidates: list) -> dict:
    return {"dashboard": dashboard, "dataset": dataset, "slice_name": name,
            "description": description, "candidates": candidates}


def _table(cols: list[str], limit: int = 100, order: list | None = None) -> tuple:
    form = {"query_mode": "raw", "all_columns": cols, "row_limit": limit,
            "include_search": True, "order_desc": True,
            "table_timestamp_format": "smart_date", "table_format": "flexible"}
    if order:
        form["order_by_cols"] = order
    return ("table", form)


def _big(metric, subheader: str, fmt: str = ",d", color: dict | None = None) -> list:
    form = {"metric": metric, "subheader": subheader, "y_axis_format": fmt,
            "header_font_size": 0.48, "subheader_font_size": 0.26}
    if color:
        form["color_picker"] = color
    return [("big_number_total", form)]


def _hbar(x: str, metric, limit: int, fmt: str = ",.0f", ascending: bool = False) -> list:
    return [
        ("echarts_timeseries_bar", {
            "x_axis": x, "metrics": [metric], "orientation": "horizontal",
            "show_value": True, "x_axis_sort": (metric["label"] if isinstance(metric, dict) else metric),
            "x_axis_sort_asc": ascending, "y_axis_format": fmt, "row_limit": limit,
        }),
    ]


def _simple_filter(col: str, op: str, val: str) -> dict:
    return {"expressionType": "SIMPLE", "subject": col, "operator": op,
            "comparator": val, "clause": "WHERE"}
