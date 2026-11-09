#!/usr/bin/env python3
"""Automated bootstrap script for Apache Superset in Health Colombia ODS 3.
Registers the PostgreSQL DW connection, datasets for R1-R7, and creates the dashboard.
"""

import os
import sys
import time
import json
import logging
import urllib.request
import urllib.error
from http.cookiejar import CookieJar

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("superset_bootstrap")

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://superset:8088").rstrip("/")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
# La URI del DW se arma desde el entorno (.env, gitignored); nunca se
# publican credenciales reales en el repositorio.
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

cookie_jar = CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))


def request_json(method: str, path: str, payload: dict = None, token: str = None, csrf: str = None) -> tuple[int, dict]:
    url = f"{SUPERSET_URL}{path}"
    data = json.dumps(payload).encode("utf-8") if payload else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if csrf:
        req.add_header("X-CSRFToken", csrf)

    try:
        with opener.open(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            try:
                data = json.loads(body) if body else {}
            except Exception:
                data = {"raw": body}
            return resp.status, data
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8") if e.fp else ""
        try:
            return e.code, json.loads(err_body)
        except Exception:
            return e.code, {"error": err_body}
    except Exception as exc:
        return 0, {"error": str(exc)}


def wait_for_superset(max_retries: int = 40, delay: int = 3):
    log.info(f"Waiting for Superset at {SUPERSET_URL}...")
    for i in range(1, max_retries + 1):
        try:
            status, _ = request_json("GET", "/health")
            if status in (200, 302):
                log.info("Superset is responsive!")
                return True
        except Exception:
            pass
        time.sleep(delay)
    log.error("Timed out waiting for Superset.")
    return False


def login() -> str:
    log.info(f"Logging in to Superset as {ADMIN_USER}...")
    payload = {
        "username": ADMIN_USER,
        "password": ADMIN_PASS,
        "provider": "db",
        "refresh": True,
    }
    status, res = request_json("POST", "/api/v1/security/login", payload)
    if status != 200 or "access_token" not in res:
        raise RuntimeError(f"Login failed: status={status}, response={res}")
    token = res["access_token"]
    log.info("Login successful.")
    return token


def get_csrf_token(token: str) -> str:
    status, res = request_json("GET", "/api/v1/security/csrf_token/", token=token)
    if status == 200 and "result" in res:
        return res["result"]
    return ""


def ensure_database(token: str, csrf: str) -> int:
    db_name = "Salud Colombia DW"
    log.info(f"Checking if database '{db_name}' exists...")
    status, res = request_json("GET", "/api/v1/database/", token=token)
    if status == 200 and "result" in res:
        for db in res["result"]:
            if db.get("database_name") == db_name:
                log.info(f"Database '{db_name}' already registered (id={db['id']}).")
                return db["id"]

    log.info(f"Registering database '{db_name}'...")
    payload = {
        "database_name": db_name,
        "sqlalchemy_uri": DW_URI,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
    }
    status, res = request_json("POST", "/api/v1/database/", payload, token=token, csrf=csrf)
    if status in (200, 201) and "id" in res:
        log.info(f"Database registered successfully (id={res['id']}).")
        return res["id"]
    raise RuntimeError(f"Failed to register database: {res}")


def ensure_dataset(token: str, db_id: int, table_name: str, schema: str = "public", csrf: str = None) -> int:
    log.info(f"Checking dataset '{table_name}'...")
    status, res = request_json("GET", f"/api/v1/dataset/?q=(filters:[{{col:table_name,opr:eq,value:{table_name}}}])", token=token)
    if status == 200 and "result" in res and len(res["result"]) > 0:
        ds_id = res["result"][0]["id"]
        log.info(f"Dataset '{table_name}' already exists (id={ds_id}).")
        return ds_id

    log.info(f"Creating dataset '{table_name}'...")
    payload = {
        "database": db_id,
        "schema": schema,
        "table_name": table_name,
    }
    status, res = request_json("POST", "/api/v1/dataset/", payload, token=token, csrf=csrf)
    if status in (200, 201) and "id" in res:
        log.info(f"Dataset '{table_name}' created (id={res['id']}).")
        return res["id"]
    log.warning(f"Could not create dataset '{table_name}': {res}")
    return 0


def ensure_dashboard(token: str, title: str, slug: str, csrf: str = None) -> int:
    log.info(f"Checking dashboard '{title}'...")
    status, res = request_json("GET", f"/api/v1/dashboard/?q=(filters:[{{col:slug,opr:eq,value:{slug}}}])", token=token)
    if status == 200 and "result" in res and len(res["result"]) > 0:
        dash_id = res["result"][0]["id"]
        log.info(f"Dashboard '{title}' already exists (id={dash_id}).")
        return dash_id

    payload = {
        "dashboard_title": title,
        "slug": slug,
        "published": True,
    }
    status, res = request_json("POST", "/api/v1/dashboard/", payload, token=token, csrf=csrf)
    if status in (200, 201) and "id" in res:
        log.info(f"Dashboard '{title}' created (id={res['id']}).")
        return res["id"]
    log.warning(f"Could not create dashboard '{title}': {res}")
    return 0


def main():
    log.info("Starting Superset Bootstrap for Health Colombia ODS 3...")
    if not wait_for_superset():
        sys.exit(1)

    try:
        token = login()
        csrf = get_csrf_token(token)
        db_id = ensure_database(token, csrf)

        datasets = [
            "v_affiliates_summary",
            "v_facility_summary",
            "v_r6_demographic_coverage",
            "v_r7_territorial_capacity",
            "fact_affiliates",
            "fact_facility_capacity",
            "dim_geografia",
        ]

        ds_map = {}
        for ds in datasets:
            ds_id = ensure_dataset(token, db_id, ds, csrf=csrf)
            ds_map[ds] = ds_id

        dash_id = ensure_dashboard(
            token,
            "ODS 3 - Salud Colombia: Disparidades y Cobertura Territorial (R1–R7)",
            "ods3-salud-colombia",
            csrf=csrf
        )

        log.info("=" * 60)
        log.info("SUPERSET BOOTSTRAP FINISHED SUCCESSFULLY!")
        log.info(f"Superset URL:        {SUPERSET_URL}")
        log.info(f"Dashboard:           {SUPERSET_URL}/superset/dashboard/ods3-salud-colombia/")
        log.info(f"Registered datasets: {list(ds_map.keys())}")
        log.info("=" * 60)

    except Exception as e:
        log.error(f"Superset bootstrap error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
