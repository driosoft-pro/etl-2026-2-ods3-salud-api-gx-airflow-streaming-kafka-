"""Controlled Quality Failure Pipeline for Health Colombia (ODS 3 - Test B: Controlled Quality Failure).
Demonstrates observable failure detection and automatic downstream blocking in Apache Airflow 3.1.8.

In Airflow UI during the presentation:
  - extract_primary      -> SUCCESS (Green)
  - extract_api          -> SUCCESS (Green)
  - validate_primary_raw -> FAILED (Red) [Detected critical rule violation DQ-02]
  - transform_and_integrate -> UPSTREAM_FAILED (Orange) [Safely Blocked]
  - validate_prepared    -> UPSTREAM_FAILED [Safely Blocked]
  - load_dimensional_dw  -> UPSTREAM_FAILED [Warehouse Protected from Bad Data]
  - verify_dw_and_kpis   -> UPSTREAM_FAILED
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

try:
    from airflow.sdk import dag, task, get_current_context
except ImportError:
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context

from src.extract import extract_all
from src.api_extract import extract_api_all
from src.transform import (
    clean_affiliates, clean_facilities,
    build_dim_time, build_dim_geografia,
    build_dim_regime, build_dim_facility, build_dim_capacity_type,
    build_fact_affiliates, build_fact_facility_capacity
)
from src.quality import (
    validate_primary_raw, validate_api_raw, validate_prepared_data,
    CriticalQualityError
)
from src.load import load_all, export_to_csv, get_connection
from src import audit

log = logging.getLogger(__name__)


def _audit_run_id() -> int | None:
    """Registro idempotente de la corrida en audit.etl_run (clave = dag_run.run_id)."""
    try:
        context = get_current_context()
        dag_run = context.get("dag_run")
        return audit.ensure_run(
            run_key=dag_run.run_id if dag_run else None,
            run_type="AIRFLOW",
            dag_id=context.get("dag").dag_id if context.get("dag") else None,
            trigger_source="airflow_test_b",
        )
    except Exception as e:
        log.warning(f"Audit: no se pudo obtener run_id de Airflow ({e})")
        return None

DEFAULT_ARGS = {
    "owner": "health-data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # CRITICAL: Quality gates must NOT retry deterministic corrupt data
}

@dag(
    dag_id="bad_health_pipeline",
    default_args=DEFAULT_ARGS,
    description="Test B: Demostracion de Falla Controlada — Bloqueo en Gate 1 DQ-02 (Proteccion del DW)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test-b", "controlled-failure", "blocked-gate", "dq-02", "ods3", "demo-presentation"]
)
def bad_health_pipeline():

    @task(task_id="extract_primary")
    def task_extract_primary() -> Dict[str, Any]:
        """Extract primary CSV datasets. Injects corrupt regime records into affiliate microdata."""
        log.info("Task [extract_primary]: Loading raw healthcare datasets for controlled failure demo...")
        raw_data = extract_all()
        aff_df = raw_data['affiliates'].copy()
        
        # Inject corrupted regime code to simulate corrupted upstream raw feed
        aff_df.loc[0, 'regime_id'] = 'INVALID_Z'
        log.warning("DEMO TEST B: Injected invalid regime_id='INVALID_Z' at row 0 to test Gate 1.")
        
        return {
            "status": "extracted_with_synthetic_bad_record",
            "batch_id": f"bad_batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
            "affiliates_count": len(aff_df),
            "facilities_count": len(raw_data['facilities'])
        }

    @task(task_id="extract_api")
    def task_extract_api() -> Dict[str, Any]:
        """Extract external API Colombia datasets normally."""
        log.info("Task [extract_api]: Extracting demographic data from API Colombia...")
        api_data = extract_api_all(force_cache=True)
        return {
            "status": "extracted",
            "departments_count": len(api_data['departments']),
            "regions_count": len(api_data['regions'])
        }

    @task(task_id="validate_primary_raw")
    def task_validate_primary_raw(extract_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Gate 1: Great Expectations raw validation on primary datasets.
        Evaluates DQ-02 against injected invalid regime code.
        Catches the error, logs evidence, and raises CriticalQualityError to halt the DAG.
        """
        log.info("Task [validate_primary_raw]: Running Gate 1 with corrupt data input...")
        raw_data = extract_all()
        aff_df = raw_data['affiliates'].copy()
        aff_df.loc[0, 'regime_id'] = 'INVALID_Z'
        
        # This call deterministically raises CriticalQualityError
        run_id = _audit_run_id()
        try:
            validate_primary_raw(
                aff_df,
                raw_data['facilities'],
                simulate_failure="DQ-02"
            )
        except CriticalQualityError as cqe:
            audit.log_dq_results(run_id, cqe.details)
            audit.log_step(run_id, 'VALIDATE', 'gate_1_primary_raw', 'FAILED', message=str(cqe))
            audit.finish_run(run_id, audit.STATUS_BLOCKED, stage_reached='VALIDATE',
                             error_message=str(cqe))
            raise

        return {"status": "passed"}

    @task(task_id="validate_api_raw")
    def task_validate_api_raw(api_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Gate 2: Runs in parallel for API Colombia data."""
        log.info("Task [validate_api_raw]: Validating API data...")
        api_data = extract_api_all(force_cache=True)
        result = validate_api_raw(api_data['departments'], api_data['regions'], simulate_failure=None)
        return {"status": "passed"}

    @task(task_id="transform_and_integrate")
    def task_transform_and_integrate(primary_gate_res: Dict[str, Any], api_gate_res: Dict[str, Any]) -> Dict[str, Any]:
        """Downstream Task: Will be marked as UPSTREAM_FAILED and NEVER executed."""
        log.critical("SECURITY VIOLATION: transform_and_integrate should never run on corrupt data!")
        raise RuntimeError("Gate 1 failed to block invalid data!")

    @task(task_id="validate_prepared")
    def task_validate_prepared(transform_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Downstream Task: Will be marked as UPSTREAM_FAILED and NEVER executed."""
        return {"status": "skipped"}

    @task(task_id="load_dimensional_dw")
    def task_load_dw(prepared_gate_res: Dict[str, Any]) -> Dict[str, Any]:
        """Downstream Task: Warehouse is PROTECTED from corrupted facts."""
        log.critical("SECURITY VIOLATION: load_dimensional_dw should never run on corrupt data!")
        raise RuntimeError("Corrupt data reached the warehouse load task!")

    @task(task_id="verify_dw_and_kpis")
    def task_verify_dw(load_res: Dict[str, Any]) -> Dict[str, Any]:
        """Downstream Task: Verification skipped."""
        return {"status": "skipped"}

    # --- Dependencies ---
    meta_prim = task_extract_primary()
    gate1 = task_validate_primary_raw(meta_prim)

    meta_api = task_extract_api()
    gate2 = task_validate_api_raw(meta_api)

    # Convergence: gate1 failure blocks transform_and_integrate and all downstream tasks
    meta_transform = task_transform_and_integrate(gate1, gate2)
    gate3 = task_validate_prepared(meta_transform)
    meta_load = task_load_dw(gate3)
    task_verify_dw(meta_load)

dag_bad = bad_health_pipeline()
