"""Automated validation gates using Great Expectations semantics.
Serializes outputs to docs/evidence/gx/ and enforces severity policy.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List
import pandas as pd

from .rules import RULES, CriticalQualityError
from .gx_backend import (
    apply_gx_to_results,
    combine_reports,
    run_gx_suite,
    value_set,
    with_key,
    with_numeric,
)

logger = logging.getLogger(__name__)

EVIDENCE_DIR = os.getenv('GX_EVIDENCE_DIR', os.path.join(os.path.dirname(__file__), '..', '..', 'docs', 'evidence', 'gx'))

def _json_default(obj):
    """Convierte tipos numpy a primitivos de Python para la serialización de evidencia."""
    import numpy as np
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def _persist_evidence(gate_name: str, payload: dict):
    try:
        os.makedirs(EVIDENCE_DIR, exist_ok=True)
        path = os.path.join(EVIDENCE_DIR, f"{gate_name}_results.json")
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=_json_default)
        logger.info(f"Persisted validation evidence to: {path}")
    except Exception as e:
        logger.warning(f"Could not persist validation evidence to {EVIDENCE_DIR}: {e}")

def validate_primary_raw(df_affiliates: pd.DataFrame, df_facilities: pd.DataFrame,
                         simulate_failure: str = None) -> Dict[str, Any]:
    """Validation Gate 1: Primary raw source branch (affiliates + facilities).
    Protects downstream transformation from malformed inputs.
    """
    logger.info("Executing Primary Raw Validation Gate (GX Suite: primary_raw_suite)...")
    results = []
    has_critical_failure = False
    failing_rule = None
    failing_msg = ""

    # Check DQ-01: Mandatory columns affiliates
    r1 = RULES["DQ-01"]
    req_aff_cols = ['department_code', 'department', 'municipality_code', 'municipality',
                    'regime_id', 'year', 'month', 'num_persons']
    missing_aff = [c for c in req_aff_cols if c not in df_affiliates.columns]
    success_dq1 = len(missing_aff) == 0
    results.append({
        "rule_id": "DQ-01",
        "description": r1["rule"],
        "severity": r1["severity"],
        "success": success_dq1,
        "details": {"missing_columns": missing_aff, "total_columns": len(df_affiliates.columns)}
    })
    if not success_dq1:
        has_critical_failure = True
        failing_rule = "DQ-01"
        failing_msg = f"Missing mandatory columns: {missing_aff}"

    # Check DQ-02: Regime codes in {S, C, E, I}
    r2 = RULES["DQ-02"]
    if simulate_failure in ("DQ-02", "critical_primary", "test_b"):
        regimes = set(df_affiliates['regime_id'].dropna().unique()) | {'INVALID_Z'}
    else:
        regimes = set(df_affiliates['regime_id'].dropna().unique())
    valid_regimes = {'S', 'C', 'E', 'I'}
    invalid_regimes = list(regimes - valid_regimes)
    success_dq2 = len(invalid_regimes) == 0
    results.append({
        "rule_id": "DQ-02",
        "description": r2["rule"],
        "severity": r2["severity"],
        "success": success_dq2,
        "details": {"invalid_regimes": invalid_regimes, "found_regimes": list(regimes)}
    })
    if not success_dq2 and not has_critical_failure:
        has_critical_failure = True
        failing_rule = "DQ-02"
        failing_msg = f"Found invalid healthcare regimes: {invalid_regimes}"

    # Check DQ-03: num_persons non-negative numeric
    r3 = RULES["DQ-03"]
    vals = pd.to_numeric(df_affiliates['num_persons'].astype(str).str.replace('.', '', regex=False), errors='coerce')
    negative_count = int((vals < 0).sum())
    null_count = int(vals.isna().sum())
    success_dq3 = (negative_count == 0) and (null_count == 0)
    results.append({
        "rule_id": "DQ-03",
        "description": r3["rule"],
        "severity": r3["severity"],
        "success": success_dq3,
        "details": {"negative_count": negative_count, "null_count": null_count}
    })
    if not success_dq3 and not has_critical_failure:
        has_critical_failure = True
        failing_rule = "DQ-03"
        failing_msg = f"Invalid persons count (negatives={negative_count}, nulls={null_count})"

    # Check DQ-04: Mandatory facility columns
    r4 = RULES["DQ-04"]
    req_fac_cols = ['provider_code', 'provider_name', 'department', 'municipality', 'installed_capacity']
    missing_fac = [c for c in req_fac_cols if c not in df_facilities.columns]
    success_dq4 = len(missing_fac) == 0
    results.append({
        "rule_id": "DQ-04",
        "description": r4["rule"],
        "severity": r4["severity"],
        "success": success_dq4,
        "details": {"missing_columns": missing_fac}
    })
    if not success_dq4 and not has_critical_failure:
        has_critical_failure = True
        failing_rule = "DQ-04"
        failing_msg = f"Missing facility columns: {missing_fac}"

    # Check DQ-05: installed_capacity non-negative
    r5 = RULES["DQ-05"]
    cap_vals = pd.to_numeric(df_facilities['installed_capacity'], errors='coerce').fillna(0)
    cap_neg = int((cap_vals < 0).sum())
    success_dq5 = cap_neg == 0
    results.append({
        "rule_id": "DQ-05",
        "description": r5["rule"],
        "severity": r5["severity"],
        "success": success_dq5,
        "details": {"negative_capacity_count": cap_neg}
    })
    if not success_dq5 and not has_critical_failure:
        has_critical_failure = True
        failing_rule = "DQ-05"
        failing_msg = f"Installed capacity has negative values: {cap_neg}"

    # ------------------------------------------------------------------
    # Great Expectations (suites GX reales) — DQ-01 .. DQ-05.
    # Cada expectativa se mapea a su regla DQ y se combina con el chequeo
    # nativo: la regla solo pasa si AMBOS controles la dan por buena.
    # ------------------------------------------------------------------
    df_aff_gx = with_numeric(
        df_affiliates, "num_persons", "num_persons_num", strip_thousands=True
    )
    df_fac_gx = with_numeric(
        df_facilities, "installed_capacity", "installed_capacity_num", fillna=0
    )
    gx_reports = [
        # Esquema sobre el crudo SIN columnas derivadas (column_set es exacto).
        run_gx_suite(
            "primary_raw_affiliates_schema",
            df_affiliates,
            [
                ("DQ-01", "ExpectTableColumnsToMatchSet",
                 {"column_set": sorted(req_aff_cols)}),
                ("DQ-02", "ExpectColumnValuesToBeInSet",
                 {"column": "regime_id", "value_set": ["C", "E", "I", "S"]}),
            ],
        ),
        # Medidas sobre el crudo con la columna numérica derivada (espejo del
        # chequeo nativo: puntos de miles eliminados).
        run_gx_suite(
            "primary_raw_affiliates_measures",
            df_aff_gx,
            [
                ("DQ-03", "ExpectColumnValuesToNotBeNull",
                 {"column": "num_persons_num"}),
                ("DQ-03", "ExpectColumnValuesToBeBetween",
                 {"column": "num_persons_num", "min_value": 0}),
            ],
        ),
        run_gx_suite(
            "primary_raw_facilities",
            df_fac_gx,
            [
                ("DQ-04", "ExpectColumnValuesToNotBeNull",
                 {"column": col, "mostly": 0.995})
                for col in req_fac_cols
            ] + [
                ("DQ-05", "ExpectColumnValuesToNotBeNull",
                 {"column": "installed_capacity"}),
                ("DQ-05", "ExpectColumnValuesToBeBetween",
                 {"column": "installed_capacity_num", "min_value": 0}),
            ],
        ),
    ]
    gx_summary = combine_reports(gx_reports)
    apply_gx_to_results(results, gx_reports)
    if any(not r["success"] for r in results):
        has_critical_failure = True
        if failing_rule is None:
            first_fail = next(r for r in results if not r["success"])
            failing_rule = first_fail["rule_id"]
            failing_msg = (
                f"Great Expectations expectation failed for rule {first_fail['rule_id']}"
            )

    report = {
        "gate": "primary_raw_validation",
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "total_rules": len(results),
        "passed_rules": sum(1 for r in results if r["success"]),
        "failed_rules": sum(1 for r in results if not r["success"]),
        "has_critical_failure": has_critical_failure,
        "gx": gx_summary,
        "evaluations": results
    }

    _persist_evidence("primary_raw", report)

    if has_critical_failure:
        logger.error(f"[GATE REJECTION] Primary raw validation failed rule {failing_rule}: {failing_msg}")
        raise CriticalQualityError(failing_rule, failing_msg, report)

    logger.info(
        f"Primary Raw Validation Gate passed successfully "
        f"(GX {gx_summary['expectations_passed']}/{gx_summary['expectations_total']} expectations)."
    )
    return report

def validate_api_raw(df_departments: pd.DataFrame, df_regions: pd.DataFrame,
                     simulate_failure: str = None) -> Dict[str, Any]:
    """Validation Gate 2: API Colombia raw source branch.
    Verifies that external API payloads contain required demographic attributes.
    """
    logger.info("Executing API Colombia Raw Validation Gate (GX Suite: api_raw_suite)...")
    results = []
    has_critical_failure = False
    failing_rule = None
    failing_msg = ""

    # Check DQ-06: API departments must contain name and id
    r6 = RULES["DQ-06"]
    if df_departments.empty:
        success_dq6 = False
        details_dq6 = {"reason": "API department dataset is empty"}
    else:
        null_names = int(df_departments['department_name'].isna().sum())
        null_ids = int(df_departments['api_id'].isna().sum())
        success_dq6 = (null_names == 0) and (null_ids == 0)
        details_dq6 = {"null_names": null_names, "null_ids": null_ids, "records": len(df_departments)}

    results.append({
        "rule_id": "DQ-06",
        "description": r6["rule"],
        "severity": r6["severity"],
        "success": success_dq6,
        "details": details_dq6
    })
    if not success_dq6:
        has_critical_failure = True
        failing_rule = "DQ-06"
        failing_msg = "API departments missing name or ID"

    # Check DQ-07: Population and surface must be positive
    r7 = RULES["DQ-07"]
    if simulate_failure in ("DQ-07", "critical_api"):
        pop_valid = False
        surf_valid = False
        pop_neg = 5
        surf_neg = 3
    else:
        pop_series = pd.to_numeric(df_departments['population'], errors='coerce')
        surf_series = pd.to_numeric(df_departments['surface'], errors='coerce')
        pop_neg = int((pop_series <= 0).sum())
        surf_neg = int((surf_series <= 0).sum())
        pop_valid = pop_neg == 0
        surf_valid = surf_neg == 0

    success_dq7 = pop_valid and surf_valid
    results.append({
        "rule_id": "DQ-07",
        "description": r7["rule"],
        "severity": r7["severity"],
        "success": success_dq7,
        "details": {"invalid_population_count": pop_neg, "invalid_surface_count": surf_neg}
    })
    if not success_dq7 and not has_critical_failure:
        has_critical_failure = True
        failing_rule = "DQ-07"
        failing_msg = f"API demographics invalid: non-positive population ({pop_neg}) or surface ({surf_neg})"

    # ------------------------------------------------------------------
    # Great Expectations (suites GX reales) — DQ-06 .. DQ-07
    # ------------------------------------------------------------------
    df_dept_gx = with_numeric(df_departments, "population", "population_num")
    df_dept_gx = with_numeric(df_dept_gx, "surface", "surface_num")
    gx_reports = [
        run_gx_suite(
            "api_raw_departments",
            df_dept_gx,
            [
                ("DQ-06", "ExpectColumnValuesToNotBeNull",
                 {"column": "department_name"}),
                ("DQ-06", "ExpectColumnValuesToNotBeNull", {"column": "api_id"}),
                ("DQ-07", "ExpectColumnValuesToBeBetween",
                 {"column": "population_num", "min_value": 0, "strict_min": True}),
                ("DQ-07", "ExpectColumnValuesToBeBetween",
                 {"column": "surface_num", "min_value": 0, "strict_min": True}),
            ],
        ),
    ]
    gx_summary = combine_reports(gx_reports)
    apply_gx_to_results(results, gx_reports)
    if any(not r["success"] for r in results):
        has_critical_failure = True
        if failing_rule is None:
            first_fail = next(r for r in results if not r["success"])
            failing_rule = first_fail["rule_id"]
            failing_msg = (
                f"Great Expectations expectation failed for rule {first_fail['rule_id']}"
            )

    report = {
        "gate": "api_raw_validation",
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "total_rules": len(results),
        "passed_rules": sum(1 for r in results if r["success"]),
        "failed_rules": sum(1 for r in results if not r["success"]),
        "has_critical_failure": has_critical_failure,
        "gx": gx_summary,
        "evaluations": results
    }

    _persist_evidence("api_raw", report)

    if has_critical_failure:
        logger.error(f"[GATE REJECTION] API raw validation failed rule {failing_rule}: {failing_msg}")
        raise CriticalQualityError(failing_rule, failing_msg, report)

    logger.info(
        f"API Colombia Raw Validation Gate passed successfully "
        f"(GX {gx_summary['expectations_passed']}/{gx_summary['expectations_total']} expectations)."
    )
    return report

def validate_prepared_data(dimensions: Dict[str, pd.DataFrame], facts: Dict[str, pd.DataFrame],
                           raw_total_persons: int = None,
                           simulate_failure: str = None) -> Dict[str, Any]:
    """Validation Gate 3: Prepared data gate before Data Warehouse load.
    Evaluates Integration Contract, grain uniqueness, referential integrity, and reconciliation.
    """
    logger.info("Executing Prepared Data Validation Gate (GX Suite: prepared_gate_suite)...")
    results = []
    has_critical_failure = False
    failing_rule = None
    failing_msg = ""

    dim_geo = dimensions['geografia']
    dim_time = dimensions['time']
    dim_reg = dimensions['regime']
    dim_fac = dimensions['facility']
    dim_ct = dimensions['capacity_type']

    fact_aff = facts['affiliates']
    fact_cap = facts['capacity']

    # DQ-08: Integration key match rate (Department resolution between primary and API)
    r8 = RULES["DQ-08"]
    departments_total = dim_geo['departamento'].nunique()
    departments_with_pop = dim_geo[dim_geo['population'].notna() & (dim_geo['population'] > 0)]['departamento'].nunique()
    match_rate = (departments_with_pop / departments_total) * 100.0 if departments_total > 0 else 0.0
    success_dq8 = match_rate >= 95.0
    results.append({
        "rule_id": "DQ-08",
        "description": r8["rule"],
        "severity": r8["severity"],
        "success": success_dq8,
        "details": {
            "departments_total": departments_total,
            "departments_enriched": departments_with_pop,
            "match_rate_pct": round(match_rate, 2),
            "threshold": "95.0%"
        }
    })
    if not success_dq8:
        has_critical_failure = True
        failing_rule = "DQ-08"
        failing_msg = f"Department API integration match rate below threshold: {match_rate:.2f}% < 95.0%"

    # DQ-09: Fact Affiliates Grain Uniqueness (sk_time, sk_geografia, sk_regime)
    r9 = RULES["DQ-09"]
    aff_dups = int(fact_aff.duplicated(subset=['sk_time', 'sk_geografia', 'sk_regime'], keep=False).sum())
    success_dq9 = aff_dups == 0
    results.append({
        "rule_id": "DQ-09",
        "description": r9["rule"],
        "severity": r9["severity"],
        "success": success_dq9,
        "details": {"duplicate_rows": aff_dups, "total_rows": len(fact_aff)}
    })
    if not success_dq9 and not has_critical_failure:
        has_critical_failure = True
        failing_rule = "DQ-09"
        failing_msg = f"fact_affiliates has {aff_dups} duplicate records on grain (sk_time, sk_geografia, sk_regime)"

    # DQ-10: Fact Capacity Grain Uniqueness (sk_time, sk_facility, sk_capacity_type, sk_geografia)
    r10 = RULES["DQ-10"]
    cap_dups = int(fact_cap.duplicated(subset=['sk_time', 'sk_facility', 'sk_capacity_type', 'sk_geografia'], keep=False).sum())
    success_dq10 = cap_dups == 0
    results.append({
        "rule_id": "DQ-10",
        "description": r10["rule"],
        "severity": r10["severity"],
        "success": success_dq10,
        "details": {"duplicate_rows": cap_dups, "total_rows": len(fact_cap)}
    })
    if not success_dq10 and not has_critical_failure:
        has_critical_failure = True
        failing_rule = "DQ-10"
        failing_msg = f"fact_facility_capacity has {cap_dups} duplicate records on declared grain"

    # DQ-11: Referential Integrity (No orphan FKs)
    r11 = RULES["DQ-11"]
    orphan_errors = []
    
    # Check fact_affiliates FKs
    if not fact_aff['sk_time'].isin(dim_time['sk_time']).all():
        orphan_errors.append("fact_affiliates.sk_time has orphan keys")
    if not fact_aff['sk_geografia'].isin(dim_geo['sk_geografia']).all():
        orphan_errors.append("fact_affiliates.sk_geografia has orphan keys")
    if not fact_aff['sk_regime'].isin(dim_reg['sk_regime']).all():
        orphan_errors.append("fact_affiliates.sk_regime has orphan keys")
        
    # Check fact_facility_capacity FKs
    if not fact_cap['sk_time'].isin(dim_time['sk_time']).all():
        orphan_errors.append("fact_facility_capacity.sk_time has orphan keys")
    if not fact_cap['sk_facility'].isin(dim_fac['sk_facility']).all():
        orphan_errors.append("fact_facility_capacity.sk_facility has orphan keys")
    if not fact_cap['sk_capacity_type'].isin(dim_ct['sk_capacity_type']).all():
        orphan_errors.append("fact_facility_capacity.sk_capacity_type has orphan keys")
    if not fact_cap['sk_geografia'].isin(dim_geo['sk_geografia']).all():
        orphan_errors.append("fact_facility_capacity.sk_geografia has orphan keys")

    success_dq11 = len(orphan_errors) == 0
    results.append({
        "rule_id": "DQ-11",
        "description": r11["rule"],
        "severity": r11["severity"],
        "success": success_dq11,
        "details": {"orphan_fk_violations": orphan_errors}
    })
    if not success_dq11 and not has_critical_failure:
        has_critical_failure = True
        failing_rule = "DQ-11"
        failing_msg = f"Referential integrity failure: {orphan_errors}"

    # DQ-12: Reconciliation of measures with raw input
    r12 = RULES["DQ-12"]
    fact_total = int(fact_aff['numero_afiliados'].sum())
    if raw_total_persons is not None and raw_total_persons > 0:
        diff = abs(fact_total - raw_total_persons)
        tolerance = raw_total_persons * 0.0001 # 0.01%
        success_dq12 = diff <= tolerance
        details_dq12 = {
            "fact_total": fact_total,
            "raw_total": raw_total_persons,
            "absolute_diff": diff,
            "diff_pct": round((diff / raw_total_persons) * 100, 4),
            "tolerance_threshold": tolerance
        }
    else:
        success_dq12 = True
        details_dq12 = {"fact_total": fact_total, "note": "No raw total provided, bypassed"}

    results.append({
        "rule_id": "DQ-12",
        "description": r12["rule"],
        "severity": r12["severity"],
        "success": success_dq12,
        "details": details_dq12
    })
    if not success_dq12 and not has_critical_failure:
        has_critical_failure = True
        failing_rule = "DQ-12"
        failing_msg = f"Reconciliation sum mismatch: fact={fact_total} vs raw={raw_total_persons}"

    # ------------------------------------------------------------------
    # Great Expectations (suites GX reales) — DQ-09 .. DQ-12
    #   * granos multi-columna -> clave derivada + expect_column_values_to_be_unique
    #   * integridad referencial -> expect_column_values_to_be_in_set (FK -> dim)
    #   * reconciliación -> expect_column_sum_to_be_between (tolerancia 0.01%)
    # ------------------------------------------------------------------
    fact_aff_gx = with_key(
        fact_aff, "grain_key", ["sk_time", "sk_geografia", "sk_regime"]
    )
    fact_cap_gx = with_key(
        fact_cap, "grain_key",
        ["sk_time", "sk_facility", "sk_capacity_type", "sk_geografia"]
    )
    aff_specs: list = [
        ("DQ-09", "ExpectColumnValuesToBeUnique", {"column": "grain_key"}),
        ("DQ-11", "ExpectColumnValuesToBeInSet",
         {"column": "sk_time", "value_set": value_set(dim_time["sk_time"])}),
        ("DQ-11", "ExpectColumnValuesToBeInSet",
         {"column": "sk_geografia", "value_set": value_set(dim_geo["sk_geografia"])}),
        ("DQ-11", "ExpectColumnValuesToBeInSet",
         {"column": "sk_regime", "value_set": value_set(dim_reg["sk_regime"])}),
    ]
    cap_specs: list = [
        ("DQ-10", "ExpectColumnValuesToBeUnique", {"column": "grain_key"}),
        ("DQ-11", "ExpectColumnValuesToBeInSet",
         {"column": "sk_time", "value_set": value_set(dim_time["sk_time"])}),
        ("DQ-11", "ExpectColumnValuesToBeInSet",
         {"column": "sk_facility", "value_set": value_set(dim_fac["sk_facility"])}),
        ("DQ-11", "ExpectColumnValuesToBeInSet",
         {"column": "sk_capacity_type",
          "value_set": value_set(dim_ct["sk_capacity_type"])}),
        ("DQ-11", "ExpectColumnValuesToBeInSet",
         {"column": "sk_geografia", "value_set": value_set(dim_geo["sk_geografia"])}),
    ]
    if raw_total_persons is not None and raw_total_persons > 0:
        tol_gx = raw_total_persons * 0.0001
        aff_specs.append(
            ("DQ-12", "ExpectColumnSumToBeBetween",
             {"column": "numero_afiliados",
              "min_value": float(raw_total_persons - tol_gx),
              "max_value": float(raw_total_persons + tol_gx)})
        )
    gx_reports = [
        run_gx_suite("prepared_fact_affiliates", fact_aff_gx, aff_specs),
        run_gx_suite("prepared_fact_capacity", fact_cap_gx, cap_specs),
    ]
    gx_summary = combine_reports(gx_reports)
    apply_gx_to_results(results, gx_reports)
    if any(not r["success"] for r in results):
        has_critical_failure = True
        if failing_rule is None:
            first_fail = next(r for r in results if not r["success"])
            failing_rule = first_fail["rule_id"]
            failing_msg = (
                f"Great Expectations expectation failed for rule {first_fail['rule_id']}"
            )

    report = {
        "gate": "prepared_data_validation",
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "total_rules": len(results),
        "passed_rules": sum(1 for r in results if r["success"]),
        "failed_rules": sum(1 for r in results if not r["success"]),
        "has_critical_failure": has_critical_failure,
        "gx": gx_summary,
        "evaluations": results
    }

    _persist_evidence("prepared", report)

    if has_critical_failure:
        logger.error(f"[GATE REJECTION] Prepared validation failed rule {failing_rule}: {failing_msg}")
        raise CriticalQualityError(failing_rule, failing_msg, report)

    logger.info(
        f"Prepared Data Validation Gate passed successfully "
        f"(GX {gx_summary['expectations_passed']}/{gx_summary['expectations_total']} expectations)."
    )
    return report
