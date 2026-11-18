#!/usr/bin/env python3
"""Reproducible EDA evidence for both CSVs and cached API Colombia snapshots.
Generates profiles, integration checks and API quality results for the final delivery.
"""

import os
import sys
import json
import logging
import pandas as pd

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extract import extract_all
from src.api_extract import extract_api_all
from src.config import DEPT_NORMALIZE, DISTRICT_TO_DEPT, REGION_MAP, normalize_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("profiling")

EVIDENCE_DIR = os.path.join(os.path.dirname(__file__), '..', 'docs', 'evidence', 'profiling')

def profile_dataframe(df: pd.DataFrame, dataset_name: str) -> dict:
    log.info(f"Profiling dataset: {dataset_name} ({len(df)} rows, {len(df.columns)} columns)...")
    summary = {
        "dataset_name": dataset_name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicate_rows": int(df.duplicated().sum()),
        "total_nulls": int(df.isna().sum().sum()),
        "columns": {}
    }

    for col in df.columns:
        s = df[col]
        null_count = int(s.isna().sum())
        null_pct = round((null_count / len(df)) * 100, 2) if len(df) > 0 else 0
        unique_count = int(s.nunique(dropna=True))
        
        col_profile = {
            "dtype": str(s.dtype),
            "null_count": null_count,
            "null_pct": null_pct,
            "unique_count": unique_count,
        }
        
        # Numeric / categorical specific
        if pd.api.types.is_numeric_dtype(s):
            col_profile["min"] = float(s.min()) if s.count() > 0 else None
            col_profile["max"] = float(s.max()) if s.count() > 0 else None
            col_profile["mean"] = round(float(s.mean()), 2) if s.count() > 0 else None
        else:
            top_vals = s.value_counts(dropna=False).head(5).to_dict()
            col_profile["top_values"] = {str(k): int(v) for k, v in top_vals.items()}

        summary["columns"][col] = col_profile

    return summary


def profile_api_quality(departments: pd.DataFrame, regions: pd.DataFrame) -> dict:
    """Check demographic completeness and geography against the same API snapshot."""
    numeric_checks = {}
    for column in ("population", "surface"):
        values = pd.to_numeric(departments[column], errors="coerce")
        numeric_checks[column] = {
            "null_or_non_numeric": int(values.isna().sum()),
            "non_positive": int((values <= 0).sum()),
            "min": float(values.min()) if values.notna().any() else None,
            "max": float(values.max()) if values.notna().any() else None,
        }
    aliases = {"BOGOTA": "BOGOTA D.C.", "SAN ANDRES Y PROVIDENCIA": "SAN ANDRES"}
    region_names = regions.set_index("region_id")["region_name"].to_dict()
    mismatches = []
    for row in departments.to_dict("records"):
        department = aliases.get(row["department_name"], row["department_name"])
        expected = region_names.get(row["region_id"], "")
        configured = REGION_MAP.get(department, "")
        if normalize_text(configured) != normalize_text(expected):
            mismatches.append({"department": department, "configured": configured,
                               "api_region": expected})
    return {
        "department_id_duplicates": int(departments["api_id"].duplicated().sum()),
        "department_name_duplicates": int(departments["department_name"].duplicated().sum()),
        "region_id_duplicates": int(regions["region_id"].duplicated().sum()),
        "unknown_region_references": int((~departments["region_id"].isin(regions["region_id"])).sum()),
        "numeric_checks": numeric_checks,
        "region_mapping_mismatches": mismatches,
    }


def profile_cross_source_integration(df_aff: pd.DataFrame, df_fac: pd.DataFrame, df_api: pd.DataFrame) -> dict:
    log.info("Profiling cross-source integration compatibility...")
    
    # Primary departments (normalized)
    aff_depts = df_aff['department'].dropna().apply(normalize_text).map(lambda x: DEPT_NORMALIZE.get(x, x)).unique()
    fac_depts = df_fac['department'].dropna().apply(normalize_text).map(lambda x: DISTRICT_TO_DEPT.get(x, x)).map(lambda x: DEPT_NORMALIZE.get(x, x)).unique()
    
    primary_depts = set(aff_depts) | set(fac_depts)
    primary_depts.discard('SIN DEPARTAMENTO')
    
    # Raw API names
    api_depts_raw = set(df_api['department_name'].dropna().unique())
    raw_matched = primary_depts.intersection(api_depts_raw)
    raw_match_rate = round((len(raw_matched) / len(primary_depts)) * 100, 2) if primary_depts else 0.0

    # Harmonized API names per Integration Contract
    API_ALIAS = {
        'BOGOTA': 'BOGOTA D.C.',
        'SAN ANDRES Y PROVIDENCIA': 'SAN ANDRES',
    }
    api_depts_harmonized = {API_ALIAS.get(d, d) for d in api_depts_raw}
    harmonized_matched = primary_depts.intersection(api_depts_harmonized)
    harmonized_match_rate = round((len(harmonized_matched) / len(primary_depts)) * 100, 2) if primary_depts else 0.0

    return {
        "primary_departments_count": len(primary_depts),
        "api_departments_count": len(api_depts_raw),
        "raw_matched_count": len(raw_matched),
        "raw_match_rate_pct": raw_match_rate,
        "raw_unmatched_primary": sorted(list(primary_depts - api_depts_raw)),
        "harmonized_matched_count": len(harmonized_matched),
        "harmonized_match_rate_pct": harmonized_match_rate,
        "harmonization_rules": API_ALIAS,
        "integration_feasibility": "PERFECT (100% coverage achieved post-harmonization)" if harmonized_match_rate == 100 else "PARTIAL"
    }

def run_profiling():
    os.makedirs(EVIDENCE_DIR, exist_ok=True)
    
    raw_primary = extract_all()
    raw_api = extract_api_all(force_cache=True)
    
    p_aff = profile_dataframe(raw_primary['affiliates'], "primary_raw_affiliates")
    p_fac = profile_dataframe(raw_primary['facilities'], "primary_raw_facilities")
    p_api_dept = profile_dataframe(raw_api['departments'], "api_colombia_departments")
    p_api_reg = profile_dataframe(raw_api['regions'], "api_colombia_regions")
    
    p_integration = profile_cross_source_integration(
        raw_primary['affiliates'],
        raw_primary['facilities'],
        raw_api['departments']
    )
    
    full_evidence = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_mode": "versioned_csv_and_api_cache (no live API calls)",
        "datasets": {
            "affiliates": p_aff,
            "facilities": p_fac,
            "api_departments": p_api_dept,
            "api_regions": p_api_reg
        },
        "integration_profile": p_integration,
        "api_quality": profile_api_quality(raw_api['departments'], raw_api['regions']),
    }
    
    output_path = os.path.join(EVIDENCE_DIR, "data_profiling_evidence.json")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(full_evidence, f, ensure_ascii=False, indent=2)
        
    log.info(f"Profiling evidence written to: {output_path}")
    print("\nPROFILING SUMMARY:")
    print(f"  Affiliates:        {p_aff['row_count']} rows, {p_aff['total_nulls']} total nulls")
    print(f"  Facilities:        {p_fac['row_count']} rows, {p_fac['columns']['installed_capacity']['unique_count']} capacity values")
    print(f"  API Departments:   {p_api_dept['row_count']} records")
    print(f"  API Regions:       {p_api_reg['row_count']} records")
    print(f"  Region mismatches: {len(full_evidence['api_quality']['region_mapping_mismatches'])}")
    print(f"  Integration Match: {p_integration['harmonized_match_rate_pct']}% matching (harmonized) / {p_integration['raw_match_rate_pct']}% (raw)\n")
    return full_evidence

if __name__ == '__main__':
    run_profiling()
