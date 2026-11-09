#!/usr/bin/env python3
"""Mandatory Reliability Tests Suite — Health Colombia ODS 3.
Implements and documents:
  - Test A: Successful Run (Section 20.1)
  - Test B: Controlled Critical Quality Failure (Section 20.2)
  - Test C: Safe Rerun and Repeatability (Section 20.3)
"""

import sys
import os
import json
import logging
from datetime import datetime, timezone

# Ensure project root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl_main import run_etl
from src.quality import CriticalQualityError
from src.load import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("reliability_tests")

def test_a_successful_run() -> bool:
    log.info("=" * 60)
    log.info("RUNNING TEST A: SUCCESSFUL RUN (Section 20.1)")
    log.info("=" * 60)
    try:
        res = run_etl(simulate_failure=None, offline_api=False)
        log.info("ETL execution returned success.")
        
        # Verify DW state
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_affiliates;")
            aff_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM fact_facility_capacity;")
            cap_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM v_r6_demographic_coverage;")
            r6_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM v_r7_territorial_capacity;")
            r7_count = cur.fetchone()[0]
        conn.close()
        
        log.info(f"DW Verification: fact_affiliates={aff_count}, fact_facility_capacity={cap_count}")
        log.info(f"Analytical Views: v_r6={r6_count} rows, v_r7={r7_count} rows")
        
        assert aff_count > 0, "fact_affiliates is empty!"
        assert cap_count > 0, "fact_facility_capacity is empty!"
        assert r6_count > 0, "v_r6_demographic_coverage is empty!"
        assert r7_count > 0, "v_r7_territorial_capacity is empty!"
        
        log.info(">>> TEST A: PASSED SUCCESSFULLY! <<<\n")
        return True
    except Exception as e:
        log.error(f">>> TEST A: FAILED with error: {e} <<<\n", exc_info=True)
        return False

def test_b_controlled_critical_failure() -> bool:
    log.info("=" * 60)
    log.info("RUNNING TEST B: CONTROLLED CRITICAL FAILURE (Section 20.2)")
    log.info("Simulating violation of rule DQ-02 (Invalid Healthcare Regime)")
    log.info("Expected: Quality gate detects violation, raises CriticalQualityError, and blocks DW load.")
    log.info("=" * 60)
    try:
        run_etl(simulate_failure="DQ-02")
        log.error(">>> TEST B: FAILED! Pipeline was expected to fail on critical violation but continued! <<<")
        return False
    except CriticalQualityError as cqe:
        log.info(f"Observed expected failure: {cqe}")
        log.info("Downstream loading was safely prevented. Validation result logged.")
        log.info(">>> TEST B: PASSED (Controlled failure behaved as expected) <<<\n")
        return True
    except Exception as e:
        log.error(f">>> TEST B: Encountered unexpected exception: {e} <<<", exc_info=True)
        return False

def test_c_safe_rerun() -> bool:
    log.info("=" * 60)
    log.info("RUNNING TEST C: SAFE RERUN & REPEATABILITY (Section 20.3)")
    log.info("Ensures repeated runs do not duplicate facts or corrupt measures.")
    log.info("=" * 60)
    try:
        # First execution (Baseline)
        log.info("Executing baseline run...")
        run_etl(simulate_failure=None)
        
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), SUM(numero_afiliados) FROM fact_affiliates;")
            aff_count_before, aff_sum_before = cur.fetchone()
            cur.execute("SELECT COUNT(*), SUM(capacity_amount) FROM fact_facility_capacity;")
            cap_count_before, cap_sum_before = cur.fetchone()
        conn.close()
        
        log.info(f"Before Rerun: Affiliates rows={aff_count_before}, sum={aff_sum_before}")
        log.info(f"Before Rerun: Capacity rows={cap_count_before}, sum={cap_sum_before}")
        
        # Second execution (Rerun with identical input batch)
        log.info("Executing identical batch rerun...")
        run_etl(simulate_failure=None)
        
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), SUM(numero_afiliados) FROM fact_affiliates;")
            aff_count_after, aff_sum_after = cur.fetchone()
            cur.execute("SELECT COUNT(*), SUM(capacity_amount) FROM fact_facility_capacity;")
            cap_count_after, cap_sum_after = cur.fetchone()
        conn.close()
        
        log.info(f"After Rerun:  Affiliates rows={aff_count_after}, sum={aff_sum_after}")
        log.info(f"After Rerun:  Capacity rows={cap_count_after}, sum={cap_sum_after}")
        
        assert aff_count_before == aff_count_after, f"Row count changed in affiliates: {aff_count_before} -> {aff_count_after}"
        assert aff_sum_before == aff_sum_after, f"Measure sum changed in affiliates: {aff_sum_before} -> {aff_sum_after}"
        assert cap_count_before == cap_count_after, f"Row count changed in capacity: {cap_count_before} -> {cap_count_after}"
        assert cap_sum_before == cap_sum_after, f"Measure sum changed in capacity: {cap_sum_before} -> {cap_sum_after}"
        
        log.info(">>> TEST C: PASSED (Exact idempotency and repeatability verified) <<<\n")
        return True
    except Exception as e:
        log.error(f">>> TEST C: FAILED with error: {e} <<<\n", exc_info=True)
        return False

def main():
    test_arg = sys.argv[1].lower() if len(sys.argv) > 1 else "all"
    
    results = {}
    if test_arg in ("a", "test-a", "all"):
        results["Test A (Successful Run)"] = test_a_successful_run()
    if test_arg in ("b", "test-b", "all"):
        results["Test B (Controlled Failure)"] = test_b_controlled_critical_failure()
    if test_arg in ("c", "test-c", "all"):
        results["Test C (Safe Rerun)"] = test_c_safe_rerun()
        
    log.info("=" * 60)
    log.info("RELIABILITY TESTS SUMMARY")
    log.info("=" * 60)
    all_ok = True
    for name, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        log.info(f"  {name:<35}: {status}")
        if not passed:
            all_ok = False
            
    sys.exit(0 if all_ok else 1)

if __name__ == '__main__':
    main()
