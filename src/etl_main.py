import logging
import sys
import os
import argparse
import pandas as pd
from .extract import extract_all
from .api_extract import extract_api_all
from .transform import (
    clean_affiliates, clean_facilities,
    build_dim_time, build_dim_geografia,
    build_dim_regime, build_dim_facility, build_dim_capacity_type,
    build_fact_affiliates, build_fact_facility_capacity
)
from .quality import (
    validate_primary_raw, validate_api_raw, validate_prepared_data,
    CriticalQualityError
)
from .load import load_all, export_to_csv
from . import audit

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), '..', 'etl.log'))
    ]
)
logger = logging.getLogger(__name__)

def run_etl(simulate_failure: str = None, offline_api: bool = False, skip_load: bool = False):
    logger.info("=" * 70)
    logger.info("ETL PROCESS STARTED - HEALTH COLOMBIA (ODS 3 - SECOND DELIVERY)")
    logger.info("=" * 70)

    run_id = audit.ensure_run(
        run_type='CLI',
        trigger_source='etl_main.py',
        source_counts={'simulate_failure': simulate_failure, 'offline_api': offline_api,
                       'skip_load': skip_load}
    )

    try:
        # -------------------------------------------------------------
        # PHASE 1: EXTRACTION (Primary CSVs + API Colombia)
        # -------------------------------------------------------------
        logger.info("PHASE 1: EXTRACTION - Primary and API branches")
        primary_raw = extract_all()
        api_raw = extract_api_all(force_cache=offline_api)
        audit.log_step(
            run_id, 'EXTRACT', 'extract_primary_and_api', 'SUCCESS',
            rows_out=len(primary_raw['affiliates']) + len(primary_raw['facilities']),
            message=(f"affiliates={len(primary_raw['affiliates'])}, "
                     f"facilities={len(primary_raw['facilities'])}, "
                     f"api_departments={len(api_raw['departments'])}, "
                     f"api_regions={len(api_raw['regions'])}")
        )

        # -------------------------------------------------------------
        # PHASE 1.5: RAW VALIDATION GATES (Great Expectations Gates 1 & 2)
        # -------------------------------------------------------------
        logger.info("PHASE 1.5: RAW VALIDATION - Enforcing Quality Rules")
        logger.info("  -> Running Primary Raw Validation Gate (DQ-01 to DQ-05)...")
        audit.log_step(run_id, 'VALIDATE', 'gate_1_primary_raw', 'RUNNING')
        try:
            gate1 = validate_primary_raw(
                primary_raw['affiliates'],
                primary_raw['facilities'],
                simulate_failure=simulate_failure
            )
            audit.log_dq_results(run_id, gate1)
            audit.log_step(run_id, 'VALIDATE', 'gate_1_primary_raw', 'SUCCESS',
                           message=f"{gate1['passed_rules']}/{gate1['total_rules']} rules")
        except CriticalQualityError as cqe:
            audit.log_dq_results(run_id, cqe.details)
            audit.log_step(run_id, 'VALIDATE', 'gate_1_primary_raw', 'FAILED', message=str(cqe))
            raise

        logger.info("  -> Running API Raw Validation Gate (DQ-06 to DQ-07)...")
        audit.log_step(run_id, 'VALIDATE', 'gate_2_api_raw', 'RUNNING')
        try:
            gate2 = validate_api_raw(
                api_raw['departments'],
                api_raw['regions'],
                simulate_failure=simulate_failure
            )
            audit.log_dq_results(run_id, gate2)
            audit.log_step(run_id, 'VALIDATE', 'gate_2_api_raw', 'SUCCESS',
                           message=f"{gate2['passed_rules']}/{gate2['total_rules']} rules")
        except CriticalQualityError as cqe:
            audit.log_dq_results(run_id, cqe.details)
            audit.log_step(run_id, 'VALIDATE', 'gate_2_api_raw', 'FAILED', message=str(cqe))
            raise

        # -------------------------------------------------------------
        # PHASE 2: TRANSFORMATION & INTEGRATION (Integration Contract)
        # -------------------------------------------------------------
        logger.info("PHASE 2: TRANSFORMATION & API INTEGRATION")
        raw_total = pd.to_numeric(
            primary_raw['affiliates']['num_persons'].astype(str).str.replace('.', '', regex=False),
            errors='coerce'
        ).fillna(0).sum()

        df_affiliates = clean_affiliates(primary_raw['affiliates'])
        df_facilities = clean_facilities(primary_raw['facilities'])

        logger.info("Building conformed dimensions with API Colombia enrichment...")
        dim_time = build_dim_time(df_affiliates, df_facilities)
        dim_geografia = build_dim_geografia(
            df_affiliates, df_facilities,
            api_departments=api_raw['departments'],
            api_regions=api_raw['regions']
        )
        dim_regime = build_dim_regime(df_affiliates)
        dim_facility = build_dim_facility(df_facilities, dim_geografia)
        dim_capacity_type = build_dim_capacity_type(df_facilities)

        logger.info("Building fact tables with declared granularities...")
        fact_affiliates = build_fact_affiliates(df_affiliates, dim_time, dim_geografia, dim_regime)
        fact_capacity = build_fact_facility_capacity(
            df_facilities, dim_time, dim_facility, dim_capacity_type, dim_geografia
        )

        audit.log_step(
            run_id, 'TRANSFORM', 'build_dimensions_and_facts', 'SUCCESS',
            rows_in=len(primary_raw['affiliates']) + len(primary_raw['facilities']),
            rows_out=len(dim_geografia) + len(dim_facility) + len(fact_affiliates) + len(fact_capacity),
            message=(f"dim_geografia={len(dim_geografia)}, dim_facility={len(dim_facility)}, "
                     f"fact_affiliates={len(fact_affiliates)}, fact_capacity={len(fact_capacity)}")
        )

        dimensions = {
            'time': dim_time,
            'geografia': dim_geografia,
            'regime': dim_regime,
            'facility': dim_facility,
            'capacity_type': dim_capacity_type
        }

        facts = {
            'affiliates': fact_affiliates,
            'capacity': fact_capacity
        }

        # -------------------------------------------------------------
        # PHASE 2.5: PREPARED DATA VALIDATION GATE (Great Expectations Gate 3)
        # -------------------------------------------------------------
        logger.info("PHASE 2.5: PREPARED DATA VALIDATION (DQ-08 to DQ-12)...")
        audit.log_step(run_id, 'VALIDATE', 'gate_3_prepared', 'RUNNING')
        try:
            gate3 = validate_prepared_data(
                dimensions, facts,
                raw_total_persons=raw_total,
                simulate_failure=simulate_failure
            )
            audit.log_dq_results(run_id, gate3)
            audit.log_step(run_id, 'VALIDATE', 'gate_3_prepared', 'SUCCESS',
                           message=f"{gate3['passed_rules']}/{gate3['total_rules']} rules")
        except CriticalQualityError as cqe:
            audit.log_dq_results(run_id, cqe.details)
            audit.log_step(run_id, 'VALIDATE', 'gate_3_prepared', 'FAILED', message=str(cqe))
            raise

        # -------------------------------------------------------------
        # PHASE 3: LOADING TO DIMENSIONAL DATA WAREHOUSE
        # -------------------------------------------------------------
        rows_loaded = {
            'dim_time': len(dim_time),
            'dim_geografia': len(dim_geografia),
            'dim_regime': len(dim_regime),
            'dim_facility': len(dim_facility),
            'dim_capacity_type': len(dim_capacity_type),
            'fact_affiliates': len(fact_affiliates),
            'fact_facility_capacity': len(fact_capacity),
        }
        if not skip_load:
            logger.info("PHASE 3: LOADING TO POSTGRESQL DATA WAREHOUSE...")
            load_all(dimensions, facts, run_id=run_id)
            audit.log_step(run_id, 'LOAD', 'load_dimensional_dw', 'SUCCESS',
                           rows_out=sum(rows_loaded.values()))
        else:
            logger.info("PHASE 3: LOADING SKIPPED (Dry run)")
            audit.log_step(run_id, 'LOAD', 'load_dimensional_dw', 'SKIPPED',
                           message='--skip-load (dry run)')
            rows_loaded = None

        # -------------------------------------------------------------
        # PHASE 4: CSV EXPORT FOR BI TOOLS
        # -------------------------------------------------------------
        logger.info("PHASE 4: EXPORTING PROCESSED CSVs...")
        export_to_csv(dimensions, facts)
        audit.log_step(run_id, 'EXPORT', 'export_processed_csvs', 'SUCCESS',
                       rows_out=len(dimensions) + len(facts))

        audit.finish_run(run_id, audit.STATUS_SUCCESS, stage_reached='EXPORT', rows_loaded=rows_loaded)

        logger.info("=" * 70)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

        print("\n================ LOAD SUMMARY ================")
        print(f"  dim_time:              {len(dim_time):>6} records")
        print(f"  dim_geografia:         {len(dim_geografia):>6} records (Enriched with API Colombia)")
        print(f"  dim_regime:            {len(dim_regime):>6} records")
        print(f"  dim_facility:          {len(dim_facility):>6} records")
        print(f"  dim_capacity_type:     {len(dim_capacity_type):>6} records")
        print(f"  fact_affiliates:       {len(fact_affiliates):>6} records (Quarterly grain)")
        print(f"  fact_capacity:         {len(fact_capacity):>6} records (Snapshot Q4 2022)")
        print(f"  Quality Gates:         ALL PASSED (Gates 1, 2, and 3)")
        print(f"  R1-R7 Readiness:       SUPPORTED (Views v_r6, v_r7 active)")
        print(f"  Audit run_id:          {run_id} (tabla audit.etl_run)")
        print("==============================================\n")

        return {
            "status": "success",
            "run_id": run_id,
            "dimensions": {k: len(v) for k, v in dimensions.items()},
            "facts": {k: len(v) for k, v in facts.items()}
        }

    except CriticalQualityError as cqe:
        audit.finish_run(run_id, audit.STATUS_BLOCKED,
                         stage_reached='VALIDATE', error_message=str(cqe))
        logger.critical(f"PIPELINE BLOCKED BY QUALITY GATE: {cqe}")
        raise
    except Exception as e:
        audit.finish_run(run_id, audit.STATUS_FAILED, error_message=str(e))
        logger.error(f"ETL pipeline fatal error: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Health Colombia ETL Runner")
    parser.add_argument('--simulate-failure', type=str, default=None,
                        help="Simulate a critical rule failure (e.g., DQ-02 for Test B)")
    parser.add_argument('--offline-api', action='store_true',
                        help="Force using cached raw API response instead of live HTTP call")
    parser.add_argument('--skip-load', action='store_true',
                        help="Skip database load phase")
    args = parser.parse_args()
    
    try:
        run_etl(
            simulate_failure=args.simulate_failure,
            offline_api=args.offline_api,
            skip_load=args.skip_load
        )
    except CriticalQualityError as cqe:
        print(f"\n[CRITICAL QUALITY FAILURE] {cqe}")
        sys.exit(2)
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        sys.exit(1)
