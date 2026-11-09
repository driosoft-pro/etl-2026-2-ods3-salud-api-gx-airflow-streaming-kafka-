"""Quality rules catalog and error definitions for Health Colombia ETL.
Aligned with ETL_2026-2_Project_SecondDelivery_v2.pdf (Sections 8, 9, 10, 14).
"""

from typing import Dict, Any

class CriticalQualityError(Exception):
    """Raised when a data quality rule marked with CRITICAL severity is violated.
    This deterministically fails the validation task and blocks unsafe downstream loading.
    """
    def __init__(self, rule_id: str, message: str, details: Dict[str, Any] = None):
        super().__init__(f"[{rule_id} - CRITICAL FAILURE] {message}")
        self.rule_id = rule_id
        self.details = details or {}


RULES: Dict[str, Dict[str, Any]] = {
    "DQ-01": {
        "layer": "raw_affiliates",
        "dimension": "Completeness",
        "attribute": "department_code,department,municipality_code,municipality,regime_id,year,month,num_persons",
        "rule": "Mandatory columns in raw affiliates must not be missing or completely null",
        "threshold": "100%",
        "severity": "CRITICAL",
        "expectation": "expect_table_columns_to_match_set",
        "requirement": "R1, R2, R5, R6",
        "pipeline_action": "Block transformation and integration"
    },
    "DQ-02": {
        "layer": "raw_affiliates",
        "dimension": "Validity",
        "attribute": "regime_id",
        "rule": "Healthcare regime code must belong to allowed set {S, C, E, I}",
        "threshold": "100%",
        "severity": "CRITICAL",
        "expectation": "expect_column_values_to_be_in_set",
        "requirement": "R1, R5",
        "pipeline_action": "Block transformation and integration"
    },
    "DQ-03": {
        "layer": "raw_affiliates",
        "dimension": "Validity",
        "attribute": "num_persons",
        "rule": "Affiliate count must be non-negative and parseable as numeric",
        "threshold": "100%",
        "severity": "CRITICAL",
        "expectation": "expect_column_values_to_be_between",
        "requirement": "R1, R2, R6",
        "pipeline_action": "Block transformation and integration"
    },
    "DQ-04": {
        "layer": "raw_facilities",
        "dimension": "Completeness",
        "attribute": "provider_code,provider_name,department,municipality,installed_capacity",
        "rule": "Essential facility identity and capacity attributes must be present",
        "threshold": ">= 99.5%",
        "severity": "CRITICAL",
        "expectation": "expect_column_values_to_not_be_null",
        "requirement": "R3, R4, R7",
        "pipeline_action": "Block transformation and integration"
    },
    "DQ-05": {
        "layer": "raw_facilities",
        "dimension": "Validity",
        "attribute": "installed_capacity",
        "rule": "Installed capacity measure must be non-negative (>= 0)",
        "threshold": "100%",
        "severity": "CRITICAL",
        "expectation": "expect_column_values_to_be_between",
        "requirement": "R3, R4, R7",
        "pipeline_action": "Block transformation and integration"
    },
    "DQ-06": {
        "layer": "raw_api_departments",
        "dimension": "Completeness",
        "attribute": "department_name,api_id",
        "rule": "API Colombia department records must contain name and id",
        "threshold": "100%",
        "severity": "CRITICAL",
        "expectation": "expect_column_values_to_not_be_null",
        "requirement": "R6, R7",
        "pipeline_action": "Block integration branch"
    },
    "DQ-07": {
        "layer": "raw_api_departments",
        "dimension": "Validity",
        "attribute": "population,surface",
        "rule": "Official population and surface from API must be positive (> 0)",
        "threshold": "100%",
        "severity": "CRITICAL",
        "expectation": "expect_column_values_to_be_between",
        "requirement": "R6, R7",
        "pipeline_action": "Block integration branch"
    },
    "DQ-08": {
        "layer": "integration_gate",
        "dimension": "Referential Integrity / Integration",
        "attribute": "department_code_match",
        "rule": "At least 95% of departments in primary data must resolve to API Colombia records",
        "threshold": ">= 95.0%",
        "severity": "CRITICAL",
        "expectation": "expect_integration_key_match_rate",
        "requirement": "R6, R7",
        "pipeline_action": "Block prepared loading"
    },
    "DQ-09": {
        "layer": "prepared_fact_affiliates",
        "dimension": "Uniqueness / Grain",
        "attribute": "sk_time,sk_geografia,sk_regime",
        "rule": "Declared grain of fact_affiliates (time, geo, regime) must be unique with zero duplicate rows",
        "threshold": "100%",
        "severity": "CRITICAL",
        "expectation": "expect_compound_columns_to_be_unique",
        "requirement": "R1, R2, R5, R6",
        "pipeline_action": "Prevent Data Warehouse load"
    },
    "DQ-10": {
        "layer": "prepared_fact_capacity",
        "dimension": "Uniqueness / Grain",
        "attribute": "sk_time,sk_facility,sk_capacity_type,sk_geografia",
        "rule": "Declared grain of fact_facility_capacity must be unique with zero duplicates",
        "threshold": "100%",
        "severity": "CRITICAL",
        "expectation": "expect_compound_columns_to_be_unique",
        "requirement": "R3, R4, R7",
        "pipeline_action": "Prevent Data Warehouse load"
    },
    "DQ-11": {
        "layer": "prepared_facts",
        "dimension": "Referential Integrity",
        "attribute": "foreign_keys",
        "rule": "Every foreign key in fact tables must exist in the respective dimension (zero orphans)",
        "threshold": "100%",
        "severity": "CRITICAL",
        "expectation": "expect_foreign_keys_to_exist_in_dimensions",
        "requirement": "All (R1-R7)",
        "pipeline_action": "Prevent Data Warehouse load"
    },
    "DQ-12": {
        "layer": "prepared_facts",
        "dimension": "Consistency / Reconciliation",
        "attribute": "numero_afiliados_sum",
        "rule": "Sum of numero_afiliados in fact table must reconcile with raw person totals within 0.01% tolerance",
        "threshold": "Tolerance < 0.01%",
        "severity": "CRITICAL",
        "expectation": "expect_sum_reconciliation",
        "requirement": "R1, R6",
        "pipeline_action": "Prevent Data Warehouse load"
    }
}
