"""Tests for Great Expectations Quality Gates and Reliability Mechanisms."""

import pytest
import pandas as pd
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
    CriticalQualityError, RULES
)

@pytest.fixture(scope="module")
def primary_data():
    return extract_all()

@pytest.fixture(scope="module")
def api_data():
    return extract_api_all(force_cache=True)

@pytest.fixture(scope="module")
def prepared_environment(primary_data, api_data):
    df_aff = clean_affiliates(primary_data['affiliates'])
    df_fac = clean_facilities(primary_data['facilities'])
    
    dim_time = build_dim_time(df_aff, df_fac)
    dim_geo = build_dim_geografia(
        df_aff, df_fac,
        api_departments=api_data['departments'],
        api_regions=api_data['regions']
    )
    dim_reg = build_dim_regime(df_aff)
    dim_fac = build_dim_facility(df_fac, dim_geo)
    dim_ct = build_dim_capacity_type(df_fac)
    
    fact_aff = build_fact_affiliates(df_aff, dim_time, dim_geo, dim_reg)
    fact_cap = build_fact_facility_capacity(df_fac, dim_time, dim_fac, dim_ct, dim_geo)
    
    dims = {
        'time': dim_time, 'geografia': dim_geo, 'regime': dim_reg,
        'facility': dim_fac, 'capacity_type': dim_ct
    }
    facts = {'affiliates': fact_aff, 'capacity': fact_cap}
    return dims, facts

class TestQualityGates:

    def test_rules_catalog_completeness(self):
        assert len(RULES) >= 12
        for r_id, rule in RULES.items():
            assert "severity" in rule
            assert rule["severity"] in ("CRITICAL", "WARNING")
            assert "requirement" in rule
            assert "pipeline_action" in rule

    def test_gate1_primary_raw_passes(self, primary_data):
        res = validate_primary_raw(primary_data['affiliates'], primary_data['facilities'])
        assert res["has_critical_failure"] is False
        assert res["passed_rules"] == res["total_rules"]

    def test_gate1_primary_raw_critical_failure(self, primary_data):
        with pytest.raises(CriticalQualityError) as exc_info:
            validate_primary_raw(primary_data['affiliates'], primary_data['facilities'], simulate_failure="DQ-02")
        assert "DQ-02" in str(exc_info.value)

    def test_gate2_api_raw_passes(self, api_data):
        res = validate_api_raw(api_data['departments'], api_data['regions'])
        assert res["has_critical_failure"] is False
        assert res["passed_rules"] == res["total_rules"]

    def test_gate2_api_raw_critical_failure(self, api_data):
        with pytest.raises(CriticalQualityError) as exc_info:
            validate_api_raw(api_data['departments'], api_data['regions'], simulate_failure="DQ-07")
        assert "DQ-07" in str(exc_info.value)

    def test_gate3_prepared_data_passes(self, prepared_environment):
        dims, facts = prepared_environment
        res = validate_prepared_data(dims, facts)
        assert res["has_critical_failure"] is False
        assert res["passed_rules"] == res["total_rules"]

    def test_integration_contract_department_coverage(self, prepared_environment):
        dims, _ = prepared_environment
        dim_geo = dims['geografia']
        # Every department must have population > 0 from API Colombia
        with_pop = dim_geo[dim_geo['population'].notna() & (dim_geo['population'] > 0)]['departamento'].nunique()
        total_dept = dim_geo[dim_geo['departamento'] != 'SIN DEPARTAMENTO']['departamento'].nunique()
        assert with_pop == total_dept
        assert with_pop >= 32
