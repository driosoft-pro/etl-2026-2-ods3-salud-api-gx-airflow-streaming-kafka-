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


@pytest.fixture(autouse=True)
def isolated_quality_evidence(tmp_path, monkeypatch):
    """Regression tests must never replace the delivery's real GX evidence."""
    import src.quality.gx_validator as validator
    monkeypatch.setattr(validator, "EVIDENCE_DIR", str(tmp_path / "gx"))

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

    @pytest.mark.parametrize("invalid", [None, "not-a-number", "NaN", "inf", "-inf", -1, "1,5"])
    def test_installed_capacity_must_be_finite_numeric(self, primary_data, invalid):
        facilities = primary_data['facilities'].copy()
        facilities.loc[facilities.index[0], 'installed_capacity'] = invalid
        with pytest.raises(CriticalQualityError) as error:
            validate_primary_raw(primary_data['affiliates'], facilities)
        assert any(e['rule_id'] == 'DQ-05' and not e['success'] for e in error.value.details['evaluations'])

    def test_capacity_thousands_separator_is_preserved(self, primary_data):
        facilities = primary_data['facilities'].copy()
        facilities.loc[facilities.index[0], 'installed_capacity'] = '1,000'
        result = validate_primary_raw(primary_data['affiliates'], facilities)
        assert result['has_critical_failure'] is False
        cleaned = clean_facilities(facilities)
        assert cleaned.loc[facilities.index[0], 'installed_capacity'] == 1000

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

    @pytest.mark.parametrize("column", ["population", "surface"])
    @pytest.mark.parametrize("value", [None, "not-a-number", 0, -1, float("nan"), float("inf"), -float("inf")])
    def test_api_demographics_reject_missing_invalid_or_non_positive_values(self, api_data, column, value):
        departments = api_data['departments'].copy()
        departments[column] = departments[column].astype(object)
        departments.loc[departments.index[0], column] = value
        with pytest.raises(CriticalQualityError) as exc_info:
            validate_api_raw(departments, api_data['regions'])
        assert exc_info.value.rule_id == "DQ-07"
        rule = next(r for r in exc_info.value.details["evaluations"] if r["rule_id"] == "DQ-07")
        assert rule["details"][f"invalid_{column}_count"] == 1

    def test_missing_gx_blocks_the_gate_instead_of_using_native_success(self, api_data, monkeypatch):
        import src.quality.gx_backend as backend
        monkeypatch.setattr(backend, "GX_AVAILABLE", False)
        monkeypatch.setattr(backend, "GX_IMPORT_ERROR", "test: library unavailable")
        with pytest.raises(CriticalQualityError) as exc_info:
            validate_api_raw(api_data['departments'], api_data['regions'])
        gx = exc_info.value.details["gx"]
        assert gx["available"] is False
        assert gx["suites_run"] == 0
        assert gx["expectations_failed"] == gx["expectations_total"] == 6
        assert "library unavailable" in gx["blocked_reason"]

    def test_partial_gx_execution_cannot_report_an_available_success(self, primary_data, monkeypatch):
        import src.quality.gx_backend as backend
        real_get_context = backend.gx.get_context
        calls = 0

        def fail_second_suite(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("test: suite backend failed")
            return real_get_context(*args, **kwargs)

        monkeypatch.setattr(backend.gx, "get_context", fail_second_suite)
        with pytest.raises(CriticalQualityError) as exc_info:
            validate_primary_raw(primary_data['affiliates'], primary_data['facilities'])
        gx = exc_info.value.details["gx"]
        assert gx["available"] is False
        assert gx["suites_run"] == 2
        assert gx["suites_requested"] == 3
        assert gx["expectations_failed"] == 2
        assert "suite backend failed" in gx["blocked_reason"]

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
