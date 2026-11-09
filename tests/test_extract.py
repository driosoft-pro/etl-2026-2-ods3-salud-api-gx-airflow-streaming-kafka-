import pytest
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.extract import extract_affiliates, extract_facilities, extract_all

@pytest.mark.unit
class TestExtractAffiliates:
    
    def test_extracts_dataframe(self):
        df = extract_affiliates()
        assert isinstance(df, pd.DataFrame), "Must return a DataFrame"
    
    def test_not_empty(self):
        df = extract_affiliates()
        assert len(df) > 0, "DataFrame must not be empty"
    
    def test_renamed_columns(self):
        df = extract_affiliates()
        expected_columns = ['department_code', 'department', 'municipality_code', 
                            'municipality', 'regime_id', 'year', 'month', 'num_persons']
        assert list(df.columns) == expected_columns, \
            f"Incorrect columns: {list(df.columns)}"
    
    def test_string_types(self):
        df = extract_affiliates()
        for col in df.columns:
            assert pd.api.types.is_string_dtype(df[col]), f"Column {col} must be string"


@pytest.mark.unit
class TestExtractFacilities:
    
    def test_extracts_dataframe(self):
        df = extract_facilities()
        assert isinstance(df, pd.DataFrame), "Must return a DataFrame"
    
    def test_not_empty(self):
        df = extract_facilities()
        assert len(df) > 0, "DataFrame must not be empty"
    
    def test_renamed_columns(self):
        df = extract_facilities()
        assert 'provider_code' in df.columns, "Missing provider_code column"
        assert 'provider_name' in df.columns, "Missing provider_name column"
        assert 'capacity_group' in df.columns, "Missing capacity_group column"


@pytest.mark.unit
class TestExtractAll:
    
    def test_returns_dictionary(self):
        result = extract_all()
        assert isinstance(result, dict), "Must return a dictionary"
    
    def test_contains_keys(self):
        result = extract_all()
        assert 'affiliates' in result, "Missing 'affiliates' key"
        assert 'facilities' in result, "Missing 'facilities' key"
    
    def test_values_are_dataframes(self):
        result = extract_all()
        assert isinstance(result['affiliates'], pd.DataFrame)
        assert isinstance(result['facilities'], pd.DataFrame)