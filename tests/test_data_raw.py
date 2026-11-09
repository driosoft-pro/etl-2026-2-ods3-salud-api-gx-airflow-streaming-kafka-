import pytest
import pandas as pd
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')

AFFILIATES_FILE = 'affiliates_by_department_municipality_regime_20260906.csv'
FACILITIES_FILE = 'healthcare_facilities_by_level_capacity_20260906.csv'

@pytest.fixture(scope="session")
def df_affiliates_raw():
    path = os.path.join(DATA_DIR, AFFILIATES_FILE)
    return pd.read_csv(path, dtype=str)

@pytest.fixture(scope="session")
def df_facilities_raw():
    path = os.path.join(DATA_DIR, FACILITIES_FILE)
    return pd.read_csv(path, dtype=str)