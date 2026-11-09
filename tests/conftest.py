import pytest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "unit: marca para tests unitarios"
    )
    config.addinivalue_line(
        "markers", "integration: marca para tests de integración"
    )
    config.addinivalue_line(
        "markers", "data: marca para tests de validación de datos"
    )

@pytest.fixture(scope="session")
def df_affiliates_raw():
    import pandas as pd
    from src.config import DATA_DIR, DATASETS
    path = os.path.join(DATA_DIR, DATASETS['affiliates'])
    return pd.read_csv(path, dtype=str)

@pytest.fixture(scope="session")
def df_facilities_raw():
    import pandas as pd
    from src.config import DATA_DIR, DATASETS
    path = os.path.join(DATA_DIR, DATASETS['facilities'])
    return pd.read_csv(path, dtype=str)