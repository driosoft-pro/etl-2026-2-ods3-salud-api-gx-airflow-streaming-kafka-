import pandas as pd
from .config import DATA_DIR, DATASETS
import logging

logger = logging.getLogger(__name__)

def extract_affiliates() -> pd.DataFrame:
    path = f"{DATA_DIR}/{DATASETS['affiliates']}"
    logger.info(f"Extracting affiliates from: {path}")
    df = pd.read_csv(path, dtype=str)
    df.columns = ['department_code', 'department', 'municipality_code', 'municipality',
                   'regime_id', 'year', 'month', 'num_persons']
    logger.info(f"Records extracted: {len(df)}")
    return df

def extract_facilities() -> pd.DataFrame:
    path = f"{DATA_DIR}/{DATASETS['facilities']}"
    logger.info(f"Extracting facilities from: {path}")
    df = pd.read_csv(path, dtype=str)
    df.columns = ['department', 'municipality', 'provider_code', 'provider_name',
                   'nit', 'verification_digit', 'nature', 'care_level',
                   'branch_code', 'branch_number', 'branch_name', 'manager', 'address',
                   'email', 'phone', 'capacity_group', 'capacity_description',
                   'installed_capacity', 'cutoff_date', 'source']
    logger.info(f"Records extracted: {len(df)}")
    return df

def extract_all() -> dict:
    return {
        'affiliates': extract_affiliates(),
        'facilities': extract_facilities()
    }