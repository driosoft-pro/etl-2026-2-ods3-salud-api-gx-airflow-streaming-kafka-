import os
import json
import logging
import unicodedata
from datetime import datetime, timezone
import pandas as pd
import requests

logger = logging.getLogger(__name__)

API_BASE = os.getenv('API_COLOMBIA_BASE_URL', 'https://api-colombia.com/api/v1')
DATA_RAW_DIR = os.getenv('DATA_RAW_DIR', os.path.join(os.path.dirname(__file__), '..', 'data', 'raw'))

DEPT_CACHE_FILE = os.path.join(DATA_RAW_DIR, 'api_departments_raw.json')
REGION_CACHE_FILE = os.path.join(DATA_RAW_DIR, 'api_regions_raw.json')

def _normalize(text: str) -> str:
    if not isinstance(text, str):
        return text
    import re
    nfkd = unicodedata.normalize('NFKD', text)
    result = ''.join(c for c in nfkd if not unicodedata.combining(c))
    result = re.sub(r'[.,]', '', result)
    return result.upper().strip()

def fetch_departments_from_api(force_cache: bool = False) -> tuple[pd.DataFrame, dict]:
    """Fetch department data from API Colombia or cache, returning DataFrame and metadata."""
    url = f"{API_BASE}/Department"
    data = None
    source = "live_api"
    os.makedirs(DATA_RAW_DIR, exist_ok=True)

    if not force_cache:
        try:
            logger.info(f"Connecting to API Colombia at {url}...")
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            with open(DEPT_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved API Colombia departments to cache: {DEPT_CACHE_FILE}")
        except Exception as e:
            logger.warning(f"Live API call to {url} failed: {e}. Falling back to cached raw file...")
            source = "fallback_cache"

    if data is None:
        if os.path.exists(DEPT_CACHE_FILE):
            logger.info(f"Loading departments from raw cache: {DEPT_CACHE_FILE}")
            with open(DEPT_CACHE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            source = "local_cache"
        else:
            logger.error("No API response and no local cache available for departments")
            return pd.DataFrame(), {"status": "failed", "source": "none", "records": 0}

    records = []
    for dept in data:
        capital = dept.get('cityCapital', {})
        capital_name = capital.get('name', '') if isinstance(capital, dict) else ''
        records.append({
            'department_name_raw': dept.get('name', ''),
            'department_name': _normalize(dept.get('name', '')),
            'api_id': dept.get('id', None),
            'capital': capital_name,
            'surface': dept.get('surface', None),
            'population': dept.get('population', None),
            'municipalities_count': dept.get('municipalities', None),
            'phone_prefix': dept.get('phonePrefix', ''),
            'region_id': dept.get('regionId', None),
        })

    df = pd.DataFrame(records)
    metadata = {
        "status": "success",
        "source": source,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "records": len(df),
        "endpoint": url,
    }
    logger.info(f"Fetched {len(df)} departments (source={source})")
    return df, metadata

def fetch_regions_from_api(force_cache: bool = False) -> tuple[pd.DataFrame, dict]:
    """Fetch region data from API Colombia or cache, returning DataFrame and metadata."""
    url = f"{API_BASE}/Region"
    data = None
    source = "live_api"
    os.makedirs(DATA_RAW_DIR, exist_ok=True)

    if not force_cache:
        try:
            logger.info(f"Connecting to API Colombia at {url}...")
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            with open(REGION_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved API Colombia regions to cache: {REGION_CACHE_FILE}")
        except Exception as e:
            logger.warning(f"Live API call to {url} failed: {e}. Falling back to cached raw file...")
            source = "fallback_cache"

    if data is None:
        if os.path.exists(REGION_CACHE_FILE):
            logger.info(f"Loading regions from raw cache: {REGION_CACHE_FILE}")
            with open(REGION_CACHE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            source = "local_cache"
        else:
            logger.error("No API response and no local cache available for regions")
            return pd.DataFrame(), {"status": "failed", "source": "none", "records": 0}

    records = []
    for reg in data:
        records.append({
            'region_id': reg.get('id', None),
            'region_name': reg.get('name', ''),
        })

    df = pd.DataFrame(records)
    metadata = {
        "status": "success",
        "source": source,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "records": len(df),
        "endpoint": url,
    }
    logger.info(f"Fetched {len(df)} regions (source={source})")
    return df, metadata

def extract_api_all(force_cache: bool = False) -> dict:
    """Extract both departments and regions from API Colombia with metadata."""
    dept_df, dept_meta = fetch_departments_from_api(force_cache=force_cache)
    reg_df, reg_meta = fetch_regions_from_api(force_cache=force_cache)
    return {
        'departments': dept_df,
        'regions': reg_df,
        'metadata': {
            'departments': dept_meta,
            'regions': reg_meta,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    }
