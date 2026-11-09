import hashlib
import pandas as pd
import numpy as np
from .config import (
    MONTHS, REGION_MAP, REGIME_MAP, DEPT_NORMALIZE,
    DEPT_DANE_CODES, DISTRICT_TO_DEPT, normalize_text
)
from .api_extract import fetch_departments_from_api, fetch_regions_from_api
import logging

logger = logging.getLogger(__name__)

def clean_affiliates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning affiliates dataset...")
    df = df.copy()
    
    df['num_persons'] = df['num_persons'].str.replace('.', '', regex=False)
    df['num_persons'] = pd.to_numeric(df['num_persons'], errors='coerce').fillna(0).astype(int)
    
    df['year'] = df['year'].astype(int)
    df['month'] = df['month'].astype(int)
    
    df['department'] = df['department'].apply(normalize_text)
    df['municipality'] = df['municipality'].apply(normalize_text)
    
    df['department'] = df['department'].map(lambda x: DEPT_NORMALIZE.get(x, x))
    df['region'] = df['department'].map(REGION_MAP).fillna('Sin Region')
    df['department_code'] = df['department'].map(DEPT_DANE_CODES)
    df.loc[df['department'] == 'SIN DEPARTAMENTO', 'municipality_code'] = '00000'
    df = df.dropna(subset=['department'])
    
    df = df[df['num_persons'] > 0].copy()
    
    df['quarter'] = (df['month'] - 1) // 3 + 1
    df['periodo_codigo'] = df.apply(lambda r: f"{r['year']}-Q{r['quarter']}", axis=1)
    df['data_source'] = 'affiliates'
    df['data_snapshot_date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')
    
    logger.info(f"Records after cleaning: {len(df)}")
    return df

def clean_facilities(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning facilities dataset...")
    df = df.copy()
    
    care_level_raw = pd.to_numeric(df['care_level'], errors='coerce')
    df['care_level'] = care_level_raw.astype(object).where(care_level_raw.notna(), other=None)
    df['installed_capacity'] = pd.to_numeric(df['installed_capacity'], errors='coerce').fillna(0).astype(int)
    
    df['nit'] = df['nit'].str.replace(',', '', regex=False)
    df['phone'] = df['phone'].str.extract(r'(\d+)')[0]
    
    df['department'] = df['department'].apply(normalize_text)
    df['municipality'] = df['municipality'].apply(normalize_text)
    
    df['department'] = df['department'].map(lambda x: DISTRICT_TO_DEPT.get(x, x))
    df['department'] = df['department'].map(lambda x: DEPT_NORMALIZE.get(x, x))
    df = df.dropna(subset=['department'])
    
    df = df.dropna(subset=['provider_code', 'provider_name'])
    
    df['data_source'] = 'facilities'
    df['data_snapshot_date'] = pd.to_datetime('2022-11-01')
    
    logger.info(f"Records after cleaning: {len(df)}")
    return df

def build_dim_time(df_affiliates: pd.DataFrame, df_facilities: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building time dimension...")
    
    time_records = []
    
    aff_periods = df_affiliates[['year', 'quarter']].drop_duplicates()
    for _, row in aff_periods.iterrows():
        time_records.append({
            'year': int(row['year']),
            'quarter': int(row['quarter']),
            'data_source': 'affiliates'
        })
    
    time_records.append({
        'year': 2022,
        'quarter': 4,
        'data_source': 'facilities'
    })
    
    dim_time = pd.DataFrame(time_records).drop_duplicates(subset=['year', 'quarter']).reset_index(drop=True)
    
    quarter_month_map = {1: 1, 2: 4, 3: 7, 4: 10}
    dim_time['month'] = dim_time['quarter'].map(quarter_month_map)
    dim_time['month_name'] = dim_time['month'].map(MONTHS)
    dim_time['semester'] = np.where(dim_time['quarter'] <= 2, 1, 2)
    dim_time['periodo_codigo'] = dim_time.apply(
        lambda r: f"{int(r['year'])}-Q{int(r['quarter'])}", axis=1
    )
    dim_time['full_date'] = pd.to_datetime(
        dim_time['year'].astype(str) + '-' + dim_time['month'].astype(str) + '-01'
    )
    dim_time = dim_time.drop(columns=['data_source'])
    
    dim_time = dim_time.sort_values(['year', 'quarter']).reset_index(drop=True)
    dim_time['sk_time'] = dim_time.index + 1
    
    logger.info(f"Time dimension records: {len(dim_time)}")
    return dim_time

def build_dim_geografia(df_affiliates: pd.DataFrame, df_facilities: pd.DataFrame,
                        api_departments: pd.DataFrame = None, api_regions: pd.DataFrame = None) -> pd.DataFrame:
    """Build unified geography dimension with API Colombia enrichment."""
    logger.info("Building unified geography dimension (with API enrichment)...")
    
    # --- Step 1: Build base geography from CSV data ---
    geo_aff = df_affiliates[['municipality_code', 'municipality', 'department_code',
                              'department', 'region']].drop_duplicates()
    geo_aff.columns = ['codigo_dane_municipio', 'municipio', 'codigo_dane_depto',
                        'departamento', 'region']
    
    dept_map = df_affiliates.drop_duplicates('department')[['department', 'department_code']].set_index('department')['department_code'].to_dict()
    
    geo_fac = df_facilities[['municipality', 'department']].drop_duplicates()
    geo_fac['codigo_dane_depto'] = geo_fac['department'].map(dept_map)
    geo_fac = geo_fac.dropna(subset=['codigo_dane_depto'])
    geo_fac['codigo_dane_municipio'] = geo_fac.apply(
        lambda r: f"{r['codigo_dane_depto']}{int.from_bytes(hashlib.md5(r['municipality'].encode()).digest()[:4], 'big') % 10000:04d}", axis=1
    )
    geo_fac['region'] = geo_fac['department'].map(REGION_MAP).fillna('Sin Region')
    geo_fac = geo_fac[['codigo_dane_municipio', 'municipality', 'codigo_dane_depto',
                        'department', 'region']]
    geo_fac.columns = ['codigo_dane_municipio', 'municipio', 'codigo_dane_depto',
                        'departamento', 'region']
    
    dim_geo = pd.concat([geo_aff, geo_fac]).drop_duplicates(
        subset=['codigo_dane_municipio']
    ).reset_index(drop=True)
    
    # --- Step 2: Enrich with API Colombia data ---
    if api_departments is not None:
        api_df = api_departments.copy()
    else:
        api_df, _ = fetch_departments_from_api()
    
    # Initialize API columns with defaults
    dim_geo['capital'] = ''
    dim_geo['surface'] = None
    dim_geo['population'] = None
    dim_geo['municipalities_count'] = None
    dim_geo['phone_prefix'] = ''
    dim_geo['region_api'] = ''
    
    if not api_df.empty:
        if api_regions is not None:
            regions_df = api_regions.copy()
        else:
            regions_df, _ = fetch_regions_from_api()
        region_map = {}
        if not regions_df.empty:
            region_map = regions_df.set_index('region_id')['region_name'].to_dict()
        
        # Map API department names to DANE codes using our normalized names
        dept_name_to_code = dim_geo.drop_duplicates('departamento').set_index('departamento')['codigo_dane_depto'].to_dict()
        
        api_df['department_code'] = api_df['department_name'].map(
            lambda n: dept_name_to_code.get(n, None)
        )
        
        # Handle special aliases
        API_ALIAS = {
            'BOGOTA': 'BOGOTA D.C.',
            'BOGOTA D.C.': 'BOGOTA D.C.',
            'SAN ANDRES Y PROVIDENCIA': 'SAN ANDRES',
        }
        for api_name, dept_name in API_ALIAS.items():
            mask = api_df['department_name'] == api_name
            if mask.any() and dept_name in dept_name_to_code:
                api_df.loc[mask, 'department_code'] = dept_name_to_code[dept_name]
        
        api_df['region_api'] = api_df['region_id'].map(region_map).fillna('')
        
        matched = api_df['department_code'].notna().sum()
        logger.info(f"API departments matched to DANE codes: {matched}/{len(api_df)}")
        
        # Build lookup from DANE dept code to API data
        api_lookup = api_df.dropna(subset=['department_code']).set_index('department_code')
        
        # Enrich each row in dim_geo by department code
        for idx, row in dim_geo.iterrows():
            dept_code = row['codigo_dane_depto']
            if dept_code in api_lookup.index:
                api_row = api_lookup.loc[dept_code]
                # Get first match if multiple
                if isinstance(api_row, pd.DataFrame):
                    api_row = api_row.iloc[0]
                dim_geo.at[idx, 'capital'] = api_row.get('capital', '')
                dim_geo.at[idx, 'surface'] = api_row.get('surface', None)
                dim_geo.at[idx, 'population'] = api_row.get('population', None)
                dim_geo.at[idx, 'municipalities_count'] = api_row.get('municipalities_count', None)
                dim_geo.at[idx, 'phone_prefix'] = api_row.get('phone_prefix', '')
                dim_geo.at[idx, 'region_api'] = api_row.get('region_api', '')
    
    dim_geo['sk_geografia'] = dim_geo.index + 1
    
    logger.info(f"Geography dimension records: {len(dim_geo)}")
    return dim_geo

def build_dim_regime(df_affiliates: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building regime dimension...")
    
    dim_reg = df_affiliates[['regime_id']].drop_duplicates().reset_index(drop=True)
    dim_reg.columns = ['code']
    dim_reg['description'] = dim_reg['code'].map(REGIME_MAP).fillna('UNKNOWN')
    dim_reg['sk_regime'] = dim_reg.index + 1
    
    logger.info(f"Regime dimension records: {len(dim_reg)}")
    return dim_reg

def build_dim_facility(df_facilities: pd.DataFrame, dim_geo: pd.DataFrame) -> pd.DataFrame:
    """Build facility dimension with sk_geografia FK (unified geography)."""
    logger.info("Building facility dimension...")
    
    dim_fac = df_facilities[['provider_code', 'provider_name', 'nit', 'nature',
                              'care_level', 'manager', 'address', 'email', 
                              'phone', 'municipality', 'department']].drop_duplicates().reset_index(drop=True)
    
    dim_fac.columns = ['provider_code', 'name', 'nit', 'nature',
                        'care_level', 'manager', 'address', 'email',
                        'phone', 'municipality_name', 'dept_name']
    
    # Map to sk_geografia using (municipio, departamento)
    geo_key = dim_geo.set_index(['municipio', 'departamento'])['sk_geografia'].to_dict()
    dim_fac['sk_geografia'] = dim_fac.apply(
        lambda r: geo_key.get((r['municipality_name'], r['dept_name'])), axis=1
    )
    dim_fac = dim_fac.dropna(subset=['sk_geografia'])
    dim_fac['sk_geografia'] = dim_fac['sk_geografia'].astype(int)
    
    dim_fac = dim_fac.drop(columns=['municipality_name', 'dept_name'])
    dim_fac = dim_fac.drop_duplicates(subset=['provider_code', 'sk_geografia']).reset_index(drop=True)
    dim_fac['sk_facility'] = dim_fac.index + 1
    
    logger.info(f"Facility dimension records: {len(dim_fac)}")
    return dim_fac

def build_dim_capacity_type(df_facilities: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building capacity type dimension...")
    
    dim_ct = df_facilities[['capacity_group', 'capacity_description']].drop_duplicates().reset_index(drop=True)
    dim_ct.columns = ['group', 'description']
    dim_ct['sk_capacity_type'] = dim_ct.index + 1
    
    logger.info(f"Capacity type dimension records: {len(dim_ct)}")
    return dim_ct

def build_fact_affiliates(df_affiliates: pd.DataFrame, dim_time: pd.DataFrame,
                          dim_geo: pd.DataFrame, dim_reg: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building affiliates fact table (quarterly granularity)...")
    
    time_map = dim_time.set_index(['year', 'quarter'])['sk_time'].to_dict()
    geo_map = dim_geo.set_index('codigo_dane_municipio')['sk_geografia'].to_dict()
    reg_map = dim_reg.set_index('code')['sk_regime'].to_dict()
    
    fact = df_affiliates.copy()
    fact['quarter'] = (fact['month'] - 1) // 3 + 1
    
    fact['sk_time'] = fact.apply(lambda r: time_map.get((r['year'], r['quarter'])), axis=1)
    fact['sk_geografia'] = fact['municipality_code'].map(geo_map)
    fact['sk_regime'] = fact['regime_id'].map(reg_map)
    
    fact = fact.dropna(subset=['sk_time', 'sk_geografia', 'sk_regime'])
    fact['sk_time'] = fact['sk_time'].astype(int)
    fact['sk_geografia'] = fact['sk_geografia'].astype(int)
    fact['sk_regime'] = fact['sk_regime'].astype(int)
    
    fact_agg = fact.groupby(['sk_time', 'sk_geografia', 'sk_regime']).agg(
        numero_afiliados=('num_persons', 'sum')
    ).reset_index()
    
    fact_agg['sk_affiliate'] = fact_agg.index + 1
    
    logger.info(f"Affiliates fact records (quarterly): {len(fact_agg)}")
    return fact_agg

def build_fact_facility_capacity(df_facilities: pd.DataFrame, dim_time: pd.DataFrame,
                                  dim_fac: pd.DataFrame, dim_ct: pd.DataFrame,
                                  dim_geo: pd.DataFrame) -> pd.DataFrame:
    """Build facility capacity fact table using unified geography."""
    logger.info("Building facility capacity fact table...")
    
    time_key = (2022, 4)
    sk_time_val = dim_time.set_index(['year', 'quarter']).loc[time_key, 'sk_time']
    if isinstance(sk_time_val, pd.Series):
        sk_time_val = sk_time_val.iloc[0]
    
    fact = df_facilities[['provider_code', 'capacity_group', 'capacity_description',
                           'installed_capacity', 'municipality', 'department']].copy()
    
    # Map to sk_geografia using (municipio, departamento)
    geo_key = dim_geo.set_index(['municipio', 'departamento'])['sk_geografia'].to_dict()
    fact['sk_geografia'] = fact.apply(
        lambda r: geo_key.get((r['municipality'], r['department'])), axis=1
    )
    
    # Map to sk_facility using (provider_code, sk_geografia)
    fac_key = dim_fac.set_index(['provider_code', 'sk_geografia'])['sk_facility'].to_dict()
    fact['sk_facility'] = fact.apply(
        lambda r: fac_key.get((r['provider_code'], r['sk_geografia'])), axis=1
    )
    
    ct_key = dim_ct.set_index(['group', 'description'])['sk_capacity_type'].to_dict()
    fact['sk_capacity_type'] = fact.apply(
        lambda r: ct_key.get((r['capacity_group'], r['capacity_description'])), axis=1
    )
    
    fact['sk_time'] = sk_time_val
    
    fact = fact.dropna(subset=['sk_time', 'sk_facility', 'sk_capacity_type', 'sk_geografia'])
    fact['sk_time'] = fact['sk_time'].astype(int)
    fact['sk_facility'] = fact['sk_facility'].astype(int)
    fact['sk_capacity_type'] = fact['sk_capacity_type'].astype(int)
    fact['sk_geografia'] = fact['sk_geografia'].astype(int)
    
    fact = fact.groupby(['sk_time', 'sk_facility', 'sk_capacity_type', 'sk_geografia']).agg(
        capacity_amount=('installed_capacity', 'sum')
    ).reset_index()
    
    fact['sk_capacity'] = fact.index + 1
    
    logger.info(f"Facility capacity fact records: {len(fact)}")
    return fact
