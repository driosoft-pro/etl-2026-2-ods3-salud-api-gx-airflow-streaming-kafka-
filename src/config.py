import os
import unicodedata

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'salud_colombia'),
    'user': os.getenv('DB_USER', 'etl_user'),
    # Nunca se publican credenciales reales: el valor sale del entorno
    # (.env, gitignored) y el fallback es el placeholder de .env.example.
    'password': (
        os.getenv('DB_PASSWORD')
        or os.getenv('POSTGRES_PASSWORD')
        or 'change_me_2026'
    ),
}

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')

DATASETS = {
    'affiliates': 'affiliates_by_department_municipality_regime_20260906.csv',
    'facilities': 'healthcare_facilities_by_level_capacity_20260906.csv'
}

MONTHS = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}

def normalize_text(text: str) -> str:
    if not isinstance(text, str):
        return text
    text = text.strip()
    nfkd = unicodedata.normalize('NFKD', text)
    without_accents = ''.join(c for c in nfkd if not unicodedata.combining(c))
    return without_accents.upper()

DEPT_NORMALIZE = {
    'ATLANTICO': 'ATLANTICO',
    'BOLIVAR': 'BOLIVAR',
    'BOYACA': 'BOYACA',
    'CAQUETA': 'CAQUETA',
    'CHOCO': 'CHOCO',
    'CORDOBA': 'CORDOBA',
    'GUAINIA': 'GUAINIA',
    'NARINO': 'NARINO',
    'QUINDIO': 'QUINDIO',
    'VAUPES': 'VAUPES',
    'SAN ANDRES': 'SAN ANDRES',
    'SAN ANDRES Y PROVIDENCIA': 'SAN ANDRES',
    'VALLE DEL CAUCA': 'VALLE DEL CAUCA',
    'VALLE': 'VALLE DEL CAUCA',
    'BOGOTA D.C.': 'BOGOTA D.C.',
    'BOGOTA D.C': 'BOGOTA D.C.',
    'BOGOTA': 'BOGOTA D.C.',
    'NO APLICA': 'SIN DEPARTAMENTO',
}

# Distritos Especiales de Salud: REPS los reporta como "departamento",
# pero son ciudades con autoridad sanitaria propia dentro de su departamento real.
DISTRICT_TO_DEPT = {
    'CARTAGENA': 'BOLIVAR',
    'SANTA MARTA': 'MAGDALENA',
    'CALI': 'VALLE DEL CAUCA',
    'BARRANQUILLA': 'ATLANTICO',
    'BUENAVENTURA': 'VALLE DEL CAUCA',
}

REGIME_MAP = {
    'S': 'SUBSIDIZED',
    'C': 'CONTRIBUTORY',
    'E': 'SPECIAL',
    'I': 'INDIVIDUAL',
}

DEPT_DANE_CODES = {
    'ANTIOQUIA': '05',
    'ATLANTICO': '08',
    'BOGOTA D.C.': '11',
    'BOLIVAR': '13',
    'BOYACA': '15',
    'CALDAS': '17',
    'CAQUETA': '18',
    'CAUCA': '19',
    'AMAZONAS': '91',
    'GUAVIARE': '95',
    'CASANARE': '85',
    'CESAR': '20',
    'CHOCO': '27',
    'CORDOBA': '23',
    'CUNDINAMARCA': '25',
    'GUAINIA': '94',
    'HUILA': '41',
    'LA GUAJIRA': '44',
    'MAGDALENA': '47',
    'META': '50',
    'NARINO': '52',
    'NORTE DE SANTANDER': '54',
    'PUTUMAYO': '86',
    'QUINDIO': '63',
    'RISARALDA': '66',
    'SANTANDER': '68',
    'SUCRE': '70',
    'TOLIMA': '73',
    'VALLE DEL CAUCA': '76',
    'VAUPES': '97',
    'VICHADA': '99',
    'SAN ANDRES': '88',
    'ARAUCA': '81',
    'SIN DEPARTAMENTO': '00',
}

REGION_MAP = {
    'AMAZONAS': 'Amazonia',
    'CAQUETA': 'Amazonia',
    'GUAVIARE': 'Amazonia',
    'PUTUMAYO': 'Amazonia',
    'VAUPES': 'Amazonia',
    'VICHADA': 'Amazonia',
    'ARAUCA': 'Orinoquia',
    'CASANARE': 'Orinoquia',
    'GUAINIA': 'Orinoquia',
    'META': 'Orinoquia',
    'NORTE DE SANTANDER': 'Orinoquia',
    'ANTIOQUIA': 'Andina',
    'BOYACA': 'Andina',
    'CALDAS': 'Andina',
    'CUNDINAMARCA': 'Andina',
    'HUILA': 'Andina',
    'QUINDIO': 'Andina',
    'RISARALDA': 'Andina',
    'SANTANDER': 'Andina',
    'TOLIMA': 'Andina',
    'CHOCO': 'Pacifico',
    'VALLE DEL CAUCA': 'Pacifico',
    'NARINO': 'Pacifico',
    'CAUCA': 'Pacifico',
    'ATLANTICO': 'Caribe',
    'BOLIVAR': 'Caribe',
    'CESAR': 'Caribe',
    'CORDOBA': 'Caribe',
    'LA GUAJIRA': 'Caribe',
    'MAGDALENA': 'Caribe',
    'SUCRE': 'Caribe',
    'SAN ANDRES': 'Insular',
    'BOGOTA D.C.': 'Andina',
    'SIN DEPARTAMENTO': 'No geolocalizado',
}
