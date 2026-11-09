import pytest
import pandas as pd
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')

AFFILIATES_FILE = 'affiliates_by_department_municipality_regime_20260906.csv'
FACILITIES_FILE = 'healthcare_facilities_by_level_capacity_20260906.csv'

@pytest.mark.data
class TestRawDataAffiliates:
    
    @pytest.fixture(autouse=True)
    def setup(self, df_affiliates_raw):
        self.df = df_affiliates_raw
    
    def test_file_exists(self):
        path = os.path.join(DATA_DIR, AFFILIATES_FILE)
        assert os.path.exists(path), f"File not found: {AFFILIATES_FILE}"
    
    def test_file_not_empty(self):
        assert len(self.df) > 0, "Affiliates dataset is empty"
    
    def test_expected_columns(self):
        expected_columns = ['CodDepto', 'Departamento', 'CodMunicipio', 
                            'Municipio', 'IDRegimen', 'Año', 'Mes', 'NumPersonas']
        actual_columns = list(self.df.columns)
        assert expected_columns == actual_columns, \
            f"Expected columns: {expected_columns}\nActual columns: {actual_columns}"
    
    def test_not_all_null(self):
        for col in self.df.columns:
            assert not self.df[col].isna().all(), f"Column '{col}' is completely empty"
    
    def test_minimum_records(self):
        assert len(self.df) >= 100, \
            f"Expected at least 100 records, found {len(self.df)}"
    
    def test_department_not_null(self):
        nulls = self.df['Departamento'].isna().sum()
        assert nulls == 0, f"There are {nulls} null values in Department"
    
    def test_municipality_not_null(self):
        nulls = self.df['Municipio'].isna().sum()
        assert nulls == 0, f"There are {nulls} null values in Municipality"
    
    def test_valid_regime(self):
        regimes = self.df['IDRegimen'].dropna().unique()
        valid_regimes = {'S', 'E', 'C', 'I'}
        invalid_regimes = set(regimes) - valid_regimes
        assert len(invalid_regimes) == 0, \
            f"Invalid regimes found: {invalid_regimes}"
    
    def test_year_format(self):
        years = self.df['Año'].dropna().unique()
        for year in years:
            assert year.isdigit(), f"Year with invalid format: {year}"
            assert 2000 <= int(year) <= 2030, f"Year out of range: {year}"
    
    def test_month_range(self):
        months = self.df['Mes'].dropna().unique()
        for month in months:
            assert month.isdigit(), f"Month with invalid format: {month}"
            assert 1 <= int(month) <= 12, f"Month out of range: {month}"


@pytest.mark.data
class TestRawDataFacilities:
    
    @pytest.fixture(autouse=True)
    def setup(self, df_facilities_raw):
        self.df = df_facilities_raw
    
    def test_file_exists(self):
        path = os.path.join(DATA_DIR, FACILITIES_FILE)
        assert os.path.exists(path), f"File not found: {FACILITIES_FILE}"
    
    def test_file_not_empty(self):
        assert len(self.df) > 0, "Facilities dataset is empty"
    
    def test_expected_columns(self):
        expected_columns = ['Departamento', 'Municipio', 'Código prestador', 
                            'Nombre prestador', 'nit IPS ', 'num digito_verificion',
                            'naturaleza', 'num nivel atencion', 'Código sede',
                            'Número sede', 'nom sede IPS', 'Gerente', 'Dirección',
                            'Email', 'Teléfono', 'nom grupo capacidad ',
                            'nom descripcion capacidad ', 'num cantidad capacidad instalada',
                            'Fecha Corte', 'Fuente']
        actual_columns = list(self.df.columns)
        assert expected_columns == actual_columns, \
            f"Expected columns: {expected_columns}\nActual columns: {actual_columns}"
    
    def test_minimum_records(self):
        assert len(self.df) >= 1000, \
            f"Expected at least 1000 records, found {len(self.df)}"
    
    def test_valid_nature(self):
        natures = self.df['naturaleza'].dropna().unique()
        valid_natures = {'Pública', 'Privada', 'Mixta'}
        for nat in natures:
            assert nat in valid_natures, f"Invalid nature: {nat}"
    
    def test_care_level_range(self):
        levels = self.df['num nivel atencion'].dropna().unique()
        for level in levels:
            if level.isdigit():
                assert 1 <= int(level) <= 5, f"Care level out of range: {level}"