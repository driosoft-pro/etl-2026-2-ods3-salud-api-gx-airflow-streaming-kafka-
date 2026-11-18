import pytest
import psycopg2
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.mark.unit
class TestLoadFunctions:
    
    def test_import_load(self):
        from src.load import load_dimension, load_fact, load_all
        assert callable(load_dimension)
        assert callable(load_fact)
        assert callable(load_all)
    
    def test_import_config(self):
        from src.config import DB_CONFIG
        assert 'host' in DB_CONFIG
        assert 'port' in DB_CONFIG
        assert 'database' in DB_CONFIG
        assert 'user' in DB_CONFIG
        assert 'password' in DB_CONFIG
    
    def test_config_port(self):
        from src.config import DB_CONFIG
        assert DB_CONFIG['port'] == 5432
    
    def test_config_database(self):
        from src.config import DB_CONFIG
        assert DB_CONFIG['database'] == 'salud_colombia'


@pytest.mark.unit
class TestAuditModule:

    def test_import_audit(self):
        from src import audit
        assert callable(audit.ensure_run)
        assert callable(audit.log_step)
        assert callable(audit.log_dq_results)
        assert callable(audit.finish_run)
        assert callable(audit.start_batch)
        assert callable(audit.finish_batch)

    def test_status_constants(self):
        from src import audit
        assert audit.STATUS_SUCCESS == 'SUCCESS'
        assert audit.STATUS_FAILED == 'FAILED'
        assert audit.STATUS_BLOCKED == 'BLOCKED_QUALITY'


@pytest.fixture
def db_connection():
    """Only an unavailable connection skips; SQL/schema/assertion failures fail."""
    from src.config import DB_CONFIG
    try:
        connection = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    try:
        yield connection
    finally:
        connection.close()


@pytest.mark.integration
class TestAuditTables:

    def test_audit_tables_exist(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'audit'
            """)
            tables = {row[0] for row in cursor.fetchall()}
        expected = {'etl_run', 'etl_run_step', 'dq_check_result', 'etl_batch_log'}
        missing = expected - tables
        assert not missing, f"Missing audit tables: {missing}"

    def test_batch_id_column_in_truth_tables(self, db_connection):
        """Las 5 dimensiones y 2 hechos deben tener la columna batch_id."""
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND column_name = 'batch_id'
            """)
            tables = {row[0] for row in cursor.fetchall()}
        expected = {
            'dim_time', 'dim_geografia', 'dim_regime', 'dim_facility',
            'dim_capacity_type', 'fact_affiliates', 'fact_facility_capacity',
        }
        missing = expected - tables
        assert not missing, f"Tables missing batch_id column: {missing}"

    def test_airflow_metadata_not_in_dw(self, db_connection):
        """La base del DW NO debe contener tablas de metadatos de Airflow."""
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ('dag_run', 'task_instance', 'xcom', 'job')
            """)
            leaked = {row[0] for row in cursor.fetchall()}
        assert not leaked, f"Airflow metadata leaked into DW database: {leaked}"


@pytest.mark.integration
class TestDBConnection:

    def test_connection_possible(self, db_connection):
        assert not db_connection.closed

    def test_tables_exist(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            tables = {row[0] for row in cursor.fetchall()}
        expected = {
            'dim_time', 'dim_geografia', 'dim_regime', 'dim_facility',
            'dim_capacity_type', 'fact_affiliates', 'fact_facility_capacity',
        }
        missing = expected - tables
        assert not missing, f"Missing tables: {missing}"
