-- =====================================================
-- Bootstrap: bases de datos separadas (se ejecuta SOLO en
-- el primer arranque del volumen de PostgreSQL, vía
-- /docker-entrypoint-initdb.d).
--
-- salud_colombia : Data Warehouse dimensional (público + audit)
-- airflow_meta   : metadatos exclusivos de Apache Airflow
--                  (~50 tablas internas: dag_run, task_instance, xcom...)
--
-- Separarlas garantiza que el DW contenga ÚNICAMENTE objetos del
-- modelo dimensional y que PostgREST no exponga tablas de Airflow.
-- =====================================================

SELECT 'CREATE DATABASE airflow_meta OWNER etl_user'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow_meta') \gexec
