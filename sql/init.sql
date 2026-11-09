-- =====================================================
-- DIMENSIONAL SCHEMA - HEALTH COLOMBIA
-- ODS 3 - Salud y Bienestar / Meta 3.8
--
-- METODOLOGÍA: Kimball Dimensional Modeling
--   1. Declaración de grano antes de diseñar (ver sección 2 README)
--   2. Dimensiones CONFORMED compartidas entre hechos (dim_geografia, dim_time)
--   3. Hechos por separado según su grano (stock vs flujo)
--   4. Convención de nombres: dim_* / fact_* / v_* / audit.*
--   5. Evolución ADITIVA: agregar columnas/tablas nuevas sin romper las
--      existentes. NUNCA se renombran ni eliminan columnas publicadas.
--
-- ESQUEMAS DE LA BASE 'salud_colombia':
--   public  -> modelo dimensional (dimensiones, hechos, vistas analíticas)
--   audit   -> trazabilidad del ETL (etl_run, etl_run_step, dq_check_result,
--              etl_batch_log)
--   (los ~50 metadatos de Apache Airflow viven en su PROPIA base airflow_meta)
--
-- Two fact tables with different granularities:
--   fact_affiliates: enrollment by geography/regime/quarter (Q2 2022 snapshot)
--   fact_facility_capacity: infrastructure by facility/type/geography (Q4 2022 snapshot)
--
-- Normalized schema (v3):
--   - dim_geografia is the SINGLE conformed geography dimension.
--     Contains DANE codes, municipality, department, region,
--     AND API Colombia enrichment (capital, surface, population).
--   - No redundant geographic tables.
--   - Both facts reference dim_geografia directly.
--
-- NOTA IDEMPOTENCIA: las tablas audit.* usan CREATE TABLE IF NOT EXISTS
-- y jamás se eliminan aquí, de modo que un reset del modelo dimensional
-- NO destruye el historial de corridas del ETL.
-- =====================================================

-- =====================================================
-- DROP EXISTING DIMENSIONAL OBJECTS (idempotent reset)
-- El esquema audit NO se elimina: conserva la trazabilidad.
-- =====================================================

DROP TABLE IF EXISTS fact_facility_capacity CASCADE;
DROP TABLE IF EXISTS fact_affiliates CASCADE;
DROP TABLE IF EXISTS dim_capacity_type CASCADE;
DROP TABLE IF EXISTS dim_facility CASCADE;
DROP TABLE IF EXISTS dim_regime CASCADE;
DROP TABLE IF EXISTS dim_geografia CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP VIEW IF EXISTS v_affiliates_summary CASCADE;
DROP VIEW IF EXISTS v_facility_summary CASCADE;
DROP VIEW IF EXISTS v_r6_demographic_coverage CASCADE;
DROP VIEW IF EXISTS v_r7_territorial_capacity CASCADE;

-- =====================================================
-- AUDIT SCHEMA (ETL operational traceability)
-- =====================================================

CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA audit IS
'Trazabilidad operacional del ETL: corridas, pasos y resultados de reglas de calidad DQ.';

-- -----------------------------------------------------
-- audit.etl_run: una fila por corrida completa del pipeline
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.etl_run (
    run_id          BIGSERIAL PRIMARY KEY,
    run_key         VARCHAR(120) NOT NULL UNIQUE,
    dag_id          VARCHAR(120),
    run_type        VARCHAR(30) NOT NULL,
    trigger_source  VARCHAR(60),
    status          VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    stage_reached   VARCHAR(60),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    duration_seconds NUMERIC(12,3),
    source_counts   JSONB,
    rows_loaded     JSONB,
    dq_rules_passed INTEGER,
    dq_rules_total  INTEGER,
    error_message   TEXT
);

COMMENT ON TABLE audit.etl_run IS
'Una fila por corrida ETL (CLI o Airflow). status: RUNNING | SUCCESS | FAILED | BLOCKED_QUALITY';

CREATE INDEX IF NOT EXISTS idx_etl_run_status ON audit.etl_run(status);
CREATE INDEX IF NOT EXISTS idx_etl_run_started ON audit.etl_run(started_at DESC);

-- -----------------------------------------------------
-- audit.etl_run_step: granularidad de fase/paso dentro de una corrida
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.etl_run_step (
    step_id     BIGSERIAL PRIMARY KEY,
    run_id      BIGINT NOT NULL REFERENCES audit.etl_run(run_id) ON DELETE CASCADE,
    phase       VARCHAR(40) NOT NULL,
    step_name   VARCHAR(80) NOT NULL,
    status      VARCHAR(20) NOT NULL,
    rows_in     BIGINT,
    rows_out    BIGINT,
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    message     TEXT
);

COMMENT ON TABLE audit.etl_run_step IS
'Pasos/fases de cada corrida ETL (EXTRACT, VALIDATE, TRANSFORM, LOAD, EXPORT, VERIFY).';

CREATE INDEX IF NOT EXISTS idx_etl_step_run ON audit.etl_run_step(run_id);

-- -----------------------------------------------------
-- audit.dq_check_result: resultado por regla DQ evaluada
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.dq_check_result (
    result_id     BIGSERIAL PRIMARY KEY,
    run_id        BIGINT NOT NULL REFERENCES audit.etl_run(run_id) ON DELETE CASCADE,
    rule_id       VARCHAR(20) NOT NULL,
    gate          VARCHAR(60),
    layer         VARCHAR(60),
    severity      VARCHAR(20),
    status        VARCHAR(10) NOT NULL,
    observed_value TEXT,
    threshold     VARCHAR(40),
    details       JSONB,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, rule_id)
);

COMMENT ON TABLE audit.dq_check_result IS
'Resultado persistido por regla DQ-01..DQ-12 de cada corrida (status: PASS | FAIL).';

CREATE INDEX IF NOT EXISTS idx_dq_result_run ON audit.dq_check_result(run_id);

-- -----------------------------------------------------
-- audit.etl_batch_log: una fila por lote de carga al DW
-- (batch_id se estampa en cada fila de dimensiones y hechos)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.etl_batch_log (
    batch_id            BIGSERIAL PRIMARY KEY,
    run_id              BIGINT REFERENCES audit.etl_run(run_id),
    dag_id              VARCHAR(120),
    strategy            VARCHAR(40) NOT NULL DEFAULT 'full_replace',
    target_rows_before  JSONB,
    target_rows_after   JSONB,
    status              VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    duration_seconds    NUMERIC(12,3),
    notes               TEXT
);

COMMENT ON TABLE audit.etl_batch_log IS
'Una fila por lote de carga al DW. batch_id se estampa en dim_* y fact_* como procedencia de la fila.';

CREATE INDEX IF NOT EXISTS idx_etl_batch_started ON audit.etl_batch_log(started_at DESC);

-- =====================================================
-- DIMENSIONS
-- =====================================================

-- Time Dimension (Trimestral)
CREATE TABLE dim_time (
    sk_time SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INTEGER NOT NULL,
    semester INTEGER NOT NULL,
    periodo_codigo VARCHAR(10) NOT NULL,
    full_date DATE NOT NULL,
    batch_id BIGINT REFERENCES audit.etl_batch_log(batch_id),
    UNIQUE(year, quarter)
);

COMMENT ON TABLE dim_time IS
'Dimensión de tiempo con grano trimestral (dimensión conforme compartida por ambos hechos).';

-- =====================================================
-- UNIFIED GEOGRAPHY DIMENSION (single conformed dimension)
-- Both fact tables reference this dimension.
-- Contains DANE codes + API Colombia enrichment.
-- =====================================================
CREATE TABLE dim_geografia (
    sk_geografia SERIAL PRIMARY KEY,
    codigo_dane_municipio VARCHAR(10) NOT NULL,
    municipio VARCHAR(150) NOT NULL,
    codigo_dane_depto VARCHAR(5) NOT NULL,
    departamento VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    -- API Colombia enrichment columns
    capital VARCHAR(100),
    surface NUMERIC,
    population BIGINT,
    municipalities_count INTEGER,
    phone_prefix VARCHAR(10),
    region_api VARCHAR(100),
    batch_id BIGINT REFERENCES audit.etl_batch_log(batch_id),
    UNIQUE(codigo_dane_municipio)
);

COMMENT ON TABLE dim_geografia IS
'Dimensión geográfica CONFORMED única (códigos DANE + enriquecimiento API Colombia).';

-- Regime Dimension
CREATE TABLE dim_regime (
    sk_regime SERIAL PRIMARY KEY,
    code VARCHAR(5) NOT NULL UNIQUE,
    description VARCHAR(100) NOT NULL,
    batch_id BIGINT REFERENCES audit.etl_batch_log(batch_id)
);

-- Facility Dimension (Healthcare Institution - IPS)
-- Uses sk_geografia FK (unified geography).
CREATE TABLE dim_facility (
    sk_facility SERIAL PRIMARY KEY,
    provider_code VARCHAR(20) NOT NULL,
    name VARCHAR(300) NOT NULL,
    nit VARCHAR(20),
    nature VARCHAR(20),
    care_level INTEGER,
    manager VARCHAR(200),
    address VARCHAR(300),
    email VARCHAR(200),
    phone VARCHAR(20),
    sk_geografia INTEGER NOT NULL REFERENCES dim_geografia(sk_geografia),
    batch_id BIGINT REFERENCES audit.etl_batch_log(batch_id),
    UNIQUE(provider_code, sk_geografia)
);

-- Capacity Type Dimension
CREATE TABLE dim_capacity_type (
    sk_capacity_type SERIAL PRIMARY KEY,
    "group" VARCHAR(50) NOT NULL,
    description VARCHAR(100) NOT NULL,
    batch_id BIGINT REFERENCES audit.etl_batch_log(batch_id),
    UNIQUE("group", description)
);

-- =====================================================
-- FACT TABLES
-- =====================================================

-- Fact: Affiliates (grain: quarter + geography + regime)
-- Row = total accumulated affiliates for a regime, geography, quarter and year
-- Data snapshot: April 2022 (Q2 2022)
CREATE TABLE fact_affiliates (
    sk_affiliate SERIAL PRIMARY KEY,
    sk_time INTEGER NOT NULL REFERENCES dim_time(sk_time),
    sk_geografia INTEGER NOT NULL REFERENCES dim_geografia(sk_geografia),
    sk_regime INTEGER NOT NULL REFERENCES dim_regime(sk_regime),
    numero_afiliados BIGINT NOT NULL,
    batch_id BIGINT REFERENCES audit.etl_batch_log(batch_id),
    UNIQUE(sk_time, sk_geografia, sk_regime)
);

COMMENT ON TABLE fact_affiliates IS
'Hecho de afiliados SGSSS. Grano: trimestre x municipio (DANE) x régimen. Aditivo por sumas.';

-- Fact: Facility capacity (grain: facility + capacity type + geography + snapshot)
-- Row = installed capacity for a facility, capacity type, and geography at a point in time
-- Data snapshot: November 2022 (Q4 2022)
-- NOT additive across time (infrastructure is a stock, not a flow)
CREATE TABLE fact_facility_capacity (
    sk_capacity SERIAL PRIMARY KEY,
    sk_time INTEGER NOT NULL REFERENCES dim_time(sk_time),
    sk_facility INTEGER NOT NULL REFERENCES dim_facility(sk_facility),
    sk_capacity_type INTEGER NOT NULL REFERENCES dim_capacity_type(sk_capacity_type),
    sk_geografia INTEGER NOT NULL REFERENCES dim_geografia(sk_geografia),
    capacity_amount INTEGER NOT NULL,
    batch_id BIGINT REFERENCES audit.etl_batch_log(batch_id),
    UNIQUE(sk_time, sk_facility, sk_capacity_type)
);

COMMENT ON TABLE fact_facility_capacity IS
'Hecho de capacidad instalada (stock de infraestructura, Q4 2022). NO aditivo en el tiempo.';

-- =====================================================
-- INDEXES FOR OPTIMIZATION
-- =====================================================

CREATE INDEX idx_fact_affiliates_time ON fact_affiliates(sk_time);
CREATE INDEX idx_fact_affiliates_geografia ON fact_affiliates(sk_geografia);
CREATE INDEX idx_fact_affiliates_regime ON fact_affiliates(sk_regime);

CREATE INDEX idx_fact_capacity_time ON fact_facility_capacity(sk_time);
CREATE INDEX idx_fact_capacity_facility ON fact_facility_capacity(sk_facility);
CREATE INDEX idx_fact_capacity_type ON fact_facility_capacity(sk_capacity_type);
CREATE INDEX idx_fact_capacity_geografia ON fact_facility_capacity(sk_geografia);

CREATE INDEX idx_geografia_depto ON dim_geografia(codigo_dane_depto);
CREATE INDEX idx_geografia_region ON dim_geografia(region);
CREATE INDEX idx_geografia_departamento ON dim_geografia(departamento);
CREATE INDEX idx_facility_geografia ON dim_facility(sk_geografia);

-- =====================================================
-- SUMMARY VIEWS
-- =====================================================

-- View: Affiliates summary using unified geography
CREATE VIEW v_affiliates_summary AS
SELECT
    g.departamento,
    g.municipio,
    g.region,
    g.codigo_dane_depto,
    g.codigo_dane_municipio,
    g.capital,
    g.surface,
    g.population,
    r.description AS regime,
    t.year,
    t.quarter,
    t.periodo_codigo,
    f.numero_afiliados
FROM fact_affiliates f
JOIN dim_time t ON f.sk_time = t.sk_time
JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
JOIN dim_regime r ON f.sk_regime = r.sk_regime;

-- View: Facility summary using unified geography
CREATE VIEW v_facility_summary AS
SELECT
    g.departamento AS department,
    g.region AS department_region,
    g.municipio AS municipality,
    g.capital,
    g.surface,
    g.population,
    i.name AS facility,
    i.nature,
    ct."group" AS capacity_group,
    ct.description AS capacity_type,
    t.year,
    c.capacity_amount,
    g.codigo_dane_depto,
    g.codigo_dane_municipio
FROM fact_facility_capacity c
JOIN dim_time t ON c.sk_time = t.sk_time
JOIN dim_facility i ON c.sk_facility = i.sk_facility
JOIN dim_geografia g ON c.sk_geografia = g.sk_geografia
JOIN dim_capacity_type ct ON c.sk_capacity_type = ct.sk_capacity_type;

-- View: R6 - Cobertura demográfica (afiliados SGSSS vs población API Colombia)
CREATE VIEW v_r6_demographic_coverage AS
WITH dept_affiliates AS (
    SELECT
        g.codigo_dane_depto,
        g.departamento,
        g.region,
        COALESCE(MAX(g.population), 0) AS poblacion_oficial_api,
        SUM(f.numero_afiliados) AS total_afiliados_sgsss,
        SUM(CASE WHEN r.description = 'SUBSIDIZED'    THEN f.numero_afiliados ELSE 0 END) AS afiliados_subsidiado,
        SUM(CASE WHEN r.description = 'CONTRIBUTORY'  THEN f.numero_afiliados ELSE 0 END) AS afiliados_contributivo,
        SUM(CASE WHEN r.description NOT IN ('SUBSIDIZED', 'CONTRIBUTORY') THEN f.numero_afiliados ELSE 0 END) AS afiliados_otros
    FROM fact_affiliates f
    JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
    JOIN dim_regime r ON f.sk_regime = r.sk_regime
    WHERE g.codigo_dane_depto <> '00'
      AND g.departamento <> 'SIN DEPARTAMENTO'
    GROUP BY g.codigo_dane_depto, g.departamento, g.region
)
SELECT
    codigo_dane_depto,
    departamento,
    region,
    poblacion_oficial_api,
    total_afiliados_sgsss,
    afiliados_subsidiado,
    afiliados_contributivo,
    afiliados_otros,
    ROUND(total_afiliados_sgsss / NULLIF(poblacion_oficial_api, 0)::numeric * 100, 2) AS cobertura_afiliacion_pct,
    poblacion_oficial_api - total_afiliados_sgsss AS brecha_poblacional,
    CASE
        WHEN poblacion_oficial_api IS NULL OR poblacion_oficial_api = 0 THEN 'Sin Dato Demográfico'
        WHEN (total_afiliados_sgsss / poblacion_oficial_api::numeric) >= 1.05 THEN 'Alta Concentración / Movilidad'
        WHEN (total_afiliados_sgsss / poblacion_oficial_api::numeric) >= 0.90 THEN 'Meta Universal Adecuada'
        ELSE 'Déficit de Cobertura'
    END AS categoria_cobertura
FROM dept_affiliates
ORDER BY cobertura_afiliacion_pct DESC NULLS LAST;

-- View: R7 - Capacidad territorial (camas/1000 hab y densidad por km²)
CREATE VIEW v_r7_territorial_capacity AS
WITH dept_capacity AS (
    SELECT
        g.codigo_dane_depto,
        g.departamento,
        g.region,
        COALESCE(MAX(g.population), 0) AS poblacion_oficial_api,
        COALESCE(MAX(g.surface), 0) AS superficie_km2,
        SUM(c.capacity_amount) AS capacidad_total_instalada,
        SUM(CASE WHEN ct."group" ILIKE '%CAMA%' THEN c.capacity_amount ELSE 0 END) AS total_camas,
        SUM(CASE WHEN ct."group" ILIKE '%SALA%' THEN c.capacity_amount ELSE 0 END) AS total_salas,
        COUNT(DISTINCT c.sk_facility) AS total_ips_habilitadas
    FROM fact_facility_capacity c
    JOIN dim_geografia g ON c.sk_geografia = g.sk_geografia
    JOIN dim_capacity_type ct ON c.sk_capacity_type = ct.sk_capacity_type
    WHERE g.codigo_dane_depto <> '00'
      AND g.departamento <> 'SIN DEPARTAMENTO'
    GROUP BY g.codigo_dane_depto, g.departamento, g.region
)
SELECT
    codigo_dane_depto,
    departamento,
    region,
    poblacion_oficial_api,
    superficie_km2,
    total_ips_habilitadas,
    total_camas,
    total_salas,
    capacidad_total_instalada,
    ROUND(total_camas::numeric / NULLIF(poblacion_oficial_api, 0)::numeric * 1000, 2) AS camas_por_1000_hab,
    ROUND(capacidad_total_instalada::numeric / NULLIF(superficie_km2, 0)::numeric * 1000, 2) AS capacidad_por_1000_km2,
    ROUND(capacidad_total_instalada::numeric / NULLIF(superficie_km2, 0)::numeric, 4) AS densidad_territorial_infraestructura,
    CASE
        WHEN poblacion_oficial_api = 0 OR superficie_km2 = 0 THEN 'Sin Dato Base'
        WHEN (total_camas::numeric / NULLIF(poblacion_oficial_api, 0)::numeric * 1000) >= 2.5 THEN 'Capacidad Hospitalaria Alta'
        WHEN (total_camas::numeric / NULLIF(poblacion_oficial_api, 0)::numeric * 1000) >= 1.5 THEN 'Capacidad Hospitalaria Moderada'
        ELSE 'Déficit Hospitalario Crítico'
    END AS nivel_alerta_hospitalaria
FROM dept_capacity
ORDER BY camas_por_1000_hab DESC NULLS LAST;
