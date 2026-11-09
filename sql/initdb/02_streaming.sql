-- ============================================================================
-- Streaming schema for Health Colombia (ODS 3) — section 2.6/2.7
-- AUTO-GENERATED from src/streaming/store.py (SCHEMA_SQL). Do not edit by
-- hand; update store.py and regenerate. The application also executes this
-- schema idempotently at startup (store.ensure_schema).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE IF NOT EXISTS streaming.event_log (
    event_id        UUID PRIMARY KEY,
    event_type      VARCHAR(40)  NOT NULL,
    event_key       VARCHAR(160) NOT NULL,
    metric_name     VARCHAR(120) NOT NULL,
    event_timestamp TIMESTAMPTZ  NOT NULL,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    value           NUMERIC,
    previous_value  NUMERIC,
    delta           NUMERIC,
    unit            VARCHAR(40),
    simulation      BOOLEAN      NOT NULL DEFAULT FALSE,
    payload         JSONB        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_log_received   ON streaming.event_log (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_event_log_metric     ON streaming.event_log (metric_name, received_at DESC);

CREATE TABLE IF NOT EXISTS streaming.invalid_events (
    invalid_id  BIGSERIAL PRIMARY KEY,
    reason      TEXT        NOT NULL,
    errors      JSONB,
    raw_payload TEXT,
    message_key TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_invalid_events_received ON streaming.invalid_events (received_at DESC);

CREATE TABLE IF NOT EXISTS streaming.metric_state (
    metric_name     VARCHAR(120) PRIMARY KEY,
    metric_value    NUMERIC,
    previous_value  NUMERIC,
    delta           NUMERIC,
    unit            VARCHAR(40),
    event_count     INTEGER      NOT NULL DEFAULT 0,
    invalid_count   INTEGER      NOT NULL DEFAULT 0,
    last_event_id   UUID,
    last_event_at   TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS streaming.producer_state (
    metric_name   VARCHAR(160) PRIMARY KEY,
    last_value    NUMERIC,
    published_at  TIMESTAMPTZ,
    publish_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS streaming.consumer_heartbeat (
    consumer_id      VARCHAR(60) PRIMARY KEY,
    status           VARCHAR(20)  NOT NULL DEFAULT 'running',
    events_processed BIGINT       NOT NULL DEFAULT 0,
    invalid_events   BIGINT       NOT NULL DEFAULT 0,
    last_heartbeat   TIMESTAMPTZ  NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Seeds the streaming metrics consumed by the real-time dashboard.
INSERT INTO streaming.metric_state (metric_name, metric_value, unit, event_count, invalid_count)
VALUES
    ('events_processed_total',     0, 'events',   0, 0),
    ('invalid_events_total',       0, 'events',   0, 0),
    ('threshold_alerts_total',     0, 'events',   0, 0),
    ('affiliates_latest_total',    0, 'persons',  0, 0),
    ('affiliates_net_delta',       0, 'persons',  0, 0),
    ('capacity_latest_total',      0, 'slots',    0, 0),
    ('events_per_minute',          0, 'events/m', 0, 0)
ON CONFLICT (metric_name) DO NOTHING;
