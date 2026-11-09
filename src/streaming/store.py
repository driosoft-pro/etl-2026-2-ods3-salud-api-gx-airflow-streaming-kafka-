"""PostgreSQL persistence for the streaming component (ODS 3).

Creates and operates the ``streaming`` schema:

==========================  ==================================================
Table                       Purpose
==========================  ==================================================
streaming.event_log         every contract-valid event consumed (event_id PK
                            => idempotent re-delivery)
streaming.invalid_events    dead-letter store for rejected events + reasons
streaming.metric_state      the >=3 streaming metrics updated by the consumer
streaming.producer_state    last value published per metric by the producer
                            (avoids republishing unchanged KPIs)
streaming.consumer_heartbeat  liveness of the consumer (interruption demos)
==========================  ==================================================
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

from src.config import DB_CONFIG

log = logging.getLogger(__name__)

SCHEMA_SQL = """
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
"""


def get_connection():
    """Open a psycopg2 connection using src.config.DB_CONFIG."""
    return psycopg2.connect(**DB_CONFIG)


def connect_with_retry(retries: int = 30, delay: float = 2.0):
    """Wait for PostgreSQL to become available (container startup races)."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection()
            ensure_schema(conn)
            return conn
        except psycopg2.OperationalError as e:
            last_error = e
            log.warning("PostgreSQL not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to PostgreSQL: {last_error}")


def ensure_schema(conn=None) -> None:
    """Create the streaming schema/tables (idempotent)."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
    finally:
        if own:
            conn.close()


def record_event(conn, event: Dict[str, Any]) -> bool:
    """Persist a valid event. Returns False when it was already stored."""
    payload = event["payload"]
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streaming.event_log
                (event_id, event_type, event_key, metric_name, event_timestamp,
                 value, previous_value, delta, unit, simulation, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event["event_id"],
                event["event_type"],
                event["event_key"],
                event["metric_name"],
                event["timestamp"],
                payload.get("value"),
                payload.get("previous_value"),
                payload.get("delta"),
                payload.get("unit"),
                bool(event.get("source", {}).get("simulation", False)),
                psycopg2.extras.Json(payload),
            ),
        )
        return cur.rowcount > 0


def record_invalid_event(conn, raw: str, errors: List[str], message_key: Optional[str]) -> None:
    """Dead-letter store for events rejected by the contract."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streaming.invalid_events (reason, errors, raw_payload, message_key)
            VALUES (%s, %s, %s, %s)
            """,
            ("contract_violation", psycopg2.extras.Json(errors), raw, message_key),
        )


def bump_metric(conn, metric_name: str, value: Optional[float] = None,
                previous_value: Optional[float] = None, delta: Optional[float] = None,
                unit: Optional[str] = None, event_id: Optional[str] = None,
                event_at: Optional[str] = None,
                increment_value: float = 0.0, increment_count: int = 1) -> None:
    """Update one streaming metric (UPSERT).

    * ``value`` given        -> gauge: metric_value is SET to ``value``
                                (e.g. rollups such as affiliates_latest_total)
    * ``value`` None         -> counter: metric_value INCREASES by
                                ``increment_value`` (e.g. events_processed_total)
    ``increment_count`` only affects the ``event_count`` bookkeeping column.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streaming.metric_state
                (metric_name, metric_value, previous_value, delta, unit,
                 event_count, last_event_id, last_event_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (metric_name) DO UPDATE SET
                metric_value   = CASE
                                     WHEN EXCLUDED.metric_value IS NOT NULL
                                         THEN EXCLUDED.metric_value
                                     ELSE COALESCE(streaming.metric_state.metric_value, 0)
                                              + %s
                                 END,
                previous_value = COALESCE(EXCLUDED.previous_value, streaming.metric_state.previous_value),
                delta          = COALESCE(EXCLUDED.delta, streaming.metric_state.delta),
                unit           = COALESCE(EXCLUDED.unit, streaming.metric_state.unit),
                event_count    = streaming.metric_state.event_count + EXCLUDED.event_count,
                last_event_id  = COALESCE(EXCLUDED.last_event_id, streaming.metric_state.last_event_id),
                last_event_at  = COALESCE(EXCLUDED.last_event_at, streaming.metric_state.last_event_at),
                updated_at     = now()
            """,
            (metric_name, value, previous_value, delta, unit, increment_count,
             event_id, event_at, increment_value),
        )


def heartbeat(conn, consumer_id: str, processed: int, invalid: int,
              status: str = "running") -> None:
    """Refresh consumer liveness (used to demonstrate interruption behaviour)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streaming.consumer_heartbeat
                (consumer_id, status, events_processed, invalid_events, last_heartbeat, updated_at)
            VALUES (%s, %s, %s, %s, now(), now())
            ON CONFLICT (consumer_id) DO UPDATE SET
                status           = EXCLUDED.status,
                events_processed = EXCLUDED.events_processed,
                invalid_events   = EXCLUDED.invalid_events,
                last_heartbeat   = now(),
                updated_at       = now()
            """,
            (consumer_id, status, processed, invalid),
        )


def get_producer_state(conn) -> Dict[str, float]:
    """Last value published per metric (key => value)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT metric_name, last_value FROM streaming.producer_state")
        return {row["metric_name"]: float(row["last_value"]) for row in cur.fetchall()}


def save_producer_state(conn, metric_name: str, value: float) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streaming.producer_state (metric_name, last_value, published_at, publish_count)
            VALUES (%s, %s, now(), 1)
            ON CONFLICT (metric_name) DO UPDATE SET
                last_value    = EXCLUDED.last_value,
                published_at  = now(),
                publish_count = streaming.producer_state.publish_count + 1
            """,
            (metric_name, value),
        )


def fetch_metrics(conn) -> List[Dict[str, Any]]:
    """All streaming metrics for the real-time dashboard."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT metric_name, metric_value, previous_value, delta, unit,
                   event_count, invalid_count, last_event_at, updated_at
            FROM streaming.metric_state
            ORDER BY metric_name
            """
        )
        rows = [dict(row) for row in cur.fetchall()]
    conn.commit()  # release the snapshot: keeps now() fresh on the next read
    return rows


def fetch_recent_events(conn, limit: int = 25) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT event_id, event_type, event_key, metric_name, value, delta,
                   unit, simulation, event_timestamp, received_at
            FROM streaming.event_log
            ORDER BY received_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    conn.commit()
    return rows


def fetch_recent_invalid(conn, limit: int = 10) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT invalid_id, reason, errors, raw_payload, received_at
            FROM streaming.invalid_events
            ORDER BY received_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    conn.commit()
    return rows


def fetch_heartbeat(conn) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT consumer_id, status, events_processed, invalid_events,
                   last_heartbeat,
                   EXTRACT(EPOCH FROM (clock_timestamp() - last_heartbeat)) AS seconds_since
            FROM streaming.consumer_heartbeat
            ORDER BY last_heartbeat DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else None


def fetch_metric_series(conn, metric_name: str, limit: int = 60) -> List[Dict[str, Any]]:
    """Event-value time series for a metric (trend chart in the dashboard)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT received_at, value, delta, event_type, simulation
            FROM streaming.event_log
            WHERE metric_name = %s
            ORDER BY received_at DESC
            LIMIT %s
            """,
            (metric_name, limit),
        )
        rows = [dict(row) for row in reversed(cur.fetchall())]
    conn.commit()
    return rows


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
