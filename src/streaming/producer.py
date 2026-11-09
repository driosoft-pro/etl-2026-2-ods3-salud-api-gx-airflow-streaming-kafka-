"""Kafka producer for Health Colombia (ODS 3) — section 2.6.

Publishes *meaningful metric updates derived from the Data Warehouse fact
tables* instead of blindly replaying unchanged KPI values.

Modes
-----
``--mode snapshot`` (default, production behaviour)
    Reads the current aggregates from ``fact_affiliates`` and
    ``fact_facility_capacity``, compares them with
    ``streaming.producer_state`` and publishes **only the metrics whose value
    changed** since the previous publication. When a change exceeds
    ``--alert-threshold`` (default 5 %) an extra ``threshold_alert`` event is
    emitted so downstream consumers can raise an operational warning.

``--mode replay`` (clearly identified as a SIMULATION)
    Replays the historical values stored in the DW fact tables as a sequence
    of metric-update events (``source.mode="replay"``, ``source.simulation=true``).
    Used to demonstrate end-to-end event flow when the warehouse is static.

Usage
-----
    python -m src.streaming.producer --mode snapshot --interval 10 --loop
    python -m src.streaming.producer --mode replay --interval 0.5
    python -m src.streaming.producer --mode replay --cycles 2 --interval 0.2
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.streaming import store
from src.streaming.contract import build_event

log = logging.getLogger("streaming.producer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "salud.metric.updates")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "health-colombia-producer")

# --- DW queries (metric definitions) ---------------------------------------
# Metric families published by the producer. Each row is keyed by
# ``metric_name`` + department code, which becomes the Kafka message key.
Q_AFFILIATES_BY_DEPT = """
    SELECT g.codigo_dane_depto AS dept_code,
           g.departamento      AS dept_name,
           g.region            AS region,
           SUM(f.numero_afiliados)::numeric AS value
    FROM fact_affiliates f
    JOIN dim_geografia g ON g.sk_geografia = f.sk_geografia
    GROUP BY g.codigo_dane_depto, g.departamento, g.region
    ORDER BY g.codigo_dane_depto
"""

Q_CAPACITY_BY_DEPT = """
    SELECT g.codigo_dane_depto AS dept_code,
           g.departamento      AS dept_name,
           g.region            AS region,
           SUM(c.capacity_amount)::numeric AS value
    FROM fact_facility_capacity c
    JOIN dim_geografia g ON g.sk_geografia = c.sk_geografia
    GROUP BY g.codigo_dane_depto, g.departamento, g.region
    ORDER BY g.codigo_dane_depto
"""

METRIC_QUERIES: Dict[str, Tuple[str, str]] = {
    # metric_name -> (SQL, unit)
    "affiliates_by_department": (Q_AFFILIATES_BY_DEPT, "persons"),
    "capacity_by_department": (Q_CAPACITY_BY_DEPT, "slots"),
}


def _connect_db():
    return store.connect_with_retry()


def _connect_kafka(retries: int = 30, delay: float = 2.0) -> KafkaProducer:
    """Create the Kafka producer, waiting for the broker to become ready."""
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id=CLIENT_ID,
                acks="all",
                retries=5,
                linger_ms=20,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            log.info("Kafka producer connected to %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable as e:
            last_error = e
            log.warning("Kafka not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka at {BOOTSTRAP_SERVERS}: {last_error}")


def load_dw_metrics(conn) -> List[Dict[str, Any]]:
    """Read the current metric values from the DW fact tables."""
    metrics: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        for metric_name, (sql, unit) in METRIC_QUERIES.items():
            cur.execute(sql)
            for dept_code, dept_name, region, value in cur.fetchall():
                metrics.append(
                    {
                        "metric_name": metric_name,
                        "event_key": f"{metric_name}:{dept_code}",
                        "value": float(value or 0),
                        "unit": unit,
                        "dimension": {
                            "department_code": dept_code,
                            "department": dept_name,
                            "region": region,
                        },
                    }
                )
    return metrics


def _publish(producer: KafkaProducer, event: Dict[str, Any]) -> None:
    future = producer.send(
        TOPIC,
        key=event["event_key"],
        value=event,
        headers=[
            ("event_type", event["event_type"].encode()),
            ("simulation", str(bool(event.get("source", {}).get("simulation"))).encode()),
        ],
    )
    record = future.get(timeout=10)
    log.info(
        "published %s key=%s value=%s (%s) -> topic=%s partition=%s offset=%s",
        event["event_type"],
        event["event_key"],
        event["payload"]["value"],
        event["payload"]["unit"],
        TOPIC,
        record.partition,
        record.offset,
    )


def run_snapshot(producer: KafkaProducer, conn, alert_threshold: float) -> int:
    """Publish only the metrics whose DW value changed since last publication."""
    metrics = load_dw_metrics(conn)
    state = store.get_producer_state(conn)
    published = 0

    for metric in metrics:
        name = metric["metric_name"]
        key = metric["event_key"]
        state_key = key  # metric_name:dept_code — same key stored in producer_state
        previous = state.get(state_key)

        # --- No analytical justification to publish: value unchanged ---
        if previous is not None and abs(previous - metric["value"]) < 1e-9:
            continue

        delta = None if previous is None else metric["value"] - previous
        source = {"mode": "snapshot", "simulation": False, "batch_id": None}

        event = build_event(
            event_type="metric_update",
            event_key=key,
            metric_name=name,
            value=metric["value"],
            unit=metric["unit"],
            previous_value=previous,
            delta=delta,
            dimension=metric["dimension"],
            source=source,
        )
        _publish(producer, event)
        store.save_producer_state(conn, state_key, metric["value"])
        published += 1

        # --- threshold_alert: relative change above the alert threshold ---
        if previous not in (None, 0) and delta is not None:
            rel_change = abs(delta) / abs(previous)
            if rel_change > alert_threshold:
                alert = build_event(
                    event_type="threshold_alert",
                    event_key=key,
                    metric_name=name,
                    value=metric["value"],
                    unit=metric["unit"],
                    previous_value=previous,
                    delta=delta,
                    dimension=metric["dimension"],
                    source=source,
                    threshold=alert_threshold,
                )
                _publish(producer, alert)
                log.warning(
                    "threshold_alert: %s changed %.2f%% (> %.2f%%)",
                    state_key,
                    rel_change * 100,
                    alert_threshold * 100,
                )

    conn.commit()
    return published


def run_replay(producer: KafkaProducer, conn, interval: float, cycles: int) -> int:
    """Replay historical DW values as events (SIMULATION).

    Every event carries ``source.mode='replay'`` and ``source.simulation=true``
    so consumers and dashboards can flag the data as simulated arrivals.
    """
    metrics = load_dw_metrics(conn)
    published = 0
    previously_published: Dict[str, float] = {}

    for cycle in range(1, cycles + 1):
        log.info("replay cycle %d/%d (%d historical metric values)", cycle, cycles, len(metrics))
        for metric in metrics:
            # First cycle simulates arrival from zero; later cycles carry the
            # previously replayed value as previous_value (delta = 0).
            previous = previously_published.get(metric["event_key"], 0.0)
            event = build_event(
                event_type="metric_update",
                event_key=metric["event_key"],
                metric_name=metric["metric_name"],
                value=metric["value"],
                unit=metric["unit"],
                previous_value=previous,
                delta=metric["value"] - previous,
                dimension={**metric["dimension"], "replay_cycle": cycle},
                source={
                    "mode": "replay",
                    "simulation": True,
                    "batch_id": None,
                    "replay_cycle": cycle,
                },
            )
            _publish(producer, event)
            previously_published[metric["event_key"]] = metric["value"]
            published += 1
            if interval > 0:
                time.sleep(interval)

    conn.commit()
    return published


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Kafka metric-update producer (ODS 3)")
    parser.add_argument("--mode", choices=["snapshot", "replay"], default="snapshot",
                        help="snapshot = publish only DW changes; replay = simulate historical arrivals")
    parser.add_argument("--interval", type=float, default=10.0,
                        help="seconds between snapshot checks / between replay events")
    parser.add_argument("--loop", action="store_true",
                        help="run forever (service mode) instead of a single pass")
    parser.add_argument("--cycles", type=int, default=1,
                        help="number of replay cycles")
    parser.add_argument("--alert-threshold", type=float, default=0.05,
                        help="relative change that triggers threshold_alert (0.05 = 5%%)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    conn = _connect_db()
    producer = _connect_kafka()
    simulation = args.mode == "replay"
    log.info(
        "producer started | mode=%s simulation=%s topic=%s bootstrap=%s",
        args.mode, simulation, TOPIC, BOOTSTRAP_SERVERS,
    )

    total = 0
    try:
        while True:
            started = time.time()
            if args.mode == "snapshot":
                published = run_snapshot(producer, conn, args.alert_threshold)
            else:
                published = run_replay(producer, conn, args.interval, args.cycles)
            total += published
            log.info("pass complete: %d events published (total=%d)", published, total)

            if not args.loop:
                break
            # Snapshot mode sleeps between checks; replay loops cycles.
            if args.mode == "snapshot":
                time.sleep(max(0.0, args.interval - (time.time() - started)))
            elif args.cycles <= 0:
                time.sleep(max(0.0, args.interval))
    except KeyboardInterrupt:
        log.info("producer interrupted by user (total=%d)", total)
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)
        conn.close()
    log.info("producer finished | events_published=%d", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
