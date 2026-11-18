"""Streaming demo & experiment tooling for Health Colombia (ODS 3).

Used to generate the evidence required by section 2.6 of the Final Delivery:

  * ``--inject-invalid``   publish an event that VIOLATES the contract
                           (missing fields / wrong types). Expected behaviour:
                           the consumer rejects it, stores it in
                           ``streaming.invalid_events``, increments
                           ``invalid_events_total`` and keeps running.
  * ``--inject-json-invalid`` publish a payload that is not even JSON
                           (same dead-letter path).
  * ``--inject-alert``     publish a valid ``threshold_alert`` event.
  * ``--listen N``         print the next N raw events from the topic
                           (proof of what actually flows through Kafka).
  * ``--simulate valid|mixed --count N --interval S`` publish bounded synthetic
                           affiliate/capacity updates, optionally alerts/rejects.

Valid injected events carry ``source.mode='demo'`` and ``simulation=true``;
intentionally invalid payloads may lack these fields.

Usage:
    python -m scripts.streaming_demo --inject-invalid
    python -m scripts.streaming_demo --inject-json-invalid
    python -m scripts.streaming_demo --inject-alert
    python -m scripts.streaming_demo --listen 10
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from typing import List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.streaming.contract import build_event

log = logging.getLogger("streaming.demo")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "salud.metric.updates")
DEMO_SOURCE = {"mode": "demo", "simulation": True, "batch_id": None}


def _connect_producer(retries: int = 30, delay: float = 2.0) -> KafkaProducer:
    last = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id="health-demo",
                acks="all",
                retries=3,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
                if isinstance(v, (dict, list))
                else str(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        except NoBrokersAvailable as e:
            last = e
            log.warning("Kafka not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError(f"Kafka unavailable: {last}")


def inject_invalid(producer: KafkaProducer) -> None:
    """Contract violation: no event_id, bad schema_version, wrong value type."""
    broken = {
        "event_type": "metric_update",          # missing event_id
        "schema_version": 999,                   # unsupported version
        "event_key": "",                         # empty key
        "timestamp": "not-a-date",               # malformed timestamp
        "metric_name": "affiliates_by_department",
        "payload": {"metric_name": "affiliates_by_department",
                    "value": "NaN",              # wrong type
                    "unit": "persons"},
        # missing 'source'
    }
    producer.send(TOPIC, key="demo:invalid", value=broken).get(timeout=10)
    log.info("published INVALID event (contract violation) -> %s", TOPIC)


def inject_json_invalid(producer: KafkaProducer) -> None:
    """Payload that cannot even be parsed as JSON."""
    producer.send(TOPIC, key="demo:garbage", value="{{{not-json!!").get(timeout=10)
    log.info("published NON-JSON payload -> %s", TOPIC)


def inject_alert(producer: KafkaProducer) -> None:
    """Valid threshold_alert event (large affiliate swing detected)."""
    event = build_event(
        event_type="threshold_alert",
        event_key="affiliates_by_department:11",
        metric_name="affiliates_by_department",
        value=8_500_000,
        unit="persons",
        previous_value=7_900_000,
        delta=600_000,
        dimension={"department_code": "11", "department": "BOGOTA D.C.",
                   "region": "Andina"},
        source=DEMO_SOURCE,
        threshold=0.05,
    )
    producer.send(TOPIC, key=event["event_key"], value=event).get(timeout=10)
    log.info("published valid threshold_alert event_id=%s -> %s",
             event["event_id"], TOPIC)


def listen(count: int, timeout: int = 30) -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,                 # read from end, no committed offsets
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda raw: raw.decode("utf-8", errors="replace"),
        key_deserializer=lambda raw: raw.decode("utf-8", errors="replace") if raw else None,
    )
    seen = 0
    print(f"--- listening on {TOPIC} (max {count} events, {timeout}s timeout) ---")
    for record in consumer:
        print(f"[partition={record.partition} offset={record.offset}] "
              f"key={record.key}\n  {record.value}")
        seen += 1
        if seen >= count:
            break
    consumer.close()
    print(f"--- received {seen} event(s) ---")


def simulate(producer: KafkaProducer, count: int, interval: float, mixed: bool = False) -> None:
    """Publish bounded synthetic traffic through the normal consumer pipeline.

    Uses existing department keys: observed streaming values change, DW rows do not.
    Every ten mixed messages contain seven metric updates, one alert and two rejects.
    """
    departments = (("11", "BOGOTA D.C.", 8_000_000, 30_000),
                   ("05", "ANTIOQUIA", 6_000_000, 25_000),
                   ("76", "VALLE DEL CAUCA", 4_000_000, 18_000))
    previous = {}
    metric_index = 0
    for index in range(count):
        slot = index % 10
        if mixed and slot == 7:
            inject_alert(producer)
        elif mixed and slot == 8:
            inject_invalid(producer)
        elif mixed and slot == 9:
            inject_json_invalid(producer)
        else:
            code, department, affiliates, capacity = departments[(metric_index // 2) % 3]
            is_capacity = metric_index % 2 == 1
            name = "capacity_by_department" if is_capacity else "affiliates_by_department"
            base = capacity if is_capacity else affiliates
            key = f"{name}:{code}"
            cycle = metric_index // 6 + 1
            value = round(base * (1 + 0.025 * math.sin(cycle * 0.7)))
            before = previous.get(key, base)
            event = build_event(
                event_type="metric_update", event_key=key, metric_name=name,
                value=value, unit="slots" if is_capacity else "persons",
                previous_value=before, delta=value - before,
                dimension={"department_code": code, "department": department},
                source=dict(DEMO_SOURCE),
            )
            producer.send(TOPIC, key=key, value=event).get(timeout=10)
            previous[key] = value
            metric_index += 1
            log.info("published demo metric %s value=%s delta=%s", key, value, value - before)
        log.info("demo progress: %d/%d acknowledged by Kafka (consumer processing is separate)", index + 1, count)
        if index + 1 < count:
            time.sleep(interval)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Streaming demo / experiments (ODS 3)")
    parser.add_argument("--inject-invalid", action="store_true",
                        help="publish an event that violates the contract")
    parser.add_argument("--inject-json-invalid", action="store_true",
                        help="publish a non-JSON payload")
    parser.add_argument("--inject-alert", action="store_true",
                        help="publish a valid threshold_alert event")
    parser.add_argument("--listen", type=int, metavar="N",
                        help="print the next N raw events from the topic")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--summary", action="store_true", help="read-only history, event states and consumer lag")
    parser.add_argument("--simulate", choices=("valid", "mixed"),
                        help="publish synthetic metric updates, optionally with alerts and rejects")
    parser.add_argument("--count", type=int, default=60, help="simulation messages (1-1000)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between messages (0.05-10)")
    args = parser.parse_args(argv)
    if args.summary:
        from scripts.streaming_summary import main as summary_main
        return summary_main()
    if not 1 <= args.count <= 1000:
        parser.error("--count must be between 1 and 1000")
    if not math.isfinite(args.interval) or not 0.05 <= args.interval <= 10:
        parser.error("--interval must be finite and between 0.05 and 10 seconds")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    if not any([args.inject_invalid, args.inject_json_invalid,
                args.inject_alert, args.listen, args.simulate]):
        parser.print_help()
        return 1

    if args.listen:
        listen(args.listen)

    if args.inject_invalid or args.inject_json_invalid or args.inject_alert or args.simulate:
        producer = _connect_producer()
        try:
            if args.inject_invalid:
                inject_invalid(producer)
            if args.inject_json_invalid:
                inject_json_invalid(producer)
            if args.inject_alert:
                inject_alert(producer)
            if args.simulate:
                simulate(producer, args.count, args.interval, mixed=args.simulate == "mixed")
            producer.flush(timeout=10)
        finally:
            producer.close(timeout=10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
