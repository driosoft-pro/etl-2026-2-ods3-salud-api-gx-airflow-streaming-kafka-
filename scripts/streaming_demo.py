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

All injected events carry ``source.mode='demo'`` so they can be told apart
from producer traffic.

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
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    if not any([args.inject_invalid, args.inject_json_invalid,
                args.inject_alert, args.listen]):
        parser.print_help()
        return 1

    if args.listen:
        listen(args.listen)

    if args.inject_invalid or args.inject_json_invalid or args.inject_alert:
        producer = _connect_producer()
        try:
            if args.inject_invalid:
                inject_invalid(producer)
            if args.inject_json_invalid:
                inject_json_invalid(producer)
            if args.inject_alert:
                inject_alert(producer)
            producer.flush(timeout=10)
        finally:
            producer.close(timeout=10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
