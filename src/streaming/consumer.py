"""Kafka consumer for Health Colombia (ODS 3) — section 2.6.

Subscribes to the metric-update topic, validates every incoming event against
the contract (src/streaming/contract.py) and maintains the streaming metrics
read by the real-time dashboard (section 2.7).

Behaviour
---------
* Valid event    -> stored in ``streaming.event_log`` (event_id PK, idempotent),
                    streaming metrics updated, offsets committed
                    (at-least-once delivery).
* Invalid event  -> never crashes the loop: the raw payload and the list of
                    contract violations are stored in ``streaming.invalid_events``
                    (dead-letter store), the event is republished to the DLQ
                    topic (best effort), the ``invalid_events_total`` metric is
                    incremented and the offset is still committed so a poison
                    message cannot block the partition.
* Consumer stop  -> ``streaming.consumer_heartbeat`` stops advancing; the
                    real-time dashboard shows the consumer as OFFLINE after
                    ``STREAMING_HEARTBEAT_TIMEOUT`` seconds. On restart the
                    consumer resumes from the last committed offset and
                    back-fills the events published while it was down.

Streaming metrics updated (>= 3 required by the specification):
    1. events_processed_total      valid events consumed
    2. invalid_events_total        events rejected by the contract
    3. threshold_alerts_total      alert events received
    4. affiliates_latest_total     SUM(latest value per department) of
                                   affiliates_by_department events
    5. affiliates_net_delta        cumulative net change of affiliates
    6. capacity_latest_total       SUM(latest value per department) of
                                   capacity_by_department events
    7. events_per_minute           rolling 60s throughput

Usage
-----
    python -m src.streaming.consumer
    python -m src.streaming.consumer --consumer-id consumer-2 --verbose
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

from src.streaming import store
from src.streaming.contract import validate_event

log = logging.getLogger("streaming.consumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "salud.metric.updates")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "salud.metric.dlq")
GROUP_ID = os.getenv("STREAMING_CONSUMER_GROUP", "health-metrics-consumers")
CONSUMER_ID = os.getenv("STREAMING_CONSUMER_ID", "consumer-1")
HEARTBEAT_TIMEOUT_S = int(os.getenv("STREAMING_HEARTBEAT_TIMEOUT", "15"))

# SQL helpers for the rollup metrics ----------------------------------------
SQL_AFFILIATES_LATEST = """
    SELECT COALESCE(SUM(value), 0) FROM (
        SELECT DISTINCT ON (event_key) value
        FROM streaming.event_log
        WHERE metric_name = 'affiliates_by_department'
        ORDER BY event_key, received_at DESC
    ) latest
"""

SQL_CAPACITY_LATEST = """
    SELECT COALESCE(SUM(value), 0) FROM (
        SELECT DISTINCT ON (event_key) value
        FROM streaming.event_log
        WHERE metric_name = 'capacity_by_department'
        ORDER BY event_key, received_at DESC
    ) latest
"""

SQL_AFFILIATES_NET_DELTA = """
    SELECT COALESCE(SUM(delta), 0)
    FROM streaming.event_log
    WHERE metric_name = 'affiliates_by_department' AND delta IS NOT NULL
"""

SQL_EVENTS_PER_MINUTE = """
    SELECT COUNT(*) FROM streaming.event_log
    WHERE received_at > now() - interval '1 minute'
"""


class StreamingConsumer:
    """Kafka consumer + contract validation + metric materialisation."""

    def __init__(self, consumer_id: str = CONSUMER_ID, verbose: bool = False):
        self.consumer_id = consumer_id
        self.processed = 0
        self.invalid = 0
        self.running = True
        self.verbose = verbose

        self.conn = store.connect_with_retry()

        self.consumer = self._connect_consumer()
        self._ensure_dlq_topic()
        self.dlq_producer = self._connect_dlq()

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    # -- setup -------------------------------------------------------------
    def _connect_consumer(self, retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
        last_error: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                consumer = KafkaConsumer(
                    TOPIC,
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    group_id=GROUP_ID,
                    client_id=f"{CONSUMER_ID}-{os.getpid()}",
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,  # explicit commit => at-least-once
                    max_poll_records=500,
                    value_deserializer=lambda raw: raw,  # decode manually (JSON errors handled)
                    key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
                )
                log.info("consumer subscribed to topic=%s group=%s", TOPIC, GROUP_ID)
                return consumer
            except NoBrokersAvailable as e:
                last_error = e
                log.warning("Kafka not ready (attempt %d/%d): %s", attempt, retries, e)
                time.sleep(delay)
        raise RuntimeError(f"Could not connect to Kafka at {BOOTSTRAP_SERVERS}: {last_error}")

    def _ensure_dlq_topic(self) -> None:
        """Crea el topic DLQ si no existe (auto-create tardío = publish perdido)."""
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id=f"{CONSUMER_ID}-admin",
            )
            try:
                listed = admin.list_topics()
                names = set(listed.topics) if hasattr(listed, "topics") else set(listed)
                if DLQ_TOPIC not in names:
                    admin.create_topics([
                        NewTopic(name=DLQ_TOPIC, num_partitions=1, replication_factor=1)
                    ])
                    log.info("DLQ topic created: %s", DLQ_TOPIC)
                else:
                    log.info("DLQ topic ready: %s", DLQ_TOPIC)
            finally:
                admin.close()
        except TopicAlreadyExistsError:
            log.info("DLQ topic already exists: %s", DLQ_TOPIC)
        except Exception as e:  # noqa: BLE001 - la DLQ es best effort
            log.warning("No se pudo asegurar el topic DLQ (%s: %s); se intentará al publicar",
                        type(e).__name__, e)

    def _connect_dlq(self) -> Optional[KafkaProducer]:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id=f"{CONSUMER_ID}-dlq",
                acks="all",
                retries=3,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
        except Exception as e:  # pragma: no cover - DLQ is best effort
            log.warning("DLQ producer unavailable (%s); invalid events go to PostgreSQL only", e)
            return None

    def _handle_signal(self, signum, _frame):  # pragma: no cover
        log.info("signal %s received: draining and shutting down...", signum)
        self.running = False

    # -- processing --------------------------------------------------------
    def _handle_invalid(self, raw: str, errors: List[str], message_key: Optional[str]) -> None:
        """Dead-letter an invalid event without stopping the consumer."""
        self.invalid += 1
        store.record_invalid_event(self.conn, raw, errors, message_key)
        store.bump_metric(self.conn, "invalid_events_total", increment_value=1)
        self.conn.commit()
        log.warning("INVALID EVENT rejected (%d errors): %s | errors=%s",
                    len(errors), raw[:300], errors)

        if self.dlq_producer is not None:
            try:
                self.dlq_producer.send(
                    DLQ_TOPIC,
                    key=message_key,
                    value={"raw": raw, "errors": errors, "consumer": self.consumer_id},
                ).get(timeout=5)
                log.info("event routed to DLQ topic=%s key=%s", DLQ_TOPIC, message_key)
            except Exception as e:  # pragma: no cover
                log.warning("DLQ publish failed (best effort): %s: %s",
                            type(e).__name__, repr(e))

    def _refresh_rollups(self, event: Dict[str, Any]) -> None:
        """Recompute the derived streaming metrics after a relevant event."""
        metric = event["metric_name"]

        if metric == "affiliates_by_department":
            with self.conn.cursor() as cur:
                cur.execute(SQL_AFFILIATES_LATEST)
                latest = float(cur.fetchone()[0])
                cur.execute(SQL_AFFILIATES_NET_DELTA)
                net_delta = float(cur.fetchone()[0])
            store.bump_metric(self.conn, "affiliates_latest_total", value=latest,
                              unit="persons", event_id=event["event_id"],
                              event_at=event["timestamp"], increment_count=0)
            store.bump_metric(self.conn, "affiliates_net_delta", value=net_delta,
                              unit="persons", event_id=event["event_id"],
                              event_at=event["timestamp"], increment_count=0)

        elif metric == "capacity_by_department":
            with self.conn.cursor() as cur:
                cur.execute(SQL_CAPACITY_LATEST)
                latest = float(cur.fetchone()[0])
            store.bump_metric(self.conn, "capacity_latest_total", value=latest,
                              unit="slots", event_id=event["event_id"],
                              event_at=event["timestamp"], increment_count=0)

        # Throughput metric (any event)
        with self.conn.cursor() as cur:
            cur.execute(SQL_EVENTS_PER_MINUTE)
            per_minute = int(cur.fetchone()[0])
        store.bump_metric(self.conn, "events_per_minute", value=per_minute,
                          unit="events/m", event_id=event["event_id"],
                          event_at=event["timestamp"], increment_count=0)

    def _handle_valid(self, event: Dict[str, Any]) -> None:
        """Persist a valid event and update the streaming metrics."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM streaming.event_log WHERE event_id = %s",
                        (event["event_id"],))
            already_seen = cur.fetchone() is not None

        inserted = store.record_event(self.conn, event)
        self.processed += 1

        store.bump_metric(self.conn, "events_processed_total",
                          increment_value=1, unit="events",
                          event_id=event["event_id"], event_at=event["timestamp"])
        if event["event_type"] == "threshold_alert":
            store.bump_metric(self.conn, "threshold_alerts_total", increment_value=1,
                              event_id=event["event_id"], event_at=event["timestamp"])
            log.warning("THRESHOLD ALERT %s value=%s delta=%s (%s)",
                        event["metric_name"], event["payload"].get("value"),
                        event["payload"].get("delta"), event["payload"].get("unit"))

        self._refresh_rollups(event)
        store.heartbeat(self.conn, self.consumer_id, self.processed, self.invalid)
        self.conn.commit()

        if inserted or self.verbose:
            log.info("consumed %s key=%s metric=%s value=%s%s",
                     event["event_type"], event["event_key"], event["metric_name"],
                     event["payload"].get("value"),
                     " [DUPLICATE ignored]" if not inserted and already_seen else "")

    def _process_record(self, record) -> None:
        raw = record.value.decode("utf-8", errors="replace") if isinstance(record.value, bytes) \
            else str(record.value)
        try:
            event = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            self._handle_invalid(raw, [f"payload is not valid JSON: {e}"], record.key)
            return

        valid, errors = validate_event(event)
        if not valid:
            self._handle_invalid(raw, errors, record.key)
            return

        self._handle_valid(event)

    # -- main loop ---------------------------------------------------------
    def run(self) -> None:
        log.info("consumer %s starting | topic=%s group=%s bootstrap=%s",
                 self.consumer_id, TOPIC, GROUP_ID, BOOTSTRAP_SERVERS)
        store.heartbeat(self.conn, self.consumer_id, self.processed, self.invalid,
                        status="running")
        self.conn.commit()
        last_beat = 0.0

        try:
            while self.running:
                records = self.consumer.poll(timeout_ms=1000)
                for _tp, messages in records.items():
                    for record in messages:
                        try:
                            self._process_record(record)
                        except Exception as e:  # keep the loop alive
                            log.exception("unexpected error processing a record: %s", e)
                            self.conn.rollback()

                # Heartbeat even when idle so the dashboard can tell
                # "alive but no events" from "consumer stopped".
                now = time.time()
                if now - last_beat >= 2:
                    store.heartbeat(self.conn, self.consumer_id,
                                    self.processed, self.invalid, status="running")
                    self.conn.commit()
                    last_beat = now

                if records:
                    self.consumer.commit()  # offsets after successful processing
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        try:
            store.heartbeat(self.conn, self.consumer_id, self.processed, self.invalid,
                            status="stopped")
            self.conn.commit()
        except Exception as e:  # pragma: no cover
            log.debug("final heartbeat failed: %s", e)
        try:
            self.consumer.close()
        except Exception:  # pragma: no cover
            pass
        if self.dlq_producer is not None:
            try:
                self.dlq_producer.flush(timeout=5)
                self.dlq_producer.close(timeout=5)
            except Exception:  # pragma: no cover
                pass
        self.conn.close()
        log.info("consumer %s stopped | processed=%d invalid=%d",
                 self.consumer_id, self.processed, self.invalid)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Kafka metric consumer (ODS 3)")
    parser.add_argument("--consumer-id", default=CONSUMER_ID)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    StreamingConsumer(consumer_id=args.consumer_id, verbose=args.verbose).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
