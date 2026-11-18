"""Regressions for duplicate delivery, idle windows, replay and safe offsets."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.streaming.consumer import StreamingConsumer
from src.streaming.contract import build_event
from src.streaming import producer, store


def consumer():
    obj = StreamingConsumer.__new__(StreamingConsumer)
    obj.conn = MagicMock()
    obj.consumer = MagicMock()
    obj.consumer_id = "test-consumer"
    obj.processed = obj.invalid = 0
    obj.running = True
    obj.verbose = False
    obj.dlq_producer = None
    return obj


def event():
    return build_event("metric_update", "affiliates_by_department:11",
                       "affiliates_by_department", 100.0, "persons")


@pytest.mark.unit
def test_duplicate_delivery_does_not_increment_metrics():
    obj = consumer()
    obj._refresh_rollups = MagicMock()
    with patch.object(store, "record_event", return_value=False), \
            patch.object(store, "bump_metric") as bump:
        obj._handle_valid(event())
    assert obj.processed == 0
    bump.assert_not_called()
    obj._refresh_rollups.assert_not_called()


@pytest.mark.unit
def test_failed_database_batch_does_not_confirm_kafka_offsets():
    obj = consumer()
    obj.consumer.poll.return_value = {"partition": [object()]}
    obj._process_record = MagicMock(side_effect=RuntimeError("temporary database error"))
    obj.shutdown = MagicMock()
    with patch.object(store, "heartbeat"), pytest.raises(RuntimeError):
        obj.run()
    obj.conn.rollback.assert_called_once()
    obj.consumer.commit.assert_not_called()


@pytest.mark.unit
def test_idle_cycle_refreshes_the_rolling_window():
    obj = consumer()
    def idle_poll(**kwargs):
        obj.running = False
        return {}
    obj.consumer.poll.side_effect = idle_poll
    obj._refresh_event_rate = MagicMock()
    obj.shutdown = MagicMock()
    with patch.object(store, "heartbeat"):
        obj.run()
    obj._refresh_event_rate.assert_called_once()


@pytest.mark.unit
def test_kafka_key_must_match_contract_key():
    obj = consumer()
    obj._handle_invalid = MagicMock()
    obj._handle_valid = MagicMock()
    obj._process_record(SimpleNamespace(value=json.dumps(event()).encode(), key="wrong:key"))
    obj._handle_invalid.assert_called_once()
    obj._handle_valid.assert_not_called()


@pytest.mark.unit
def test_replay_loop_keeps_baseline_instead_of_inventing_growth():
    metrics = [{"metric_name": "affiliates_by_department", "event_key": "affiliates_by_department:11",
                "value": 100.0, "unit": "persons", "dimension": {"department": "BOGOTA D.C."}}]
    shared_state = {}
    emitted = []
    with patch.object(producer, "load_dw_metrics", return_value=metrics), \
            patch.object(producer, "_publish", side_effect=lambda _, e: emitted.append(e)):
        producer.run_replay(MagicMock(), MagicMock(), 0, 1, shared_state)
        producer.run_replay(MagicMock(), MagicMock(), 0, 1, shared_state)
    assert emitted[0]["payload"]["delta"] is None
    assert emitted[1]["payload"]["delta"] == 0
    assert all(e["source"]["simulation"] for e in emitted)


@pytest.mark.integration
def test_net_delta_ignores_alerts_and_duplicate_updates():
    """Exercise the real SQL in a rolled-back transaction; no durable test data."""
    from src.streaming.consumer import SQL_AFFILIATES_NET_DELTA
    import psycopg2
    import uuid
    try:
        conn = store.get_connection()
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    try:
        conn.set_session(isolation_level="REPEATABLE READ")
        with conn.cursor() as cur:
            cur.execute(SQL_AFFILIATES_NET_DELTA)
            baseline = cur.fetchone()[0]
            key = "affiliates_by_department:test-" + uuid.uuid4().hex
            first = build_event("metric_update", key, "affiliates_by_department", 100, "persons")
            update = build_event("metric_update", key, "affiliates_by_department", 130, "persons", previous_value=100, delta=30)
            alert = build_event("threshold_alert", key, "affiliates_by_department", 130, "persons", previous_value=100, delta=30, threshold=0.05)
            assert store.record_event(conn, first)
            # Explicit timestamps avoid transaction-stable now() tying observations.
            cur.execute("UPDATE streaming.event_log SET received_at=clock_timestamp()-interval '2 seconds' WHERE event_id=%s", (first["event_id"],))
            assert store.record_event(conn, update)
            assert store.record_event(conn, alert)
            assert not store.record_event(conn, update)
            cur.execute(SQL_AFFILIATES_NET_DELTA)
            assert cur.fetchone()[0] - baseline == 30
    finally:
        conn.rollback()
        conn.close()
