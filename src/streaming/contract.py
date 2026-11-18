"""Event contract for the Health Colombia streaming pipeline (ODS 3).

The contract is the agreement between producer and consumer. Every event that
travels through the Kafka topic MUST satisfy it, otherwise the consumer
rejects it, stores it in ``streaming.invalid_events`` and increments the
``invalid_events_total`` streaming metric (see src/streaming/consumer.py).

Contract summary
----------------
=====================  =====================================================
Field                  Rule
=====================  =====================================================
event_id               required, UUID v4 string, unique per event
event_type             required, one of EVENT_TYPES
schema_version         required, integer, must equal CONTRACT_VERSION
event_key              required, non-empty string. It is also the Kafka
                       message key (partitioning / ordering per key)
timestamp              required, ISO-8601 UTC string with timezone
metric_name            required, non-empty string (metric being updated)
payload                required, object; per-type schema in PAYLOAD_SCHEMA
source                 optional, object: {"mode": "snapshot"|"replay"|"demo",
                       "simulation": bool, "batch_id": int|None}
=====================  =====================================================

Event types
-----------
``metric_update``    incremental value of a DW-derived metric (low latency KPI)
``threshold_alert``  emitted by the producer when |delta| exceeds the
                     configured alert threshold (analytical justification)

Payload metrics use finite numbers (never booleans, NaN or Infinity); ``unit``
and ``metric_name`` are non-empty strings. ``dimension`` is an object and
``threshold_alert`` additionally requires a finite non-negative ``threshold``.
When present, ``source.mode`` and its boolean simulation flag must agree:
snapshot=False; replay/demo=True. ``source.batch_id`` is an integer or null.
"""

from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

CONTRACT_VERSION = 1

EVENT_TYPES = ("metric_update", "threshold_alert")

REQUIRED_FIELDS: Dict[str, Tuple[type, ...]] = {
    "event_id": (str,),
    "event_type": (str,),
    "schema_version": (int,),
    "event_key": (str,),
    "timestamp": (str,),
    "metric_name": (str,),
    "payload": (dict,),
}

# Fields required inside ``payload`` for every event type.
PAYLOAD_SCHEMA: Tuple[str, ...] = (
    "metric_name",
    "value",
    "unit",
)


class EventValidationError(ValueError):
    """Raised when an event does not satisfy the contract."""

    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


def _is_number(value: Any) -> bool:
    # bool subclasses int in Python; NaN/Infinity are not valid JSON metrics.
    return (
        isinstance(value, int) and not isinstance(value, bool)
    ) or (isinstance(value, float) and math.isfinite(value))


def build_event(
    event_type: str,
    event_key: str,
    metric_name: str,
    value: float,
    unit: str,
    previous_value: Optional[float] = None,
    delta: Optional[float] = None,
    dimension: Optional[Dict[str, Any]] = None,
    source: Optional[Dict[str, Any]] = None,
    threshold: Optional[float] = None,
) -> Dict[str, Any]:
    """Build a contract-compliant event (UTC, UUID v4)."""
    payload: Dict[str, Any] = {
        "metric_name": metric_name,
        "value": value,
        "unit": unit,
        "previous_value": previous_value,
        "delta": delta,
        "dimension": dimension or {},
    }
    if threshold is not None:
        payload["threshold"] = threshold

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "schema_version": CONTRACT_VERSION,
        "event_key": event_key,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metric_name": metric_name,
        "payload": payload,
        "source": source or {"mode": "snapshot", "simulation": False, "batch_id": None},
    }


def validate_event(event: Any) -> Tuple[bool, List[str]]:
    """Validate an event against the contract.

    Returns ``(is_valid, errors)``. Never raises, so the caller can route
    invalid events to the dead-letter store instead of crashing the consumer.
    """
    errors: List[str] = []

    if not isinstance(event, dict):
        return False, ["event must be a JSON object"]

    # 1. Required top-level fields and their types
    for field, types in REQUIRED_FIELDS.items():
        if field not in event:
            errors.append(f"missing required field '{field}'")
            continue
        value = event[field]
        if not isinstance(value, types) or (field == "schema_version" and isinstance(value, bool)):
            expected = "/".join(t.__name__ for t in types)
            errors.append(f"field '{field}' must be {expected}, got {type(value).__name__}")

    if errors:
        return False, errors

    # 2. event_id must be a UUID
    try:
        event_uuid = uuid.UUID(event["event_id"])
        if event_uuid.version != 4:
            errors.append("field 'event_id' must be a UUID v4")
    except (ValueError, AttributeError, TypeError):
        errors.append("field 'event_id' must be a valid UUID")

    # 3. event_type must belong to the catalog
    if event["event_type"] not in EVENT_TYPES:
        errors.append(
            f"unknown event_type '{event['event_type']}' (expected one of {list(EVENT_TYPES)})"
        )

    # 4. schema_version must match this contract
    if event["schema_version"] != CONTRACT_VERSION:
        errors.append(
            f"unsupported schema_version {event['schema_version']} "
            f"(expected {CONTRACT_VERSION})"
        )

    # 5. event_key: non-empty string, used as Kafka message key
    if not str(event["event_key"]).strip():
        errors.append("field 'event_key' must be a non-empty string")
    elif len(event["event_key"]) > 160:
        errors.append("field 'event_key' must not exceed 160 characters")

    # 6. timestamp must be ISO-8601 and timezone-aware
    try:
        parsed = datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            errors.append("field 'timestamp' must include a timezone (UTC)")
        elif parsed.utcoffset() != timedelta(0):
            errors.append("field 'timestamp' must use UTC")
    except ValueError:
        errors.append("field 'timestamp' must be a valid ISO-8601 datetime")

    # 7. metric_name: non-empty string
    if not str(event["metric_name"]).strip():
        errors.append("field 'metric_name' must be a non-empty string")
    elif len(event["metric_name"]) > 120:
        errors.append("field 'metric_name' must not exceed 120 characters")

    # 8. payload: object with the per-type schema
    payload = event["payload"]
    try:
        json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError):
        errors.append("payload must contain JSON-compatible values with finite numbers")
    for field in PAYLOAD_SCHEMA:
        if field not in payload:
            errors.append(f"payload is missing required field '{field}'")

    for field in ("metric_name", "unit"):
        if field in payload and (
            not isinstance(payload[field], str) or not payload[field].strip()
        ):
            errors.append(f"payload '{field}' must be a non-empty string")
    if isinstance(payload.get("unit"), str) and len(payload["unit"]) > 40:
        errors.append("payload 'unit' must not exceed 40 characters")

    if "value" in payload and not _is_number(payload["value"]):
        errors.append("payload 'value' must be a number (finite, excluding booleans)")

    if "previous_value" in payload and payload["previous_value"] is not None:
        if not _is_number(payload["previous_value"]):
            errors.append("payload 'previous_value' must be a finite number or null")

    if "delta" in payload and payload["delta"] is not None:
        if not _is_number(payload["delta"]):
            errors.append("payload 'delta' must be a finite number or null")

    if "dimension" in payload and not isinstance(payload["dimension"], dict):
        errors.append("payload 'dimension' must be an object")

    if event["event_type"] == "threshold_alert" and "threshold" not in payload:
        errors.append("payload is missing required field 'threshold' for threshold_alert")
    if "threshold" in payload and (
        not _is_number(payload["threshold"]) or payload["threshold"] < 0
    ):
        errors.append("payload 'threshold' must be a finite non-negative number")

    # 9. source, when present, must carry the simulation flag
    source = event.get("source")
    if "source" in event:
        if not isinstance(source, dict):
            errors.append("field 'source' must be an object")
        else:
            mode = source.get("mode")
            if mode not in ("snapshot", "replay", "demo"):
                errors.append("field 'source.mode' must be snapshot, replay or demo")
            if "simulation" not in source:
                errors.append("field 'source' must declare 'simulation' (bool)")
            elif not isinstance(source["simulation"], bool):
                errors.append("field 'source.simulation' must be a boolean")
            elif mode in ("snapshot", "replay", "demo") and source["simulation"] != (mode != "snapshot"):
                errors.append("field 'source.simulation' must match source.mode")
            batch_id = source.get("batch_id")
            if batch_id is not None and (not isinstance(batch_id, int) or isinstance(batch_id, bool)):
                errors.append("field 'source.batch_id' must be an integer or null")

    # 10. cross-check: payload.metric_name must match the envelope
    if "metric_name" in payload and payload["metric_name"] != event["metric_name"]:
        errors.append("payload.metric_name does not match envelope metric_name")

    return (not errors), errors
