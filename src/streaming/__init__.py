"""Streaming component for Health Colombia (ODS 3) — Kafka producer/consumer.

Implements section 2.6 of the Final Delivery specification:
  * event contract (required fields, data types, event id, timestamp, key)
  * producer: publishes meaningful metric updates derived from the DW fact tables
  * consumer: validates events against the contract and maintains >=3
    streaming metrics in PostgreSQL (streaming.* schema)
  * dashboard: Streamlit real-time dashboard (section 2.7)
"""

from src.streaming.contract import (
    CONTRACT_VERSION,
    EVENT_TYPES,
    REQUIRED_FIELDS,
    EventValidationError,
    build_event,
    validate_event,
)

__all__ = [
    "CONTRACT_VERSION",
    "EVENT_TYPES",
    "REQUIRED_FIELDS",
    "EventValidationError",
    "build_event",
    "validate_event",
]
