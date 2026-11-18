"""Unit tests for the streaming event contract (ODS 3 — §2.6).

The contract is the gatekeeper between producer and consumer: these tests
prove that well-formed events are accepted and that every class of violation
detected by the specification (required fields, data types, event id,
timestamp, event key) is rejected.
"""

import pytest

from src.streaming.contract import (
    CONTRACT_VERSION,
    EventValidationError,
    build_event,
    validate_event,
)

pytestmark = pytest.mark.unit


def _valid_event(**overrides):
    event = build_event(
        event_type="metric_update",
        event_key="affiliates_by_department:11",
        metric_name="affiliates_by_department",
        value=8_400_000,
        unit="persons",
        previous_value=8_300_000,
        delta=100_000,
        dimension={"department_code": "11", "department": "BOGOTA D.C."},
        source={"mode": "snapshot", "simulation": False, "batch_id": 7},
    )
    event.update(overrides)
    return event


class TestValidEvents:
    def test_build_event_is_contract_compliant(self):
        valid, errors = validate_event(_valid_event())
        assert valid, errors
        assert errors == []

    def test_replay_event_is_compliant(self):
        event = _valid_event(
            source={"mode": "replay", "simulation": True, "batch_id": None}
        )
        valid, errors = validate_event(event)
        assert valid, errors

    def test_threshold_alert_is_compliant(self):
        event = _valid_event(event_type="threshold_alert")
        event["payload"]["threshold"] = 0.05
        valid, errors = validate_event(event)
        assert valid, errors

    def test_event_id_is_uuid_and_timestamp_utc(self):
        event = _valid_event()
        assert len(event["event_id"]) == 36
        assert "+" in event["timestamp"] or "Z" in event["timestamp"]


class TestRequiredFields:
    @pytest.mark.parametrize("field", [
        "event_id", "event_type", "schema_version", "event_key",
        "timestamp", "metric_name", "payload",
    ])
    def test_missing_required_field_is_rejected(self, field):
        event = _valid_event()
        del event[field]
        valid, errors = validate_event(event)
        assert not valid
        assert any(f"missing required field '{field}'" in e for e in errors)

    @pytest.mark.parametrize("field", ["event_id", "event_type", "event_key",
                                       "timestamp", "metric_name"])
    def test_wrong_type_is_rejected(self, field):
        event = _valid_event(**{field: 12345})
        valid, errors = validate_event(event)
        assert not valid
        assert any(f"field '{field}'" in e for e in errors)


class TestEventIdentity:
    @pytest.mark.parametrize("version", [True, False, 1.0, "1"])
    def test_schema_version_requires_an_integer_not_a_boolean(self, version):
        valid, errors = validate_event(_valid_event(schema_version=version))
        assert not valid
        assert any("schema_version" in error for error in errors)

    def test_non_uuid_event_id_is_rejected(self):
        event = _valid_event(event_id="not-a-uuid")
        valid, errors = validate_event(event)
        assert not valid
        assert any("UUID" in e for e in errors)

    def test_unknown_event_type_is_rejected(self):
        event = _valid_event(event_type="metric_exploded")
        valid, errors = validate_event(event)
        assert not valid
        assert any("unknown event_type" in e for e in errors)

    def test_wrong_schema_version_is_rejected(self):
        event = _valid_event(schema_version=CONTRACT_VERSION + 1)
        valid, errors = validate_event(event)
        assert not valid
        assert any("schema_version" in e for e in errors)


class TestTimestampAndKey:
    def test_timestamp_must_use_utc(self):
        valid, errors = validate_event(_valid_event(timestamp="2026-11-17T10:00:00-05:00"))
        assert not valid
        assert any("UTC" in error for error in errors)

    def test_malformed_timestamp_is_rejected(self):
        event = _valid_event(timestamp="31/12/2026 25:99")
        valid, errors = validate_event(event)
        assert not valid
        assert any("ISO-8601" in e for e in errors)

    def test_naive_timestamp_without_timezone_is_rejected(self):
        event = _valid_event(timestamp="2026-11-08T12:00:00")
        valid, errors = validate_event(event)
        assert not valid
        assert any("timezone" in e for e in errors)

    @pytest.mark.parametrize("key", ["", "   ", None])
    def test_empty_event_key_is_rejected(self, key):
        event = _valid_event(event_key=key)
        valid, errors = validate_event(event)
        assert not valid
        assert any("event_key" in e for e in errors)


class TestPayload:
    @pytest.mark.parametrize("field,limit", [("event_key", 160), ("metric_name", 120)])
    def test_envelope_strings_fit_the_persistence_schema(self, field, limit):
        valid, errors = validate_event(_valid_event(**{field: "x" * (limit + 1)}))
        assert not valid
        assert any("must not exceed" in error for error in errors)

    def test_unit_fits_the_persistence_schema(self):
        event = _valid_event()
        event["payload"]["unit"] = "x" * 41
        valid, errors = validate_event(event)
        assert not valid
        assert any("must not exceed" in error for error in errors)

    def test_nested_non_finite_values_do_not_reach_jsonb_storage(self):
        event = _valid_event()
        event["payload"]["dimension"] = {"untrusted_attribute": float("nan")}
        valid, errors = validate_event(event)
        assert not valid
        assert any("JSON-compatible" in error for error in errors)

    @pytest.mark.parametrize("field", ["value", "previous_value", "delta"])
    @pytest.mark.parametrize("value", [True, float("nan"), float("inf"), -float("inf")])
    def test_numeric_payload_fields_require_finite_numbers(self, field, value):
        event = _valid_event()
        event["payload"][field] = value
        valid, errors = validate_event(event)
        assert not valid
        assert any(field in error for error in errors)

    @pytest.mark.parametrize("value", [123, None, "", "   "])
    def test_unit_requires_a_non_empty_string(self, value):
        event = _valid_event()
        event["payload"]["unit"] = value
        valid, errors = validate_event(event)
        assert not valid
        assert any("unit" in error for error in errors)

    def test_dimension_requires_an_object(self):
        event = _valid_event()
        event["payload"]["dimension"] = "BOGOTA"
        valid, errors = validate_event(event)
        assert not valid
        assert any("dimension" in error for error in errors)

    @pytest.mark.parametrize("threshold", [None, "0.05", True, -0.1, float("nan"), float("inf")])
    def test_alert_threshold_must_be_a_finite_non_negative_number(self, threshold):
        event = _valid_event(event_type="threshold_alert")
        event["payload"]["threshold"] = threshold
        valid, errors = validate_event(event)
        assert not valid
        assert any("threshold" in error for error in errors)

    def test_alert_requires_a_threshold(self):
        valid, errors = validate_event(_valid_event(event_type="threshold_alert"))
        assert not valid
        assert any("threshold" in error for error in errors)

    def test_missing_payload_value_is_rejected(self):
        event = _valid_event()
        del event["payload"]["value"]
        valid, errors = validate_event(event)
        assert not valid
        assert any("payload is missing required field 'value'" in e for e in errors)

    def test_non_numeric_value_is_rejected(self):
        event = _valid_event()
        event["payload"]["value"] = "8400000"
        valid, errors = validate_event(event)
        assert not valid
        assert any("payload 'value' must be a number" in e for e in errors)

    def test_payload_metric_mismatch_is_rejected(self):
        event = _valid_event()
        event["payload"]["metric_name"] = "capacity_by_department"
        valid, errors = validate_event(event)
        assert not valid
        assert any("does not match" in e for e in errors)


class TestSource:
    def test_demo_source_is_compliant(self):
        valid, errors = validate_event(_valid_event(source={"mode": "demo", "simulation": True}))
        assert valid, errors

    @pytest.mark.parametrize("source", [
        None,
        {"mode": "unknown", "simulation": False},
        {"mode": "replay", "simulation": False},
        {"mode": "snapshot", "simulation": True},
        {"mode": "snapshot", "simulation": False, "batch_id": True},
        {"mode": "snapshot", "simulation": False, "batch_id": "7"},
    ])
    def test_source_fields_match_the_declared_types_and_simulation_mode(self, source):
        valid, errors = validate_event(_valid_event(source=source))
        assert not valid
        assert any("source" in error for error in errors)

    def test_source_without_simulation_flag_is_rejected(self):
        event = _valid_event(source={"mode": "snapshot"})
        valid, errors = validate_event(event)
        assert not valid
        assert any("'simulation'" in e for e in errors)

    def test_source_simulation_must_be_boolean(self):
        event = _valid_event(source={"mode": "snapshot", "simulation": "no"})
        valid, errors = validate_event(event)
        assert not valid
        assert any("boolean" in e for e in errors)


class TestRobustness:
    def test_non_dict_payload_is_rejected(self):
        valid, errors = validate_event("just a string")
        assert not valid
        assert errors == ["event must be a JSON object"]

    def test_multiple_violations_are_all_reported(self):
        event = _valid_event(event_id="xx", event_type="nope", event_key="")
        valid, errors = validate_event(event)
        assert not valid
        assert len(errors) >= 3

    def test_event_error_type_is_a_value_error(self):
        with pytest.raises(ValueError):
            raise EventValidationError(["boom"])
