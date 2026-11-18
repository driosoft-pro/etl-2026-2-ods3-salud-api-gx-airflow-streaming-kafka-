"""Synthetic demos must use real contracts and wait for Kafka acknowledgements."""
import unittest
from unittest.mock import MagicMock, patch

from scripts import streaming_demo as demo
from src.streaming.contract import validate_event


class StreamingDemoTests(unittest.TestCase):
    @patch.object(demo.time, "sleep")
    def test_valid_metrics_change_and_preserve_previous_values(self, sleep):
        producer = MagicMock()
        demo.simulate(producer, 60, 1)
        events = [call.kwargs["value"] for call in producer.send.call_args_list]
        self.assertEqual(len(events), 60)
        self.assertEqual(len({event["event_id"] for event in events}), 60)
        previous = {}
        for call, event in zip(producer.send.call_args_list, events):
            self.assertEqual(validate_event(event), (True, []))
            self.assertEqual(call.kwargs["key"], event["event_key"])
            self.assertTrue(event["source"]["simulation"])
            payload = event["payload"]
            self.assertEqual(payload["delta"], payload["value"] - payload["previous_value"])
            if event["event_key"] in previous:
                self.assertEqual(payload["previous_value"], previous[event["event_key"]])
            previous[event["event_key"]] = payload["value"]
        self.assertEqual(len(previous), 6)
        self.assertGreater(len({e["payload"]["value"] for e in events}), 6)
        self.assertEqual(sleep.call_count, 59)
        self.assertEqual(producer.send.return_value.get.call_count, 60)

    @patch.object(demo.time, "sleep")
    def test_mixed_traffic_expected_counts(self, sleep):
        producer = MagicMock()
        demo.simulate(producer, 60, 1, mixed=True)
        events = [call.kwargs["value"] for call in producer.send.call_args_list]
        valid = [event for event in events if validate_event(event)[0]]
        self.assertEqual(len(events), 60)
        self.assertEqual(len(valid), 48)
        self.assertEqual(sum(e["event_type"] == "threshold_alert" for e in valid), 6)
        self.assertEqual(sum(isinstance(e, str) for e in events), 6)

    @patch.object(demo.time, "sleep")
    def test_failed_acknowledgement_stops_the_run(self, sleep):
        producer = MagicMock()
        producer.send.return_value.get.side_effect = RuntimeError("Kafka unavailable")
        with self.assertRaises(RuntimeError):
            demo.simulate(producer, 60, 1)
        self.assertEqual(producer.send.call_count, 1)
        sleep.assert_not_called()

    @patch.object(demo, "_connect_producer")
    def test_invalid_limits_never_connect(self, connect):
        for option, value in (("--count", "0"), ("--count", "1001"),
                              ("--interval", "nan"), ("--interval", "0")):
            with self.subTest(option=option, value=value), self.assertRaises(SystemExit):
                demo.main(["--simulate", "valid", option, value])
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
