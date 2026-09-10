"""Configuration defaults, and their agreement with the deployed ConfigMap.

Every value is read once at import, so the ConfigMap is the only thing standing between a default
and production. When the two disagree the code says one thing and the cluster does another, and
neither is wrong on its own — which is why this file compares them rather than trusting either.
"""

import importlib
import os
import re
import unittest
from unittest import mock

from bridge import config

CONFIGMAP = os.path.join(os.path.dirname(__file__), "..", "..", "k8s", "base", "configmap.yaml")


def configmap_values() -> dict:
    """Parse the ConfigMap's data block without a YAML dependency.

    The tests are stdlib-only on purpose — the loader image ships confluent-kafka and
    websocket-client and nothing else, and a test runner that drags PyYAML into the image to read a
    file it could parse with a regex is a bad trade.
    """
    with open(os.path.abspath(CONFIGMAP)) as handle:
        text = handle.read()
    body = text.split("data:", 1)[1]
    values = {}
    for line in body.splitlines():
        match = re.match(r'^\s{2}(P01_[A-Z_]+):\s*"?([^"#]*?)"?\s*$', line)
        if match:
            values[match.group(1)] = match.group(2)
    return values


class DefaultsTest(unittest.TestCase):
    def reload_with(self, **env):
        with mock.patch.dict(os.environ, env, clear=True):
            return importlib.reload(config)

    def tearDown(self):
        # Other modules imported these at their own import time; restore the real values.
        importlib.reload(config)

    def test_posts_only_by_default(self):
        """The unfiltered firehose is ~500/sec and 14x the cost. Filtering is not an optimisation
        here, it is what makes the project fit the node and the budget."""
        self.assertEqual(self.reload_with().WANTED_COLLECTIONS, ["app.bsky.feed.post"])

    def test_more_than_one_jetstream_host(self):
        """Rotating hosts on reconnect turns a single-instance outage into a retry."""
        self.assertGreater(len(self.reload_with().JETSTREAM_HOSTS), 1)

    def test_replay_overlap_is_positive(self):
        """Zero overlap means a gap is possible, and a gap cannot be recovered. Duplicates can."""
        self.assertGreater(self.reload_with().REPLAY_OVERLAP_SECONDS, 0)

    def test_replay_cap_is_inside_the_measured_retention_window(self):
        """Replaying further back than Jetstream holds is not an error — the server silently starts
        from its oldest event. A cap beyond the window would therefore ask for data that cannot
        arrive and never notice."""
        fresh = self.reload_with()
        self.assertLess(fresh.MAX_REPLAY_SECONDS, fresh.JETSTREAM_RETENTION_SECONDS)

    def test_read_timeout_exceeds_connect_timeout(self):
        """Fail over a dead host quickly; tolerate a quiet minute on a healthy one."""
        fresh = self.reload_with()
        self.assertGreater(fresh.READ_TIMEOUT_SECONDS, fresh.CONNECT_TIMEOUT_SECONDS)

    def test_backoff_is_bounded(self):
        fresh = self.reload_with()
        self.assertGreater(fresh.RECONNECT_BACKOFF_MAX_SECONDS, fresh.RECONNECT_BACKOFF_SECONDS)

    def test_overrides_are_applied(self):
        fresh = self.reload_with(
            P01_TOPIC="other.topic",
            P01_KAFKA_BOOTSTRAP="elsewhere:9092",
            P01_REPLAY_OVERLAP_SECONDS="30",
            P01_WANTED_COLLECTIONS="a.b.c,d.e.f",
        )
        self.assertEqual(fresh.TOPIC, "other.topic")
        self.assertEqual(fresh.KAFKA_BOOTSTRAP, "elsewhere:9092")
        self.assertEqual(fresh.REPLAY_OVERLAP_SECONDS, 30)
        self.assertEqual(fresh.WANTED_COLLECTIONS, ["a.b.c", "d.e.f"])

    def test_list_parsing_tolerates_spacing_and_trailing_commas(self):
        fresh = self.reload_with(P01_WANTED_COLLECTIONS=" a.b.c , d.e.f , ")
        self.assertEqual(fresh.WANTED_COLLECTIONS, ["a.b.c", "d.e.f"])

    def test_a_non_numeric_override_fails_loudly_at_import(self):
        """Better to crash on startup than to run with a silently defaulted replay window."""
        with self.assertRaises(ValueError):
            self.reload_with(P01_MAX_REPLAY_SECONDS="an hour")


class ConfigMapAgreementTest(unittest.TestCase):
    """The deployed ConfigMap and the code's defaults must not drift."""

    def setUp(self):
        self.values = configmap_values()
        importlib.reload(config)

    def test_the_configmap_was_actually_parsed(self):
        """A regex that silently matches nothing would make every assertion below vacuous."""
        self.assertGreater(len(self.values), 8, f"parsed only {self.values}")

    def test_every_configmap_key_is_a_real_setting(self):
        """A key with no reader is a setting somebody believes is taking effect."""
        for key in self.values:
            with self.subTest(key=key):
                self.assertIn(
                    key.removeprefix("P01_"),
                    dir(config),
                    f"{key} is set in the ConfigMap but nothing reads it",
                )

    def test_scalar_defaults_match(self):
        expected = {
            "P01_KAFKA_BOOTSTRAP": config.KAFKA_BOOTSTRAP,
            "P01_TOPIC": config.TOPIC,
            "P01_DLQ_TOPIC": config.DLQ_TOPIC,
            "P01_CURSOR_PATH": config.CURSOR_PATH,
            "P01_REPLAY_OVERLAP_SECONDS": str(config.REPLAY_OVERLAP_SECONDS),
            "P01_MAX_REPLAY_SECONDS": str(config.MAX_REPLAY_SECONDS),
            "P01_JETSTREAM_RETENTION_SECONDS": str(config.JETSTREAM_RETENTION_SECONDS),
            "P01_CONNECT_TIMEOUT_SECONDS": str(config.CONNECT_TIMEOUT_SECONDS),
            "P01_READ_TIMEOUT_SECONDS": str(config.READ_TIMEOUT_SECONDS),
        }
        for key, value in expected.items():
            with self.subTest(key=key):
                self.assertEqual(self.values.get(key), value)

    def test_the_collection_filter_matches(self):
        self.assertEqual(
            self.values["P01_WANTED_COLLECTIONS"].split(","), config.WANTED_COLLECTIONS
        )

    def test_the_bootstrap_address_names_this_projects_kafka(self):
        """Strimzi derives the bootstrap Service name from the Kafka resource's name. A mismatch
        here fails at connect time in the cluster, which is far from where it was written."""
        self.assertIn("kafka-kafka-bootstrap", config.KAFKA_BOOTSTRAP)
        self.assertIn("p01-streaming", config.KAFKA_BOOTSTRAP)


if __name__ == "__main__":
    unittest.main()
