"""Sink configuration, and its agreement with the deployed ConfigMap.

Same reasoning as the bridge's equivalent: a value that differs between the code and the cluster is
one nobody can reason about, because each is right on its own.
"""

import importlib
import os
import re
import unittest
from unittest import mock

from sink import config

CONFIGMAP = os.path.join(
    os.path.dirname(__file__), "..", "..", "k8s", "base", "configmap-sink.yaml"
)


def configmap_values() -> dict:
    with open(os.path.abspath(CONFIGMAP)) as handle:
        text = handle.read()
    body = text.split("data:", 1)[1]
    values = {}
    for line in body.splitlines():
        match = re.match(r'^\s{2}([A-Z0-9_]+):\s*"?([^"#]*?)"?\s*$', line)
        if match:
            values[match.group(1)] = match.group(2)
    return values


class RoutesTest(unittest.TestCase):
    def test_every_route_names_a_fully_qualified_table(self):
        """A two-part name would resolve against whatever project the client defaulted to, which in
        a pod is the right one and on a laptop may not be."""
        for topic, table in config.ROUTES.items():
            with self.subTest(topic=topic):
                self.assertEqual(len(table.split(".")), 3, f"{topic} -> {table}")

    def test_routes_cover_the_raw_table_and_both_window_tables(self):
        """The fast/settled pair is the thesis. Dropping either leaves the revision view joining
        against nothing, and v_revisions would report every window as unsettled forever."""
        tables = set(config.ROUTES.values())
        self.assertTrue(any(t.endswith("p01_streaming_raw.posts") for t in tables))
        self.assertTrue(any(t.endswith("window_counts_fast") for t in tables))
        self.assertTrue(any(t.endswith("window_counts_settled") for t in tables))

    def test_topics_are_versioned(self):
        """`.v1` is the contract with projects 5 and 8. A topic without it cannot be superseded
        without breaking whoever is reading."""
        for topic in config.ROUTES:
            with self.subTest(topic=topic):
                self.assertRegex(topic, r"\.v\d+$")

    def test_no_two_topics_write_the_same_table(self):
        tables = list(config.ROUTES.values())
        self.assertEqual(len(tables), len(set(tables)))


class DefaultsTest(unittest.TestCase):
    def reload_with(self, **env):
        with mock.patch.dict(os.environ, env, clear=True):
            return importlib.reload(config)

    def tearDown(self):
        importlib.reload(config)

    def test_batching_is_bounded_both_ways(self):
        fresh = self.reload_with()
        self.assertGreater(fresh.BATCH_MAX_ROWS, 0)
        self.assertGreater(fresh.BATCH_MAX_SECONDS, 0)

    def test_the_batch_interval_is_shorter_than_a_window(self):
        """Windows are a minute wide. A batch interval longer than that would add a whole window of
        lag to a dashboard whose entire claim is that it is current."""
        self.assertLess(self.reload_with().BATCH_MAX_SECONDS, 60)

    def test_overrides_are_applied(self):
        fresh = self.reload_with(P01_SINK_BATCH_MAX_ROWS="7", GCP_PROJECT_ID="other")
        self.assertEqual(fresh.BATCH_MAX_ROWS, 7)
        self.assertTrue(all(t.startswith("other.") for t in fresh.ROUTES.values()))

    def test_a_non_numeric_override_fails_loudly_at_import(self):
        with self.assertRaises(ValueError):
            self.reload_with(P01_SINK_BATCH_MAX_ROWS="lots")


class ConfigMapAgreementTest(unittest.TestCase):
    def setUp(self):
        self.values = configmap_values()
        importlib.reload(config)

    def test_the_configmap_was_actually_parsed(self):
        self.assertGreater(len(self.values), 5, f"parsed only {self.values}")

    def test_scalar_defaults_match(self):
        expected = {
            "P01_KAFKA_BOOTSTRAP": config.KAFKA_BOOTSTRAP,
            "P01_SINK_GROUP": config.CONSUMER_GROUP,
            "P01_SINK_BATCH_MAX_ROWS": str(config.BATCH_MAX_ROWS),
            "P01_SINK_BATCH_MAX_SECONDS": str(int(config.BATCH_MAX_SECONDS)),
            "GCP_PROJECT_ID": config.PROJECT,
        }
        for key, value in expected.items():
            with self.subTest(key=key):
                self.assertEqual(self.values.get(key), value)

    def test_the_sink_group_differs_from_flinks(self):
        """Flink reads the same broker with its own two groups. Sharing a group id would make them
        steal partitions from each other and each would silently see half the stream."""
        self.assertNotIn(config.CONSUMER_GROUP, ("flink-fast", "flink-settled"))


if __name__ == "__main__":
    unittest.main()
