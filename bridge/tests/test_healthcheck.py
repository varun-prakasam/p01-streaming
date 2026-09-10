"""The connectivity probe.

Its whole value is telling you *which* dependency is broken, so the tests are mostly about it
continuing past the first failure and reporting honestly rather than about the happy path.
"""

import io
import unittest
from contextlib import redirect_stdout
from unittest import mock

from bridge import healthcheck


def metadata(topics, partitions=6):
    meta = mock.Mock()
    meta.brokers = {0: "broker-0"}
    meta.topics = {}
    for name in topics:
        topic = mock.Mock()
        topic.error = None
        topic.partitions = {i: mock.Mock() for i in range(partitions)}
        meta.topics[name] = topic
    return meta


class KafkaCheckTest(unittest.TestCase):
    def run_check(self, meta):
        admin = mock.Mock()
        admin.list_topics.return_value = meta
        buffer = io.StringIO()
        with mock.patch.object(healthcheck, "AdminClient", return_value=admin):
            with redirect_stdout(buffer):
                ok = healthcheck.check_kafka()
        return ok, buffer.getvalue()

    def test_passes_when_both_topics_exist(self):
        ok, out = self.run_check(metadata([healthcheck.TOPIC, healthcheck.DLQ_TOPIC]))
        self.assertTrue(ok)
        self.assertIn("6 partitions", out)

    def test_fails_when_the_main_topic_is_missing(self):
        """Auto-create is off, so a missing topic means the KafkaTopic resource has not reconciled.
        The bridge would otherwise discover this on its first produce, in production."""
        ok, out = self.run_check(metadata([healthcheck.DLQ_TOPIC]))
        self.assertFalse(ok)
        self.assertIn("MISSING", out)

    def test_fails_when_the_dead_letter_topic_is_missing(self):
        ok, _ = self.run_check(metadata([healthcheck.TOPIC]))
        self.assertFalse(ok)

    def test_a_topic_carrying_an_error_counts_as_missing(self):
        """Kafka reports an unauthorised or under-replicated topic as present-with-error rather
        than absent, and producing to it would fail just the same."""
        meta = metadata([healthcheck.TOPIC, healthcheck.DLQ_TOPIC])
        meta.topics[healthcheck.TOPIC].error = "UNKNOWN_TOPIC_OR_PART"
        ok, out = self.run_check(meta)
        self.assertFalse(ok)
        self.assertIn("MISSING", out)


class JetstreamCheckTest(unittest.TestCase):
    def test_passes_when_a_frame_arrives(self):
        conn = mock.Mock()
        conn.recv.return_value = b'{"kind":"commit"}'
        buffer = io.StringIO()
        with mock.patch.object(healthcheck.websocket, "create_connection", return_value=conn):
            with redirect_stdout(buffer):
                self.assertTrue(healthcheck.check_jetstream())
        self.assertIn("first frame", buffer.getvalue())
        conn.close.assert_called_once()

    def test_the_socket_is_closed_even_when_recv_fails(self):
        conn = mock.Mock()
        conn.recv.side_effect = OSError("timed out")
        with mock.patch.object(healthcheck.websocket, "create_connection", return_value=conn):
            with redirect_stdout(io.StringIO()):
                with self.assertRaises(OSError):
                    healthcheck.check_jetstream()
        conn.close.assert_called_once()


class MainTest(unittest.TestCase):
    def run_main(self, kafka, jetstream):
        buffer = io.StringIO()
        with mock.patch.object(healthcheck, "check_kafka", **kafka), mock.patch.object(
            healthcheck, "check_jetstream", **jetstream
        ):
            with redirect_stdout(buffer):
                code = healthcheck.main()
        return code, buffer.getvalue()

    def test_zero_when_everything_passes(self):
        code, out = self.run_main({"return_value": True}, {"return_value": True})
        self.assertEqual(code, 0)
        self.assertIn("all checks passed", out)

    def test_non_zero_when_a_check_returns_false(self):
        code, out = self.run_main({"return_value": False}, {"return_value": True})
        self.assertEqual(code, 1)
        self.assertIn("kafka", out)

    def test_an_exception_is_reported_rather_than_propagated(self):
        code, out = self.run_main(
            {"side_effect": RuntimeError("no route to host")}, {"return_value": True}
        )
        self.assertEqual(code, 1)
        self.assertIn("no route to host", out)

    def test_every_check_runs_even_after_one_fails(self):
        """Reporting only the first failure means two round trips to the cluster to learn two
        things, and the second is usually the interesting one."""
        jetstream = mock.Mock(return_value=False)
        with mock.patch.object(healthcheck, "check_kafka", side_effect=RuntimeError("down")):
            with mock.patch.object(healthcheck, "check_jetstream", jetstream):
                with redirect_stdout(io.StringIO()):
                    code = healthcheck.main()
        self.assertEqual(code, 1)
        jetstream.assert_called_once()


if __name__ == "__main__":
    unittest.main()
