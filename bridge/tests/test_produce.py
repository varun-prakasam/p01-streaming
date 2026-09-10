"""Producing to Kafka, and the one property that matters: it blocks rather than drops.

This is where data goes missing without anything failing. A producer that discards on a full queue
keeps the pod green, keeps the cursor advancing, and quietly emits fewer events than it consumed.
Counts are this project's product, so that corrupts the answer while looking healthy — the same
shape as project 3's pagination bug, which also failed by producing less rather than by crashing.
"""

import unittest
from unittest import mock

from confluent_kafka import KafkaException

from bridge import produce


class FakeProducer:
    """A Kafka producer whose queue can be made full on demand.

    Answers the calls the Sink actually makes rather than replaying a scripted sequence, so the test
    exercises the Sink's own retry decisions.
    """

    def __init__(self, full_for=0):
        self.full_for = full_for
        self.produced = []
        self.polls = 0
        self.flushed = 0
        self._pending_callbacks = []

    def produce(self, topic, value=None, key=None, on_delivery=None):
        if self.full_for > 0:
            self.full_for -= 1
            raise BufferError("Local: Queue full")
        self.produced.append((topic, value, key))
        self._pending_callbacks.append(on_delivery)

    def poll(self, _timeout=0):
        self.polls += 1
        # Deliver everything queued, successfully, as librdkafka would once there is room.
        while self._pending_callbacks:
            callback = self._pending_callbacks.pop(0)
            if callback:
                callback(None, mock.Mock())
        return 0

    def flush(self, _timeout=None):
        self.flushed += 1
        self.poll()
        return 0


class SendTest(unittest.TestCase):
    def setUp(self):
        self.producer = FakeProducer()
        self.sink = produce.Sink(self.producer, "posts", "dlq")

    def test_sends_to_the_topic_with_the_key_encoded(self):
        self.sink.send(b'{"a":1}', "did:plc:abc")
        topic, value, key = self.producer.produced[0]
        self.assertEqual(topic, "posts")
        self.assertEqual(value, b'{"a":1}')
        self.assertEqual(key, b"did:plc:abc")

    def test_dead_letters_go_to_the_dlq_topic_unkeyed(self):
        """No key, because a frame that failed to parse has no key worth trusting."""
        self.sink.send_dead_letter(b"garbage", "not JSON")
        topic, value, key = self.producer.produced[0]
        self.assertEqual(topic, "dlq")
        self.assertEqual(value, b"garbage")
        self.assertIsNone(key)


class BackpressureTest(unittest.TestCase):
    def setUp(self):
        self.producer = FakeProducer()
        self.sink = produce.Sink(self.producer, "posts", "dlq")

    def test_a_full_queue_blocks_and_retries_rather_than_dropping(self):
        """The whole design in one test. A full queue must propagate backpressure up to the
        WebSocket read, not silently discard the message."""
        self.producer.full_for = 3
        self.sink.send(b"payload", "k")
        self.assertEqual(len(self.producer.produced), 1, "message was dropped, not retried")
        self.assertEqual(self.producer.polls, 3, "did not service the queue while blocked")

    def test_retry_exhaustion_raises_rather_than_returning_quietly(self):
        """The branch statement coverage marks green and never runs. A broker that is up but not
        accepting writes must stop the loop — the cursor has not advanced, so a restart replays."""
        self.producer.full_for = produce.MAX_QUEUE_WAITS + 5
        with self.assertRaises(RuntimeError) as caught:
            self.sink.send(b"payload", "k")
        self.assertIn("not accepting writes", str(caught.exception))
        self.assertEqual(self.producer.produced, [], "a message escaped after exhaustion")

    def test_exhaustion_boundary(self):
        """One under the limit succeeds; the limit itself raises."""
        self.producer.full_for = produce.MAX_QUEUE_WAITS - 1
        self.sink.send(b"payload", "k")
        self.assertEqual(len(self.producer.produced), 1)

        self.producer.produced.clear()
        self.producer.full_for = produce.MAX_QUEUE_WAITS
        with self.assertRaises(RuntimeError):
            self.sink.send(b"payload", "k")


class DeliveryFailureTest(unittest.TestCase):
    """A delivery callback reporting failure must reach the loop before the cursor moves past it."""

    def setUp(self):
        self.producer = FakeProducer()
        self.sink = produce.Sink(self.producer, "posts", "dlq")

    def fail_next_delivery(self, error="Broker: Not enough in-sync replicas"):
        def poll(_timeout=0):
            self.producer.polls += 1
            while self.producer._pending_callbacks:
                callback = self.producer._pending_callbacks.pop(0)
                if callback:
                    callback(error, mock.Mock())
            return 0

        self.producer.poll = poll

    def test_a_failed_delivery_surfaces_on_poll(self):
        self.sink.send(b"payload", "k")
        self.fail_next_delivery()
        with self.assertRaises(KafkaException):
            self.sink.poll()

    def test_a_failed_delivery_surfaces_on_flush(self):
        """flush() is what gates the cursor. If a failure only surfaced on poll, a commit could
        step past a message the broker rejected."""
        self.sink.send(b"payload", "k")
        self.fail_next_delivery()
        self.producer.flush = lambda _timeout=None: (self.producer.poll(), 0)[1]
        with self.assertRaises(KafkaException):
            self.sink.flush()

    def test_failures_are_reported_once_and_then_cleared(self):
        """Otherwise every subsequent poll re-raises a stale error and the bridge can never recover
        from a single transient rejection."""
        self.sink.send(b"payload", "k")
        self.fail_next_delivery()
        with self.assertRaises(KafkaException):
            self.sink.poll()
        self.producer.poll = FakeProducer.poll.__get__(self.producer)
        self.sink.poll()

    def test_the_error_message_names_how_many_failed(self):
        for _ in range(5):
            self.sink.send(b"payload", "k")
        self.fail_next_delivery()
        with self.assertRaises(KafkaException) as caught:
            self.sink.poll()
        self.assertIn("5 message(s)", str(caught.exception))


class FlushTest(unittest.TestCase):
    def setUp(self):
        self.producer = FakeProducer()
        self.sink = produce.Sink(self.producer, "posts", "dlq")

    def test_returns_the_undelivered_count(self):
        """A non-zero return is the case that matters: those messages are still in memory and will
        die with the process, so the cursor must not advance past them."""
        self.producer.flush = lambda _timeout=None: 7
        self.assertEqual(self.sink.flush(), 7)

    def test_a_clean_flush_returns_zero(self):
        self.sink.send(b"payload", "k")
        self.assertEqual(self.sink.flush(), 0)


class ConfigTest(unittest.TestCase):
    def test_durability_settings(self):
        with mock.patch.object(produce, "Producer") as ctor:
            produce.build("broker:9092")
        config = ctor.call_args.args[0]
        self.assertEqual(config["bootstrap.servers"], "broker:9092")
        self.assertEqual(config["acks"], "all")
        self.assertTrue(config["enable.idempotence"])

    def test_the_queue_is_bounded(self):
        """The default is 100,000 messages — at ~37/sec that is 45 minutes of buffered data a pod
        restart would lose. A small queue makes backpressure arrive quickly and visibly."""
        with mock.patch.object(produce, "Producer") as ctor:
            produce.build("broker:9092")
        config = ctor.call_args.args[0]
        self.assertLessEqual(config["queue.buffering.max.messages"], 50_000)

    def test_message_timeout_outlives_the_queue_wait_budget(self):
        """Otherwise librdkafka expires a message while the send loop is still patiently waiting to
        enqueue it — a drop caused by the very mechanism meant to prevent drops."""
        with mock.patch.object(produce, "Producer") as ctor:
            produce.build("broker:9092")
        config = ctor.call_args.args[0]
        budget_ms = produce.MAX_QUEUE_WAITS * produce.QUEUE_POLL_SECONDS * 1000
        self.assertGreater(config["message.timeout.ms"], budget_ms)

    def test_overrides_are_applied(self):
        with mock.patch.object(produce, "Producer") as ctor:
            produce.build("broker:9092", {"acks": "1", "custom": "x"})
        config = ctor.call_args.args[0]
        self.assertEqual(config["acks"], "1")
        self.assertEqual(config["custom"], "x")


if __name__ == "__main__":
    unittest.main()
