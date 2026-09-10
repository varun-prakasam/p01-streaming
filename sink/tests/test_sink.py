"""The consume-write-commit loop.

The invariant mirrors the bridge's, and breaking it fails the same silent way:

    Kafka offsets are committed only after BigQuery has acknowledged the rows

Commit first and a crash between the two advances past rows that never landed. Nothing errors; the
counts simply stop matching the stream, which is indistinguishable from a quiet hour on Bluesky.
"""

import json
import unittest
from unittest import mock

from confluent_kafka import KafkaError

from sink import main
from sink.writer import WriteFailed

ROUTES = {
    "bsky.raw.v1": "proj.raw.posts",
    "bsky.counts.fast.v1": "proj.curated.fast",
}


class FakeMessage:
    def __init__(self, topic="bsky.raw.v1", value=None, error=None):
        self._topic = topic
        self._value = value if value is not None else json.dumps({"uri": "at://x"}).encode()
        self._error = error

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    """Serves a scripted list of poll results, then None forever."""

    def __init__(self, messages):
        self._messages = list(messages)
        self.commits = 0
        self.closed = False
        self.subscribed = None

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, _timeout):
        return self._messages.pop(0) if self._messages else None

    def commit(self, asynchronous=False):
        self.commits += 1

    def close(self):
        self.closed = True


class FakeWriter:
    def __init__(self, fail_on=None):
        self.appended = []
        self.fail_on = fail_on

    def append(self, table, rows):
        if self.fail_on is not None and table == self.fail_on:
            raise WriteFailed(f"{table}: rejected")
        self.appended.append((table, list(rows)))
        return len(rows)


class SinkTestCase(unittest.TestCase):
    def setUp(self):
        self.now = 1000.0

    def clock(self):
        return self.now

    def build(self, messages, writer=None, stop_after=None):
        consumer = FakeConsumer(messages)
        sink = main.Sink(consumer, writer or FakeWriter(), routes=ROUTES, clock=self.clock)
        # Stop once the scripted messages are exhausted, so run() terminates.
        original = consumer.poll

        def poll(timeout):
            msg = original(timeout)
            if msg is None:
                sink.stop()
            return msg

        consumer.poll = poll
        return sink, consumer


class OrderingTest(SinkTestCase):
    def test_offsets_are_committed_only_after_a_successful_write(self):
        writer = FakeWriter()
        sink, consumer = self.build([FakeMessage()], writer=writer)
        sink.run()
        self.assertEqual(len(writer.appended), 1)
        self.assertGreater(consumer.commits, 0)

    def test_a_write_failure_prevents_the_commit(self):
        """The case that decides whether loss is possible. Committing here would advance past rows
        BigQuery rejected, and nothing downstream would ever know they were missing."""
        writer = FakeWriter(fail_on="proj.raw.posts")
        sink, consumer = self.build([FakeMessage()], writer=writer)
        with self.assertRaises(WriteFailed):
            sink.run()
        self.assertEqual(consumer.commits, 0, "committed offsets for rows that were rejected")

    def test_offsets_are_committed_once_for_the_whole_batch(self):
        """Committing per table would advance past rows still buffered for the tables behind it if
        one of them failed partway through."""
        writer = FakeWriter()
        sink, consumer = self.build(
            [
                FakeMessage(topic="bsky.raw.v1"),
                FakeMessage(topic="bsky.counts.fast.v1", value=b'{"events": 5}'),
            ],
            writer=writer,
        )
        sink.run()
        self.assertEqual(len(writer.appended), 2, "both tables written")
        self.assertEqual(consumer.commits, 1, "committed more than once for one batch")

    def test_nothing_buffered_means_nothing_committed(self):
        sink, consumer = self.build([])
        sink.run()
        self.assertEqual(consumer.commits, 0)


class BatchingTest(SinkTestCase):
    def test_a_full_batch_flushes_immediately(self):
        writer = FakeWriter()
        messages = [FakeMessage() for _ in range(main.BATCH_MAX_ROWS)]
        sink, consumer = self.build(messages, writer=writer)
        sink.run()
        self.assertEqual(sum(len(r) for _t, r in writer.appended), main.BATCH_MAX_ROWS)

    def test_a_partial_batch_flushes_on_time(self):
        """Window aggregates arrive once a minute. Without the time bound a batch would wait for
        hundreds of siblings that never arrive, and the dashboard would lag by exactly that long."""
        writer = FakeWriter()
        sink, consumer = self.build([FakeMessage()], writer=writer)
        sink._pending = 1
        sink._batch["bsky.raw.v1"] = [{"uri": "at://x"}]
        self.assertFalse(sink._should_flush(), "flushed before the interval elapsed")
        self.now += main.BATCH_MAX_SECONDS + 1
        self.assertTrue(sink._should_flush(), "did not flush after the interval elapsed")

    def test_the_flush_boundary(self):
        sink, _ = self.build([])
        sink._pending = 1
        self.now += main.BATCH_MAX_SECONDS - 0.01
        self.assertFalse(sink._should_flush())
        self.now += 0.02
        self.assertTrue(sink._should_flush())

    def test_an_empty_buffer_never_flushes_on_time_alone(self):
        """Otherwise a quiet topic would commit offsets on a timer with nothing written."""
        sink, _ = self.build([])
        self.now += main.BATCH_MAX_SECONDS * 10
        self.assertFalse(sink._should_flush())

    def test_remaining_rows_are_flushed_on_shutdown(self):
        """SIGTERM must not discard a partial batch — those rows are already consumed from Kafka."""
        writer = FakeWriter()
        sink, consumer = self.build([FakeMessage()], writer=writer)
        sink.run()
        self.assertEqual(sum(len(r) for _t, r in writer.appended), 1)
        self.assertTrue(consumer.closed)


class MessageHandlingTest(SinkTestCase):
    def test_unparseable_rows_are_skipped_not_fatal(self):
        """One malformed row must not stop a pipeline whose entire purpose is to keep running."""
        writer = FakeWriter()
        sink, _ = self.build([FakeMessage(value=b"{not json"), FakeMessage()], writer=writer)
        sink.run()
        self.assertEqual(sink.skipped, 1)
        self.assertEqual(sum(len(r) for _t, r in writer.appended), 1)

    def test_an_unrouted_topic_is_skipped_and_counted(self):
        writer = FakeWriter()
        sink, _ = self.build([FakeMessage(topic="bsky.unknown.v1")], writer=writer)
        sink.run()
        self.assertEqual(sink.skipped, 1)
        self.assertEqual(writer.appended, [])

    def test_partition_eof_is_not_an_error(self):
        """_PARTITION_EOF means caught up, not broken. Logging it as an error would make a healthy
        idle consumer look like a failing one."""
        eof = mock.Mock()
        eof.code.return_value = KafkaError._PARTITION_EOF
        sink, _ = self.build([FakeMessage(error=eof)])
        with self.assertNoLogs("p01.sink", level="WARNING"):
            sink.run()

    def test_a_real_kafka_error_is_logged(self):
        err = mock.Mock()
        err.code.return_value = KafkaError.BROKER_NOT_AVAILABLE
        sink, _ = self.build([FakeMessage(error=err)])
        with self.assertLogs("p01.sink", level="WARNING"):
            sink.run()

    def test_rows_are_routed_to_the_table_for_their_topic(self):
        writer = FakeWriter()
        sink, _ = self.build(
            [FakeMessage(topic="bsky.counts.fast.v1", value=b'{"events": 3}')], writer=writer
        )
        sink.run()
        table, rows = writer.appended[0]
        self.assertEqual(table, "proj.curated.fast")
        self.assertEqual(rows, [{"events": 3}])

    def test_it_subscribes_to_every_routed_topic(self):
        sink, consumer = self.build([])
        sink.run()
        self.assertEqual(sorted(consumer.subscribed), sorted(ROUTES))


class ConsumerConfigTest(unittest.TestCase):
    def test_auto_commit_is_off(self):
        """Automatic commits advance on a timer regardless of whether the rows landed, which is
        exactly the loss this design refuses."""
        with mock.patch.object(main, "Consumer") as ctor:
            main.build_consumer("broker:9092", "grp")
        config = ctor.call_args.args[0]
        self.assertFalse(config["enable.auto.commit"])

    def test_a_new_group_starts_from_the_beginning(self):
        """Flink's output topics already hold computed aggregates; starting at the tail would leave
        a hole in the history for no reason."""
        with mock.patch.object(main, "Consumer") as ctor:
            main.build_consumer("broker:9092", "grp")
        self.assertEqual(ctor.call_args.args[0]["auto.offset.reset"], "earliest")

    def test_poll_interval_outlives_a_slow_write(self):
        """Otherwise a slow BigQuery append looks like a dead consumer and triggers a rebalance in
        the middle of writing."""
        with mock.patch.object(main, "Consumer") as ctor:
            main.build_consumer("broker:9092", "grp")
        config = ctor.call_args.args[0]
        self.assertGreater(config["max.poll.interval.ms"] / 1000, main.BATCH_MAX_SECONDS * 10)


class MainTest(unittest.TestCase):
    def test_main_installs_signal_handlers(self):
        handlers = {}
        with mock.patch.object(main, "build_consumer"), mock.patch.object(
            main.writer_mod, "Writer"
        ), mock.patch.object(main.writer_mod, "build_client"), mock.patch.object(
            main, "Sink"
        ) as sink_cls, mock.patch.object(
            main.signal, "signal", side_effect=lambda s, f: handlers.__setitem__(s, f)
        ):
            sink_cls.return_value.run.return_value = 0
            self.assertEqual(main.main(), 0)
        self.assertIn(main.signal.SIGTERM, handlers)
        handlers[main.signal.SIGTERM](main.signal.SIGTERM, None)
        sink_cls.return_value.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
