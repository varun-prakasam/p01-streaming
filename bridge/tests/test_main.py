"""The read-produce-commit loop.

One invariant holds the project together:

    the cursor never advances past a message the broker has not acknowledged

Most of these tests exist to pin that ordering, because breaking it produces no error. The pod stays
green, the socket stays connected, and the only symptom is that the numbers are lower than reality —
which is indistinguishable from a quiet day on Bluesky.
"""

import json
import os
import tempfile
import unittest
from unittest import mock

from bridge import cursor as cursor_mod
from bridge import main

US = 1_000_000


def frame(time_us: int, did: str = "did:plc:abc") -> bytes:
    return json.dumps(
        {
            "did": did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "rev": "r",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "k",
                "record": {"$type": "app.bsky.feed.post", "createdAt": "2026-09-10T10:00:00.000Z"},
            },
        }
    ).encode()


class Dropped(Exception):
    """The socket died. Named so the tests read as what they simulate."""


class FakeConn:
    """Serves a scripted list of frames, then does whatever `then` says."""

    def __init__(self, frames, then=Dropped):
        self._frames = list(frames)
        self._then = then
        self.closed = False

    def recv(self):
        if self._frames:
            return self._frames.pop(0)
        if isinstance(self._then, type) and issubclass(self._then, BaseException):
            raise self._then("socket closed")
        return self._then()

    def close(self):
        self.closed = True


class FakeSink:
    def __init__(self, undelivered=0):
        self.sent = []
        self.dead_lettered = []
        self.flushes = 0
        self.undelivered = undelivered

    def send(self, value, key):
        self.sent.append((value, key))

    def send_dead_letter(self, value, reason):
        self.dead_lettered.append((value, reason))

    def poll(self):
        pass

    def flush(self, timeout=30.0):
        self.flushes += 1
        return self.undelivered


class BridgeTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "cursor")
        self.now = 1_789_000_000.0
        self.sink = FakeSink()
        # Patch the flush interval to zero so a commit is attempted on every message; the interval
        # itself is tested separately rather than slept through here.
        patcher = mock.patch.object(main, "CURSOR_FLUSH_SECONDS", 0)
        patcher.start()
        self.addCleanup(patcher.stop)

    def clock(self):
        return self.now

    def sleep(self, seconds):
        # Time only moves when the code under test sleeps. A frozen clock makes
        # _interruptible_sleep spin forever, which is how this suite first hung.
        self.now += seconds

    def build(self, frames, then=Dropped, sink=None, hosts=None):
        conn = FakeConn(frames, then)
        bridge = main.Bridge(
            sink or self.sink,
            lambda url: conn,
            cursor_path=self.path,
            hosts=hosts or ["host-a", "host-b"],
            clock=self.clock,
            sleep=self.sleep,
        )
        return bridge, conn


class CursorOrderingTest(BridgeTestCase):
    def test_cursor_advances_only_behind_a_successful_flush(self):
        bridge, _ = self.build([frame(100 * US)])
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertEqual(cursor_mod.load(self.path), 100 * US)
        self.assertGreater(self.sink.flushes, 0, "committed without flushing")

    def test_cursor_does_not_advance_when_messages_are_undelivered(self):
        """The case that decides whether data loss is possible. Those messages are still in memory
        and will die with the process; recording them as delivered is a lie the next restart acts
        on by resuming past data that never arrived."""
        sink = FakeSink(undelivered=3)
        bridge, _ = self.build([frame(100 * US)], sink=sink)
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertIsNone(cursor_mod.load(self.path), "cursor advanced past undelivered messages")
        self.assertIsNone(bridge.committed)

    def test_cursor_tracks_the_latest_message(self):
        bridge, _ = self.build([frame(100 * US), frame(200 * US), frame(300 * US)])
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertEqual(cursor_mod.load(self.path), 300 * US)

    def test_the_cursor_is_committed_on_the_way_out_even_when_the_socket_dies(self):
        """A dropped socket must not discard position already earned."""
        bridge, conn = self.build([frame(100 * US)])
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertTrue(conn.closed)
        self.assertEqual(cursor_mod.load(self.path), 100 * US)

    def test_no_messages_means_no_cursor_write(self):
        bridge, _ = self.build([])
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertIsNone(cursor_mod.load(self.path))

    def test_commit_is_rate_limited(self):
        """Flushing per message would be a synchronous broker round-trip per event.

        Two flushes for three messages, not three: the first commit is not suppressed (there is no
        previous flush to be too close to), the next two are, and the forced commit on the way out
        catches up. Committing eagerly and then throttling is the right order — it means a bridge
        that dies after one message still remembers that message."""
        with mock.patch.object(main, "CURSOR_FLUSH_SECONDS", 1000):
            bridge, _ = self.build([frame(100 * US), frame(200 * US), frame(300 * US)])
            with self.assertRaises(Dropped):
                bridge.run_once()
        self.assertEqual(self.sink.flushes, 2)
        self.assertEqual(cursor_mod.load(self.path), 300 * US)

    def test_recommitting_an_unchanged_cursor_is_a_no_op(self):
        bridge, _ = self.build([frame(100 * US)])
        with self.assertRaises(Dropped):
            bridge.run_once()
        flushes = self.sink.flushes
        bridge._commit(force=True)
        self.assertEqual(self.sink.flushes, flushes, "flushed with nothing to commit")


class FrameHandlingTest(BridgeTestCase):
    def test_control_frames_do_not_end_the_connection(self):
        """recv() returns "" for a control frame it handled internally. Treating that as a
        disconnect would drop a healthy socket on the first ping Jetstream sends."""
        bridge, conn = self.build(["", frame(100 * US), ""])
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertEqual(len(self.sink.sent), 1)
        self.assertEqual(cursor_mod.load(self.path), 100 * US)

    def test_text_frames_are_encoded(self):
        bridge, _ = self.build([frame(100 * US).decode()])
        with self.assertRaises(Dropped):
            bridge.run_once()
        value, _key = self.sink.sent[0]
        self.assertIsInstance(value, bytes)

    def test_unparseable_frames_are_dead_lettered_not_fatal(self):
        bridge, _ = self.build([b"{garbage", frame(100 * US)])
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertEqual(len(self.sink.dead_lettered), 1)
        self.assertEqual(len(self.sink.sent), 1)
        self.assertEqual(bridge.dead_lettered, 1)

    def test_a_dead_lettered_frame_does_not_move_the_cursor(self):
        """It has no trustworthy time_us, so there is no position it could represent."""
        bridge, _ = self.build([b"{garbage"])
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertIsNone(cursor_mod.load(self.path))

    def test_skipped_kinds_are_neither_sent_nor_dead_lettered(self):
        envelope = json.loads(frame(100 * US))
        envelope["kind"] = "identity"
        bridge, _ = self.build([json.dumps(envelope).encode()])
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertEqual(self.sink.sent, [])
        self.assertEqual(self.sink.dead_lettered, [])

    def test_the_partition_key_is_the_author(self):
        bridge, _ = self.build([frame(100 * US, did="did:plc:xyz")])
        with self.assertRaises(Dropped):
            bridge.run_once()
        _value, key = self.sink.sent[0]
        self.assertEqual(key, "did:plc:xyz")


class GapDetectionTest(BridgeTestCase):
    def test_the_gap_is_checked_once_against_the_first_frame(self):
        cursor_mod.save(self.path, int((self.now - 100) * US))
        bridge, _ = self.build([frame(int(self.now * US)), frame(int(self.now * US) + 1)])
        with mock.patch.object(cursor_mod, "report_gap", return_value=0) as report:
            with self.assertRaises(Dropped):
                bridge.run_once()
        self.assertEqual(report.call_count, 1, "gap checked more than once per connection")

    def test_an_unparseable_first_frame_does_not_break_the_check(self):
        cursor_mod.save(self.path, int((self.now - 100) * US))
        bridge, _ = self.build([b"{garbage", frame(int(self.now * US))])
        with self.assertRaises(Dropped):
            bridge.run_once()
        self.assertEqual(len(self.sink.dead_lettered), 1)


class ReconnectTest(BridgeTestCase):
    def test_run_reconnects_and_stops_on_signal(self):
        conns = [FakeConn([frame(100 * US)]), FakeConn([frame(200 * US)])]
        made = []

        def connect(url):
            made.append(url)
            if len(made) > 2:
                bridge.stop()
                return FakeConn([], then=Dropped)
            return conns[len(made) - 1]

        bridge = main.Bridge(
            self.sink,
            connect,
            cursor_path=self.path,
            hosts=["host-a", "host-b"],
            clock=self.clock,
            sleep=self.sleep,
        )
        self.assertEqual(bridge.run(), 0)
        self.assertGreaterEqual(len(made), 2)
        self.assertEqual(cursor_mod.load(self.path), 200 * US)

    def test_hosts_rotate_across_reconnects(self):
        """A single-instance Jetstream outage should become a retry, not an outage of our own."""
        made = []

        def connect(url):
            made.append(url)
            if len(made) >= 3:
                bridge.stop()
            return FakeConn([], then=Dropped)

        bridge = main.Bridge(
            self.sink,
            connect,
            cursor_path=self.path,
            hosts=["host-a", "host-b"],
            clock=self.clock,
            sleep=self.sleep,
        )
        bridge.run()
        self.assertIn("host-a", made[0])
        self.assertIn("host-b", made[1])

    def test_backoff_grows_between_attempts_and_is_capped(self):
        """A Jetstream outage must back off rather than crash-loop hot against the node's CPU, and
        the backoff must stop growing or a brief blip becomes an hour of silence."""
        waits = []
        attempts = []

        def connect(_url):
            attempts.append(self.now)
            if len(attempts) > 5:
                bridge.stop()
            return FakeConn([], then=Dropped)

        def sleep(seconds):
            self.now += seconds
            waits.append(seconds)

        bridge = main.Bridge(
            self.sink,
            connect,
            cursor_path=self.path,
            hosts=["h"],
            clock=self.clock,
            sleep=sleep,
        )
        with mock.patch.object(main, "RECONNECT_BACKOFF_MAX_SECONDS", 4):
            bridge.run()

        # _interruptible_sleep slices each wait, so the wall time between attempts is the signal.
        gaps = [round(b - a, 3) for a, b in zip(attempts, attempts[1:])]
        self.assertGreater(gaps[1], gaps[0], "backoff did not grow")
        self.assertLessEqual(max(gaps), 4.5, "backoff exceeded the cap")

    def test_backoff_resets_after_a_healthy_connection(self):
        """Otherwise an hour of intermittent blips leaves the bridge sleeping for a minute at a
        time even though it is reconnecting successfully."""
        attempts = []

        def connect(_url):
            attempts.append(self.now)
            if len(attempts) > 4:
                bridge.stop()
            # Every connection delivers before dropping, so the backoff must never escalate.
            return FakeConn([frame(len(attempts) * US)], then=Dropped)

        bridge = main.Bridge(
            self.sink,
            connect,
            cursor_path=self.path,
            hosts=["h"],
            clock=self.clock,
            sleep=self.sleep,
        )
        bridge.run()
        gaps = [round(b - a, 3) for a, b in zip(attempts, attempts[1:])]
        # One base backoff, plus at most one 0.1s slice of overshoot from the interruptible sleep.
        self.assertLessEqual(max(gaps), main.RECONNECT_BACKOFF_SECONDS + 0.2,
                             f"backoff escalated across healthy reconnects: {gaps}")

    def test_stopping_during_backoff_does_not_wait_it_out(self):
        """SIGTERM must not sit behind a 60-second sleep — Kubernetes sends SIGKILL after 30."""
        bridge = main.Bridge(
            self.sink,
            lambda _u: FakeConn([], then=Dropped),
            cursor_path=self.path,
            hosts=["h"],
            clock=self.clock,
            sleep=lambda _s: bridge.stop(),
        )
        bridge.run()


class UrlTest(unittest.TestCase):
    def test_live_tail_has_no_cursor(self):
        url = main.build_url("example.net", None)
        self.assertNotIn("cursor", url)
        self.assertIn("wantedCollections=app.bsky.feed.post", url)

    def test_resume_includes_the_cursor(self):
        url = main.build_url("example.net", 1789036937215397)
        self.assertIn("cursor=1789036937215397", url)

    def test_it_is_a_secure_websocket_url(self):
        self.assertTrue(main.build_url("example.net", None).startswith("wss://"))

    def test_every_wanted_collection_is_a_separate_parameter(self):
        """Jetstream expects repeated wantedCollections, not a comma-joined list. Joining them
        yields one filter that matches nothing, and an empty stream reads as a quiet day."""
        with mock.patch.object(main, "WANTED_COLLECTIONS", ["a.b.c", "d.e.f"]):
            url = main.build_url("example.net", None)
        self.assertEqual(url.count("wantedCollections="), 2)


class HeartbeatTest(BridgeTestCase):
    def test_a_liveness_line_is_emitted_on_schedule(self):
        """The platform's staleness alert watches for this. A streaming bridge has no natural
        'done', so silence has to be distinguishable from health some other way."""
        bridge, _ = self.build([frame(100 * US)])
        with mock.patch.object(main, "HEARTBEAT_SECONDS", 0):
            with self.assertLogs("p01.bridge", level="INFO") as captured:
                with self.assertRaises(Dropped):
                    bridge.run_once()
        self.assertTrue(any("=== alive ===" in line for line in captured.output))

    def test_the_heartbeat_is_rate_limited(self):
        bridge, _ = self.build([frame(i * US) for i in range(1, 6)])
        with mock.patch.object(main, "HEARTBEAT_SECONDS", 1000):
            with self.assertLogs("p01.bridge", level="INFO") as captured:
                with self.assertRaises(Dropped):
                    bridge.run_once()
        beats = [line for line in captured.output if "=== alive ===" in line]
        self.assertLessEqual(len(beats), 1)


if __name__ == "__main__":
    unittest.main()


class ShutdownAndEntryPointTest(BridgeTestCase):
    def test_a_broken_socket_close_does_not_mask_the_real_failure(self):
        """close() on an already-dead socket can itself raise. The interesting error is the one
        that killed the connection, not the one from tidying up after it."""

        class UncloseableConn(FakeConn):
            def close(self):
                raise OSError("socket already gone")

        conn = UncloseableConn([frame(100 * US)])
        bridge = main.Bridge(
            self.sink,
            lambda _u: conn,
            cursor_path=self.path,
            hosts=["h"],
            clock=self.clock,
            sleep=self.sleep,
        )
        with self.assertRaises(Dropped):
            bridge.run_once()
        # The cursor still committed, because the close failure did not abort the finally block.
        self.assertEqual(cursor_mod.load(self.path), 100 * US)

    def test_stopping_while_a_connection_is_failing_exits_promptly(self):
        """SIGTERM during a reconnect storm must not wait out the backoff. Kubernetes sends SIGKILL
        30 seconds after SIGTERM, and the backoff caps at 60."""
        attempts = []

        def connect(_url):
            attempts.append(1)
            bridge.stop()
            raise Dropped("refused")

        bridge = main.Bridge(
            self.sink,
            connect,
            cursor_path=self.path,
            hosts=["h"],
            clock=self.clock,
            sleep=self.sleep,
        )
        self.assertEqual(bridge.run(), 0)
        self.assertEqual(len(attempts), 1, "kept retrying after being told to stop")


class ConnectTest(unittest.TestCase):
    def test_connect_uses_a_short_connect_budget_then_a_long_read_budget(self):
        """websocket-client has one timeout covering both, so they are set in sequence. Failing
        over a dead host in ten seconds while tolerating a minute of quiet on a healthy one needs
        both, and create_connection(connect_timeout=...) does not exist — it would land silently in
        **options and be ignored."""
        conn = mock.Mock()
        with mock.patch.object(main.websocket, "create_connection", return_value=conn) as create:
            returned = main.connect("wss://example.net/subscribe")
        self.assertIs(returned, conn)
        self.assertEqual(create.call_args.kwargs["timeout"], main.CONNECT_TIMEOUT_SECONDS)
        conn.settimeout.assert_called_once_with(main.READ_TIMEOUT_SECONDS)

    def test_the_read_budget_is_longer_than_the_connect_budget(self):
        self.assertGreater(main.READ_TIMEOUT_SECONDS, main.CONNECT_TIMEOUT_SECONDS)


class MainTest(unittest.TestCase):
    def test_main_wires_the_sink_and_installs_signal_handlers(self):
        """A container that ignores SIGTERM is killed nine seconds later mid-write. The handler is
        what turns a stop into a flush-and-commit rather than a lost queue."""
        handlers = {}

        with mock.patch.object(main.produce, "build") as build, mock.patch.object(
            main.produce, "Sink"
        ) as sink_cls, mock.patch.object(main, "Bridge") as bridge_cls, mock.patch.object(
            main.signal, "signal", side_effect=lambda sig, fn: handlers.__setitem__(sig, fn)
        ):
            bridge_cls.return_value.run.return_value = 0
            self.assertEqual(main.main(), 0)

        build.assert_called_once_with(main.KAFKA_BOOTSTRAP)
        sink_cls.assert_called_once_with(build.return_value, main.TOPIC, main.DLQ_TOPIC)
        self.assertIn(main.signal.SIGTERM, handlers)
        self.assertIn(main.signal.SIGINT, handlers)

        # The handler must ask the bridge to stop rather than exiting the process itself.
        handlers[main.signal.SIGTERM](main.signal.SIGTERM, None)
        bridge_cls.return_value.stop.assert_called_once()
