"""Hold one WebSocket to Jetstream and produce what it says into Kafka.

Run with `python -m bridge.main`. The whole program is one loop with one invariant:

    the cursor never advances past a message the broker has not acknowledged

Everything else — the reconnect backoff, the host rotation, the bounded producer queue — exists to
keep that invariant true while the process stays up. Get it backwards and the failure is silent: the
pod stays green and the numbers are simply lower than reality.
"""

import itertools
import logging
import signal
import sys
import time
from urllib.parse import urlencode

import websocket

from bridge import cursor as cursor_mod
from bridge import parse, produce
from bridge.config import (
    CONNECT_TIMEOUT_SECONDS,
    CURSOR_FLUSH_SECONDS,
    CURSOR_PATH,
    DLQ_TOPIC,
    HEARTBEAT_SECONDS,
    JETSTREAM_HOSTS,
    KAFKA_BOOTSTRAP,
    READ_TIMEOUT_SECONDS,
    RECONNECT_BACKOFF_MAX_SECONDS,
    RECONNECT_BACKOFF_SECONDS,
    TOPIC,
    WANTED_COLLECTIONS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("p01.bridge")


def build_url(host: str, resume: int | None) -> str:
    query = [("wantedCollections", c) for c in WANTED_COLLECTIONS]
    if resume is not None:
        query.append(("cursor", str(resume)))
    return f"wss://{host}/subscribe?{urlencode(query)}"


class Bridge:
    """The read-produce-commit loop.

    Collaborators are injected rather than constructed here — the socket factory, the sink, the
    clock and the sleep — because every interesting behaviour in this class is a timing or ordering
    decision, and none of them are testable against a real socket.
    """

    def __init__(
        self,
        sink,
        connect,
        cursor_path: str = CURSOR_PATH,
        hosts=None,
        clock=time.time,
        sleep=time.sleep,
    ):
        self._sink = sink
        self._connect = connect
        self._cursor_path = cursor_path
        self._hosts = itertools.cycle(hosts or JETSTREAM_HOSTS)
        self._clock = clock
        self._sleep_fn = sleep
        self._stopping = False

        self.committed: int | None = cursor_mod.load(cursor_path)
        self.pending: int | None = None
        self._last_flush = 0.0
        self._last_heartbeat = 0.0
        self.events = 0
        self.dead_lettered = 0

    def stop(self) -> None:
        self._stopping = True

    # -- cursor ------------------------------------------------------------------------------------

    def _commit(self, force: bool = False) -> None:
        """Persist the cursor, but only behind a successful flush.

        The ordering is the invariant. flush() first, so every message up to `pending` has been
        acknowledged by the broker; only then write the position. Reverse them and a crash between
        the two loses whatever was still queued while recording that it was delivered.
        """
        if self.pending is None or self.pending == self.committed:
            return
        if not force and self._clock() - self._last_flush < CURSOR_FLUSH_SECONDS:
            return

        remaining = self._sink.flush()
        if remaining:
            # Do not advance. Those messages are still in memory and will die with the process;
            # saying otherwise is a lie the next restart would act on.
            log.error("%d message(s) undelivered at flush; not advancing the cursor", remaining)
            return

        cursor_mod.save(self._cursor_path, self.pending)
        self.committed = self.pending
        self._last_flush = self._clock()

    def _heartbeat(self) -> None:
        now = self._clock()
        if now - self._last_heartbeat < HEARTBEAT_SECONDS:
            return
        self._last_heartbeat = now
        # The line the platform's staleness alert watches for. A streaming bridge has no natural
        # "done", so it reports liveness on a schedule instead.
        log.info(
            "=== alive === events=%d dead_lettered=%d cursor=%s",
            self.events,
            self.dead_lettered,
            self.committed,
        )

    # -- messages ----------------------------------------------------------------------------------

    def handle(self, raw: bytes) -> None:
        try:
            event = parse.parse(raw)
        except parse.Undeliverable as exc:
            self._sink.send_dead_letter(raw, str(exc))
            self.dead_lettered += 1
            return

        if event is None:
            return

        self._sink.send(event.raw, event.key)
        self.pending = event.time_us
        self.events += 1

    # -- connection --------------------------------------------------------------------------------

    def run_once(self) -> int:
        """One connection, held until it drops or we are asked to stop.

        Returns how many events this connection delivered. The caller uses that to decide whether
        the connection was healthy, which cannot be inferred from how it ended: a socket that
        streamed happily for six hours and a host that rejects us both terminate by raising.
        """
        delivered = 0
        resume = cursor_mod.resume_from(self.committed, now=self._clock())
        host = next(self._hosts)
        log.info("connecting to %s (%s)", host, "live tail" if resume is None else f"cursor {resume}")

        conn = self._connect(build_url(host, resume))
        checked_gap = False
        try:
            while not self._stopping:
                raw = conn.recv()

                # recv() returns "" for a control frame it handled internally. That is not a
                # disconnect — a real close raises — so reconnecting here would drop a healthy
                # socket on the first ping.
                if raw == "" or raw is None:
                    continue
                if isinstance(raw, str):
                    raw = raw.encode()

                if not checked_gap:
                    checked_gap = True
                    self._check_gap(resume, raw)

                self.handle(raw)
                delivered += 1
                self._sink.poll()
                self._commit()
                self._heartbeat()
        finally:
            # Whatever happens on the way out — dropped socket, SIGTERM, exception — settle what is
            # already queued while the connection state still means something.
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - closing a broken socket must not mask the real error
                pass
            self._commit(force=True)

        return delivered

    def _check_gap(self, resume: int | None, raw: bytes) -> None:
        """Compare where we asked to resume against where the server actually started us."""
        try:
            first = parse.parse(raw)
        except parse.Undeliverable:
            return
        if first is not None:
            cursor_mod.report_gap(resume, first.time_us)

    def run(self) -> int:
        backoff = RECONNECT_BACKOFF_SECONDS
        while not self._stopping:
            # Measured across the call rather than taken from its return value, because the normal
            # way a connection ends is by raising — so a return value only arrives on the path that
            # almost never runs. Two versions of this reset were dead code before a test caught it.
            before = self.events
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001 - a bridge that exits on a blip is useless
                if self._stopping:
                    break
                log.warning("connection failed (%s); reconnecting in %.0fs", exc, backoff)
                self._interruptible_sleep(backoff)

            if self.events > before:
                # The connection did real work. Reset, so hours of healthy streaming followed by a
                # single dropped socket reconnects immediately instead of inheriting an escalation
                # from whatever went wrong last night.
                backoff = RECONNECT_BACKOFF_SECONDS
            else:
                backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_SECONDS)

        log.info("shutting down; flushing")
        self._commit(force=True)
        log.info("stopped after %d events, %d dead-lettered", self.events, self.dead_lettered)
        return 0

    def _interruptible_sleep(self, seconds: float) -> None:
        """Sleep in slices, so SIGTERM is not stuck behind a 60-second backoff."""
        deadline = self._clock() + seconds
        while self._clock() < deadline and not self._stopping:
            self._sleep_fn(0.1)


def connect(url: str):
    """Open the socket with a short connect budget, then a long read budget.

    websocket-client has one timeout covering both, so the two are set in sequence. Failing over a
    dead host in ten seconds while tolerating a minute of quiet on a healthy one needs both, and
    `create_connection(connect_timeout=...)` does not exist — it lands in **options and is ignored.
    """
    conn = websocket.create_connection(
        url,
        timeout=CONNECT_TIMEOUT_SECONDS,
        header={"User-Agent": "p01-streaming-bridge/1.0"},
    )
    conn.settimeout(READ_TIMEOUT_SECONDS)
    return conn


def main() -> int:
    producer = produce.build(KAFKA_BOOTSTRAP)
    sink = produce.Sink(producer, TOPIC, DLQ_TOPIC)
    bridge = Bridge(sink, connect)

    def on_signal(signum, _frame):
        log.info("signal %d received; stopping after the current message", signum)
        bridge.stop()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    return bridge.run()


if __name__ == "__main__":
    sys.exit(main())
