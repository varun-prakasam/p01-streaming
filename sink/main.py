"""Read Flink's output topics and write them to BigQuery.

Run with `python -m sink.main`. The invariant mirrors the bridge's:

    Kafka offsets are committed only after BigQuery has acknowledged the rows

Which makes duplicates possible on restart and loss impossible. That is the right way round when
counts are the product, and it is why deduplication happens downstream rather than being wished
away here.
"""

import json
import logging
import signal
import sys
import time
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError

from sink import writer as writer_mod
from sink.config import (
    BATCH_MAX_ROWS,
    BATCH_MAX_SECONDS,
    CONSUMER_GROUP,
    HEARTBEAT_SECONDS,
    KAFKA_BOOTSTRAP,
    POLL_TIMEOUT_SECONDS,
    ROUTES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("p01.sink")


def build_consumer(bootstrap: str, group: str, extra: dict | None = None) -> Consumer:
    config = {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        # Offsets are committed by hand, after BigQuery confirms. Automatic commits would advance on
        # a timer regardless of whether the rows landed, which is exactly the loss this design
        # refuses.
        "enable.auto.commit": False,
        # A new consumer group starts at the beginning: Flink's output topics hold the aggregates
        # already computed, and skipping them would leave a hole in the history for no reason.
        "auto.offset.reset": "earliest",
        # Longer than the batch interval, so a slow BigQuery append cannot look like a dead consumer
        # and trigger a rebalance mid-write.
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": 45_000,
    }
    config.update(extra or {})
    return Consumer(config)


class Sink:
    def __init__(self, consumer, writer, routes=None, clock=time.time):
        self._consumer = consumer
        self._writer = writer
        self._routes = routes if routes is not None else ROUTES
        self._clock = clock
        self._stopping = False

        self._batch: dict[str, list] = defaultdict(list)
        self._pending = 0
        self._last_flush = clock()
        self._last_heartbeat = 0.0
        self.written = 0
        self.skipped = 0

    def stop(self) -> None:
        self._stopping = True

    def _should_flush(self) -> bool:
        if not self._pending:
            return False
        if self._pending >= BATCH_MAX_ROWS:
            return True
        # The time bound matters more than the size bound here. Window aggregates arrive once a
        # minute, so without it a batch would wait for hundreds of siblings that never come and the
        # dashboard would lag by however long that took.
        return self._clock() - self._last_flush >= BATCH_MAX_SECONDS

    def flush(self) -> None:
        """Write every buffered batch, then commit offsets.

        Offsets are committed once, after all tables have been written. Committing per table would
        mean a failure partway through advanced past rows still buffered for the tables behind it.
        """
        if not self._pending:
            return

        # No empty-list guard: keys are only created when a row is appended, and the writer no-ops
        # on an empty batch anyway. A branch that cannot be false is a branch nobody can test.
        for topic, rows in self._batch.items():
            self.written += self._writer.append(self._routes[topic], rows)

        self._consumer.commit(asynchronous=False)
        self._batch.clear()
        self._pending = 0
        self._last_flush = self._clock()

    def handle(self, message) -> None:
        topic = message.topic()
        table = self._routes.get(topic)
        if table is None:
            # Subscribed to something unrouted. Not fatal, but it means a topic is being read that
            # nothing will write, which is worth seeing.
            log.warning("no route for topic %s; skipping", topic)
            self.skipped += 1
            return

        try:
            row = json.loads(message.value())
        except (ValueError, TypeError) as exc:
            # Flink produced something unparseable. Skipped rather than fatal — one malformed row
            # must not stop a pipeline whose whole purpose is to keep running — but counted, so the
            # rate is visible.
            log.warning("unparseable row on %s: %s", topic, exc)
            self.skipped += 1
            return

        self._batch[topic].append(row)
        self._pending += 1

    def _heartbeat(self) -> None:
        now = self._clock()
        if now - self._last_heartbeat < HEARTBEAT_SECONDS:
            return
        self._last_heartbeat = now
        log.info("=== alive === written=%d skipped=%d buffered=%d", self.written, self.skipped, self._pending)

    def run(self) -> int:
        self._consumer.subscribe(list(self._routes))
        log.info("consuming %s", ", ".join(sorted(self._routes)))

        while not self._stopping:
            message = self._consumer.poll(POLL_TIMEOUT_SECONDS)
            if message is not None:
                error = message.error()
                if error is not None:
                    # _PARTITION_EOF is informational — it means "caught up", not "broken".
                    if error.code() != KafkaError._PARTITION_EOF:
                        log.warning("kafka error: %s", error)
                else:
                    self.handle(message)

            if self._should_flush():
                self.flush()
            self._heartbeat()

        log.info("stopping; flushing %d buffered row(s)", self._pending)
        self.flush()
        self._consumer.close()
        log.info("stopped after %d rows written, %d skipped", self.written, self.skipped)
        return 0


def main() -> int:
    consumer = build_consumer(KAFKA_BOOTSTRAP, CONSUMER_GROUP)
    sink = Sink(consumer, writer_mod.Writer(writer_mod.build_client()))

    def on_signal(signum, _frame):
        log.info("signal %d received; stopping after the current batch", signum)
        sink.stop()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    return sink.run()


if __name__ == "__main__":
    sys.exit(main())
