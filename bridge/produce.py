"""Kafka producer with the one property that matters: it blocks rather than drops.

The bridge is the only place data can go missing without anything failing. A producer that discards
on a full queue would keep the pod green, keep the cursor advancing, and quietly emit fewer events
than it consumed. Counts are this project's product, so that failure would corrupt the answer while
looking perfectly healthy.

So the queue is bounded, a full queue blocks the read loop, and the cursor only ever advances past a
message the broker has acknowledged.
"""

import logging

from confluent_kafka import KafkaException, Producer

log = logging.getLogger("p01.bridge.produce")

# How long a single produce() attempt waits for queue space before trying again. Short enough that
# shutdown stays responsive, long enough that a brief broker hiccup does not spin.
QUEUE_POLL_SECONDS = 0.5

# Give up after this many consecutive blocked attempts. At QUEUE_POLL_SECONDS each that is a couple
# of minutes of a broker that is up but not accepting writes — a disk-full or under-replicated
# broker. Failing then is right: the cursor has not advanced, so a restart replays from the last
# acknowledged message and nothing is lost.
MAX_QUEUE_WAITS = 240


def build(bootstrap: str, extra: dict | None = None) -> Producer:
    config = {
        "bootstrap.servers": bootstrap,
        # Every in-sync replica must acknowledge. With one broker this is the same as acks=1, but it
        # is the correct setting the day the cluster grows, and getting it wrong then would be a
        # silent durability regression rather than an error.
        "acks": "all",
        "enable.idempotence": True,
        # Bounded on purpose. The default is 100,000 messages, which at ~37/sec is 45 minutes of
        # buffered data that a pod restart would lose. A small queue makes backpressure arrive
        # quickly and visibly.
        "queue.buffering.max.messages": 10_000,
        "queue.buffering.max.kbytes": 32_768,
        # Batch briefly. At 37/sec this trades ~50ms of latency for far fewer requests, and the
        # windows downstream are a minute wide.
        "linger.ms": 50,
        "compression.type": "zstd",
        "retries": 10,
        "retry.backoff.ms": 250,
        # Must exceed the queue wait budget, or librdkafka expires a message that the loop above is
        # still patiently waiting to enqueue.
        "message.timeout.ms": 300_000,
    }
    config.update(extra or {})
    return Producer(config)


class Sink:
    """Produces to Kafka, blocking on backpressure and tracking what has been acknowledged."""

    def __init__(self, producer: Producer, topic: str, dlq_topic: str):
        self._producer = producer
        self._topic = topic
        self._dlq_topic = dlq_topic
        self._failed: list[str] = []

    def _on_delivery(self, err, msg) -> None:
        # Called from poll(). A permanent delivery failure must not be swallowed: the cursor is
        # about to advance past this message, so if it never reached the broker the loop has to
        # find out before that happens.
        if err is not None:
            self._failed.append(str(err))

    def _produce(self, topic: str, value: bytes, key: str | None) -> None:
        waits = 0
        while True:
            try:
                self._producer.produce(
                    topic,
                    value=value,
                    key=key.encode() if key is not None else None,
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:
                # The queue is full: the broker is slower than the firehose. Blocking here is the
                # entire point — it propagates backpressure up to the WebSocket read, so we stop
                # consuming rather than start discarding.
                waits += 1
                if waits >= MAX_QUEUE_WAITS:
                    raise RuntimeError(
                        f"producer queue full for {waits * QUEUE_POLL_SECONDS:.0f}s; "
                        f"the broker is not accepting writes"
                    )
                if waits == 1 or waits % 20 == 0:
                    log.warning(
                        "producer queue full, blocking (%.0fs so far)", waits * QUEUE_POLL_SECONDS
                    )
                self._producer.poll(QUEUE_POLL_SECONDS)

    def send(self, value: bytes, key: str) -> None:
        self._produce(self._topic, value, key)

    def send_dead_letter(self, value: bytes, reason: str) -> None:
        """A frame that could not be parsed. Keyed by nothing, because there is no key to trust."""
        log.warning("dead-lettering a frame: %s", reason)
        self._produce(self._dlq_topic, value, None)

    def poll(self) -> None:
        """Service delivery callbacks. Cheap, and must be called regularly or they never fire."""
        self._producer.poll(0)
        self._raise_if_failed()

    def flush(self, timeout: float = 30.0) -> int:
        """Block until the queue drains. Returns the number of messages still undelivered.

        A non-zero return is the case worth handling rather than logging: it means the cursor must
        not be saved, because messages the loop believes it sent are still in memory and will die
        with the process.
        """
        remaining = self._producer.flush(timeout)
        self._raise_if_failed()
        return remaining

    def _raise_if_failed(self) -> None:
        if self._failed:
            errors = "; ".join(self._failed[:3])
            count = len(self._failed)
            self._failed.clear()
            raise KafkaException(f"{count} message(s) failed delivery: {errors}")
