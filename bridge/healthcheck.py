"""Confirm the bridge can reach what it needs, before a real run finds out the hard way.

Run inside the cluster:
    kubectl -n p01-streaming exec deploy/p01-bridge -- python -m bridge.healthcheck

Checks the two things that can be wrong without the pod failing to start: whether the broker is
reachable and has the topics the bridge produces to, and whether Jetstream accepts a connection.
Both are network-dependent, so neither belongs in the unit suite — this is the manual counterpart to
it, and the same rule applies as in project 3: a laptop authenticates and routes differently from a
pod, so verify in-cluster before calling anything done.
"""

import sys

import websocket
from confluent_kafka.admin import AdminClient

from bridge.config import (
    CONNECT_TIMEOUT_SECONDS,
    DLQ_TOPIC,
    JETSTREAM_HOSTS,
    KAFKA_BOOTSTRAP,
    TOPIC,
    WANTED_COLLECTIONS,
)
from bridge.main import build_url


def check_kafka() -> bool:
    print(f"broker           : {KAFKA_BOOTSTRAP}")
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    metadata = admin.list_topics(timeout=10)

    brokers = ", ".join(str(b) for b in metadata.brokers.values())
    print(f"brokers reachable: {brokers or 'none'}")

    ok = True
    for topic in (TOPIC, DLQ_TOPIC):
        found = metadata.topics.get(topic)
        if found is None or found.error is not None:
            # Auto-create is off, so a missing topic means the KafkaTopic resource has not
            # reconciled — the bridge would otherwise fail on its first produce.
            print(f"topic {topic:20s}: MISSING")
            ok = False
        else:
            print(f"topic {topic:20s}: {len(found.partitions)} partitions")
    return ok


def check_jetstream() -> bool:
    host = JETSTREAM_HOSTS[0]
    print(f"jetstream        : {host}")
    print(f"collections      : {', '.join(WANTED_COLLECTIONS)}")
    conn = websocket.create_connection(build_url(host, None), timeout=CONNECT_TIMEOUT_SECONDS)
    try:
        # One frame is enough. If the filter matched nothing this blocks until the timeout, which is
        # itself the answer — a wantedCollections typo produces an empty stream, not an error.
        frame = conn.recv()
        print(f"first frame      : {len(frame)} bytes")
        return True
    finally:
        conn.close()


def main() -> int:
    failures = []
    for name, check in (("kafka", check_kafka), ("jetstream", check_jetstream)):
        try:
            if not check():
                failures.append(name)
        except Exception as exc:  # noqa: BLE001 - report every check, do not stop at the first
            print(f"{name} check failed: {exc}")
            failures.append(name)
        print()

    if failures:
        print(f"FAILED: {', '.join(failures)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
