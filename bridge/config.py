"""Runtime configuration, all overridable by environment variable.

Every value here is mirrored in k8s/base/configmap.yaml, and a test asserts the two agree. A default
that drifts from the deployed ConfigMap is a setting nobody can reason about: the code says one
thing, the cluster does another, and neither is wrong on its own.
"""

import os

# --- Source ---------------------------------------------------------------------------------------

# Jetstream publishes four interchangeable public instances. Rotating through them on reconnect
# turns a single-instance outage into a retry rather than an outage of our own.
JETSTREAM_HOSTS = [
    h.strip()
    for h in os.environ.get(
        "P01_JETSTREAM_HOSTS",
        "jetstream2.us-east.bsky.network,jetstream1.us-east.bsky.network,"
        "jetstream2.us-west.bsky.network,jetstream1.us-west.bsky.network",
    ).split(",")
    if h.strip()
]

# Server-side filtering, and the single most consequential setting in the project. Unfiltered the
# firehose is ~500 events/sec, overwhelmingly likes and follows; filtered to posts it measures ~37.
# That is 14x less Cloud NAT egress, broker disk and TaskManager work, for the only collection the
# analysis reads.
WANTED_COLLECTIONS = [
    c.strip()
    for c in os.environ.get("P01_WANTED_COLLECTIONS", "app.bsky.feed.post").split(",")
    if c.strip()
]

# --- Destination ----------------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("P01_KAFKA_BOOTSTRAP", "kafka-kafka-bootstrap.p01-streaming:9092")
TOPIC = os.environ.get("P01_TOPIC", "bsky.posts.v1")
DLQ_TOPIC = os.environ.get("P01_DLQ_TOPIC", "bsky.posts.dlq.v1")

# --- Cursor and replay ----------------------------------------------------------------------------

CURSOR_PATH = os.environ.get("P01_CURSOR_PATH", "/var/lib/bridge/cursor")

# Deliberate overlap on reconnect. Replaying a few seconds guarantees duplicates, which the
# downstream dedup removes; not replaying them would risk a gap, which nothing can recover. Counts
# are the product here, so the asymmetry is the whole argument.
REPLAY_OVERLAP_SECONDS = int(os.environ.get("P01_REPLAY_OVERLAP_SECONDS", "5"))

# A cursor older than this is abandoned in favour of the live tail. Reconnecting after a long outage
# with an ancient cursor replays at many times realtime and floods the broker; past a point the
# freshest data matters more than the backlog.
MAX_REPLAY_SECONDS = int(os.environ.get("P01_MAX_REPLAY_SECONDS", "3600"))

# Measured, not guessed: Jetstream serves roughly 36 hours of history. Beyond that it does not error
# — it silently starts from the oldest event it still holds, so a longer outage looks exactly like a
# clean reconnect. The bridge compares what it asked for against what it got and reports the gap.
JETSTREAM_RETENTION_SECONDS = int(os.environ.get("P01_JETSTREAM_RETENTION_SECONDS", str(36 * 3600)))

# How often the cursor is written. Every message would be a synchronous disk write per event; this
# bounds replay-on-restart to a few seconds of duplicates instead.
CURSOR_FLUSH_SECONDS = float(os.environ.get("P01_CURSOR_FLUSH_SECONDS", "5"))

# --- Connection behaviour -------------------------------------------------------------------------

# Separate connect and read budgets. A single number applies per socket operation, so a server that
# accepts the connection and then stalls gets the whole allowance twice over — the failure that cost
# project 3 half an hour of a backfill.
CONNECT_TIMEOUT_SECONDS = int(os.environ.get("P01_CONNECT_TIMEOUT_SECONDS", "10"))

# Jetstream is continuous at ~37/sec, so silence is a symptom rather than a lull. Well above the
# longest gap observed, and far below anything a human would notice.
READ_TIMEOUT_SECONDS = int(os.environ.get("P01_READ_TIMEOUT_SECONDS", "60"))

RECONNECT_BACKOFF_SECONDS = float(os.environ.get("P01_RECONNECT_BACKOFF_SECONDS", "1"))
RECONNECT_BACKOFF_MAX_SECONDS = float(os.environ.get("P01_RECONNECT_BACKOFF_MAX_SECONDS", "60"))

# --- Liveness -------------------------------------------------------------------------------------

# The line the staleness alert watches for. A streaming job has no natural "done", so it reports
# health on a heartbeat instead; the platform's pipeline_heartbeats entry keys off this.
HEARTBEAT_SECONDS = float(os.environ.get("P01_HEARTBEAT_SECONDS", "60"))
