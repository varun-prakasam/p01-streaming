"""Runtime configuration for the BigQuery sink, all overridable by environment variable.

Mirrored in k8s/base/configmap-sink.yaml, and a test asserts the two agree.
"""

import os

PROJECT = os.environ.get("GCP_PROJECT_ID", "varun-data-engineering")

KAFKA_BOOTSTRAP = os.environ.get("P01_KAFKA_BOOTSTRAP", "kafka-kafka-bootstrap.p01-streaming:9092")

# One consumer group for the sink. Flink has its own two.
CONSUMER_GROUP = os.environ.get("P01_SINK_GROUP", "bq-sink")

# Topic -> fully qualified BigQuery table. Flink writes each of these; the sink is a dumb writer and
# does no transformation at all, which is what keeps the pipeline's logic reviewable as SQL rather
# than split across two languages.
ROUTES = {
    "bsky.raw.v1": f"{PROJECT}.p01_streaming_raw.posts",
    "bsky.counts.fast.v1": f"{PROJECT}.p01_streaming_curated.window_counts_fast",
    "bsky.counts.settled.v1": f"{PROJECT}.p01_streaming_curated.window_counts_settled",
    "bsky.health.v1": f"{PROJECT}.p01_streaming_curated.pipeline_health",
}

# Rows are appended in batches. The Storage Write API bills per GB, not per request, but each
# AppendRows call has overhead and BigQuery rejects requests over 10MB — this bounds both.
BATCH_MAX_ROWS = int(os.environ.get("P01_SINK_BATCH_MAX_ROWS", "500"))

# ...and a time bound, so a quiet topic still lands. Without it, a window aggregate emitted once a
# minute would sit in a buffer waiting for 499 friends that never arrive, and the dashboard would
# lag by however long that took.
BATCH_MAX_SECONDS = float(os.environ.get("P01_SINK_BATCH_MAX_SECONDS", "10"))

# Kafka offsets are committed only after BigQuery acknowledges the append. That makes duplicates
# possible on restart and loss impossible, which is the same trade the bridge makes and for the same
# reason: the counts are the product.
POLL_TIMEOUT_SECONDS = float(os.environ.get("P01_SINK_POLL_TIMEOUT_SECONDS", "1.0"))

HEARTBEAT_SECONDS = float(os.environ.get("P01_SINK_HEARTBEAT_SECONDS", "60"))
