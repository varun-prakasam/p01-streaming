-- Outputs. All Kafka: the BigQuery write happens in the sink service, because Google's Flink
-- BigQuery connector is published only for Flink 1.17 and building on it would pin this project to
-- a 2023 release.
--
-- At-least-once, stated rather than inherited. It is the connector's default, but it is also a design
-- decision: exactly-once here would need Kafka transactions and a read_committed sink consumer, and
-- would still not survive the sink's own at-least-once hop into BigQuery. Duplicates are expected
-- and the curated views collapse them; that is the chosen trade, so it is written down where it is
-- made.
--
-- Every column is emitted as a string or a plain scalar that BigQuery coerces. The sink is a dumb
-- writer by design — it does no transformation at all — so what these tables emit is exactly what
-- lands in the warehouse, and the schema lives in two places (here and the table DDL) rather than
-- three.

CREATE TABLE out_raw (
    uri             STRING,
    did             STRING,
    collection      STRING,
    rkey            STRING,
    rev             STRING,
    operation       STRING,
    observed_time   STRING,
    claimed_time    STRING,
    claimed_time_raw STRING,
    lag_ms          BIGINT,
    clock_suspect   BOOLEAN,
    langs           ARRAY<STRING>,
    lang_primary    STRING,
    text_length     INT,
    is_reply        BOOLEAN,
    reply_root_uri  STRING,
    kafka_partition INT,
    kafka_offset    BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsky.raw.v1',
    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap.p01-streaming:9092',
    'format' = 'json',
    'sink.delivery-guarantee' = 'at-least-once'
);

CREATE TABLE out_counts_fast (
    window_start      STRING,
    window_end        STRING,
    events            BIGINT,
    distinct_authors  BIGINT,
    replies           BIGINT,
    suspect_clocks    BIGINT,
    future_dated      BIGINT,
    avg_lag_ms        BIGINT,
    emitted_at        STRING,
    watermark_delay_s INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsky.counts.fast.v1',
    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap.p01-streaming:9092',
    'format' = 'json',
    'sink.delivery-guarantee' = 'at-least-once'
);

CREATE TABLE out_counts_settled (
    window_start      STRING,
    window_end        STRING,
    events            BIGINT,
    distinct_authors  BIGINT,
    replies           BIGINT,
    suspect_clocks    BIGINT,
    future_dated      BIGINT,
    avg_lag_ms        BIGINT,
    emitted_at        STRING,
    watermark_delay_s INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsky.counts.settled.v1',
    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap.p01-streaming:9092',
    'format' = 'json',
    'sink.delivery-guarantee' = 'at-least-once'
);

-- The freshness signal. One row per closed window that carried traffic — a windowed GROUP BY emits
-- nothing at all for an empty window, so this is not a heartbeat that survives silence and must not
-- be read as one. A stopped pipeline shows up as the absence of recent rows, and the dashboard's
-- freshness banner compares max(window_end) against wall clock rather than trusting a count.
--
-- Unlike the count tables this includes deletes, so it stays non-zero in a minute where nothing was
-- created.
CREATE TABLE out_health (
    window_end        STRING,
    max_observed_time STRING,
    events            BIGINT,
    emitted_at        STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsky.health.v1',
    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap.p01-streaming:9092',
    'format' = 'json',
    'sink.delivery-guarantee' = 'at-least-once'
);
