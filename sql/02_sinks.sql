-- Sinks into BigQuery.
--
-- The tables are created by DDL in sql/ddl/, not by the connector. The dataset carries
-- `default_table_expiration_days = 14`, and that is a *table* expiration: a table the connector
-- auto-creates inherits it and vanishes after a fortnight, taking the write destination with it.
-- The sink then fails in a way that reads as a permissions problem. Created explicitly, the tables
-- carry a *partition* expiration instead and the table itself is permanent.

CREATE TABLE raw_posts (
    uri            STRING,
    did            STRING,
    collection     STRING,
    rkey           STRING,
    rev            STRING,
    operation      STRING,
    observed_time  TIMESTAMP_LTZ(3),
    claimed_time   TIMESTAMP(3),
    lag_ms         BIGINT,
    clock_suspect  BOOLEAN,
    langs          ARRAY<STRING>,
    lang_primary   STRING,
    text_length    INT,
    is_reply       BOOLEAN,
    reply_root_uri STRING,
    kafka_partition INT,
    kafka_offset    BIGINT
) WITH (
    'connector' = 'bigquery',
    'project' = 'varun-data-engineering',
    'dataset' = 'p01_streaming_raw',
    'table' = 'posts',
    -- At-least-once. Exactly-once would need two-phase commit against the Storage Write API, and
    -- the duplicates it would prevent are already removed downstream by deduplicating on
    -- (uri, operation, rev) — which is necessary anyway, because the bridge deliberately replays
    -- five seconds on every reconnect.
    'delivery.guarantee' = 'at-least-once'
);

CREATE TABLE window_counts_fast (
    window_start   TIMESTAMP(3),
    window_end     TIMESTAMP(3),
    events         BIGINT,
    distinct_authors BIGINT,
    replies        BIGINT,
    suspect_clocks BIGINT,
    p50_lag_ms     BIGINT,
    p95_lag_ms     BIGINT,
    emitted_at     TIMESTAMP_LTZ(3),
    watermark_delay_s INT
) WITH (
    'connector' = 'bigquery',
    'project' = 'varun-data-engineering',
    'dataset' = 'p01_streaming_curated',
    'table' = 'window_counts_fast',
    'delivery.guarantee' = 'at-least-once'
);

CREATE TABLE window_counts_settled (
    LIKE window_counts_fast
) WITH (
    'connector' = 'bigquery',
    'project' = 'varun-data-engineering',
    'dataset' = 'p01_streaming_curated',
    'table' = 'window_counts_settled',
    'delivery.guarantee' = 'at-least-once'
);

-- The freshness signal the dashboard reads. One row per minute regardless of traffic, so silence is
-- distinguishable from zero — a pipeline that has stopped and a quiet minute on Bluesky look
-- identical in a count, and only one of them is a problem.
CREATE TABLE pipeline_health (
    window_end        TIMESTAMP(3),
    max_observed_time TIMESTAMP_LTZ(3),
    events            BIGINT,
    emitted_at        TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'bigquery',
    'project' = 'varun-data-engineering',
    'dataset' = 'p01_streaming_curated',
    'table' = 'pipeline_health',
    'delivery.guarantee' = 'at-least-once'
);
