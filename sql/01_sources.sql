-- Sources: the same Kafka topic, read twice, with different patience.
--
-- This file is the project's argument. Both tables read `bsky.posts.v1`; they differ only in how
-- long the watermark waits before declaring a window closed. `posts_fast` publishes at five seconds
-- — the number a dashboard could show immediately. `posts_settled` waits ten minutes — the number
-- that turns out to be true. The difference between them is the product.
--
-- Two consumer groups over one topic, which at ~40 events/sec is free.

CREATE TABLE posts_raw (
    -- Jetstream's own clock, stamped on receipt. Microseconds since epoch.
    time_us            BIGINT,
    did                STRING,
    kind               STRING,
    `commit`           ROW<
                          rev        STRING,
                          operation  STRING,
                          collection STRING,
                          rkey       STRING,
                          cid        STRING,
                          record     ROW<
                                        createdAt STRING,
                                        langs     ARRAY<STRING>,
                                        `text`    STRING,
                                        reply     ROW<
                                                     root ROW<uri STRING>
                                                  >
                                     >
                       >,

    -- The trustworthy clock, and the only one used for ordering. Jetstream assigns it, so it is
    -- monotonic and cannot be influenced by whoever wrote the post.
    observed_time AS TO_TIMESTAMP_LTZ(time_us, 6),

    -- What the client claims. Parsed leniently: 6.7% of real posts are dated in the future and some
    -- carry timestamps that are not valid RFC-3339 at all, so a strict cast would fail the job on
    -- ordinary traffic. TRY_CAST yields NULL and the row survives to be counted as suspect.
    claimed_time AS TRY_CAST(
        REGEXP_REPLACE(`commit`.record.createdAt, 'Z$', '') AS TIMESTAMP(3)
    ),

    kafka_partition   INT      METADATA FROM 'partition' VIRTUAL,
    kafka_offset      BIGINT   METADATA FROM 'offset'    VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsky.posts.v1',
    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap.p01-streaming:9092',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'json',
    -- A single malformed value must not stop the stream. The bridge already dead-letters anything
    -- that is not parseable JSON, so what reaches here and still fails is a shape change worth
    -- surviving rather than dying on.
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- The fast view: five seconds of tolerance for out-of-order arrival.
CREATE TABLE posts_fast (
    LIKE posts_raw
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsky.posts.v1',
    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap.p01-streaming:9092',
    'properties.group.id' = 'flink-fast',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- The settled view: ten minutes, which is long enough for a reconnect and its replay burst to land.
CREATE TABLE posts_settled (
    LIKE posts_raw
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsky.posts.v1',
    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap.p01-streaming:9092',
    'properties.group.id' = 'flink-settled',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);
