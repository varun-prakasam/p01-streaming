-- Sources: the same Kafka topic, read twice, with different patience.
--
-- This file is the project's argument. Both tables read `bsky.posts.v1`; they differ only in how
-- long the watermark waits before declaring a window closed. `posts_fast` gives up after five
-- seconds — the number a dashboard could publish the moment a minute ends. `posts_settled` waits
-- ten minutes — the number that turns out to be true. The difference between them is the product.
--
-- Two consumer groups over one topic, which at ~70 events/sec costs nothing.

CREATE TABLE posts_fast (
    time_us  BIGINT,
    did      STRING,
    kind     STRING,
    `commit` ROW<
                rev        STRING,
                operation  STRING,
                collection STRING,
                rkey       STRING,
                cid        STRING,
                record     ROW<
                              createdAt STRING,
                              langs     ARRAY<STRING>,
                              `text`    STRING,
                              reply     ROW<root ROW<uri STRING>>
                           >
             >,

    kafka_partition INT    METADATA FROM 'partition' VIRTUAL,
    kafka_offset    BIGINT METADATA FROM 'offset'    VIRTUAL,

    -- Jetstream's own clock, stamped on receipt. Microseconds, hence the 6.
    --
    -- This is the only clock used for ordering, and that decision is the whole project. The obvious
    -- alternative — record.createdAt, which is what the post *says* its time is — is written by the
    -- client and unverified: 6.7% of real posts are dated in the future, the furthest by two hours,
    -- and one is backdated 260 days. A watermark built on it is dragged forward by the worst clock
    -- on the network several times a minute, and then discards everything arriving normally.
    observed_time AS TO_TIMESTAMP_LTZ(time_us, 6),
    WATERMARK FOR observed_time AS observed_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsky.posts.v1',
    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap.p01-streaming:9092',
    'properties.group.id' = 'flink-fast',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'json',
    -- A single malformed value must not stop the stream. The bridge already dead-letters anything
    -- that is not parseable JSON, so what reaches here and still fails is a shape change worth
    -- surviving rather than dying on.
    'json.ignore-parse-errors' = 'true',
    -- Without this an idle partition halts the *global* watermark and no window ever closes. The
    -- job stays green, checkpoints keep succeeding, and the dashboard simply stops advancing — the
    -- most likely silent failure in the project, and one line to prevent.
    'scan.watermark.idle-timeout' = '30s'
);

CREATE TABLE posts_settled (
    time_us  BIGINT,
    did      STRING,
    kind     STRING,
    `commit` ROW<
                rev        STRING,
                operation  STRING,
                collection STRING,
                rkey       STRING,
                cid        STRING,
                record     ROW<
                              createdAt STRING,
                              langs     ARRAY<STRING>,
                              `text`    STRING,
                              reply     ROW<root ROW<uri STRING>>
                           >
             >,

    kafka_partition INT    METADATA FROM 'partition' VIRTUAL,
    kafka_offset    BIGINT METADATA FROM 'offset'    VIRTUAL,

    observed_time AS TO_TIMESTAMP_LTZ(time_us, 6),
    -- Ten minutes. Long enough for a bridge reconnect and its replay burst to land inside the
    -- window they belong to, which is precisely the data the fast view misses.
    WATERMARK FOR observed_time AS observed_time - INTERVAL '10' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsky.posts.v1',
    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap.p01-streaming:9092',
    'properties.group.id' = 'flink-settled',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.watermark.idle-timeout' = '30s'
);
