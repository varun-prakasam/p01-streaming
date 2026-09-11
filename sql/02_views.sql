-- Enrichment: the parsing and derivation, written once per source and used by everything downstream.
--
-- Why a view rather than repeating this inside each INSERT: the raw projection and both window
-- aggregates must agree on what `lag_ms` and `clock_suspect` mean. If they drift, the dashboard
-- compares a fast number computed one way against a settled number computed another, and the
-- revision it reports is an artefact of our own SQL rather than of the watermark — which is the one
-- conclusion this project must not get wrong.
--
-- The two views below are byte-identical apart from their name and their source table, and
-- sqlrunner/tests/test_sql_files.py asserts exactly that. They cannot be collapsed into one view
-- because they read two different tables: the whole point is that `posts_fast` and `posts_settled`
-- carry different watermarks over the same topic.
--
-- Projections preserve the rowtime attribute, so `observed_time` is still a valid time attribute in
-- these views and TUMBLE works against them.

CREATE TEMPORARY VIEW posts_fast_enriched AS
SELECT
    'at://' || did || '/' || `commit`.collection || '/' || `commit`.rkey AS uri,
    did,
    `commit`.collection                                   AS collection,
    `commit`.rkey                                         AS rkey,
    `commit`.rev                                          AS rev,
    `commit`.operation                                    AS operation,

    observed_time,
    claimed_time,
    -- Kept only when parsing failed, so a lexicon change is readable rather than merely counted.
    -- A rising clock_suspect rate with nothing in this column means bad clocks; with values in it,
    -- it means the timestamp format moved.
    CASE WHEN claimed_millis IS NULL THEN claimed_raw END  AS claimed_time_raw,

    lag_ms,
    clock_suspect,

    langs,
    -- The first language, pulled out because BigQuery cannot cluster on an array and every
    -- interesting question about language is about the primary one.
    CASE WHEN CARDINALITY(langs) > 0 THEN langs[1] END     AS lang_primary,
    text_length,
    is_reply,
    reply_root_uri,

    kafka_partition,
    kafka_offset
FROM (
    SELECT
        *,
        observed_millis - claimed_millis                   AS lag_ms,
        -- Backdated more than thirty days, dated more than an hour ahead, or unparseable. Such a
        -- row is counted and kept; it is simply never allowed to influence an average. Bounds are
        -- exclusive, so exactly thirty days is not suspect.
        claimed_millis IS NULL
            OR observed_millis - claimed_millis > 2592000000
            OR observed_millis - claimed_millis < -3600000 AS clock_suspect
    FROM (
        SELECT
            parsed.*,
            -- Epoch milliseconds for both clocks, so the lag is exact. TIMESTAMPDIFF would give
            -- whole seconds, and at a measured median skew of ~1.1s against a five-second
            -- watermark that bins away most of the distribution the project exists to show.
            time_us / 1000                                 AS observed_millis,
            CASE WHEN claimed_time IS NOT NULL THEN
                UNIX_TIMESTAMP(
                    DATE_FORMAT(claimed_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'
                ) * 1000 + CAST(DATE_FORMAT(claimed_time, 'SSS') AS BIGINT)
            END                                            AS claimed_millis
        FROM (
            SELECT
                *,
                `commit`.record.createdAt                 AS claimed_raw,
                `commit`.record.langs                     AS langs,
                CHAR_LENGTH(COALESCE(`commit`.record.`text`, '')) AS text_length,
                `commit`.record.reply IS NOT NULL         AS is_reply,
                `commit`.record.reply.root.uri            AS reply_root_uri,
                -- Lenient by design. Clients write whatever they like here, and a strict cast
                -- would fail the job on ordinary traffic; TRY_CAST yields NULL so the row survives
                -- to be counted as suspect. Note this accepts only UTC — an offset such as
                -- '+05:30' does not parse and lands in claimed_time_raw, which is how we would
                -- find out that it happens.
                TRY_CAST(REGEXP_REPLACE(`commit`.record.createdAt, 'Z$', '') AS TIMESTAMP(3))
                                                           AS claimed_time
            FROM posts_fast
            WHERE kind = 'commit'
        ) AS parsed
    ) AS with_millis
) AS with_lag;

CREATE TEMPORARY VIEW posts_settled_enriched AS
SELECT
    'at://' || did || '/' || `commit`.collection || '/' || `commit`.rkey AS uri,
    did,
    `commit`.collection                                   AS collection,
    `commit`.rkey                                         AS rkey,
    `commit`.rev                                          AS rev,
    `commit`.operation                                    AS operation,

    observed_time,
    claimed_time,
    -- Kept only when parsing failed, so a lexicon change is readable rather than merely counted.
    -- A rising clock_suspect rate with nothing in this column means bad clocks; with values in it,
    -- it means the timestamp format moved.
    CASE WHEN claimed_millis IS NULL THEN claimed_raw END  AS claimed_time_raw,

    lag_ms,
    clock_suspect,

    langs,
    -- The first language, pulled out because BigQuery cannot cluster on an array and every
    -- interesting question about language is about the primary one.
    CASE WHEN CARDINALITY(langs) > 0 THEN langs[1] END     AS lang_primary,
    text_length,
    is_reply,
    reply_root_uri,

    kafka_partition,
    kafka_offset
FROM (
    SELECT
        *,
        observed_millis - claimed_millis                   AS lag_ms,
        -- Backdated more than thirty days, dated more than an hour ahead, or unparseable. Such a
        -- row is counted and kept; it is simply never allowed to influence an average. Bounds are
        -- exclusive, so exactly thirty days is not suspect.
        claimed_millis IS NULL
            OR observed_millis - claimed_millis > 2592000000
            OR observed_millis - claimed_millis < -3600000 AS clock_suspect
    FROM (
        SELECT
            parsed.*,
            -- Epoch milliseconds for both clocks, so the lag is exact. TIMESTAMPDIFF would give
            -- whole seconds, and at a measured median skew of ~1.1s against a five-second
            -- watermark that bins away most of the distribution the project exists to show.
            time_us / 1000                                 AS observed_millis,
            CASE WHEN claimed_time IS NOT NULL THEN
                UNIX_TIMESTAMP(
                    DATE_FORMAT(claimed_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'
                ) * 1000 + CAST(DATE_FORMAT(claimed_time, 'SSS') AS BIGINT)
            END                                            AS claimed_millis
        FROM (
            SELECT
                *,
                `commit`.record.createdAt                 AS claimed_raw,
                `commit`.record.langs                     AS langs,
                CHAR_LENGTH(COALESCE(`commit`.record.`text`, '')) AS text_length,
                `commit`.record.reply IS NOT NULL         AS is_reply,
                `commit`.record.reply.root.uri            AS reply_root_uri,
                -- Lenient by design. Clients write whatever they like here, and a strict cast
                -- would fail the job on ordinary traffic; TRY_CAST yields NULL so the row survives
                -- to be counted as suspect. Note this accepts only UTC — an offset such as
                -- '+05:30' does not parse and lands in claimed_time_raw, which is how we would
                -- find out that it happens.
                TRY_CAST(REGEXP_REPLACE(`commit`.record.createdAt, 'Z$', '') AS TIMESTAMP(3))
                                                           AS claimed_time
            FROM posts_settled
            WHERE kind = 'commit'
        ) AS parsed
    ) AS with_millis
) AS with_lag;
