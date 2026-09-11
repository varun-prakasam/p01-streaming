-- The pipeline. Four inserts, executed as one statement set by sqlrunner, so all four outputs share
-- a single job and a single checkpoint — four separate jobs would each hold their own Kafka
-- connection, their own state and their own failure mode.
--
--   posts_fast_enriched    (watermark  5s) ─┬─► out_raw             every commit, projected
--                                           ├─► out_counts_fast     what you could publish at close
--                                           └─► out_health          liveness, traffic or not
--   posts_settled_enriched (watermark 10m) ──► out_counts_settled   what turns out to be true
--
-- Everything derived — lag, clock_suspect, is_reply — comes from the views in 02_views.sql, so the
-- fast and settled numbers are computed by identical code and their difference can only be the
-- watermark. That is the entire claim of the project.
--
-- Timestamps are emitted as strings: the sink is a dumb writer and BigQuery coerces them. The
-- runner sets table.local-time-zone=UTC, without which CAST(ts AS STRING) renders in whatever zone
-- the TaskManager happens to have and the warehouse silently gains an offset.

-- ==================================================================================================
-- raw: every commit, projected. An audit and inspection surface, not a batch layer — nothing
-- recomputes the counts below from it, and if anything ever did, this would have quietly become
-- lambda.
--
-- Unlike the aggregates, this keeps deletes and updates. What happened to a post is exactly the
-- thing you want the raw table for.
-- ==================================================================================================

INSERT INTO out_raw
SELECT
    uri,
    did,
    collection,
    rkey,
    rev,
    operation,
    CAST(observed_time AS STRING),
    CAST(claimed_time AS STRING),
    claimed_time_raw,
    lag_ms,
    clock_suspect,
    langs,
    lang_primary,
    text_length,
    is_reply,
    reply_root_uri,
    kafka_partition,
    kafka_offset
FROM posts_fast_enriched;

-- ==================================================================================================
-- The two halves of the thesis. Identical aggregation, different watermark patience.
--
-- Both count creates only. A delete is a commit on the same topic but it is not a post being
-- written, and mixing the two would make "events" mean nothing in particular. Deletes remain
-- visible in raw and in pipeline_health.
-- ==================================================================================================

INSERT INTO out_counts_fast
SELECT
    CAST(window_start AS STRING),
    CAST(window_end AS STRING),
    COUNT(*)                                                          AS events,
    COUNT(DISTINCT did)                                               AS distinct_authors,
    COUNT(*) FILTER (WHERE is_reply)                                  AS replies,
    COUNT(*) FILTER (WHERE clock_suspect)                             AS suspect_clocks,
    -- Negative lag means the post claims to come from the future. 6.7% of real traffic does, which
    -- is why the watermark is built on observed_time and this column exists to prove the point.
    COUNT(*) FILTER (WHERE NOT clock_suspect AND lag_ms < 0)          AS future_dated,
    -- Suspect clocks excluded, or a single timestamp from 2099 destroys the average for the minute.
    -- Percentiles are deliberately not computed here: every row's lag_ms lands in the raw table, so
    -- BigQuery can take exact quantiles over it for free rather than Flink approximating them.
    CAST(AVG(CASE WHEN NOT clock_suspect THEN lag_ms END) AS BIGINT)  AS avg_lag_ms,
    CAST(CURRENT_TIMESTAMP AS STRING)                                 AS emitted_at,
    5                                                                 AS watermark_delay_s
FROM TABLE(TUMBLE(TABLE posts_fast_enriched, DESCRIPTOR(observed_time), INTERVAL '1' MINUTE))
WHERE operation = 'create'
GROUP BY window_start, window_end;

INSERT INTO out_counts_settled
SELECT
    CAST(window_start AS STRING),
    CAST(window_end AS STRING),
    COUNT(*)                                                          AS events,
    COUNT(DISTINCT did)                                               AS distinct_authors,
    COUNT(*) FILTER (WHERE is_reply)                                  AS replies,
    COUNT(*) FILTER (WHERE clock_suspect)                             AS suspect_clocks,
    COUNT(*) FILTER (WHERE NOT clock_suspect AND lag_ms < 0)          AS future_dated,
    CAST(AVG(CASE WHEN NOT clock_suspect THEN lag_ms END) AS BIGINT)  AS avg_lag_ms,
    CAST(CURRENT_TIMESTAMP AS STRING)                                 AS emitted_at,
    600                                                               AS watermark_delay_s
FROM TABLE(TUMBLE(TABLE posts_settled_enriched, DESCRIPTOR(observed_time), INTERVAL '1' MINUTE))
WHERE operation = 'create'
GROUP BY window_start, window_end;

-- ==================================================================================================
-- Liveness. One row per closed window that carried traffic — a windowed GROUP BY emits nothing for
-- a window with no input rows, so this is not, and cannot be, a heartbeat that survives silence.
-- The signal is the *absence* of recent rows, which is what the freshness banner and the Phase 8
-- alert both watch.
--
-- What it adds over the count tables: max_observed_time shows how far event time actually advanced
-- inside the window, emitted_at minus window_end is the true end-to-end latency, and the count here
-- includes deletes, so it stays non-zero in a minute where nothing was created.
-- ==================================================================================================

INSERT INTO out_health
SELECT
    CAST(window_end AS STRING),
    CAST(MAX(observed_time) AS STRING),
    COUNT(*),
    CAST(CURRENT_TIMESTAMP AS STRING)
FROM TABLE(TUMBLE(TABLE posts_fast_enriched, DESCRIPTOR(observed_time), INTERVAL '1' MINUTE))
-- Both window columns, even though only window_end is selected. Flink recognises a windowed
-- aggregation only when the GROUP BY carries window_start and window_end; with just one of them
-- this becomes an unbounded group-by over an ever-growing key space, which produces a retract
-- stream that an append-only Kafka sink refuses — and it fails at submit, not at review.
GROUP BY window_start, window_end;
