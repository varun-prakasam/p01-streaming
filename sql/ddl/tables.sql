-- BigQuery tables, created explicitly rather than by the sink.
--
-- The trap this file exists to avoid: `p01_streaming_raw` carries
-- `default_table_expiration_days = 14`, and that is a **table** expiration. A table that inherits it
-- disappears after a fortnight, taking the write destination with it — after which the sink fails
-- with an error that reads like a permissions problem.
--
-- And the trap has a second floor. Writing `expiration_timestamp = NULL` in CREATE TABLE OPTIONS
-- does **not** prevent the dataset default: BigQuery applies it anyway, and the first run of this
-- file produced tables due to vanish in exactly 14 days despite the option being present. Only
-- ALTER TABLE afterwards actually clears it, which is why the statements at the bottom are not
-- redundant. Verify rather than assume:
--
--   bq show --format=prettyjson p01_streaming_raw.posts | grep -E 'expirationTime|expirationMs'
--
-- Run with:
--   bq query --use_legacy_sql=false --project_id=varun-data-engineering < sql/ddl/tables.sql

-- ==================================================================================================
-- raw: the audit and replay surface. Not a batch layer — nothing recomputes the curated tables from
-- it, and if anything ever did, this architecture would have quietly become lambda.
-- ==================================================================================================

CREATE TABLE IF NOT EXISTS `varun-data-engineering.p01_streaming_raw.posts`
(
  uri             STRING   NOT NULL OPTIONS(description="at://did/collection/rkey — the post's stable identity"),
  did             STRING   NOT NULL OPTIONS(description="Author. The Kafka partition key, so one author's posts stay ordered"),
  collection      STRING,
  rkey            STRING,
  rev             STRING   OPTIONS(description="Repository revision. Part of the dedup key with uri and operation"),
  operation       STRING   OPTIONS(description="create, update or delete"),

  observed_time   TIMESTAMP NOT NULL OPTIONS(description="Jetstream's clock, assigned on receipt. The watermark basis"),
  claimed_time    TIMESTAMP OPTIONS(description="record.createdAt, written by the client. Unverified and often wrong"),
  claimed_time_raw STRING   OPTIONS(description="The unparsed createdAt, kept only when parsing failed. Values here mean the timestamp format moved; a rising clock_suspect rate with this column empty means bad client clocks. The two are different problems."),
  lag_ms          INT64    OPTIONS(description="observed_time - claimed_time, in milliseconds. Negative means the client claims the future"),
  clock_suspect   BOOL     OPTIONS(description="claimed_time outside [observed - 30d, observed + 1h]. Counted, never used for ordering"),

  langs           ARRAY<STRING>,
  lang_primary    STRING,
  text_length     INT64,
  is_reply        BOOL,
  reply_root_uri  STRING,

  kafka_partition INT64,
  kafka_offset    INT64
)
PARTITION BY DATE(observed_time)
CLUSTER BY collection, operation, did
OPTIONS(
  -- Kept for intent, but it does not work on its own: BigQuery applies the dataset's
  -- default_table_expiration_days regardless. The ALTER TABLE at the bottom of this file is what
  -- actually clears it.
  expiration_timestamp = NULL,
  partition_expiration_days = 14,
  -- A query without a date predicate would scan every partition. On a table taking ~6M rows a day
  -- that is the difference between a free dashboard and a bill.
  require_partition_filter = TRUE,
  description = "Raw Bluesky posts as landed. Partitioned on observed_time, never on claimed_time — client clocks would scatter partitions from 1970 to 2099."
);

CREATE TABLE IF NOT EXISTS `varun-data-engineering.p01_streaming_raw.parse_errors`
(
  observed_time TIMESTAMP NOT NULL,
  payload       STRING OPTIONS(description="The frame as received, so the shape change can be read"),
  error         STRING
)
PARTITION BY DATE(observed_time)
OPTIONS(
  expiration_timestamp = NULL,
  partition_expiration_days = 14,
  description = "Frames the bridge could not parse. Should be empty; a non-zero rate means Bluesky changed the lexicon."
);

-- ==================================================================================================
-- curated: the history. No expiry — this is what the project is for.
-- ==================================================================================================

-- The two halves of the thesis. Identical shape, different watermark patience: `fast` is the number
-- a dashboard could publish at window close, `settled` the number that turns out to be true.
CREATE TABLE IF NOT EXISTS `varun-data-engineering.p01_streaming_curated.window_counts_fast`
(
  window_start      TIMESTAMP NOT NULL,
  window_end        TIMESTAMP NOT NULL,
  events            INT64,
  distinct_authors  INT64,
  replies           INT64,
  suspect_clocks    INT64 OPTIONS(description="Rows whose claimed_time is implausible. Counted, and excluded from avg_lag_ms"),
  future_dated      INT64 OPTIONS(description="Plausible rows claiming a time ahead of observation. ~6-7% of real traffic"),
  avg_lag_ms        INT64 OPTIONS(description="Mean lag over non-suspect rows. Percentiles are not computed in Flink — v_skew_profile takes them exactly from raw"),
  emitted_at        TIMESTAMP OPTIONS(description="When Flink emitted this row, not when the window ended"),
  watermark_delay_s INT64
)
PARTITION BY DATE(window_start)
OPTIONS(description="One row per minute, emitted five seconds after the window closes.");

CREATE TABLE IF NOT EXISTS `varun-data-engineering.p01_streaming_curated.window_counts_settled`
(
  window_start      TIMESTAMP NOT NULL,
  window_end        TIMESTAMP NOT NULL,
  events            INT64,
  distinct_authors  INT64,
  replies           INT64,
  suspect_clocks    INT64,
  future_dated      INT64,
  avg_lag_ms        INT64,
  emitted_at        TIMESTAMP,
  watermark_delay_s INT64
)
PARTITION BY DATE(window_start)
OPTIONS(description="The same windows, emitted ten minutes after close. The difference from _fast is the product.");

-- The freshness signal. One row per closed window that carried traffic — a windowed GROUP BY emits
-- nothing for an empty window, so absence is the alarm, not a zero. Includes deletes, so it stays
-- non-zero in a minute where nothing was created.
CREATE TABLE IF NOT EXISTS `varun-data-engineering.p01_streaming_curated.pipeline_health`
(
  window_end        TIMESTAMP NOT NULL,
  max_observed_time TIMESTAMP,
  events            INT64,
  emitted_at        TIMESTAMP
)
PARTITION BY DATE(window_end)
OPTIONS(description="Liveness heartbeat from the Flink job.");

-- ==================================================================================================
-- Views. Two jobs: deduplicate, and answer the question.
--
-- Deduplication is not optional here. The pipeline is at-least-once end to end on purpose — the
-- bridge rewinds its cursor on reconnect, and the sink commits Kafka offsets only after BigQuery
-- acknowledges, so a crash between those two points rewrites a batch. Both choices make loss
-- impossible and duplicates certain, which is the right trade when counts are the product, but it
-- means every read of these tables must collapse them first.
--
-- Without this, a duplicated `window_counts_fast` row would fan out the join in v_revisions and
-- report a revision that is an artefact of a retry rather than of the watermark. That is precisely
-- the conclusion this project must not get wrong.
-- ==================================================================================================

CREATE OR REPLACE VIEW `varun-data-engineering.p01_streaming_curated.v_revisions` AS
WITH fast AS (
  SELECT * FROM `varun-data-engineering.p01_streaming_curated.window_counts_fast`
  -- Last writer wins. Re-emissions of the same window carry a later emitted_at, and a re-emission
  -- is a recomputation over more complete state, so the newest row is also the most correct one.
  QUALIFY ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY emitted_at DESC) = 1
),
settled AS (
  SELECT * FROM `varun-data-engineering.p01_streaming_curated.window_counts_settled`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY emitted_at DESC) = 1
)
SELECT
  f.window_start,
  f.window_end,
  f.events                                        AS published,
  s.events                                        AS settled,
  s.events - f.events                             AS delta,
  SAFE_DIVIDE(s.events - f.events, f.events)      AS pct_revision,
  f.avg_lag_ms,
  f.suspect_clocks,
  f.future_dated,
  -- A window that never settled is itself a finding: either the pipeline stopped, or the ten-minute
  -- watermark has not passed yet. The dashboard distinguishes the two by window age, which is why
  -- window_end is exposed rather than only the flag.
  s.events IS NULL                                AS unsettled
FROM fast f
LEFT JOIN settled s
  USING (window_start);

-- The skew distribution, taken exactly rather than approximated.
--
-- Flink deliberately does not compute percentiles: every row's lag_ms already lands in raw, so
-- BigQuery can take true quantiles over it. A streaming sketch would be an approximation of data we
-- are keeping anyway.
--
-- The date predicate is not tidiness. `posts` sets require_partition_filter, so a view without one
-- fails at query time rather than merely costing money; two days also bounds the scan to roughly
-- 12M rows however long the table lives.
CREATE OR REPLACE VIEW `varun-data-engineering.p01_streaming_curated.v_skew_profile` AS
WITH deduped AS (
  SELECT observed_time, lag_ms, clock_suspect
  FROM `varun-data-engineering.p01_streaming_raw.posts`
  WHERE DATE(observed_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND operation = 'create'
  -- The raw table takes duplicates from the same at-least-once path. rev distinguishes a genuine
  -- update from a retry of the same write.
  QUALIFY ROW_NUMBER() OVER (PARTITION BY uri, operation, rev ORDER BY kafka_offset) = 1
)
SELECT
  TIMESTAMP_TRUNC(observed_time, MINUTE)                          AS window_start,
  COUNT(*)                                                        AS events,
  COUNTIF(clock_suspect)                                          AS suspect_clocks,
  COUNTIF(NOT clock_suspect AND lag_ms < 0)                       AS future_dated,
  COUNTIF(NOT clock_suspect AND lag_ms > 0)                       AS backdated,
  -- Suspect clocks excluded throughout: one timestamp from 2099 would otherwise own the p99.
  APPROX_QUANTILES(IF(clock_suspect, NULL, lag_ms), 100)[OFFSET(50)] AS p50_lag_ms,
  APPROX_QUANTILES(IF(clock_suspect, NULL, lag_ms), 100)[OFFSET(95)] AS p95_lag_ms,
  APPROX_QUANTILES(IF(clock_suspect, NULL, lag_ms), 100)[OFFSET(99)] AS p99_lag_ms
FROM deduped
GROUP BY window_start;

-- Deduplicated raw, for anything that counts rather than inspects. Inspection wants the duplicates
-- visible; counting never does.
CREATE OR REPLACE VIEW `varun-data-engineering.p01_streaming_curated.v_posts` AS
SELECT * EXCEPT(row_num) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY uri, operation, rev ORDER BY kafka_offset) AS row_num
  FROM `varun-data-engineering.p01_streaming_raw.posts`
  WHERE DATE(observed_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
)
WHERE row_num = 1;

-- ==================================================================================================
-- Clear the inherited table expiration.
--
-- Not redundant with the OPTIONS above. `expiration_timestamp = NULL` in CREATE TABLE is ignored in
-- favour of the dataset's default_table_expiration_days, so without these two statements both raw
-- tables are scheduled for deletion fourteen days after creation — including the table definition
-- itself, not just its data. Idempotent, so re-running this file is safe.
-- ==================================================================================================

ALTER TABLE `varun-data-engineering.p01_streaming_raw.posts`
  SET OPTIONS (expiration_timestamp = NULL);

ALTER TABLE `varun-data-engineering.p01_streaming_raw.parse_errors`
  SET OPTIONS (expiration_timestamp = NULL);
