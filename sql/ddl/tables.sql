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
  lag_ms          INT64    OPTIONS(description="observed_time - claimed_time. Negative means the client claims the future"),
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
  suspect_clocks    INT64 OPTIONS(description="Rows whose claimed_time is implausible. Counted, and excluded from the percentiles"),
  p50_lag_ms        INT64,
  p95_lag_ms        INT64,
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
  p50_lag_ms        INT64,
  p95_lag_ms        INT64,
  emitted_at        TIMESTAMP,
  watermark_delay_s INT64
)
PARTITION BY DATE(window_start)
OPTIONS(description="The same windows, emitted ten minutes after close. The difference from _fast is the product.");

-- The freshness signal. One row per minute regardless of traffic, so a stopped pipeline and a quiet
-- minute on Bluesky are distinguishable — in a count of posts alone they are not.
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
-- The revision: a view, so it cannot go stale and costs nothing to keep.
-- ==================================================================================================

CREATE OR REPLACE VIEW `varun-data-engineering.p01_streaming_curated.v_revisions` AS
SELECT
  f.window_start,
  f.window_end,
  f.events                                        AS published,
  s.events                                        AS settled,
  s.events - f.events                             AS delta,
  SAFE_DIVIDE(s.events - f.events, f.events)      AS pct_revision,
  f.p50_lag_ms,
  f.suspect_clocks,
  -- A window that never settled is itself a finding: either the pipeline stopped, or the ten-minute
  -- watermark has not passed yet. The dashboard distinguishes the two by window age.
  s.events IS NULL                                AS unsettled
FROM `varun-data-engineering.p01_streaming_curated.window_counts_fast` f
LEFT JOIN `varun-data-engineering.p01_streaming_curated.window_counts_settled` s
  USING (window_start);

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
