# p01-streaming — streaming pipeline

Project 1 of the data platform portfolio. Discipline: **stream processing**.

The Bluesky firehose lands in [Apache Kafka](https://kafka.apache.org) via
[Strimzi](https://strimzi.io), is processed by [Flink SQL](https://flink.apache.org) with event-time
windowing, and is published to BigQuery through the Storage Write API. A Cloud Run API serves the
result on a public URL that changes while you watch it.

| | |
|---|---|
| Source | Bluesky Jetstream (`app.bsky.feed.post`, ~37–45 events/sec) |
| Broker | Apache Kafka in KRaft mode, one broker, managed by Strimzi |
| Processing | Flink SQL — 1-minute tumbling windows, watermarks, allowed lateness |
| Warehouse | `p01_streaming_raw` → `p01_streaming_curated` |
| Identity | Workload Identity as `wl-p01-streaming@…` — no key files anywhere |

## The question this answers

**How wrong is a real-time dashboard at the moment it publishes a number?**

Every event carries two clocks. `time_us` is stamped by the Jetstream relay on receipt.
`commit.record.createdAt` is written by whichever client posted, and is entirely unverified. Measured
over 1,085 real posts in 30 seconds:

| | |
|---|---|
| Median skew (`time_us` − `createdAt`) | 1.1s |
| **Future-dated posts** | **73 of 1085 — 6.7%** |
| Furthest ahead | 2 hours |
| Furthest backdated | 260 days |

So the pipeline **watermarks on `time_us`** and treats `createdAt` as data. A watermark built on
`createdAt` would be dragged two hours into the future by one bad client clock several times a
minute, and then discard everything arriving normally.

On that foundation it emits every window twice — once at a 5-second watermark delay, the number you
could publish immediately, and once at a 10-minute delay, the number that turns out to be true — and
reports the revision between them. Clock skew is the explanation; the revision is the product.

Time-to-trending was rejected as a thesis: top-k terms is the canonical firehose demo, and a
five-minute top-ten recomputed every five minutes is indistinguishable from the streaming version,
so the machinery would demonstrate nothing.

## Architecture

```
Bluesky Jetstream  ~37/sec, posts only, HTTP/1.1 WebSocket
        │  cursor-resumable
        ▼
  bridge          Deployment · Recreate · cursor PVC
        │  key = did, topic bsky.posts.v1
        ▼
  kafka           Strimzi Kafka CR · 1 broker · KRaft · 32Gi
        │  the same topic read twice: 5s watermark and 10min watermark
        ▼
  flink           FlinkDeployment · Flink SQL · checkpoints to GCS
        │  bsky.raw.v1 · bsky.counts.{fast,settled}.v1 · bsky.health.v1
        ▼
  kafka           (the same broker)
        ▼
  bq-sink         Deployment · commits offsets only after BigQuery acknowledges
        ├──► p01_streaming_raw.posts
        └──► p01_streaming_curated.*       deduplicated by the views over them
                        ▼
              Cloud Run API + static page   (not yet built)
```

**Why a bridge exists.** Jetstream speaks WebSocket and Kafka does not, and Flink SQL has no
WebSocket connector. Folding the socket into Flink would drop it on every redeploy and couple ingest
availability to job availability; Kafka in between is what makes the stream replayable, and what
lets projects 5 and 8 read it later.

**Why a sink rather than a Flink BigQuery connector.** Google publishes its Flink connector only for
Flink 1.17, a 2023 release, and building on it would pin the whole project there. Flink writes its
results back to Kafka, and a small Python service writes them to BigQuery. It does no
transformation, so every derived value is still in the SQL.

## Durability

Every hop either replays or blocks. Nothing drops silently.

The bridge persists its Jetstream cursor **after** the producer acknowledges, and deliberately
rewinds 5 seconds on reconnect — duplicates are certain and a gap is impossible, which is the right
trade when counts are the product. Verified against the live firehose: a restart resumed exactly
5.0s behind the previous cursor and replayed 257 events rather than skipping any.

**Jetstream replays about 36 hours**, measured rather than assumed. Past that it does *not* error —
it silently starts from the oldest event it still holds, so a two-day outage reconnects, streams
happily, and looks identical to a clean restart while two days of posts are gone. The bridge compares
the first event it receives against the cursor it asked for and reports the gap.

The sink makes the same trade in the other direction. It commits Kafka offsets only after BigQuery
has acknowledged every row in the batch, and a rejected row raises rather than being skipped — so a
schema mismatch crash-loops the sink with its rows still waiting in Kafka, instead of quietly
advancing past them. Both hops are at-least-once on purpose, and the curated views deduplicate: a
duplicate window row would otherwise fan out the join in `v_revisions` and report a revision caused
by a retry.

## Local development

```
uv venv --python 3.12 .venv
uv pip install --python .venv --only-binary=:all: \
    -r bridge/requirements-dev.txt -r sink/requirements-dev.txt
```

**Tests.** One combined coverage report across all three components, so the gate cannot be met by
one while another slips:

```
.venv/bin/python -m coverage run -m unittest discover -s bridge/tests -t .
.venv/bin/python -m coverage run --append -m unittest discover -s sink/tests -t .
.venv/bin/python -m coverage run --append -m unittest discover -s sqlrunner/tests -t .
.venv/bin/python -m coverage report
.venv/bin/python -m sqlrunner.main --sql-dir sql --dry-run
```

The sqlrunner suite also checks the SQL files against each other and against the sink's routing
table: the two enrichment views must be identical apart from their source, both aggregations must be
computed identically, each emitted `watermark_delay_s` must match its source's real watermark, and
every Flink sink table must name exactly the columns of the BigQuery table it lands in. Each of
those can be broken while every Python test passes, and most of them fail silently rather than
loudly.

No network and no credentials. A fake WebSocket serves scripted frames and a fake producer can be
made to reject on demand, so the retry and backpressure paths are exercised rather than described.
`.coveragerc` enforces **branch** coverage at 100%: statement coverage is what called project 3's
retry loop covered while its exhaustion path fell through returning `None`, and the same shape of
bug is available here in the producer, where the failure is not a crash but quietly producing fewer
events than were consumed.

**Checking connectivity from inside the cluster**, which is where it counts — a laptop authenticates
and routes differently from a pod:

```
kubectl -n p01-streaming exec deploy/bridge -- python -m bridge.healthcheck
```

## Deploying

Pushing to `main` is the deployment. CI runs the tests, then builds three images through Cloud
Build; the platform's ArgoCD Applications sync `k8s/operators` and `k8s/overlays/prod` from this
repository with `selfHeal` and `prune` enabled.

CI authenticates by exchanging the workflow's own OIDC token for a short-lived GCP credential. There
is no key file and no long-lived repository secret.

The bridge and the sink pin `:latest` with `imagePullPolicy: Always`, so a code change is not an
ArgoCD diff — the manifest describes how to run them, not which build is current.

**The Flink job is the exception, and pins its image by digest.** A stateful job cannot float. With
`:latest`, any pod restart — a crash, a node upgrade — would silently pick up whatever SQL was
pushed last and try to restore it from a checkpoint taken by the old job graph. So a SQL change is
two steps:

1. Push the change. CI tests it and builds `p01-flink:<sha>`.
2. Put that image's digest in `k8s/base/flink.yaml` and push again. The operator sees the image
   change and upgrades the job from its last checkpoint.

A change that alters the job's state — a new aggregation, a different key — will not restore from
the old checkpoint. That is the kappa case: run it as a new job with a new consumer group reading
from the start of the topic, verify, and swap.

## Layout

```
bridge/      WebSocket to Kafka, and its tests
sink/        Kafka to BigQuery, and its tests
sql/         Flink SQL — the pipeline itself; ddl/ holds the BigQuery tables and views
sqlrunner/   splits sql/ and submits it to Flink as one job; its tests check the SQL files agree
api/         Cloud Run FastAPI serving the dashboard (not yet built)
docker/      container images
cloudbuild/  Cloud Build configs
k8s/         operators/ (shared, ns operators) + base + overlays/prod
```

## Status

- [x] Repo, bridge, sink and SQL runner — 203 tests at 100% branch coverage, CI
- [x] Kafka via Strimzi, six topics as `KafkaTopic` resources
- [x] Bridge live against the firehose: 1.9M posts in its first 11 hours, none dead-lettered
- [ ] Flink SQL job — written and tested, not yet running
- [ ] BigQuery sink and curated views — sink deployed and idle until Flink produces
- [ ] Cloud Run API and dashboard
- [ ] Hardening: disruption budgets, alerts, the pipeline heartbeat
