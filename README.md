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
        ▼
  flink           FlinkDeployment · checkpoints to GCS
        ├──► p01_streaming_raw.posts
        └──► p01_streaming_curated.*
                        ▼
              Cloud Run API + static page
```

**Why a bridge exists.** Jetstream speaks WebSocket and Kafka does not, and Flink SQL has no
WebSocket connector. Folding the socket into Flink would drop it on every redeploy and couple ingest
availability to job availability; Kafka in between is what makes the stream replayable, and what
lets projects 5 and 8 read it later.

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

## Local development

```
uv venv --python 3.12 .venv
uv pip install --python .venv --only-binary=:all: -r bridge/requirements-dev.txt
```

**Tests.**

```
.venv/bin/python -m coverage run -m unittest discover -s bridge/tests -t .
.venv/bin/python -m coverage report
```

No network and no credentials. A fake WebSocket serves scripted frames and a fake producer can be
made to reject on demand, so the retry and backpressure paths are exercised rather than described.
`.coveragerc` enforces **branch** coverage at 100%: statement coverage is what called project 3's
retry loop covered while its exhaustion path fell through returning `None`, and the same shape of
bug is available here in the producer, where the failure is not a crash but quietly producing fewer
events than were consumed.

**Checking connectivity from inside the cluster**, which is where it counts — a laptop authenticates
and routes differently from a pod:

```
kubectl -n p01-streaming exec deploy/p01-bridge -- python -m bridge.healthcheck
```

## Deploying

Pushing to `main` is the deployment. CI runs the tests, then builds the image through Cloud Build;
the platform's ArgoCD Applications sync `k8s/operators` and `k8s/overlays/prod` from this repository
with `selfHeal` and `prune` enabled.

CI authenticates by exchanging the workflow's own OIDC token for a short-lived GCP credential. There
is no key file and no long-lived repository secret.

The bridge Deployment pins `:latest` with `imagePullPolicy: Always`, so a code change is not an
ArgoCD diff — the manifest describes how to run the bridge, not which build is current.

## Layout

```
bridge/      WebSocket to Kafka, and its tests
sql/         Flink SQL — the pipeline itself
sqlrunner/   the ~60-line launcher that feeds sql/ to Flink
api/         Cloud Run FastAPI serving the dashboard
docker/      container images
cloudbuild/  Cloud Build configs
k8s/         operators/ (shared, ns operators) + base + overlays/prod, dev/ outside the overlay
```

## Status

- [x] Repo, bridge, 98 tests at 100% branch coverage, CI
- [ ] Kafka via Strimzi
- [ ] Flink operator and SQL job
- [ ] BigQuery sinks and the curated aggregates
- [ ] Cloud Run API and dashboard
