"""Append rows to BigQuery.

This component exists because Google's Flink BigQuery connector is published only for Flink 1.17 —
a 2023 release — and building on it would pin the whole project to that version. Flink writes its
results to Kafka instead, and this reads them and writes to BigQuery.

**It uses the legacy streaming API, not the Storage Write API, and that is a deliberate deviation
from the plan.** The Storage Write API is free below 2 TiB where the legacy one costs $0.05/GB, but
at the measured ~70 events/sec that is 54 GB/month — a difference of **$2.72 a month**. The Python
Storage Write client speaks protobuf only, so using it means either hand-rolling the wire format or
maintaining a `.proto` and a build step: a third definition of the same schema after the table DDL
and the Flink DDL, and the third definition is always the one that drifts.

`insert_rows_json` takes dictionaries, returns per-row errors, and is a few lines. Revisit if the
volume ever grows an order of magnitude, at which point the arithmetic changes and a generated proto
earns its place.
"""

import logging

from google.cloud import bigquery

log = logging.getLogger("p01.sink.writer")


class WriteFailed(Exception):
    """BigQuery rejected one or more rows.

    Raised rather than logged and skipped. The caller commits Kafka offsets only after a successful
    write, so swallowing this would advance past rows that never landed — the counts would silently
    stop matching the stream, which is the exact failure this project is about.
    """


class Writer:
    def __init__(self, client=None):
        self._client = client or bigquery.Client()

    def append(self, table: str, rows: list[dict]) -> int:
        """Write a batch to one table. Returns the number of rows written.

        Blocks until BigQuery answers, because the caller commits offsets immediately afterwards and
        has to know the rows are durable before it does.
        """
        if not rows:
            return 0

        errors = self._client.insert_rows_json(table, rows)
        if errors:
            # Per-row detail, truncated. The first failure is nearly always the whole story — a
            # schema change affects every row in the batch identically.
            first = errors[0]
            raise WriteFailed(
                f"{table}: {len(errors)} of {len(rows)} rows rejected; first: {first}"
            )
        return len(rows)


def build_client() -> bigquery.Client:
    """Application Default Credentials, which under Workload Identity resolve to wl-p01-streaming@."""
    return bigquery.Client()
