"""Agreement between the SQL files, and between them and the sink's routing table.

None of this is testable by exercising Python. Every check below is a way the pipeline can be
wrong while every Python test still passes, and most of them fail quietly rather than loudly:

  * a Flink sink column renamed but not the BigQuery one — insert_rows_json reports the row as an
    unknown-field error and the dashboard simply stops advancing;
  * the fast watermark changed from 5s to 15s but the emitted watermark_delay_s left at 5 — the
    dashboard then mislabels the one number the project exists to publish;
  * the two enrichment views drifting apart — fast and settled would be computed differently, and
    the revision between them would be an artefact of our SQL rather than of the watermark.
"""

import os
import re
import unittest

from sink.config import ROUTES
from sqlrunner.statements import split_statements
from sqlrunner.tests import sqlparse

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SOURCES = os.path.join(ROOT, "sql", "01_sources.sql")
VIEWS = os.path.join(ROOT, "sql", "02_views.sql")
SINKS = os.path.join(ROOT, "sql", "03_sinks.sql")
PIPELINE = os.path.join(ROOT, "sql", "04_pipeline.sql")
DDL = os.path.join(ROOT, "sql", "ddl", "tables.sql")


def squeeze(text):
    return re.sub(r"\s+", " ", text).strip()


def topics(path):
    """{flink table: kafka topic} from the connector options."""
    found = {}
    for statement in split_statements(sqlparse.read(path)):
        clean = sqlparse.strip_comments(statement)
        table = re.match(r"CREATE\s+TABLE\s+(\w+)\s*\(", clean, re.I)
        topic = re.search(r"'topic'\s*=\s*'([^']+)'", clean)
        if table and topic:
            found[table.group(1)] = topic.group(1)
    return found


class EnrichmentViewsTest(unittest.TestCase):
    """The two views must differ only in their name and their source."""

    def setUp(self):
        self.statements = split_statements(sqlparse.read(VIEWS))

    def test_there_are_exactly_two(self):
        self.assertEqual(len(self.statements), 2)

    def test_they_are_identical_apart_from_name_and_source(self):
        """They cannot be one view — they read two tables with different watermarks, which is the
        entire design. So the only thing keeping them in step is this assertion."""
        def normalise(text):
            text = text.replace("posts_fast_enriched", "ENRICHED")
            text = text.replace("posts_settled_enriched", "ENRICHED")
            text = re.sub(r"FROM posts_(fast|settled)\b", "FROM SOURCE", text)
            return squeeze(text)

        first, second = (normalise(s) for s in self.statements)
        self.assertEqual(first, second, "the two enrichment views have drifted apart")

    def test_each_reads_the_source_its_name_promises(self):
        """Normalising the two for comparison would happily pass if both read posts_fast."""
        fast = next(s for s in self.statements if "posts_fast_enriched" in s)
        settled = next(s for s in self.statements if "posts_settled_enriched" in s)
        self.assertIn("FROM posts_fast", fast)
        self.assertNotIn("FROM posts_settled", fast)
        self.assertIn("FROM posts_settled", settled)
        self.assertNotIn("FROM posts_fast", settled)


class WatermarkTest(unittest.TestCase):
    def setUp(self):
        self.marks = sqlparse.watermark_seconds(SOURCES)

    def test_both_sources_watermark_on_the_observed_clock(self):
        """The single most important line in the project. A watermark on claimed_time is dragged
        forward by the worst client clock on the network — measured at two hours ahead, several
        times a minute — and then discards everything arriving normally."""
        for table, mark in self.marks.items():
            with self.subTest(table=table):
                self.assertEqual(mark["column"], "observed_time")

    def test_the_two_delays_actually_differ(self):
        """Equal delays would make fast and settled identical, v_revisions would report a delta of
        zero for every window, and the dashboard would look fine while proving nothing."""
        self.assertNotEqual(self.marks["posts_fast"]["seconds"], self.marks["posts_settled"]["seconds"])

    def test_settled_waits_longer_than_fast(self):
        self.assertGreater(self.marks["posts_settled"]["seconds"], self.marks["posts_fast"]["seconds"])

    def test_settled_covers_a_bridge_reconnect(self):
        """The replay burst after a reconnect is precisely the data the fast view misses. A settled
        watermark shorter than that would miss it too, and the two views would agree for the wrong
        reason."""
        self.assertGreaterEqual(self.marks["posts_settled"]["seconds"], 300)


class AggregateTest(unittest.TestCase):
    def setUp(self):
        self.inserts = sqlparse.inserts(PIPELINE)
        self.marks = sqlparse.watermark_seconds(SOURCES)

    def test_the_emitted_delay_matches_the_actual_watermark(self):
        """watermark_delay_s is what the dashboard labels each series with. If someone widens the
        fast watermark and leaves this literal alone, every chart is captioned with a number that
        is no longer true — and nothing anywhere else would notice."""
        pairs = {
            "out_counts_fast": "posts_fast",
            "out_counts_settled": "posts_settled",
        }
        for target, source in pairs.items():
            with self.subTest(target=target):
                body = self.inserts[target]["body"]
                literal = re.search(r"(\d+)\s+AS watermark_delay_s", body)
                self.assertIsNotNone(literal, "no watermark_delay_s literal in " + target)
                self.assertEqual(int(literal.group(1)), self.marks[source]["seconds"])

    def test_each_aggregate_reads_the_matching_enriched_view(self):
        self.assertIn("posts_fast_enriched", self.inserts["out_counts_fast"]["from"])
        self.assertIn("posts_settled_enriched", self.inserts["out_counts_settled"]["from"])

    def test_the_two_aggregations_are_computed_identically(self):
        """If they are not, the difference between them is our SQL rather than the watermark, and
        the project's only claim evaporates."""
        def normalise(text):
            text = re.sub(r"posts_(fast|settled)_enriched", "SRC", text)
            text = re.sub(r"out_counts_(fast|settled)", "OUT", text)
            text = re.sub(r"\d+\s+AS watermark_delay_s", "N AS watermark_delay_s", text)
            return squeeze(text)

        self.assertEqual(
            normalise(self.inserts["out_counts_fast"]["body"]),
            normalise(self.inserts["out_counts_settled"]["body"]),
        )

    def test_both_aggregates_count_the_same_operations(self):
        """Counting creates in one and every commit in the other would produce a revision that is
        really a difference in definition."""
        for target in ("out_counts_fast", "out_counts_settled"):
            with self.subTest(target=target):
                self.assertIn("operation = 'create'", self.inserts[target]["body"])

    def test_the_raw_projection_reads_the_fast_view(self):
        """Raw has no reason to wait ten minutes, and reading the settled view would delay the
        audit surface behind the thing it exists to audit."""
        self.assertIn("posts_fast_enriched", self.inserts["out_raw"]["from"])


class ColumnAgreementTest(unittest.TestCase):
    def setUp(self):
        self.flink = sqlparse.flink_tables(SINKS)
        self.bigquery = sqlparse.bigquery_tables(DDL)
        self.topics = topics(SINKS)

    def test_every_sink_table_has_a_route(self):
        """A Flink table writing a topic nothing consumes produces data that reaches no warehouse,
        and every component involved reports itself healthy."""
        for table, topic in self.topics.items():
            with self.subTest(table=table):
                self.assertIn(topic, ROUTES, "{} writes {}, which the sink does not read".format(table, topic))

    def test_every_route_has_a_producer(self):
        """And the reverse: a route with no producer is a consumer group that will sit at zero
        forever while looking subscribed."""
        for topic in ROUTES:
            with self.subTest(topic=topic):
                self.assertIn(topic, set(self.topics.values()))

    def test_flink_and_bigquery_columns_match_exactly(self):
        """The sink writes JSON keyed by column name and does no mapping at all. A name that exists
        on one side and not the other is an unknown-field error per row — the sink logs it, the job
        stays green, and the table stops filling."""
        for table, topic in self.topics.items():
            with self.subTest(table=table):
                bq_columns = self.bigquery[ROUTES[topic]]
                self.assertEqual(self.flink[table], bq_columns)

    def test_each_insert_supplies_every_column_of_its_target(self):
        """A short select list is a submit-time error, which is survivable. A long one is not
        obviously either, and neither is worth discovering on a cluster."""
        for target, insert in sqlparse.inserts(PIPELINE).items():
            with self.subTest(target=target):
                self.assertEqual(len(insert["select"]), len(self.flink[target]))


class SourceTest(unittest.TestCase):
    def setUp(self):
        self.statements = split_statements(sqlparse.read(SOURCES))

    def test_both_sources_read_the_same_topic(self):
        """Two watermarks over one log. Two topics would be two pipelines, and the comparison would
        be meaningless."""
        found = topics(SOURCES)
        self.assertEqual(len(set(found.values())), 1)

    def test_the_sources_use_different_consumer_groups(self):
        """A shared group id makes them steal partitions from each other, and each would silently
        see part of the stream — which looks exactly like data loss upstream."""
        groups = re.findall(r"'properties.group.id'\s*=\s*'([^']+)'", sqlparse.read(SOURCES))
        self.assertEqual(len(groups), 2)
        self.assertEqual(len(set(groups)), 2)

    def test_both_sources_set_an_idle_timeout(self):
        """Without it a single idle partition halts the global watermark: no window ever closes,
        checkpoints keep succeeding, the job stays green and the dashboard silently stops. It is
        the most likely silent failure in the project and one line to prevent."""
        for statement in self.statements:
            with self.subTest(statement=statement.splitlines()[0]):
                self.assertIn("scan.watermark.idle-timeout", statement)


if __name__ == "__main__":
    unittest.main()
