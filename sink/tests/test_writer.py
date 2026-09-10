"""The BigQuery append.

Small on purpose. The whole argument for this component is in its docstring — Google's Flink
BigQuery connector exists only for Flink 1.17, so the write happens here instead — and the whole
argument for using the legacy streaming API rather than the Storage Write API is $2.72 a month
against a hand-maintained protobuf schema.

What matters is that a rejection is never swallowed: the caller commits Kafka offsets immediately
after this returns.
"""

import unittest
from unittest import mock

from sink.writer import WriteFailed, Writer


class AppendTest(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()
        self.client.insert_rows_json.return_value = []
        self.writer = Writer(self.client)

    def test_writes_rows_and_returns_the_count(self):
        rows = [{"uri": "at://a"}, {"uri": "at://b"}]
        self.assertEqual(self.writer.append("proj.ds.tbl", rows), 2)
        self.client.insert_rows_json.assert_called_once_with("proj.ds.tbl", rows)

    def test_an_empty_batch_makes_no_call(self):
        """A quiet poll should not become a BigQuery round trip."""
        self.assertEqual(self.writer.append("proj.ds.tbl", []), 0)
        self.client.insert_rows_json.assert_not_called()

    def test_a_rejection_raises_rather_than_returning_quietly(self):
        """The caller commits offsets on the strength of this returning. Swallowing a rejection
        would advance past rows that never landed, and the counts would stop matching the stream
        with nothing in any log to say so."""
        self.client.insert_rows_json.return_value = [
            {"index": 0, "errors": [{"reason": "invalid", "message": "no such field: nope"}]}
        ]
        with self.assertRaises(WriteFailed) as caught:
            self.writer.append("proj.ds.tbl", [{"nope": 1}])
        self.assertIn("proj.ds.tbl", str(caught.exception))

    def test_the_error_names_how_many_rows_were_rejected(self):
        self.client.insert_rows_json.return_value = [
            {"index": i, "errors": [{"reason": "invalid"}]} for i in range(3)
        ]
        with self.assertRaises(WriteFailed) as caught:
            self.writer.append("proj.ds.tbl", [{"a": 1}] * 5)
        self.assertIn("3 of 5", str(caught.exception))

    def test_a_client_exception_propagates(self):
        """A network failure must reach the caller too, for the same reason a rejection does."""
        self.client.insert_rows_json.side_effect = RuntimeError("deadline exceeded")
        with self.assertRaises(RuntimeError):
            self.writer.append("proj.ds.tbl", [{"a": 1}])

    def test_it_builds_its_own_client_when_not_given_one(self):
        with mock.patch("sink.writer.bigquery.Client") as ctor:
            Writer()
        ctor.assert_called_once()


class ClientTest(unittest.TestCase):
    def test_build_client_uses_application_default_credentials(self):
        """Under Workload Identity this resolves to wl-p01-streaming@ with no key file — the
        platform's hard rule."""
        with mock.patch("sink.writer.bigquery.Client") as ctor:
            from sink.writer import build_client

            build_client()
        ctor.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
