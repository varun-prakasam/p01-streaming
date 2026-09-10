"""Frame classification.

Three outcomes, and confusing any two of them loses data quietly:

  * an Event      — produce it, and advance the cursor to its time_us
  * None          — a kind this project does not model; ignore it
  * Undeliverable — dead-letter it, because something upstream changed

The fixtures are real frames captured from wss://jetstream2.us-east.bsky.network, trimmed of post
text. Synthetic envelopes would test the parser against my idea of the format rather than Bluesky's.
"""

import json
import unittest

from bridge import parse

# A real create, as captured.
COMMIT_CREATE = json.dumps(
    {
        "did": "did:plc:stphynjoait4z26lv32cgdl6",
        "time_us": 1789036937215397,
        "kind": "commit",
        "commit": {
            "rev": "3mv5ujlsrir2b",
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "3mv5ujlnfss2i",
            "record": {
                "$type": "app.bsky.feed.post",
                "createdAt": "2026-09-10T10:42:16.672Z",
                "langs": ["en"],
                "text": "hello",
            },
            "cid": "bafyreiapynpnaoruuyfo3bdbnnsrmqgffyxtrjjxjxn52h6sggyh4xpu74",
        },
    }
).encode()

# A delete. Note the absent `record` — 89 of 1,174 sampled frames looked like this.
COMMIT_DELETE = json.dumps(
    {
        "did": "did:plc:abc",
        "time_us": 1789036937215400,
        "kind": "commit",
        "commit": {
            "rev": "3mv5ujlsrir2c",
            "operation": "delete",
            "collection": "app.bsky.feed.post",
            "rkey": "3mv5ujlnfss2j",
        },
    }
).encode()


def frame(**overrides) -> bytes:
    """A real create frame with top-level fields replaced."""
    envelope = json.loads(COMMIT_CREATE)
    envelope.update(overrides)
    return json.dumps(envelope).encode()


def without(key: str) -> bytes:
    envelope = json.loads(COMMIT_CREATE)
    envelope.pop(key, None)
    return json.dumps(envelope).encode()


class UsableFramesTest(unittest.TestCase):
    def test_create_is_produced(self):
        event = parse.parse(COMMIT_CREATE)
        self.assertEqual(event.key, "did:plc:stphynjoait4z26lv32cgdl6")
        self.assertEqual(event.time_us, 1789036937215397)
        self.assertEqual(event.raw, COMMIT_CREATE)

    def test_delete_is_produced_despite_having_no_record(self):
        """A delete carries no `record`. Requiring one would dead-letter a twelfth of the stream,
        and the SQL needs deletes to attribute revisions to retraction later."""
        event = parse.parse(COMMIT_DELETE)
        self.assertIsNotNone(event)
        self.assertEqual(event.key, "did:plc:abc")

    def test_raw_bytes_are_forwarded_untouched(self):
        """The bridge projects and routes; it does not transform. Flink parses the original."""
        event = parse.parse(COMMIT_CREATE)
        self.assertIs(event.raw, COMMIT_CREATE)

    def test_an_unmodelled_collection_still_passes(self):
        """Server-side filtering decides what arrives. Second-guessing it here would mean a config
        change to wantedCollections silently produced nothing."""
        envelope = json.loads(COMMIT_CREATE)
        envelope["commit"]["collection"] = "app.bsky.graph.follow"
        self.assertIsNotNone(parse.parse(json.dumps(envelope).encode()))


class IgnoredFramesTest(unittest.TestCase):
    def test_identity_and_account_are_skipped_not_dead_lettered(self):
        """Handle and status changes are real Jetstream traffic this project does not model.
        Dead-lettering them would make the DLQ rate alert fire on healthy operation."""
        for kind in ("identity", "account"):
            with self.subTest(kind=kind):
                self.assertIsNone(parse.parse(frame(kind=kind)))


class UndeliverableTest(unittest.TestCase):
    def assertUndeliverable(self, raw: bytes):
        with self.assertRaises(parse.Undeliverable):
            parse.parse(raw)

    def test_not_json(self):
        self.assertUndeliverable(b"{not json")

    def test_invalid_utf8(self):
        self.assertUndeliverable(b"\xff\xfe\x00")

    def test_json_that_is_not_an_object(self):
        for payload in (b"[1,2,3]", b'"a string"', b"42", b"null"):
            with self.subTest(payload=payload):
                self.assertUndeliverable(payload)

    def test_unknown_kind(self):
        """A new `kind` means Bluesky changed the protocol. Someone should see that."""
        self.assertUndeliverable(frame(kind="something_new"))

    def test_missing_kind(self):
        self.assertUndeliverable(without("kind"))

    # -- time_us: the cursor depends on it, so every bad shape must be caught ---------------------

    def test_time_us_missing(self):
        self.assertUndeliverable(without("time_us"))

    def test_time_us_not_an_integer(self):
        for value in ("1789036937215397", 1.5, None, [], {}):
            with self.subTest(value=value):
                self.assertUndeliverable(frame(time_us=value))

    def test_time_us_booleans_are_rejected(self):
        """bool is a subclass of int in Python, so `isinstance(True, int)` is True. Without the
        explicit check, True would become a cursor of 1 — an epoch timestamp of 1970, which on the
        next reconnect requests a replay from before Jetstream existed."""
        for value in (True, False):
            with self.subTest(value=value):
                self.assertUndeliverable(frame(time_us=value))

    def test_time_us_zero_or_negative(self):
        for value in (0, -1):
            with self.subTest(value=value):
                self.assertUndeliverable(frame(time_us=value))

    # -- did: the partition key --------------------------------------------------------------------

    def test_did_missing_or_wrong_type(self):
        for value in (None, "", 123, [], {}):
            with self.subTest(value=value):
                self.assertUndeliverable(frame(did=value))

    # -- commit ------------------------------------------------------------------------------------

    def test_commit_missing_or_wrong_type(self):
        for value in (None, "a string", [], 5):
            with self.subTest(value=value):
                self.assertUndeliverable(frame(commit=value))

    def test_commit_without_collection(self):
        envelope = json.loads(COMMIT_CREATE)
        del envelope["commit"]["collection"]
        self.assertUndeliverable(json.dumps(envelope).encode())

    def test_commit_collection_wrong_type(self):
        envelope = json.loads(COMMIT_CREATE)
        envelope["commit"]["collection"] = 42
        self.assertUndeliverable(json.dumps(envelope).encode())


if __name__ == "__main__":
    unittest.main()
