"""Envelope inspection.

Deliberately not a transformation layer. The bridge decides three things about a frame — is it
usable, what key does it go under, and what cursor position does it represent — and forwards the
original bytes untouched. Everything derived (`skew_seconds`, `clock_suspect`, `text_length`) is
computed in Flink SQL, where it can be read as SQL rather than as Python.

That boundary is what keeps this module worth testing exhaustively: it is all decisions and no
arithmetic, and every decision here can silently lose data.
"""

import json
from dataclasses import dataclass

# Jetstream `kind` values. `commit` carries repository writes; `identity` and `account` are handle
# and status changes that this project does not model. They are skipped rather than dead-lettered —
# a message we deliberately ignore is not a failure.
KIND_COMMIT = "commit"
SKIPPABLE_KINDS = frozenset({"identity", "account"})


@dataclass(frozen=True)
class Event:
    """One usable Jetstream frame, with just enough extracted to route and checkpoint it."""

    raw: bytes
    key: str
    time_us: int


class Undeliverable(Exception):
    """The frame cannot be produced: it is malformed, or is missing the fields routing needs.

    Distinct from "skip this kind" — an Undeliverable frame goes to the dead-letter topic, because
    it means either Bluesky changed the lexicon or something upstream is corrupting messages, and
    both are things somebody should see.
    """


def parse(raw: bytes) -> Event | None:
    """Classify one frame.

    Returns an Event to produce, or None for a frame that is legitimately ignored. Raises
    Undeliverable for a frame that should be dead-lettered.
    """
    try:
        envelope = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise Undeliverable(f"not JSON: {exc}") from exc

    if not isinstance(envelope, dict):
        raise Undeliverable(f"envelope is {type(envelope).__name__}, not an object")

    kind = envelope.get("kind")
    if kind in SKIPPABLE_KINDS:
        return None
    if kind != KIND_COMMIT:
        raise Undeliverable(f"unknown kind {kind!r}")

    # The cursor is the only piece of state the bridge owns, and it is derived from this field. A
    # frame without a usable time_us cannot be checkpointed, so producing it would mean either
    # advancing the cursor past an event we cannot describe or never advancing it at all.
    time_us = envelope.get("time_us")
    if not isinstance(time_us, int) or isinstance(time_us, bool) or time_us <= 0:
        raise Undeliverable(f"time_us is {time_us!r}")

    # The partition key. Keying by author rather than round-robin keeps one account's posts in one
    # partition and therefore in order, which is what makes a later create/delete join tractable.
    did = envelope.get("did")
    if not isinstance(did, str) or not did:
        raise Undeliverable(f"did is {did!r}")

    commit = envelope.get("commit")
    if not isinstance(commit, dict):
        raise Undeliverable("commit is missing or not an object")

    # Deletes carry no `record` at all — 89 of 1,174 sampled frames. That is normal traffic, not a
    # defect, and the SQL needs deletes to attribute revisions later. Asserting `record` here would
    # dead-letter a twelfth of the stream.
    if not isinstance(commit.get("collection"), str):
        raise Undeliverable("commit.collection is missing or not a string")

    return Event(raw=raw, key=did, time_us=time_us)
