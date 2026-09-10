"""The bridge's only piece of durable state: how far through the firehose it has got.

One integer, written to a file on a ReadWriteOnce volume. The volume matters as much as the file —
it is what makes two bridge pods impossible, and two bridges would both read the whole firehose and
produce every event twice.
"""

import logging
import os
import tempfile
import time

from bridge.config import (
    JETSTREAM_RETENTION_SECONDS,
    MAX_REPLAY_SECONDS,
    REPLAY_OVERLAP_SECONDS,
)

log = logging.getLogger("p01.bridge.cursor")


def load(path: str) -> int | None:
    """The last committed position, or None if there is nothing usable to resume from.

    A missing file is a first boot. A corrupt one is treated the same way rather than fatally,
    because the alternative — refusing to start — turns a recoverable few seconds of lost position
    into an outage that needs a human.
    """
    try:
        with open(path) as handle:
            raw = handle.read().strip()
    except FileNotFoundError:
        log.info("no cursor at %s; starting from the live tail", path)
        return None
    except OSError as exc:
        log.warning("cursor at %s unreadable (%s); starting from the live tail", path, exc)
        return None

    try:
        value = int(raw)
    except ValueError:
        log.warning("cursor at %s is %r, not an integer; starting from the live tail", path, raw)
        return None

    if value <= 0:
        log.warning("cursor at %s is %d; starting from the live tail", path, value)
        return None

    return value


def save(path: str, time_us: int) -> None:
    """Write the cursor atomically.

    Written to a temporary file in the same directory and renamed, because rename is atomic within a
    filesystem while a partial write is not. A pod killed mid-write would otherwise leave a truncated
    integer, and the next boot would resume from a plausible but wrong position — worse than no
    cursor at all, which at least announces itself.
    """
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w", dir=directory, prefix=".cursor.", delete=False
    )
    try:
        handle.write(str(time_us))
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(handle.name, path)
    except BaseException:
        handle.close()
        try:
            os.unlink(handle.name)
        except OSError:
            pass
        raise


def resume_from(cursor: int | None, now: float | None = None) -> int | None:
    """Where to actually reconnect, given the last committed position.

    Three cases, and the middle one is the interesting one:

      * No cursor — start live. There is no history worth reconstructing on a first boot.
      * A recent cursor — rewind a few seconds past it. The overlap makes duplicates certain and a
        gap impossible; downstream dedup removes the duplicates and nothing could remove a gap.
      * A cursor older than MAX_REPLAY_SECONDS — abandon it. Replaying hours of backlog floods the
        broker at many times realtime, and past a point the freshest data is worth more than the
        oldest. The gap is logged rather than hidden.
    """
    if cursor is None:
        return None

    now = time.time() if now is None else now
    age = now - cursor / 1e6

    if age > MAX_REPLAY_SECONDS:
        log.warning(
            "cursor is %.0f minutes old, past the %.0f-minute replay cap; "
            "skipping %.0f minutes of backlog and starting live",
            age / 60,
            MAX_REPLAY_SECONDS / 60,
            age / 60,
        )
        return None

    return cursor - REPLAY_OVERLAP_SECONDS * 1_000_000


def report_gap(requested: int | None, first_seen: int) -> int:
    """Compare what we asked Jetstream for against what it actually sent, and return the gap.

    This exists because of a measured behaviour that is easy to miss: a cursor older than Jetstream's
    ~36-hour buffer does not produce an error. The server silently begins at the oldest event it
    still holds, so a two-day outage reconnects, streams happily, and looks identical to a clean
    restart while two days of posts are simply gone.

    Returns the gap in microseconds, zero when there is nothing to report.
    """
    if requested is None:
        return 0

    gap_us = first_seen - requested
    if gap_us <= 0:
        return 0

    gap_seconds = gap_us / 1e6

    # A couple of seconds is the replay overlap landing slightly ahead of where it aimed, or simply
    # the next event not having existed at that exact microsecond.
    if gap_seconds < 2 * REPLAY_OVERLAP_SECONDS:
        return 0

    if gap_seconds > JETSTREAM_RETENTION_SECONDS * 0.9:
        log.error(
            "requested a cursor %.1f hours old but the first event is %.1f hours newer: "
            "the cursor fell outside Jetstream's retention window and that data is unrecoverable",
            (time.time() - requested / 1e6) / 3600,
            gap_seconds / 3600,
        )
    else:
        log.warning(
            "gap of %.1f seconds between the requested cursor and the first event received",
            gap_seconds,
        )
    return gap_us
