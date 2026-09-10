"""The cursor: load, save, where to resume from, and whether the server honoured the request.

This is the only durable state the bridge owns, and every failure mode here is silent. A cursor that
reads back wrong does not crash anything — it just resumes from the wrong place, and either replays
a mountain or skips a gap. Neither announces itself.
"""

import os
import tempfile
import unittest
from unittest import mock

from bridge import cursor
from bridge.config import (
    JETSTREAM_RETENTION_SECONDS,
    MAX_REPLAY_SECONDS,
    REPLAY_OVERLAP_SECONDS,
)

NOW = 1_789_000_000.0  # a fixed wall clock, so nothing here depends on when it runs
US = 1_000_000


class LoadTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "cursor")

    def write(self, text: str):
        with open(self.path, "w") as handle:
            handle.write(text)

    def test_missing_file_is_a_first_boot(self):
        self.assertIsNone(cursor.load(self.path))

    def test_round_trip(self):
        cursor.save(self.path, 1789036937215397)
        self.assertEqual(cursor.load(self.path), 1789036937215397)

    def test_surrounding_whitespace_is_tolerated(self):
        self.write("  1789036937215397\n")
        self.assertEqual(cursor.load(self.path), 1789036937215397)

    def test_corrupt_file_starts_live_rather_than_refusing_to_boot(self):
        """A truncated write from a killed pod must not become an outage that needs a human. Losing
        position costs seconds of duplicates; refusing to start costs however long nobody notices."""
        for content in ("", "   ", "not-a-number", "12.5", "1789036937215397extra"):
            with self.subTest(content=content):
                self.write(content)
                self.assertIsNone(cursor.load(self.path))

    def test_nonsense_values_are_rejected(self):
        for content in ("0", "-1"):
            with self.subTest(content=content):
                self.write(content)
                self.assertIsNone(cursor.load(self.path))

    def test_unreadable_file_starts_live(self):
        self.write("1789036937215397")
        with mock.patch("builtins.open", side_effect=PermissionError("nope")):
            self.assertIsNone(cursor.load(self.path))


class SaveTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "cursor")

    def test_creates_the_directory(self):
        nested = os.path.join(self.dir, "a", "b", "cursor")
        cursor.save(nested, 42)
        self.assertEqual(cursor.load(nested), 42)

    def test_overwrites_in_place(self):
        cursor.save(self.path, 1)
        cursor.save(self.path, 2)
        self.assertEqual(cursor.load(self.path), 2)

    def test_leaves_no_temporary_files_behind(self):
        cursor.save(self.path, 1789036937215397)
        self.assertEqual(os.listdir(self.dir), ["cursor"])

    def test_a_failed_write_leaves_the_previous_cursor_intact(self):
        """The write is to a temporary file and then renamed, because rename is atomic and a partial
        write is not. A pod killed mid-write must not leave a truncated integer that reads back as a
        plausible but wrong position."""
        cursor.save(self.path, 111)
        with mock.patch("os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                cursor.save(self.path, 222)
        self.assertEqual(cursor.load(self.path), 111)
        self.assertEqual(os.listdir(self.dir), ["cursor"], "temp file was not cleaned up")

    def test_a_failed_cleanup_does_not_mask_the_original_error(self):
        """If the rename fails and then removing the temporary file also fails, the caller must
        still see the disk error. Swallowing it to report the cleanup problem instead would hide
        the reason the cursor did not advance."""
        cursor.save(self.path, 111)
        with mock.patch("os.replace", side_effect=OSError("disk full")):
            with mock.patch("os.unlink", side_effect=OSError("gone already")):
                with self.assertRaises(OSError) as caught:
                    cursor.save(self.path, 222)
        self.assertIn("disk full", str(caught.exception))
        self.assertEqual(cursor.load(self.path), 111)


class ResumeFromTest(unittest.TestCase):
    def test_no_cursor_starts_live(self):
        self.assertIsNone(cursor.resume_from(None, now=NOW))

    def test_recent_cursor_rewinds_by_the_overlap(self):
        """Deliberate overlap. Duplicates are removed downstream; a gap could not be."""
        committed = int((NOW - 10) * US)
        self.assertEqual(
            cursor.resume_from(committed, now=NOW),
            committed - REPLAY_OVERLAP_SECONDS * US,
        )

    def test_cursor_just_inside_the_replay_cap_is_honoured(self):
        committed = int((NOW - (MAX_REPLAY_SECONDS - 1)) * US)
        self.assertIsNotNone(cursor.resume_from(committed, now=NOW))

    def test_cursor_past_the_replay_cap_is_abandoned(self):
        """Replaying hours of backlog floods the broker at many times realtime. Past a point the
        freshest data is worth more than the oldest, and the gap is logged rather than hidden."""
        committed = int((NOW - (MAX_REPLAY_SECONDS + 1)) * US)
        self.assertIsNone(cursor.resume_from(committed, now=NOW))

    def test_the_cap_boundary_itself(self):
        """Exactly at the cap is honoured; one second past is not. Tested at the boundary and one
        step either side, because `>` versus `>=` here is a silent hour of replay."""
        at = int((NOW - MAX_REPLAY_SECONDS) * US)
        self.assertIsNotNone(cursor.resume_from(at, now=NOW))

    def test_a_future_cursor_is_still_honoured(self):
        """Clock skew between pod restarts should not silently discard position."""
        committed = int((NOW + 30) * US)
        self.assertIsNotNone(cursor.resume_from(committed, now=NOW))


class ReportGapTest(unittest.TestCase):
    """The measured trap: a cursor older than Jetstream's ~36h buffer does not error. The server
    silently starts from the oldest event it holds, so a two-day outage reconnects, streams happily,
    and looks exactly like a clean restart while two days of posts are simply gone."""

    def test_no_request_means_nothing_to_compare(self):
        self.assertEqual(cursor.report_gap(None, int(NOW * US)), 0)

    def test_server_starting_before_the_request_is_not_a_gap(self):
        requested = int(NOW * US)
        self.assertEqual(cursor.report_gap(requested, requested - 5 * US), 0)

    def test_small_overshoot_is_not_a_gap(self):
        """The replay overlap lands slightly ahead of where it aimed, and the next event may simply
        not have existed at that microsecond. Reporting that would train everyone to ignore it."""
        requested = int(NOW * US)
        self.assertEqual(cursor.report_gap(requested, requested + 1 * US), 0)

    def test_a_real_gap_is_reported(self):
        requested = int(NOW * US)
        gap = cursor.report_gap(requested, requested + 600 * US)
        self.assertEqual(gap, 600 * US)

    def test_boundary_between_overlap_noise_and_a_real_gap(self):
        requested = int(NOW * US)
        threshold = 2 * REPLAY_OVERLAP_SECONDS
        self.assertEqual(cursor.report_gap(requested, requested + (threshold - 1) * US), 0)
        self.assertGreater(cursor.report_gap(requested, requested + (threshold + 1) * US), 0)

    def test_a_gap_past_the_retention_window_is_logged_as_unrecoverable(self):
        """The difference matters: a short gap is a hiccup, a gap the size of the retention window
        means the cursor fell out of the buffer and that data cannot be fetched by any means."""
        requested = int((NOW - JETSTREAM_RETENTION_SECONDS * 2) * US)
        first_seen = int(NOW * US)
        with self.assertLogs("p01.bridge.cursor", level="ERROR") as captured:
            gap = cursor.report_gap(requested, first_seen)
        self.assertGreater(gap, 0)
        self.assertIn("unrecoverable", captured.output[0])

    def test_a_moderate_gap_is_a_warning_not_an_error(self):
        requested = int(NOW * US)
        with self.assertLogs("p01.bridge.cursor", level="WARNING") as captured:
            cursor.report_gap(requested, requested + 600 * US)
        self.assertNotIn("ERROR", captured.output[0])


if __name__ == "__main__":
    unittest.main()
