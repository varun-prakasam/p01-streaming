"""The splitter, at the boundaries that make it worth having.

Every case below is one where splitting on ";" produces something that still parses. That is the
failure worth testing: a statement that fails loudly is a five-minute problem, and one that runs
and means something else is not.
"""

import unittest

from sqlrunner.statements import is_insert, split_statements, strip_comments


class SplitTest(unittest.TestCase):
    def test_plain_statements(self):
        self.assertEqual(split_statements("SELECT 1; SELECT 2;"), ["SELECT 1", "SELECT 2"])

    def test_a_trailing_semicolon_does_not_produce_an_empty_statement(self):
        """Flink rejects an empty statement, and the error names no file."""
        self.assertEqual(split_statements("SELECT 1;\n\n"), ["SELECT 1"])

    def test_a_missing_final_semicolon_still_yields_the_statement(self):
        self.assertEqual(split_statements("SELECT 1"), ["SELECT 1"])

    def test_semicolon_inside_a_string_literal_does_not_split(self):
        sql = "SELECT REGEXP_REPLACE(x, 'a;b', '') FROM t;"
        self.assertEqual(split_statements(sql), ["SELECT REGEXP_REPLACE(x, 'a;b', '') FROM t"])

    def test_semicolon_inside_a_quoted_identifier_does_not_split(self):
        sql = "SELECT `we;ird` FROM t;"
        self.assertEqual(split_statements(sql), ["SELECT `we;ird` FROM t"])

    def test_semicolon_inside_a_line_comment_does_not_split(self):
        sql = "SELECT 1 -- careful; here\nFROM t;"
        self.assertEqual(split_statements(sql), ["SELECT 1 -- careful; here\nFROM t"])

    def test_semicolon_inside_a_block_comment_does_not_split(self):
        sql = "SELECT /* a; b */ 1 FROM t;"
        self.assertEqual(split_statements(sql), ["SELECT /* a; b */ 1 FROM t"])

    def test_a_doubled_quote_escape_leaves_the_scanner_in_the_right_state(self):
        """'it''s' is one literal. Read as close-then-open it ends in the same state, so the
        semicolon that follows still splits."""
        self.assertEqual(split_statements("SELECT 'it''s'; SELECT 2"), ["SELECT 'it''s'", "SELECT 2"])

    def test_a_quote_inside_a_comment_is_not_a_literal(self):
        """The real files contain comments with apostrophes. Treating one as an opening quote
        swallows every following statement into a single blob."""
        sql = "-- it's fine\nSELECT 1;\nSELECT 2;"
        self.assertEqual(split_statements(sql), ["SELECT 1", "SELECT 2"])

    def test_a_line_comment_ends_at_the_newline(self):
        sql = "-- comment\nSELECT 1;SELECT 2"
        self.assertEqual(split_statements(sql), ["SELECT 1", "SELECT 2"])

    def test_a_block_comment_ends_at_the_terminator(self):
        sql = "/* a */ SELECT 1; /* b */ SELECT 2"
        self.assertEqual(len(split_statements(sql)), 2)

    def test_a_comment_only_file_yields_nothing(self):
        self.assertEqual(split_statements("-- nothing here\n\n"), [])

    def test_an_unterminated_literal_is_an_error_not_a_silent_blob(self):
        with self.assertRaises(ValueError):
            split_statements("SELECT 'oops; SELECT 2;")

    def test_an_unterminated_identifier_is_an_error(self):
        with self.assertRaises(ValueError):
            split_statements("SELECT `oops; SELECT 2;")

    def test_leading_comments_are_stripped_from_a_statement(self):
        sql = "-- header\n-- more\nCREATE TABLE t (a INT);"
        self.assertEqual(split_statements(sql), ["CREATE TABLE t (a INT)"])


class StripCommentsTest(unittest.TestCase):
    def test_inline_trailing_comments_survive(self):
        """Only whole comment lines go. An inline comment carries the reasoning next to the column
        it explains, and Flink parses it fine."""
        self.assertEqual(strip_comments("SELECT 1 -- why\nFROM t"), "SELECT 1 -- why\nFROM t")

    def test_indented_comment_lines_go(self):
        self.assertEqual(strip_comments("SELECT 1\n    -- note\nFROM t"), "SELECT 1\nFROM t")


class IsInsertTest(unittest.TestCase):
    def test_recognises_an_insert(self):
        self.assertTrue(is_insert("INSERT INTO t SELECT 1"))

    def test_is_case_and_whitespace_insensitive(self):
        self.assertTrue(is_insert("\n  insert into t SELECT 1"))

    def test_create_is_not_an_insert(self):
        self.assertFalse(is_insert("CREATE TABLE t (a INT)"))

    def test_a_word_merely_starting_with_insert_is_not_an_insert(self):
        """The trailing space in the prefix is what makes this false. Without it a hypothetical
        INSERTED_AT column at the head of a statement would be routed into the statement set."""
        self.assertFalse(is_insert("INSERTED something"))


if __name__ == "__main__":
    unittest.main()
