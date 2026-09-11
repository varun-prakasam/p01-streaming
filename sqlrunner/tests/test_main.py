"""The runner, driven by a fake TableEnvironment.

No JVM and no pyflink import: the point of keeping build_table_env in its own function is that
everything around it can be exercised in milliseconds.
"""

import os
import shutil
import sys
import tempfile
import types
import unittest

from sqlrunner import main as runner


class FakeStatementSet:
    def __init__(self):
        self.inserts = []
        self.executed = False

    def add_insert_sql(self, sql):
        self.inserts.append(sql)

    def execute(self):
        self.executed = True
        return "job-handle"


class FakeTableEnv:
    def __init__(self):
        self.executed = []
        self.statement_set = FakeStatementSet()
        self.config = {}

    def create_statement_set(self):
        return self.statement_set

    def execute_sql(self, sql):
        self.executed.append(sql)


class SqlFilesTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def write(self, name, text):
        with open(os.path.join(self.dir, name), "w") as handle:
            handle.write(text)

    def test_files_come_back_in_lexical_order(self):
        """The numeric prefixes are the dependency order: a view created before its source table
        fails, and a directory listing is not sorted on every filesystem."""
        for name in ("03_c.sql", "01_a.sql", "02_b.sql"):
            self.write(name, "SELECT 1;")
        names = [os.path.basename(p) for p in runner.sql_files(self.dir)]
        self.assertEqual(names, ["01_a.sql", "02_b.sql", "03_c.sql"])

    def test_non_sql_files_are_ignored(self):
        self.write("01_a.sql", "SELECT 1;")
        self.write("README.md", "hello")
        self.assertEqual(len(runner.sql_files(self.dir)), 1)

    def test_a_missing_directory_is_an_error(self):
        with self.assertRaises(FileNotFoundError):
            runner.sql_files(os.path.join(self.dir, "nope"))

    def test_an_empty_directory_is_an_error(self):
        """An image built without its SQL would otherwise start, submit nothing, and sit there
        looking like a healthy job."""
        with self.assertRaises(FileNotFoundError):
            runner.sql_files(self.dir)

    def test_statements_are_tagged_with_their_file(self):
        self.write("01_a.sql", "CREATE TABLE t (a INT);")
        self.write("02_b.sql", "INSERT INTO t SELECT 1;")
        loaded = runner.load_statements(self.dir)
        self.assertEqual([origin for origin, _ in loaded], ["01_a.sql", "02_b.sql"])


class RunTest(unittest.TestCase):
    def test_ddl_executes_immediately_and_inserts_are_batched(self):
        t_env = FakeTableEnv()
        statements = [
            ("a.sql", "CREATE TABLE t (a INT)"),
            ("b.sql", "INSERT INTO t SELECT 1"),
            ("b.sql", "INSERT INTO u SELECT 2"),
        ]
        self.assertEqual(runner.run(t_env, statements), "job-handle")
        self.assertEqual(t_env.executed, ["CREATE TABLE t (a INT)"])
        self.assertEqual(len(t_env.statement_set.inserts), 2)
        self.assertTrue(t_env.statement_set.executed)

    def test_every_insert_lands_in_one_statement_set(self):
        """Four jobs instead of one would let fast and settled restart independently and disagree
        for reasons that are not about watermarks. This is the assertion that keeps them together."""
        t_env = FakeTableEnv()
        runner.run(t_env, [("a.sql", "INSERT INTO {} SELECT 1".format(n)) for n in "abcd"])
        self.assertEqual(len(t_env.statement_set.inserts), 4)

    def test_a_file_set_with_no_inserts_is_an_error(self):
        """It would start, checkpoint forever and produce nothing — healthy by every signal the
        operator has."""
        t_env = FakeTableEnv()
        with self.assertRaises(ValueError):
            runner.run(t_env, [("a.sql", "CREATE TABLE t (a INT)")])
        self.assertFalse(t_env.statement_set.executed)


class MainTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def write(self, name, text):
        with open(os.path.join(self.dir, name), "w") as handle:
            handle.write(text)

    def test_main_submits_through_the_injected_environment(self):
        self.write("01.sql", "CREATE TABLE t (a INT);\nINSERT INTO t SELECT 1;")
        t_env = FakeTableEnv()
        self.assertEqual(runner.main(["--sql-dir", self.dir], env_factory=lambda: t_env), 0)
        self.assertTrue(t_env.statement_set.executed)

    def test_dry_run_touches_no_environment(self):
        self.write("01.sql", "CREATE TABLE t (a INT);\nINSERT INTO t SELECT 1;")

        def explode():
            raise AssertionError("dry run must not build a TableEnvironment")

        self.assertEqual(
            runner.main(["--sql-dir", self.dir, "--dry-run"], env_factory=explode), 0
        )

    def test_dry_run_fails_when_nothing_would_be_produced(self):
        """This is what makes --dry-run worth running in CI: it catches the edit that leaves the
        job with tables and no inserts, before it reaches a cluster."""
        self.write("01.sql", "CREATE TABLE t (a INT);")
        with self.assertRaises(ValueError):
            runner.main(["--sql-dir", self.dir, "--dry-run"], env_factory=lambda: FakeTableEnv())

    def test_the_real_sql_directory_passes_a_dry_run(self):
        """The committed pipeline, split and classified for real."""
        here = os.path.dirname(__file__)
        sql_dir = os.path.abspath(os.path.join(here, "..", "..", "sql"))
        self.assertEqual(runner.main(["--sql-dir", sql_dir, "--dry-run"]), 0)


class BuildTableEnvTest(unittest.TestCase):
    """Covers the one function that touches pyflink, using a stand-in module.

    A `pragma: no cover` here would hide the two config lines below, and those are the settings that
    decide what time zone every emitted timestamp is rendered in.
    """

    def setUp(self):
        self.settings = {}
        recorder = self.settings

        class FakeConfig:
            def set(self, key, value):
                recorder[key] = value

        class FakeTableEnv:
            def get_config(self):
                return FakeConfig()

        module = types.ModuleType("pyflink.table")
        module.EnvironmentSettings = types.SimpleNamespace(
            in_streaming_mode=staticmethod(lambda: "streaming")
        )
        module.TableEnvironment = types.SimpleNamespace(
            create=staticmethod(lambda settings: FakeTableEnv())
        )
        package = types.ModuleType("pyflink")
        package.table = module
        self.addCleanup(sys.modules.pop, "pyflink", None)
        self.addCleanup(sys.modules.pop, "pyflink.table", None)
        sys.modules["pyflink"] = package
        sys.modules["pyflink.table"] = module

    def test_the_session_time_zone_is_utc(self):
        """Without this, CAST(ts AS STRING) renders in the TaskManager's local zone and BigQuery
        gains a silent offset — indistinguishable, in this project, from the clock skew it measures."""
        runner.build_table_env()
        self.assertEqual(self.settings["table.local-time-zone"], "UTC")

    def test_the_job_is_named(self):
        runner.build_table_env()
        self.assertEqual(self.settings["pipeline.name"], runner.JOB_NAME)


if __name__ == "__main__":
    unittest.main()
