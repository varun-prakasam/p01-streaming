"""Submit the committed Flink SQL as one job.

Run as the FlinkDeployment's entry point:  python -m sqlrunner.main

The jobs are Flink SQL, as chosen. This is only the launcher, and it is deliberately the thinnest
thing that can read files in order and hand them to Flink. It defines no UDFs, so the TaskManagers
run no Python at all — the Python process exists for the seconds it takes to submit, and then the
job is pure JVM.

Every insert goes into a single statement set. That is the one non-obvious thing here and it is
load-bearing: four separate `execute_sql` calls would produce four independent jobs, each with its
own Kafka connection, its own checkpoint and its own restart. `fast` and `settled` would then be
able to restart at different moments and disagree for reasons that have nothing to do with
watermarks — which would quietly invalidate the only number this project publishes.
"""

import argparse
import logging
import os
import sys

from sqlrunner.statements import is_insert, split_statements

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("p01.sqlrunner")

# Where the SQL lives inside the image. Overridable so the same code runs from a checkout.
SQL_DIR = os.environ.get("P01_SQL_DIR", "/opt/flink/usrlib/sql")

JOB_NAME = os.environ.get("P01_JOB_NAME", "bsky-windows")

# Event time is UTC end to end: Jetstream stamps epoch microseconds, and the sink writes strings
# that BigQuery reads as UTC. Without this, CAST(ts AS STRING) renders in whatever zone the
# TaskManager's JVM defaults to and the warehouse gains a silent offset — which looks like clock
# skew, in a project whose entire subject is clock skew.
TIME_ZONE = "UTC"


def sql_files(directory):
    """The .sql files to run, in lexical order.

    Order is the dependency order — sources, then views over them, then sinks, then the inserts —
    and the numeric filename prefixes are what encode it. Sorting by name rather than by directory
    listing is the difference between a deterministic job and one that works on a laptop.
    """
    if not os.path.isdir(directory):
        raise FileNotFoundError("no SQL directory at {}".format(directory))
    names = sorted(name for name in os.listdir(directory) if name.endswith(".sql"))
    if not names:
        raise FileNotFoundError("no .sql files in {}".format(directory))
    return [os.path.join(directory, name) for name in names]


def load_statements(directory):
    """Read and split every file, tagging each statement with where it came from."""
    loaded = []
    for path in sql_files(directory):
        with open(path) as handle:
            text = handle.read()
        statements = split_statements(text)
        log.info("%s: %d statements", os.path.basename(path), len(statements))
        for statement in statements:
            loaded.append((os.path.basename(path), statement))
    return loaded


def run(t_env, statements):
    """Execute the catalog statements, then submit every insert as one statement set.

    Returns whatever the statement set's execute() returns, so the caller can name the job.
    """
    statement_set = t_env.create_statement_set()
    inserts = 0

    for origin, statement in statements:
        first_line = statement.splitlines()[0].strip()
        if is_insert(statement):
            log.info("%s: adding to statement set: %s", origin, first_line)
            statement_set.add_insert_sql(statement)
            inserts += 1
        else:
            log.info("%s: %s", origin, first_line)
            t_env.execute_sql(statement)

    # A file set that creates every table and inserts into none would start cleanly, report healthy,
    # checkpoint forever and produce nothing. It is the most plausible way to break this job with a
    # one-line edit, and the only symptom would be a dashboard that never advances.
    if inserts == 0:
        raise ValueError("no INSERT statements found; the job would run and produce nothing")

    log.info("submitting %d inserts as one job", inserts)
    return statement_set.execute()


def build_table_env():
    """Create the streaming TableEnvironment.

    Imported here rather than at module scope so the splitter and this module's own logic stay
    testable without a JVM on the path. Everything else about the job — checkpointing interval,
    state backend, savepoint directory — comes from the FlinkDeployment's flinkConfiguration, which
    is where cluster settings belong and where the operator can see them.
    """
    from pyflink.table import EnvironmentSettings, TableEnvironment

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("table.local-time-zone", TIME_ZONE)
    t_env.get_config().set("pipeline.name", JOB_NAME)
    return t_env


def main(argv=None, env_factory=build_table_env):
    parser = argparse.ArgumentParser(description="Submit the p01 Flink SQL pipeline.")
    parser.add_argument("--sql-dir", default=SQL_DIR)
    # Splits and validates without touching Flink. Cheap enough to run in CI on every push, which
    # catches a mangled file before it reaches a cluster.
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    statements = load_statements(args.sql_dir)

    if args.dry_run:
        inserts = sum(1 for _, statement in statements if is_insert(statement))
        log.info("dry run: %d statements, %d inserts", len(statements), inserts)
        if inserts == 0:
            raise ValueError("no INSERT statements found; the job would run and produce nothing")
        return 0

    run(env_factory(), statements)
    return 0


if __name__ == "__main__":
    sys.exit(main())
