"""Agreement between the Flink image, its manifest, and the versions pinned across both.

The Flink version appears in five places — the base image tag, the pyflink pin, the Kafka
connector's suffix, the manifest's flinkVersion and the driver jar in jarURI — and drift between
any two of them builds cleanly and fails only when the job starts, or later. None of it is Python,
so no other test sees it.
"""

import os
import re
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MANIFEST = os.path.join(ROOT, "k8s", "base", "flink.yaml")
DOCKERFILE = os.path.join(ROOT, "docker", "Dockerfile.flink")


def read(path):
    with open(path) as handle:
        return handle.read()


def uncommented(text):
    """YAML without its comments, so a key mentioned in prose never satisfies an assertion."""
    return "\n".join(line.split(" #")[0] for line in text.splitlines() if not line.lstrip().startswith("#"))


def setting(text, key):
    match = re.search(r"^\s*" + re.escape(key) + r":\s*(.+?)\s*$", text, re.M)
    return match.group(1).strip("\"'") if match else None


class VersionTriangleTest(unittest.TestCase):
    def setUp(self):
        self.dockerfile = read(DOCKERFILE)
        self.manifest = uncommented(read(MANIFEST))
        base = re.search(r"^FROM flink:(\d+)\.(\d+)\.(\d+)-", self.dockerfile, re.M)
        self.assertIsNotNone(base, "Dockerfile.flink no longer starts FROM flink:X.Y.Z-")
        self.major, self.minor, self.patch = base.groups()
        self.version = "{}.{}.{}".format(self.major, self.minor, self.patch)

    def arg(self, name):
        match = re.search(r"^ARG " + name + r"=(\S+)", self.dockerfile, re.M)
        self.assertIsNotNone(match, "no ARG " + name)
        return match.group(1)

    def test_the_manifest_declares_the_same_flink_minor_as_the_image(self):
        """The operator uses flinkVersion to choose how to talk to the cluster. A mismatch with the
        image is accepted by the CRD and misbehaves at runtime."""
        self.assertEqual(setting(self.manifest, "flinkVersion"), "v{}_{}".format(self.major, self.minor))

    def test_the_driver_jar_exists_at_the_image_version(self):
        """jarURI names a file inside the image by version. Bump the base image without it and the
        JobManager starts, looks for a jar that is not there, and the job never submits."""
        self.assertEqual(
            setting(self.manifest, "jarURI"),
            "local:///opt/flink/opt/flink-python-{}.jar".format(self.version),
        )

    def test_pyflink_matches_the_cluster_it_submits_to(self):
        self.assertEqual(self.arg("PYFLINK_VERSION"), self.version)

    def test_the_kafka_connector_is_built_for_this_flink_minor(self):
        """flink-sql-connector-kafka is published per Flink minor, as <connector>-<minor>. The wrong
        suffix loads, and then fails on the first API that moved between minors."""
        self.assertTrue(
            self.arg("KAFKA_CONNECTOR_VERSION").endswith("-{}.{}".format(self.major, self.minor)),
            self.arg("KAFKA_CONNECTOR_VERSION"),
        )


class ImagePinTest(unittest.TestCase):
    def setUp(self):
        self.manifest = uncommented(read(MANIFEST))
        self.image = setting(self.manifest, "image")

    def test_the_job_image_is_pinned_by_digest(self):
        """A stateful job cannot float. Under a tag, the next pod restart silently runs whatever SQL
        was pushed last and tries to restore it from the old job graph's checkpoint. This also
        fails on the PENDING placeholder, so an unfinished pin cannot reach main."""
        self.assertRegex(self.image, r"@sha256:[0-9a-f]{64}$")

    def test_the_digest_is_of_the_flink_image(self):
        """A digest copied from the wrong build output — the bridge or the sink — would pin a
        perfectly valid image that has no JobManager in it."""
        self.assertTrue(self.image.split("@")[0].endswith("/p01-flink"), self.image)


class ConfigurationTest(unittest.TestCase):
    def setUp(self):
        self.manifest = uncommented(read(MANIFEST))

    def test_no_checkpoint_key_removed_in_flink_2(self):
        """Flink ignores an unknown key rather than rejecting it. These two were removed in 2.0, and
        either one would put every checkpoint on the JobManager's local disk — lost on the first
        restart, with the job reporting healthy the whole time."""
        for key in ("state.checkpoints.dir", "state.savepoints.dir", "state.backend:"):
            with self.subTest(key=key):
                self.assertNotIn(key, self.manifest)

    def test_checkpoints_and_savepoints_go_to_gcs(self):
        for key in ("execution.checkpointing.dir", "execution.checkpointing.savepoint-dir"):
            with self.subTest(key=key):
                self.assertTrue((setting(self.manifest, key) or "").startswith("gs://"), key)

    def test_ha_storage_is_not_under_a_checkpoint_or_savepoint_prefix(self):
        """The platform's seven-day delete rule is scoped to the checkpoint and savepoint prefixes.
        HA metadata is partly written once and never rewritten; nested under either prefix, it is
        deleted on day seven and the job cannot recover from its next JobManager restart."""
        ha = setting(self.manifest, "high-availability.storageDir")
        self.assertIsNotNone(ha)
        for key in ("execution.checkpointing.dir", "execution.checkpointing.savepoint-dir"):
            with self.subTest(key=key):
                configured = setting(self.manifest, key)
                # Missing is its own failure, reported as one rather than as an AttributeError.
                self.assertIsNotNone(configured, key + " is not set")
                prefix = configured.rstrip("/") + "/"
                self.assertFalse((ha.rstrip("/") + "/").startswith(prefix), ha)

    def test_last_state_upgrades_have_ha_to_restore_from(self):
        """upgradeMode last-state restores through HA. Without it the operator refuses the upgrade,
        or on some versions falls back to a stateless start that discards every open window."""
        if setting(self.manifest, "upgradeMode") == "last-state":
            self.assertEqual(setting(self.manifest, "high-availability.type"), "kubernetes")

    def test_the_job_never_lands_on_spot(self):
        """Preempting the JobManager or TaskManager restarts the job; the base pool exists to prevent
        that for anything stateful."""
        self.assertEqual(setting(self.manifest, "priorityClassName"), "stateful-engine")
        self.assertNotIn("workload-class", self.manifest)


if __name__ == "__main__":
    unittest.main()
