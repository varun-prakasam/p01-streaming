"""Vendor the Strimzi install manifest, adjusted for a cluster-wide watch from `operators`.

Strimzi ships a manifest that assumes the operator watches its own namespace. Ours lives in
`operators` and must reconcile a Kafka in `p01-streaming` (and later p05's topics), which is the
documented multi-namespace install: STRIMZI_NAMESPACE=*, and the RoleBindings that grant rights in
*watched* namespaces become ClusterRoleBindings. Leader election stays a RoleBinding, because it is
local to the operator's own namespace.
"""

import sys

import yaml

NAMESPACE = "operators"

# These grant the operator rights inside every namespace it watches, so they must be cluster-scoped.
# Leader election is deliberately absent: it coordinates operator replicas among themselves.
CLUSTER_SCOPE = {
    "strimzi-cluster-operator-watched",
    "strimzi-cluster-operator-entity-operator-delegation",
    "strimzi-cluster-operator",
}

RESOURCES = {
    "requests": {"cpu": "200m", "memory": "384Mi"},
    "limits": {"memory": "768Mi"},
}


def main(src: str, dest: str) -> int:
    docs = [d for d in yaml.safe_load_all(open(src)) if d]
    out = []

    for doc in docs:
        kind = doc["kind"]
        meta = doc.setdefault("metadata", {})

        # Subjects always point at the operator's ServiceAccount, wherever it lives.
        for subject in doc.get("subjects", []):
            if subject.get("kind") == "ServiceAccount":
                subject["namespace"] = NAMESPACE

        if kind == "RoleBinding" and meta["name"] in CLUSTER_SCOPE:
            doc["kind"] = "ClusterRoleBinding"
            meta.pop("namespace", None)
            # RoleBindings are namespaced, so Strimzi can reuse a name that a ClusterRoleBinding
            # already holds — `strimzi-cluster-operator` is both. Promoting one to cluster scope
            # without renaming makes the second silently overwrite the first, and the operator loses
            # the global permissions it needs. Name it after the role it actually binds.
            if meta["name"] in {d["metadata"]["name"] for d in out if d["kind"] == "ClusterRoleBinding"}:
                meta["name"] = doc["roleRef"]["name"]
        elif kind in ("ServiceAccount", "ConfigMap", "Deployment", "RoleBinding"):
            meta["namespace"] = NAMESPACE

        if kind == "Deployment":
            spec = doc["spec"]["template"]["spec"]
            # Sized for a node where the margin is ~220m. The operator is a reconcile loop, not a
            # data path; it idles between Kafka spec changes.
            spec["containers"][0]["resources"] = RESOURCES
            # Explicit, per the platform rule at gitops/README.md:50 — an unlabelled pod inherits
            # pipeline-default, which is the wrong priority for something the broker depends on.
            spec["priorityClassName"] = "stateful-engine"
            for env in spec["containers"][0].get("env", []):
                if env["name"] == "STRIMZI_NAMESPACE":
                    # Watch every namespace. Anything narrower means editing this file each time a
                    # project starts using Kafka, which is the kind of step that gets forgotten.
                    env.pop("valueFrom", None)
                    env["value"] = "*"

        out.append(doc)

    with open(dest, "w") as handle:
        handle.write(
            "# Strimzi cluster operator, vendored and pinned.\n"
            "#\n"
            "# Generated, do not hand-edit — regenerate with scripts/vendor-strimzi.sh so the\n"
            "# adjustments below are reapplied on an upgrade:\n"
            "#\n"
            "#   * namespace set to `operators`, because the operator is shared infrastructure that\n"
            "#     project 1 merely installs first; project 5 runs Beam on the same broker.\n"
            "#   * STRIMZI_NAMESPACE=* and three RoleBindings promoted to ClusterRoleBindings, which\n"
            "#     is the documented multi-namespace install. Leader election stays namespaced —\n"
            "#     it coordinates operator replicas, not watched resources.\n"
            "#   * explicit resources and priorityClassName, per gitops/README.md:50.\n"
            "#\n"
            "# Vendored rather than fetched from a URL at sync time: an operator that must exist\n"
            "# before the project it serves should not depend on GitHub being reachable.\n\n"
        )
        yaml.safe_dump_all(out, handle, default_flow_style=False, sort_keys=False)

    kinds = {}
    for doc in out:
        kinds[doc["kind"]] = kinds.get(doc["kind"], 0) + 1
    print(f"wrote {dest}: {kinds}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
