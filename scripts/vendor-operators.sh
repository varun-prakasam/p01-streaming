#!/usr/bin/env bash
# Regenerate the vendored operator manifests in k8s/operators/.
#
# Both are vendored rather than fetched at sync time. An operator that has to exist before the
# project it serves should not depend on someone else's registry being reachable at that moment, and
# a chart pulled during a sync is exactly that dependency. Pinning also means an upgrade is a diff
# somebody reads rather than something that arrives on its own.
#
# Requires: helm, python with pyyaml. Run from the repository root.
set -euo pipefail

STRIMZI_VERSION="1.2.0"
FLINK_OPERATOR_VERSION="1.15.0"
OUT="k8s/operators"
PY="${PY:-.venv/bin/python}"

echo "==> Strimzi ${STRIMZI_VERSION}"
curl -sfL \
  "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml" \
  -o /tmp/strimzi-upstream.yaml
"$PY" scripts/vendor_strimzi.py /tmp/strimzi-upstream.yaml "${OUT}/strimzi-cluster-operator.yaml"

echo "==> Flink operator ${FLINK_OPERATOR_VERSION}"
# From Apache's own archive rather than the OCI registry: helm 4 cannot read the chart's OCI
# mediatype ("could not load config with mediatype application/vnd.cncf.helm.config.v1+json").
curl -sfL \
  "https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}-helm.tgz" \
  -o /tmp/flink-operator-chart.tgz

{
  sed -n '1,24p' "${OUT}/flink-kubernetes-operator.yaml"   # keep the committed header
  # --include-crds is not optional: helm template skips crds/ by default, and without the CRDs the
  # FlinkDeployment kind does not exist and the project's sync fails on an unknown kind.
  helm template flink-kubernetes-operator /tmp/flink-operator-chart.tgz \
    --namespace operators --include-crds \
    --set webhook.create=false \
    --set jobServiceAccount.create=false \
    --set operatorPod.priorityClassName=stateful-engine \
    --set operatorPod.resources.requests.cpu=150m \
    --set operatorPod.resources.requests.memory=512Mi \
    --set operatorPod.resources.limits.memory=1Gi
} > /tmp/flink-operator-rendered.yaml
mv /tmp/flink-operator-rendered.yaml "${OUT}/flink-kubernetes-operator.yaml"

echo "==> verifying"
kubectl kustomize "${OUT}" > /dev/null
echo "ok — review the diff before committing"
