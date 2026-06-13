#!/usr/bin/env bash
# Install the Strimzi cluster operator and its CRDs, watching the default
# namespace. Idempotent — safe to re-run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

# Renovate: datasource=github-releases depName=strimzi/strimzi-kafka-operator
STRIMZI_VERSION="${STRIMZI_VERSION:-0.46.0}"

log "installing Strimzi $STRIMZI_VERSION → namespace strimzi-system, watching '$BENCH_NAMESPACE'"

kubectl create namespace strimzi-system --dry-run=client -o yaml | kubectl apply -f -

# The upstream install bundle is templated by namespace via the URL query.
# Download into a tmpfile and rewrite the watched namespace so the operator
# reconciles Kafka CRs in `default` rather than `myproject`.
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
url="https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml"
log "fetching $url"
curl -fsSL "$url" -o "$tmp/strimzi.yaml"

# 1. Strimzi defaults to namespace `myproject`. Rewrite to strimzi-system
#    for the operator deployment.
sed -i "s/namespace: myproject/namespace: strimzi-system/g" "$tmp/strimzi.yaml"

# 2. Rewrite STRIMZI_NAMESPACE env var so the operator watches `default`.
python3 - "$tmp/strimzi.yaml" "$BENCH_NAMESPACE" <<'PY'
import sys, re
path, ns = sys.argv[1], sys.argv[2]
with open(path) as f:
    text = f.read()
# Strimzi sets STRIMZI_NAMESPACE on the operator Deployment via env valueFrom.
# Replace it with a literal value to watch the target namespace.
pattern = r'(-\s*name:\s*STRIMZI_NAMESPACE\s*\r?\n\s*)valueFrom:\s*\r?\n\s*fieldRef:\s*\r?\n\s*fieldPath:\s*metadata\.namespace'
text = re.sub(pattern, rf'\g<1>value: "{ns}"', text)
with open(path, 'w') as f:
    f.write(text)
PY

kubectl apply -n strimzi-system -f "$tmp/strimzi.yaml"

# Create the namespace-scoped RoleBindings in the watched namespace so that
# the operator (running in strimzi-system) has permissions to reconcile
# resources in the watched namespace.
cat <<EOF | kubectl apply -n "$BENCH_NAMESPACE" -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: strimzi-cluster-operator
subjects:
  - kind: ServiceAccount
    name: strimzi-cluster-operator
    namespace: strimzi-system
roleRef:
  kind: ClusterRole
  name: strimzi-cluster-operator-namespaced
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: strimzi-cluster-operator-entity-operator-delegation
subjects:
  - kind: ServiceAccount
    name: strimzi-cluster-operator
    namespace: strimzi-system
roleRef:
  kind: ClusterRole
  name: strimzi-entity-operator
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: strimzi-cluster-operator-watched
subjects:
  - kind: ServiceAccount
    name: strimzi-cluster-operator
    namespace: strimzi-system
roleRef:
  kind: ClusterRole
  name: strimzi-cluster-operator-watched
  apiGroup: rbac.authorization.k8s.io
EOF

# Apply the JMX-exporter ConfigMap our Kafka CR references for /metrics.
kubectl apply -f "$SCRIPT_DIR/../manifests/strimzi/jmx-exporter-configmap.yaml"

log "waiting for strimzi-cluster-operator rollout"
kubectl rollout status -n strimzi-system deploy/strimzi-cluster-operator --timeout=300s
log "Strimzi ready"
