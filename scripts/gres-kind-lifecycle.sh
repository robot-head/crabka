#!/usr/bin/env bash
# Operator-backed G-5 lifecycle and cold-start gate for a disposable Kind cluster.
set -euo pipefail

cd "$(dirname "$0")/.."

readonly CLUSTER="${CRABKA_GRES_KIND_CLUSTER:-crabka-gres-g5}"
readonly ARTIFACT_DIR="${CRABKA_GRES_KIND_ARTIFACT_DIR:-target/gres-kind-lifecycle-artifacts}"
readonly ITERATIONS="${CRABKA_GRES_COLDSTART_ITERATIONS:-10}"
readonly P95_CEILING_MS="${CRABKA_GRES_COLDSTART_P95_CEILING_MS:-30000}"
readonly PGPASSWORD_VALUE="${CRABKA_GRES_KIND_PASSWORD:-g5-secret-password}"
readonly PGDOG_IMAGE="ghcr.io/pgdogdev/pgdog:0.1.47"
readonly IMAGE_TAG="g5-e2e"
# Safety margin between a wake no-roll observation and the operator-stamped
# pgdogCredentialGraceUntilUnixMs deadline. The host and the Kind node share a
# kernel clock, so this only needs to absorb the gap between the kubectl reads
# returning and the timestamp being taken.
readonly WAKE_NOROLL_MARGIN_MS=500
PORT_FORWARD_PID=""
COMPUTE_FORWARD_PID=""
KEEPER_PID=""
BROKER_FORWARD_PID=""

fail() { echo "FAIL: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || fail "$1 is required"; }

cleanup() {
    local status=$?
    if [ -n "$PORT_FORWARD_PID" ]; then kill "$PORT_FORWARD_PID" 2>/dev/null || true; fi
    if [ -n "$KEEPER_PID" ]; then kill "$KEEPER_PID" 2>/dev/null || true; fi
    if [ -n "$COMPUTE_FORWARD_PID" ]; then kill "$COMPUTE_FORWARD_PID" 2>/dev/null || true; fi
    if [ -n "$BROKER_FORWARD_PID" ]; then kill "$BROKER_FORWARD_PID" 2>/dev/null || true; fi
    kubectl get events -A --sort-by=.lastTimestamp >"$ARTIFACT_DIR/events.txt" 2>&1 || true
    kubectl logs -n crabka-operator deploy/crabka-gres-operator --timestamps \
        >"$ARTIFACT_DIR/operator.log" 2>&1 || true
    kubectl get gres,grestenant,deploy,pod,svc -A -o yaml \
        >"$ARTIFACT_DIR/final-objects.yaml" 2>&1 || true
    if [ "$status" -ne 0 ] || [ "${CRABKA_GRES_KIND_KEEP_CLUSTER:-0}" = 1 ]; then
        echo "artifacts retained at $ARTIFACT_DIR" >&2
    else
        timeout 60s kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

deadline_wait() {
    local seconds=$1 description=$2 predicate=$3
    local deadline=$((SECONDS + seconds))
    until eval "$predicate"; do
        (( SECONDS < deadline )) || fail "timed out waiting for $description"
        sleep 1
    done
}

build_image() {
    local binary=$1 image=$2
    timeout 300s docker build --build-context binaries=target/release --build-arg "BINARY=$binary" \
        -f packaging/docker/Dockerfile.local-binary -t "$image" . \
        >"$ARTIFACT_DIR/build-${binary}.log" 2>&1
    timeout 120s kind load docker-image "$image" --name "$CLUSTER"
}

wait_lifecycle() {
    local expected=$1
    deadline_wait 180 "tenant lifecycle $expected" \
        "[ \"\$(kubectl get grestenant tenant-a -o jsonpath='{.status.lifecyclePhase}' 2>/dev/null)\" = '$expected' ]"
}

# The wake query, timed. A tenant coming out of suspend can briefly answer
# `40001 ... not the leader, retry` while range leadership settles; that is a
# retryable condition the client contract expects a caller to retry, not a
# failure of the wake. Retrying it here keeps the gate measuring cold-start
# latency instead of failing on a transient.
#
# The reported latency deliberately spans every attempt: a client that has to
# retry really did wait that long, so hiding it would defeat the p95 ceiling.
# Retries are therefore kept short, and a leader that never settles exhausts
# the budget and fails with its own message, distinct from the flake.
measure_tls_query_ms() {
    python3 - "$ARTIFACT_DIR/ca.crt" "$ARTIFACT_DIR/client.crt" "$ARTIFACT_DIR/client.key" \
        "$ARTIFACT_DIR/wake-query-attempts.tsv" <<'PY'
import os, pathlib, subprocess, sys, time
env = os.environ.copy()
env["PGPASSWORD"] = os.environ["G5_SQL_PASSWORD"]
# Only a leadership handoff is retried. Anything else — a wrong answer, a TLS
# or auth failure, a genuine query error — stays fatal on the first attempt.
RETRYABLE = ("not the leader", "40001")
RETRY_BUDGET_S = 30.0
attempts_path = pathlib.Path(sys.argv[4])
start = time.monotonic_ns()
deadline = time.monotonic() + RETRY_BUDGET_S
attempts = 0
while True:
    attempts += 1
    run = subprocess.run([
        "psql", "host=localhost port=16432 dbname=tenant-a user=alice sslmode=verify-full sslrootcert=" + sys.argv[1] + " sslcert=" + sys.argv[2] + " sslkey=" + sys.argv[3],
        "-v", "ON_ERROR_STOP=1", "-tAc", "SELECT value FROM lifecycle_marker WHERE id=1"
    ], env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
       timeout=40, check=False)
    if run.returncode == 0 and run.stdout.strip() == "survives":
        break
    combined = (run.stderr or "") + (run.stdout or "")
    if not any(marker in combined for marker in RETRYABLE):
        sys.stderr.write(run.stderr + "\nstdout=" + repr(run.stdout) + "\n")
        raise SystemExit(run.returncode or 1)
    if time.monotonic() >= deadline:
        sys.stderr.write(
            f"wake query still reported a leadership handoff after {attempts} attempts "
            f"across {RETRY_BUDGET_S:.0f}s; treating as a stuck leader\n"
            + run.stderr + "\nstdout=" + repr(run.stdout) + "\n"
        )
        raise SystemExit(run.returncode or 1)
    sys.stderr.write(f"wake query attempt {attempts} hit a leadership handoff; retrying\n")
    time.sleep(0.5)
with attempts_path.open("a") as handle:
    handle.write(f"{attempts}\n")
print((time.monotonic_ns() - start) // 1_000_000)
PY
}

for command in kind kubectl docker cargo openssl psql python3 timeout; do need "$command"; done
[[ "$ITERATIONS" =~ ^[1-9][0-9]*$ ]] || fail "iterations must be positive"
rm -rf "$ARTIFACT_DIR"
mkdir -p "$ARTIFACT_DIR"

timeout 90s kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
timeout 180s kind create cluster --name "$CLUSTER" --wait 120s

timeout 900s cargo build --locked --release \
    -p crabka-cli -p crabka-operator -p crabka-broker -p crabka-gres -p crabka-gres-activator
build_image crabka-operator "crabka-operator:$IMAGE_TAG"
build_image crabka-broker "crabka-broker:$IMAGE_TAG"
build_image crabka-gres "crabka-gres:$IMAGE_TAG"
build_image crabka-gres-activator "crabka-gres-activator:$IMAGE_TAG"
timeout 180s docker pull "$PGDOG_IMAGE"
# PgDog publishes a multi-platform OCI index that `kind load docker-image`
# cannot reliably flatten. Let containerd pull the exact pinned digest/tag.

kubectl apply -f deploy/crds/crabka.io_kafkas.yaml
kubectl apply -f deploy/crds/crabka.io_kafkanodepools.yaml
kubectl apply -f deploy/crds/crabka.io_greses.yaml
kubectl apply -f deploy/crds/crabka.io_grestenants.yaml

# Real object service used by compute final-checkpoint writes and controller
# manifest validation on the successful parking path.
kubectl apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata: {name: minio}
spec:
  replicas: 1
  selector: {matchLabels: {app: minio}}
  template:
    metadata: {labels: {app: minio}}
    spec:
      containers:
        - name: minio
          image: quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z
          args: [server, /data]
          env:
            - {name: MINIO_ROOT_USER, value: minio}
            - {name: MINIO_ROOT_PASSWORD, value: minio-secret}
          ports: [{containerPort: 9000}]
---
apiVersion: v1
kind: Service
metadata: {name: minio}
spec: {selector: {app: minio}, ports: [{port: 9000, targetPort: 9000}]}
YAML
timeout 180s kubectl rollout status deploy/minio --timeout=170s

kubectl create namespace crabka-operator
kubectl create serviceaccount crabka-gres-operator -n crabka-operator
kubectl create clusterrolebinding crabka-gres-operator-admin \
    --clusterrole=cluster-admin --serviceaccount=crabka-operator:crabka-gres-operator
kubectl apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: crabka-gres-operator, namespace: crabka-operator}
spec:
  replicas: 1
  selector: {matchLabels: {app: crabka-gres-operator}}
  template:
    metadata: {labels: {app: crabka-gres-operator}}
    spec:
      serviceAccountName: crabka-gres-operator
      containers:
        - name: operator
          image: crabka-operator:$IMAGE_TAG
          imagePullPolicy: Never
          args:
            - run
            - --default-broker-image=crabka-broker:$IMAGE_TAG
            - --default-gres-image=crabka-gres:$IMAGE_TAG
            - --default-gres-activator-image=crabka-gres-activator:$IMAGE_TAG
            - --default-pgdog-image=$PGDOG_IMAGE
            - --gres-checkpoint-store=s3
            - --gres-checkpoint-bucket=gres-checkpoints
            - --gres-checkpoint-region=us-east-1
            - --gres-checkpoint-endpoint=http://minio.default.svc:9000
            - --gres-checkpoint-allow-http
            - --gres-checkpoint-access-key-id=minio
            - --gres-checkpoint-secret-access-key=minio-secret
          env:
            - {name: OPERATOR_NAMESPACE, value: crabka-operator}
            - {name: POD_NAME, valueFrom: {fieldRef: {fieldPath: metadata.name}}}
YAML
timeout 180s kubectl rollout status -n crabka-operator deploy/crabka-gres-operator --timeout=170s

kubectl apply -f - <<'YAML'
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata: {name: demo}
spec: {kafkaVersion: "3.7.0"}
---
apiVersion: crabka.io/v1alpha1
kind: KafkaNodePool
metadata:
  name: brokers
  labels: {crabka.io/cluster: demo}
spec:
  roles: [Controller, Broker]
  replicas: 1
  nodeIdStart: 0
  storage: {type: Ephemeral}
YAML
deadline_wait 300 "Kafka Ready" \
    "[ \"\$(kubectl get kafka demo -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}' 2>/dev/null)\" = True ]"

# MinIO client creates the bucket before any final checkpoint can be published.
kubectl run minio-create --restart=Never --image=quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z \
    --env=MC_HOST_local=http://minio:minio-secret@minio:9000 -- mb --ignore-existing local/gres-checkpoints
timeout 120s kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/minio-create --timeout=110s

openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=gres-g5-ca \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -keyout "$ARTIFACT_DIR/ca.key" -out "$ARTIFACT_DIR/ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
    -keyout "$ARTIFACT_DIR/server.key" -out "$ARTIFACT_DIR/server.csr" >/dev/null 2>&1
printf 'subjectAltName=DNS:localhost,DNS:fleet-pgdog.default.svc,DNS:fleet-pgdog.default.svc.cluster.local\nextendedKeyUsage=serverAuth\n' >"$ARTIFACT_DIR/server.ext"
openssl x509 -req -days 2 -in "$ARTIFACT_DIR/server.csr" \
    -CA "$ARTIFACT_DIR/ca.crt" -CAkey "$ARTIFACT_DIR/ca.key" -CAcreateserial \
    -extfile "$ARTIFACT_DIR/server.ext" -out "$ARTIFACT_DIR/server.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj /CN=gres-g5-client \
    -keyout "$ARTIFACT_DIR/client.key" -out "$ARTIFACT_DIR/client.csr" >/dev/null 2>&1
printf 'extendedKeyUsage=clientAuth\n' >"$ARTIFACT_DIR/client.ext"
openssl x509 -req -days 2 -in "$ARTIFACT_DIR/client.csr" \
    -CA "$ARTIFACT_DIR/ca.crt" -CAkey "$ARTIFACT_DIR/ca.key" -CAcreateserial \
    -extfile "$ARTIFACT_DIR/client.ext" -out "$ARTIFACT_DIR/client.crt" >/dev/null 2>&1
chmod 0600 "$ARTIFACT_DIR/client.key"
kubectl create secret generic pgdog-tls \
    --from-file=tls.crt="$ARTIFACT_DIR/server.crt" \
    --from-file=tls.key="$ARTIFACT_DIR/server.key" \
    --from-file=ca.crt="$ARTIFACT_DIR/ca.crt" \
    --from-file=client.crt="$ARTIFACT_DIR/client.crt" \
    --from-file=client.key="$ARTIFACT_DIR/client.key"
kubectl create secret generic pgdog-admin --from-literal=password=admin-secret
kubectl create secret generic tenant-a-password --from-literal=password="$PGPASSWORD_VALUE"

kubectl apply -f - <<'YAML'
apiVersion: crabka.io/v1alpha1
kind: Gres
metadata: {name: fleet}
spec:
  kafkaCluster: demo
  pgdog:
    replicas: 1
    listenPort: 6432
    # Keep the activator route stable for the complete cold-start request on a
    # loaded CI runner.  The four-second product default is intentionally
    # aggressive, but can expire while the release compute pod is starting and
    # roll the PgDog pod underneath the request this gate is timing.
    directBootstrapGrace: 30s
    tlsSecretRef: {name: pgdog-tls}
    adminSecretRef: {name: pgdog-admin, key: password}
  defaults:
    checkpointFrames: 1
    idleSeconds: 15
---
apiVersion: crabka.io/v1alpha1
kind: GresTenant
metadata: {name: tenant-a}
spec:
  gres: fleet
  user: alice
  passwordSecretRef: {name: tenant-a-password, key: password}
  overrides:
    suspendMaxCheckpointSize: "0B"
YAML
deadline_wait 60 "tenant compute Deployment creation" "kubectl get deploy tenant-a-gres >/dev/null 2>&1"
deadline_wait 60 "PgDog Deployment creation" "kubectl get deploy fleet-pgdog >/dev/null 2>&1"
deadline_wait 60 "activator Deployment creation" "kubectl get deploy fleet-gres-activator >/dev/null 2>&1"
timeout 240s kubectl rollout status deploy/tenant-a-gres --timeout=230s
kubectl port-forward deploy/tenant-a-gres 17432:5432 >"$ARTIFACT_DIR/compute-port-forward.log" 2>&1 &
COMPUTE_FORWARD_PID=$!
deadline_wait 30 "compute port-forward" "timeout 1 bash -c '</dev/tcp/127.0.0.1/17432' 2>/dev/null"
(
    {
        printf 'BEGIN;\n'
        for _ in $(seq 1 40); do printf 'SELECT 1;\n'; sleep 1; done
    } | PGPASSWORD="$PGPASSWORD_VALUE" \
        psql "host=127.0.0.1 port=17432 dbname=crab user=alice sslmode=disable" \
        >"$ARTIFACT_DIR/open-session.log" 2>&1
) &
KEEPER_PID=$!
sleep 20
kill -0 "$KEEPER_PID" 2>/dev/null || fail "busy-session keeper exited"
[ "$(kubectl get grestenant tenant-a -o jsonpath='{.status.lifecyclePhase}')" = active ] || \
    fail "open session did not keep tenant active"
printf 'busy_session_prevented_suspend=true\n' >"$ARTIFACT_DIR/busy-session-proof.txt"
timeout 180s kubectl rollout status deploy/fleet-pgdog --timeout=170s
timeout 180s kubectl rollout status deploy/fleet-gres-activator --timeout=170s

kubectl port-forward svc/fleet-pgdog 16432:6432 >"$ARTIFACT_DIR/port-forward.log" 2>&1 &
PORT_FORWARD_PID=$!
deadline_wait 30 "PgDog port-forward" "timeout 1 bash -c '</dev/tcp/127.0.0.1/16432' 2>/dev/null"
export G5_SQL_PASSWORD="$PGPASSWORD_VALUE"

PGPASSWORD="$PGPASSWORD_VALUE" psql \
    "host=localhost port=16432 dbname=tenant-a user=alice sslmode=verify-full sslrootcert=$ARTIFACT_DIR/ca.crt sslcert=$ARTIFACT_DIR/client.crt sslkey=$ARTIFACT_DIR/client.key" \
    -v ON_ERROR_STOP=1 -c "CREATE TABLE lifecycle_marker(id int4, value text); INSERT INTO lifecycle_marker VALUES (1, 'survives');"
wait "$KEEPER_PID" 2>/dev/null || true
KEEPER_PID=""
PGPASSWORD="$PGPASSWORD_VALUE" psql \
    "host=127.0.0.1 port=17432 dbname=crab user=alice sslmode=disable" \
    -v ON_ERROR_STOP=1 -c "UPDATE lifecycle_marker SET value='survives' WHERE id=1"
sleep 3
kubectl patch grestenant tenant-a --type merge \
    -p '{"spec":{"overrides":{"checkpointFrames":1000000,"suspendMaxCheckpointSize":"0B"}}}'
deadline_wait 60 "stable size-gate config" \
    '[ "$(kubectl get grestenant tenant-a -o jsonpath='"'"'{.status.observedGeneration}'"'"')" = "$(kubectl get grestenant tenant-a -o jsonpath='"'"'{.metadata.generation}'"'"')" ]'
kubectl rollout restart deploy/tenant-a-gres
timeout 180s kubectl rollout status deploy/tenant-a-gres --timeout=170s
deadline_wait 90 "checkpoint size-gate skip" \
    "kubectl logs deploy/tenant-a-gres --since=3m | grep -q 'tenant remains warm after suspend size-gate skip'"
sleep 20
[ "$(kubectl get grestenant tenant-a -o jsonpath='{.status.lifecyclePhase}')" = active ] || \
    fail "checkpoint size gate did not keep oversized tenant active"
[ "$(kubectl get deploy tenant-a-gres -o jsonpath='{.spec.replicas}')" = 1 ] || \
    fail "checkpoint size gate scaled oversized tenant"
printf 'oversized_checkpoint_kept_active=true\n' >"$ARTIFACT_DIR/size-gate-proof.txt"
kubectl patch grestenant tenant-a --type merge -p '{"spec":{"overrides":null}}'
deadline_wait 60 "size-gate override removal" \
    '[ "$(kubectl get grestenant tenant-a -o jsonpath='"'"'{.status.observedGeneration}'"'"')" = "$(kubectl get grestenant tenant-a -o jsonpath='"'"'{.metadata.generation}'"'"')" ]'
kubectl rollout restart deploy/tenant-a-gres
timeout 180s kubectl rollout status deploy/tenant-a-gres --timeout=170s
kill "$COMPUTE_FORWARD_PID" 2>/dev/null || true
COMPUTE_FORWARD_PID=""

: >"$ARTIFACT_DIR/iteration-timings.tsv"
: >"$ARTIFACT_DIR/physical-wal-generations.tsv"
: >"$ARTIFACT_DIR/wake-noroll-proof.tsv"
noroll_within_grace=0
lifecycle_start_ns=$(date +%s%N)
for iteration in $(seq 1 "$ITERATIONS"); do
    # Real compute self-suspend closes admission, writes the final manifest,
    # exits, and lets the actual controller park generation-qualified WAL.
    wait_lifecycle suspended
    deadline_wait 180 "compute scale-to-zero" \
        "[ \"\$(kubectl get deploy tenant-a-gres -o jsonpath='{.spec.replicas}')\" = 0 ]"
    deadline_wait 30 "parked WAL log directory removal" \
        "kubectl exec demo-brokers-0 -- sh -c 'test -z \"\$(find /var/lib/crabka/data -maxdepth 1 -type d -name \"__gres_wal.tenant-a.r0*-0\" -print -quit)\"' >/dev/null 2>&1"
    deadline_wait 240 "confirmed PgDog activator route" \
        '[ "$(kubectl get gres fleet -o jsonpath='"'"'{.status.confirmedPgdogConfigHash}'"'"')" = "$(kubectl get secret fleet-pgdog-config -o jsonpath='"'"'{.metadata.annotations.crabka\.io/pgdog-config-hash}'"'"')" ]'
    deadline_wait 240 "PgDog Deployment activator hash" \
        '[ "$(kubectl get deploy fleet-pgdog -o jsonpath='"'"'{.spec.template.metadata.annotations.crabka\.io/pgdog-config-hash}'"'"')" = "$(kubectl get secret fleet-pgdog-config -o jsonpath='"'"'{.metadata.annotations.crabka\.io/pgdog-rollout-hash}'"'"')" ]'
    timeout 180s kubectl rollout status deploy/fleet-pgdog --timeout=170s
    [ "$(kubectl get grestenant tenant-a -o jsonpath='{.status.lifecyclePhase}')" = suspended ] || \
        fail "PgDog rollout woke tenant before a client arrived"
    [ "$(kubectl get deploy tenant-a-gres -o jsonpath='{.spec.replicas}')" = 0 ] || \
        fail "compute scaled before the first client wake"
    kubectl get secret fleet-pgdog-config -o jsonpath='{.data.users\.toml}' \
        | base64 -d | grep -q '^password = ' || \
        fail "suspended PgDog route lacks bounded credential fallback"
    # `kubectl port-forward service/...` binds one selected pod for its whole
    # lifetime; the config-hash rollout above intentionally replaces that pod.
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
    kubectl port-forward svc/fleet-pgdog 16432:6432 >"$ARTIFACT_DIR/port-forward.log" 2>&1 &
    PORT_FORWARD_PID=$!
    deadline_wait 30 "PgDog port-forward after activator rollout" \
        "timeout 1 bash -c '</dev/tcp/127.0.0.1/16432' 2>/dev/null"
    before_generation=$(kubectl get grestenant tenant-a -o jsonpath='{.status.registryVersion}')
    before_wake_hash=$(kubectl get secret fleet-pgdog-config -o jsonpath='{.metadata.annotations.crabka\.io/pgdog-config-hash}')
    before_wake_revision=$(kubectl get deploy fleet-pgdog -o jsonpath='{.metadata.annotations.deployment\.kubernetes\.io/revision}')
    start_ns=$(date +%s%N)
    latency_ms=$(measure_tls_query_ms)
    end_ns=$(date +%s%N)
    printf '%s\t%s\n' "$iteration" "$latency_ms" >>"$ARTIFACT_DIR/iteration-timings.tsv"
    # Observe PgDog immediately after the wake query, before the slower
    # rollout/port-forward/keeper round-trips below. The operator holds the
    # suspended activator route only until the bounded grace deadline it
    # stamps on the suspended->active transition, after which the lazy flip
    # to direct compute legitimately changes the config and rolls the pod.
    # A no-roll assertion is therefore only sound when the observation
    # provably landed inside that window.
    after_wake_hash=$(kubectl get secret fleet-pgdog-config -o jsonpath='{.metadata.annotations.crabka\.io/pgdog-config-hash}')
    after_wake_revision=$(kubectl get deploy fleet-pgdog -o jsonpath='{.metadata.annotations.deployment\.kubernetes\.io/revision}')
    observed_unix_ms=$(($(date +%s%N) / 1000000))
    wait_lifecycle active
    grace_deadline_ms=$(kubectl get grestenant tenant-a -o jsonpath='{.status.pgdogCredentialGraceUntilUnixMs}')
    [ -n "$grace_deadline_ms" ] || fail "active tenant lacks a PgDog credential grace deadline"
    if (( observed_unix_ms < grace_deadline_ms - WAKE_NOROLL_MARGIN_MS )); then
        [ "$after_wake_hash" = "$before_wake_hash" ] || \
            fail "wake path changed PgDog config before the held first session completed"
        [ "$after_wake_revision" = "$before_wake_revision" ] || \
            fail "wake path rolled PgDog before the held first session completed"
        noroll_within_grace=$((noroll_within_grace + 1))
        noroll_verdict=asserted
    else
        noroll_verdict=window-elapsed
        echo "iteration $iteration: no-roll observation at ${observed_unix_ms} missed the grace deadline ${grace_deadline_ms}; skipping" >&2
    fi
    printf '%s\t%s\t%s\t%s\n' "$iteration" "$observed_unix_ms" "$grace_deadline_ms" "$noroll_verdict" \
        >>"$ARTIFACT_DIR/wake-noroll-proof.tsv"
    timeout 180s kubectl rollout status deploy/tenant-a-gres --timeout=170s
    kubectl port-forward deploy/tenant-a-gres 17432:5432 >"$ARTIFACT_DIR/compute-port-forward.log" 2>&1 &
    COMPUTE_FORWARD_PID=$!
    deadline_wait 30 "post-wake compute port-forward" \
        "timeout 1 bash -c '</dev/tcp/127.0.0.1/17432' 2>/dev/null"
    # Hold one continuously busy backend session. Repeated short SELECTs leave
    # zero-session gaps in which the idle state machine can legitimately enter
    # Parking before the slower PgDog config proofs have converged.
    PGPASSWORD="$PGPASSWORD_VALUE" psql \
        "host=127.0.0.1 port=17432 dbname=crab user=alice sslmode=disable" \
        -v ON_ERROR_STOP=1 \
        < <({
            printf 'BEGIN;\n'
            while true; do printf 'SELECT 1;\n'; sleep 1; done
        }) >"$ARTIFACT_DIR/post-wake-keeper-${iteration}.log" 2>&1 &
    KEEPER_PID=$!
    sleep 1
    kill -0 "$KEEPER_PID" 2>/dev/null || fail "post-wake busy-session keeper exited"
    # These three gate on the operator rewriting the PgDog config AND the
    # resulting Deployment rollout landing. A rollout is given 180s of its own
    # elsewhere in this script, so a 120s budget here was internally
    # inconsistent and the first to expire on a loaded runner.
    deadline_wait 240 "active PgDog tenant credential removal" \
        "! kubectl get secret fleet-pgdog-config -o jsonpath='{.data.users\\.toml}' | base64 -d | grep -q 'g5-secret-password'"
    deadline_wait 240 "confirmed direct PgDog route" \
        '[ "$(kubectl get gres fleet -o jsonpath='"'"'{.status.confirmedPgdogConfigHash}'"'"')" = "$(kubectl get secret fleet-pgdog-config -o jsonpath='"'"'{.metadata.annotations.crabka\.io/pgdog-config-hash}'"'"')" ]'
    deadline_wait 240 "PgDog Deployment direct hash" \
        '[ "$(kubectl get deploy fleet-pgdog -o jsonpath='"'"'{.spec.template.metadata.annotations.crabka\.io/pgdog-config-hash}'"'"')" = "$(kubectl get secret fleet-pgdog-config -o jsonpath='"'"'{.metadata.annotations.crabka\.io/pgdog-rollout-hash}'"'"')" ]'
    timeout 180s kubectl rollout status deploy/fleet-pgdog --timeout=170s
    kubectl get secret fleet-pgdog-config -o jsonpath='{.data.pgdog\.toml}' \
        | base64 -d | grep -q 'host = "tenant-a-gres.default.svc.cluster.local"' || \
        fail "post-grace PgDog route did not return to direct compute"
    [ "$(kubectl get grestenant tenant-a -o jsonpath='{.status.lifecyclePhase}')" = active ] || \
        fail "tenant suspended before post-grace direct-route proof"
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
    kubectl port-forward svc/fleet-pgdog 16432:6432 >"$ARTIFACT_DIR/port-forward.log" 2>&1 &
    PORT_FORWARD_PID=$!
    deadline_wait 30 "PgDog direct-route port-forward" \
        "timeout 1 bash -c '</dev/tcp/127.0.0.1/16432' 2>/dev/null"
    PGPASSWORD="$PGPASSWORD_VALUE" timeout 20s psql \
        "host=localhost port=16432 dbname=tenant-a user=alice sslmode=verify-full sslrootcert=$ARTIFACT_DIR/ca.crt sslcert=$ARTIFACT_DIR/client.crt sslkey=$ARTIFACT_DIR/client.key" \
        -v ON_ERROR_STOP=1 -tAc "SELECT value FROM lifecycle_marker WHERE id=1" \
        | grep -qx survives || fail "post-grace direct PgDog query failed"
    if PGPASSWORD=wrong-password timeout 10s psql \
        "host=localhost port=16432 dbname=tenant-a user=alice sslmode=verify-full sslrootcert=$ARTIFACT_DIR/ca.crt sslcert=$ARTIFACT_DIR/client.crt sslkey=$ARTIFACT_DIR/client.key" \
        -tAc "SELECT 1" >/dev/null 2>&1; then
        fail "post-grace direct PgDog route accepted the wrong tenant credential"
    fi
    post_grace_pgdog_log="$ARTIFACT_DIR/post-grace-pgdog-${iteration}.log"
    kubectl logs -l app.kubernetes.io/name=crabka-pgdog,app.kubernetes.io/instance=fleet \
        --all-containers=true --prefix --ignore-errors=true --since=2m \
        >"$post_grace_pgdog_log" 2>&1
    grep -Fq 'auth: passthrough' "$post_grace_pgdog_log" || \
        fail "post-grace direct PgDog query was not passthrough authenticated"
    kill "$KEEPER_PID" 2>/dev/null || true
    wait "$KEEPER_PID" 2>/dev/null || true
    KEEPER_PID=""
    kill "$COMPUTE_FORWARD_PID" 2>/dev/null || true
    wait "$COMPUTE_FORWARD_PID" 2>/dev/null || true
    COMPUTE_FORWARD_PID=""
    physical_topic=$(kubectl exec demo-brokers-0 -- sh -c \
        'find /var/lib/crabka/data -maxdepth 1 -type d -name "__gres_wal.tenant-a.r0.g*-0" -printf "%f\n" | sort | tail -1' \
        2>/dev/null)
    [ -n "$physical_topic" ] || fail "active generation-qualified WAL directory is missing"
    printf '%s\t%s\n' "$iteration" "$physical_topic" >>"$ARTIFACT_DIR/physical-wal-generations.tsv"
    after_generation=$(kubectl get grestenant tenant-a -o jsonpath='{.status.registryVersion}')
    printf '%s\t%s\t%s\t%s\t%s\n' "$iteration" "$before_generation" "$after_generation" "$start_ns" "$end_ns" \
        >>"$ARTIFACT_DIR/generation-proof.tsv"
done
lifecycle_end_ns=$(date +%s%N)
printf '%s\t%s\n' "$lifecycle_start_ns" "$lifecycle_end_ns" >"$ARTIFACT_DIR/lifecycle-window.tsv"
# An observation that misses the 4-second grace window (loaded runner) cannot
# distinguish the legitimate lazy flip from a premature roll, so it is skipped
# above — but the run as a whole must still have exercised the property.
[ "$noroll_within_grace" -ge 1 ] || \
    fail "no wake iteration observed PgDog inside the credential grace window; the no-roll property was never exercised"

# Real missing-final-manifest refusal: stop the operator, let the real compute
# publish Suspended, delete the exact newest manifest, then restore the
# operator. Parking must fail closed without scaling the Deployment to zero or
# deleting the generation-qualified WAL.
kubectl scale deploy/crabka-gres-operator -n crabka-operator --replicas=0
kubectl port-forward svc/demo-broker-headless 19092:9092 >"$ARTIFACT_DIR/broker-port-forward.log" 2>&1 &
BROKER_FORWARD_PID=$!
deadline_wait 30 "broker port-forward" "timeout 1 bash -c '</dev/tcp/127.0.0.1/19092' 2>/dev/null"
deadline_wait 90 "real compute Suspended registry record" \
    "./target/release/crabka gres describe --bootstrap 127.0.0.1:19092 --name tenant-a 2>/dev/null | grep -qi 'suspended'"
kubectl delete pod minio-delete-manifest --ignore-not-found >/dev/null
kubectl run minio-delete-manifest --restart=Never \
    --image=minio/mc:RELEASE.2025-04-16T18-13-26Z \
    --env=MC_HOST_local=http://minio:minio-secret@minio:9000 \
    --command -- sh -c \
    'manifest=$(mc find local/gres-checkpoints/gres/tenant-a --name MANIFEST | sort | tail -1); test -n "$manifest"; printf "%s\n" "$manifest"; mc rm "$manifest"'
deadline_wait 60 "newest manifest deletion" \
    '[ "$(kubectl get pod minio-delete-manifest -o jsonpath='"'"'{.status.phase}'"'"')" = Succeeded ]'
kubectl logs minio-delete-manifest >"$ARTIFACT_DIR/deleted-manifest.txt"
kubectl scale deploy/crabka-gres-operator -n crabka-operator --replicas=1
timeout 120s kubectl rollout status deploy/crabka-gres-operator -n crabka-operator --timeout=110s
deadline_wait 90 "missing-final-manifest refusal" \
    "kubectl logs -n crabka-operator deploy/crabka-gres-operator --since=2m | grep -Eqi 'manifest.*(missing|not found)|missing.*manifest'"
[ "$(kubectl get deploy tenant-a-gres -o jsonpath='{.spec.replicas}')" = 1 ] || \
    fail "operator scaled tenant to zero despite missing final manifest"
kubectl exec demo-brokers-0 -- sh -c \
    'test -n "$(find /var/lib/crabka/data -maxdepth 1 -type d -name "__gres_wal.tenant-a.r0.g*-0" -print -quit)"' || \
    fail "operator deleted WAL despite missing final manifest"
kubectl logs -n crabka-operator deploy/crabka-gres-operator --since=2m \
    >"$ARTIFACT_DIR/missing-manifest-refusal.log"
kill "$BROKER_FORWARD_PID" 2>/dev/null || true
wait "$BROKER_FORWARD_PID" 2>/dev/null || true
BROKER_FORWARD_PID=""

python3 - "$ARTIFACT_DIR" "$ITERATIONS" "$P95_CEILING_MS" "$PGDOG_IMAGE" <<'PY'
import json, math, pathlib, platform, statistics, subprocess, sys
root, expected, ceiling, pgdog = pathlib.Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
rows = [line.split("\t") for line in (root / "iteration-timings.tsv").read_text().splitlines() if line]
if len(rows) != expected: raise SystemExit(f"expected {expected} measurements, got {len(rows)}")
generation_rows = [line.split("\t") for line in (root / "physical-wal-generations.tsv").read_text().splitlines() if line]
if len(generation_rows) != expected: raise SystemExit(f"expected {expected} WAL generation proofs, got {len(generation_rows)}")
generations = [int(topic.rsplit(".g", 1)[1].split("-", 1)[0]) for _, topic in generation_rows]
if generations != sorted(set(generations)):
    raise SystemExit(f"physical WAL generations did not strictly advance: {generations}")
values = sorted(int(row[1]) for row in rows)
rank = lambda p: values[max(0, math.ceil(p * len(values)) - 1)]
elapsed = sum(values) / 1000
window_start, window_end = map(int, (root / "lifecycle-window.tsv").read_text().split())
lifecycle_elapsed = (window_end - window_start) / 1_000_000_000
result = {
  "mode": "operator-backed-kind-gating", "iterations": len(values),
  "p50_ms": rank(.50), "p95_ms": rank(.95), "max_ms": max(values),
  "mean_ms": round(statistics.fmean(values), 2),
  "coldstart_only_rate_per_second": round(len(values) / elapsed, 4),
  "sustained_lifecycle_rate_per_second": round(len(values) / lifecycle_elapsed, 4),
  "p95_ceiling_ms": ceiling, "pgdog_image": pgdog,
  "kind_version": subprocess.check_output(["kind", "version"], text=True).strip(),
  "kubectl_version": subprocess.check_output(["kubectl", "version", "--client"], text=True).splitlines()[0],
  "host": platform.platform(), "latencies_ms": values,
  "wal_generations": generations,
  "crabka_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
  "pgdog_resolved_image": subprocess.check_output(
      ["docker", "image", "inspect", pgdog, "--format", "{{index .RepoDigests 0}}"], text=True
  ).strip(),
}
(root / "coldstart.json").write_text(json.dumps(result, indent=2) + "\n")
if result["p95_ms"] > ceiling:
    raise SystemExit(f"p95 {result['p95_ms']} exceeds the CI ceiling {ceiling}")
PY

# Retain exact controller/watch/requeue and lifecycle records. ResumeRequested
# is broker-backed and coalesced by the registry; wal_generation advancement is
# evidenced by the controller logs plus the generation proof captured above.
kubectl logs -n crabka-operator deploy/crabka-gres-operator --timestamps >"$ARTIFACT_DIR/operator.log"
grep -E 'ResumeRequested|resume_requested|parking|wal_generation|Suspended|suspended' \
    "$ARTIFACT_DIR/operator.log" >"$ARTIFACT_DIR/lifecycle-events.log" || true
echo "PASS: operator-backed Kind lifecycle and N=$ITERATIONS verified-TLS cold starts"
