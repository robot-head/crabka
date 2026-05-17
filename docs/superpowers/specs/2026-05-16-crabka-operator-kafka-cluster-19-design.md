# Slice 19: Operator — `Kafka` CRD minimal (KRaft mixed-mode cluster) — Design

**Status:** Approved 2026-05-16.

**Goal:** Replace slice 17's placeholder `Kafka` CRD with a real schema and turn the stub reconciler into one that actually materializes a single-broker KRaft mixed-mode Kafka cluster: a headless `Service`, a `ConfigMap`, a cluster-ID `Secret`, and a `StatefulSet` running the `crabka-broker` binary. Status conditions reflect `StatefulSet` rollout state. The kind-cluster e2e from slice 17 is extended to apply a `Kafka` CR, wait for `Ready=True`, and assert the broker pod responds on its Kafka listener.

---

## 1. Scope

### In

- Real `Kafka` CRD schema:
  - `spec.replicas: i32` (default `1`; this slice enforces `== 1` — multi-broker via `KafkaNodePool` arrives in slice 20).
  - `spec.image: String` (default `ghcr.io/robot-head/crabka-broker:<chart-appVersion>`).
  - `spec.kafkaVersion: String` retained as informational metadata (no schema enforcement; written into the broker pod's `app.kubernetes.io/version` label).
  - `spec.resources: ResourceRequirements` (CPU + memory requests/limits for the broker container; defaults match the operator pod's existing defaults).
  - `status.conditions: []Condition` (`Ready`, `Reconciling`); plus `status.readyReplicas` / `status.replicas` mirrors of `StatefulSet.status`.
- Reconciler creates / patches / owns:
  - One headless `Service` `<name>-broker-headless` (`clusterIP: None`, port 9092/TCP `kafka-internal`).
  - One `ConfigMap` `<name>-broker-config` containing a `broker.args` script consumed by the pod entrypoint.
  - One `Secret` `<name>-cluster-id` with a generated UUID under `clusterId`. Created on first reconcile, never overwritten.
  - One `StatefulSet` `<name>-broker` with `serviceName=<name>-broker-headless`, the requested `replicas`, an init container that runs `crabka format`, a main container that runs `crabka-broker`, an `emptyDir` `data` volume mounted at `/var/lib/crabka/data`.
- All managed objects carry an `ownerReference` back to the `Kafka` CR with `controller: true` + `blockOwnerDeletion: true`.
- `crabka-broker` CLI gains `--cluster-id <UUID>` (env `CRABKA_CLUSTER_ID`). `Controller::start` threads the value through to `CrabkaStateMachine::new` so cross-node images agree (and the operator-managed Secret is load-bearing on day one, not informational).
- `crabka format --cluster-id` is plumbed from the CLI (already present) into the init container args.
- Packaging: `packaging/melange/crabka-broker.yaml` + `packaging/apko/crabka-broker.yaml` + `tools/build-image.sh` extended to build the broker image alongside the operator image.
- E2E (kind):
  - Apply `Kafka` `demo` with `replicas: 1`.
  - Wait for `status.conditions[?type=="Ready"].status == "True"`.
  - `kubectl exec -n default demo-broker-0 -- /usr/bin/crabka-broker --version` returns 0 (smoke proof that the binary launched correctly).
  - `kubectl logs -n default demo-broker-0` contains the substring `crabka-broker listening`.

### Out (deferred)

| Concern | Slice |
|---|---|
| Multi-broker clusters via `KafkaNodePool` | 20 |
| Pod templates (affinity, tolerations, labels, annotations) | 20 |
| Rolling restart on config drift | 21 |
| `ControlledShutdown` for graceful drain | 22 (core) |
| `NetworkPolicy` generation | 23 |
| Persistent storage (PVCs, `storageClass`, retain-vs-delete) | 24 |
| External listeners (NodePort / LB / Ingress / Route) | 25–27 |
| Version upgrades / `inter.broker.protocol.version` | 28 |
| Cluster CA + clients CA, TLS, SASL listener config | 30–31 |
| `KafkaTopic` / `KafkaUser` CRDs | 35–36 |

### Constraints inherited

- `spec.replicas > 1` is rejected with `status.conditions[type=Ready] = {status: False, reason: UnsupportedReplicaCount}` and a non-requeue (operator does nothing further until the user fixes the CR). The validation lives in the CRD schema (`minimum: 1`, `maximum: 1` for slice 19) so `kubectl apply` rejects it before the operator ever sees it.
- The broker process runs as a single-node KRaft mixed-mode cluster (controller + broker in one process, self-voter). This matches the existing `crates/broker/src/bin/broker.rs` defaults; no multi-voter quorum is constructed in slice 19.
- `BootstrapMode::Bootstrap` is the only mode used. Restart of a single-replica StatefulSet pod still flips to `Rejoin` semantics on the second start because the on-disk raft log already exists; the broker handles this via the existing `(BootstrapMode::Bootstrap, log_is_empty=false)` arm in `Controller::start`.

---

## 2. CRD schema

`spec`:

```rust
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(group="crabka.io", version="v1alpha1", kind="Kafka", plural="kafkas",
       shortname="kk", namespaced, status="KafkaStatus", derive="PartialEq")]
#[serde(rename_all = "camelCase")]
pub struct KafkaSpec {
    /// Crabka version label (informational; image tag governs the actual
    /// binary version).
    pub kafka_version: String,

    /// Number of broker replicas. Slice 19 supports `1` only.
    #[serde(default = "default_replicas")]
    #[schemars(range(min = 1, max = 1))]
    pub replicas: i32,

    /// Container image. Default chosen at reconcile time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Resource requests/limits applied to the broker container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<k8s_openapi::api::core::v1::ResourceRequirements>,
}

fn default_replicas() -> i32 { 1 }
```

`status`:

```rust
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaStatus {
    #[serde(default)]
    pub conditions: Vec<KafkaCondition>,
    /// Mirrors `StatefulSet.status.replicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Mirrors `StatefulSet.status.readyReplicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
}
```

`KafkaCondition` is unchanged from slice 17.

CRD YAML is regenerated by `crabka-operator gen-crds` and committed to `deploy/crds/crabka.io_kafkas.yaml`. The codegen-drift check (`.github/workflows/codegen-check.yml`) already covers this path; no workflow change needed.

---

## 3. Reconciler

`crates/operator/src/controller/kafka.rs` is rewritten around four pure helpers, one per managed object:

```rust
fn render_service(owner: &Kafka) -> Service;
fn render_configmap(owner: &Kafka) -> ConfigMap;
fn render_secret(owner: &Kafka) -> Secret;            // generates UUID on call
fn render_statefulset(owner: &Kafka, image: &str) -> StatefulSet;
```

Each helper is a pure function of the owner spec + (for `Secret`) a `Uuid::new_v4()` call wrapped in an injectable trait so unit tests are deterministic. The reconcile fn:

1. Validates `spec.replicas == 1`; if not, sets `Ready=False, reason=UnsupportedReplicaCount` and returns.
2. Server-side applies `Service`, `ConfigMap`, `Secret` (with `if-not-exists` semantics for `Secret` to preserve the generated UUID), and `StatefulSet`. Field manager: `crabka-operator`.
3. Reads back the live `StatefulSet`, projects its `status.replicas` + `status.readyReplicas` into `KafkaStatus`, computes condition state:
   - `readyReplicas == spec.replicas` → `Ready=True, reason=Available`.
   - `readyReplicas == 0` → `Ready=False, reason=NoBrokersReady`.
   - Otherwise → `Ready=False, reason=PartiallyReady`.
4. Patches `status` subresource (`Patch::Apply` via merge — preserves any future foreign condition managers).
5. Returns `Action::requeue(Duration::from_secs(30))`.

Owner references on every managed object: `OwnerReference { apiVersion: "crabka.io/v1alpha1", kind: "Kafka", name, uid, controller: Some(true), block_owner_deletion: Some(true) }`. The reconciler refuses to create objects when the owner UID is missing (defensive: should never happen on a CR from the watch stream).

`controller_kafka::run` is updated to watch owned `StatefulSet`s as well, via `Controller::owns::<StatefulSet>(...)`, so the reconciler re-fires on rollout transitions without waiting for the 30-second periodic requeue.

### Image default

The default broker image is computed in the reconcile fn:

```rust
fn default_image(cfg: &OperatorConfig) -> String {
    cfg.default_broker_image.clone()
        .unwrap_or_else(|| format!("ghcr.io/robot-head/crabka-broker:{}", env!("CARGO_PKG_VERSION")))
}
```

`OperatorConfig` gains `default_broker_image: Option<String>` (CLI `--default-broker-image`, env `DEFAULT_BROKER_IMAGE`). The Helm chart sets this via the existing `image.repository` pattern but for the broker (new `brokerImage` value).

---

## 4. Rendered manifests

### `Service` (headless)

```yaml
apiVersion: v1
kind: Service
metadata:
  name: demo-broker-headless
  namespace: default
  ownerReferences: [...Kafka demo...]
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: crabka-broker
    app.kubernetes.io/instance: demo
  ports:
    - name: kafka-internal
      port: 9092
      protocol: TCP
      targetPort: 9092
```

### `ConfigMap`

```yaml
apiVersion: v1
kind: ConfigMap
metadata: { name: demo-broker-config, ... }
data:
  broker.env: |
    CRABKA_LISTEN_ADDR=0.0.0.0:9092
```

The advertised listener is computed inline by the pod entrypoint from `POD_NAME` and the headless service FQDN; no ConfigMap variable for it. The ConfigMap is intentionally thin in slice 19 — slice 21 (rolling restart on config drift) will be the slice that justifies a richer one.

### `Secret`

```yaml
apiVersion: v1
kind: Secret
metadata: { name: demo-cluster-id, ... }
type: Opaque
data:
  clusterId: <base64(uuid)>
```

### `StatefulSet`

Skeleton (full template lives in the helper):

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: demo-broker, ... }
spec:
  serviceName: demo-broker-headless
  replicas: 1
  podManagementPolicy: Parallel
  selector:
    matchLabels:
      app.kubernetes.io/name: crabka-broker
      app.kubernetes.io/instance: demo
  template:
    metadata:
      labels:
        app.kubernetes.io/name: crabka-broker
        app.kubernetes.io/instance: demo
        app.kubernetes.io/version: <kafkaVersion>
    spec:
      securityContext: { runAsNonRoot: true, runAsUser: 65532, fsGroup: 65532, seccompProfile: { type: RuntimeDefault } }
      initContainers:
        - name: format
          image: <broker image>
          command: [/bin/sh, -c]
          args:
            - |
              set -eu
              if [ ! -f /var/lib/crabka/data/.formatted ]; then
                /usr/bin/crabka format --log-dir /var/lib/crabka/data --cluster-id "$CRABKA_CLUSTER_ID"
                touch /var/lib/crabka/data/.formatted
              fi
          env:
            - { name: CRABKA_CLUSTER_ID, valueFrom: { secretKeyRef: { name: demo-cluster-id, key: clusterId } } }
          volumeMounts:
            - { name: data, mountPath: /var/lib/crabka/data }
          securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: [ALL] } }
      containers:
        - name: broker
          image: <broker image>
          command: [/usr/bin/crabka-broker]
          args:
            - --listen-addr=0.0.0.0:9092
            - --log-dir=/var/lib/crabka/data
            - --broker-id=0
          env:
            - { name: POD_NAME, valueFrom: { fieldRef: { fieldPath: metadata.name } } }
            - { name: POD_NAMESPACE, valueFrom: { fieldRef: { fieldPath: metadata.namespace } } }
            - { name: CRABKA_CLUSTER_ID, valueFrom: { secretKeyRef: { name: demo-cluster-id, key: clusterId } } }
            - { name: CRABKA_ADVERTISED_LISTENER, value: "$(POD_NAME).demo-broker-headless.$(POD_NAMESPACE).svc.cluster.local:9092" }
          ports:
            - { containerPort: 9092, name: kafka-internal, protocol: TCP }
          readinessProbe:
            tcpSocket: { port: 9092 }
            initialDelaySeconds: 2
            periodSeconds: 5
          livenessProbe:
            tcpSocket: { port: 9092 }
            initialDelaySeconds: 30
            periodSeconds: 10
          resources: <from spec or defaults>
          volumeMounts:
            - { name: data, mountPath: /var/lib/crabka/data }
          securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: [ALL] } }
      volumes:
        - { name: data, emptyDir: {} }
```

Note: `--advertised-listener` is not a CLI flag on the broker today (it derives from `--listen-addr`). The pod entrypoint script substitutes `CRABKA_ADVERTISED_LISTENER` into the args via shell expansion before exec. This slice keeps the broker CLI unchanged for the listener; threading an env-aware advertised listener is implicit through `--listen-addr` substitution (the broker binds `0.0.0.0:9092` and advertises the pod FQDN).

Actually we extend the broker binary to accept `--advertised-listener` from env `CRABKA_ADVERTISED_LISTENER` (clap `env = "..."` attribute). One-line CLI change. Same for `--cluster-id` (env `CRABKA_CLUSTER_ID`).

---

## 5. Broker binary changes

`crates/broker/src/bin/broker.rs`:

- Add `#[arg(long, env = "CRABKA_CLUSTER_ID")] cluster_id: Option<uuid::Uuid>`.
- Add `#[arg(long, env = "CRABKA_ADVERTISED_LISTENER")] advertised_listener_env: Option<String>` (clap will use the env value if the CLI flag is absent). The existing `advertised_listener: Option<String>` field is renamed to keep one canonical name; both env and CLI flag map to it.
- Plumb `cluster_id` into `BrokerConfig` → `ControllerConfig` → `CrabkaStateMachine::new`. `BrokerConfig::cluster_id: Option<Uuid>` (None preserves current `Uuid::nil()` for backward-compat in unit tests).

`crates/raft/src/controller.rs` line 377:

```rust
let state_machine = Arc::new(CrabkaStateMachine::new(
    config.cluster_id.unwrap_or_else(Uuid::nil),
));
```

`ControllerConfig::cluster_id: Option<Uuid>` is added with `None` default. No call site outside `Broker::start` needs to set it.

This is a small, mechanical wiring change. Existing tests pass `None` and keep `Uuid::nil()` semantics; the operator passes a real UUID through the env var.

---

## 6. Packaging

Two new packaging files mirror `crabka-operator`:

`packaging/melange/crabka-broker.yaml`:

```yaml
package:
  name: crabka-broker
  version: 0.1.1
  epoch: 0
  description: Single-node Kafka-compatible broker
  ...
pipeline:
  - name: Install pinned Rust toolchain (same as operator)
  - name: Build crabka-broker + crabka CLI
    runs: |
      cargo build --release --bin crabka-broker -p crabka-broker
      cargo build --release --bin crabka -p crabka-cli
      install -D -m 0755 target/release/crabka-broker "${{targets.contextdir}}/usr/bin/crabka-broker"
      install -D -m 0755 target/release/crabka         "${{targets.contextdir}}/usr/bin/crabka"
```

`packaging/apko/crabka-broker.yaml` composes the apk onto `wolfi-base` with the standard runtime contents.

`tools/build-image.sh` is extended with a second `melange build` + `apko build` invocation for `crabka-broker`. The operator-e2e workflow gains a Build-broker-image step parallel to the existing operator one, plus a `kind load docker-image crabka-broker:e2e` step.

---

## 7. Helm chart

`charts/crabka-operator/`:

- `values.yaml` gains `brokerImage.repository` + `brokerImage.tag` + `brokerImage.pullPolicy`, defaulting to `ghcr.io/robot-head/crabka-broker:<chart.appVersion>` / `IfNotPresent`.
- `templates/deployment.yaml` adds `--default-broker-image={{ .Values.brokerImage.repository }}:{{ .Values.brokerImage.tag }}` to the operator's `args`.
- `templates/clusterrole.yaml` is extended with verbs on `services`, `configmaps`, `secrets` (`get,list,watch,create,update,patch,delete`) in `""` apiGroup, and on `statefulsets` in `apps`. The existing Lease + events rules stay.

CRD YAML is shipped at `deploy/crds/crabka.io_kafkas.yaml` (regenerated). No `crds/` directory inside the chart — operator and CRD install are decoupled.

---

## 8. Testing

### Unit tests (`crates/operator/src/controller/kafka.rs`, additions)

- `render_service_has_clusterip_none_and_owner_ref`
- `render_configmap_carries_owner_ref`
- `render_secret_generates_uuid_on_each_call` (uses an injected RNG seam to assert reproducibility under test, and ownership)
- `render_statefulset_default_image_resolution`
- `render_statefulset_custom_image_override`
- `render_statefulset_resources_default_match_operator`
- `render_statefulset_resources_user_override`
- `validate_replicas_rejects_two`

### Reconcile tests (`crates/operator/tests/reconcile.rs`, additions)

Mocked-client tests modeled after the slice 17 pattern:

- `reconcile_applies_service_configmap_secret_statefulset_on_create` — assert four PATCH requests, the right URIs, owner refs in the bodies.
- `reconcile_status_ready_true_when_sts_ready` — preload a fake StatefulSet status with `readyReplicas == 1`; assert `Ready=True, reason=Available` patched on the CR.
- `reconcile_status_ready_false_when_sts_partial` — `readyReplicas == 0`; assert `Ready=False, reason=NoBrokersReady`.
- `reconcile_validation_rejects_replicas_two` — apply a Kafka CR with `replicas: 2`; assert status condition set and no `StatefulSet` PATCH issued.

### Broker-binary tests (`crates/broker/tests/cli_smoke.rs`, new)

A tiny test exercises the new env-var plumbing without spinning a real broker:

- `binary_accepts_cluster_id_env_var` — `crabka-broker --help` includes `--cluster-id`.
- `binary_advertised_listener_env_overrides_default` — clap-only parse-args path; no Tokio runtime.

These run on every `cargo test -p crabka-broker --bin crabka-broker` invocation.

### E2E (kind, `.github/workflows/operator-e2e.yml`)

Replace the placeholder `demo` resource with a real broker:

1. Build both `crabka-operator` and `crabka-broker` images via melange/apko, load both into kind.
2. Install CRDs + chart (chart now references the broker image via `--default-broker-image`).
3. Apply `Kafka` `demo` with `replicas: 1`.
4. Wait for `status.conditions[?type=="Ready"].status == "True"` (up to 5 minutes — first-boot is slower than the slice 17 stub).
5. Smoke: `kubectl exec demo-broker-0 -c broker -- /usr/bin/crabka-broker --version` → exit 0.
6. Log probe: `kubectl logs demo-broker-0 -c broker` contains `crabka-broker listening`.
7. Cleanup: `kubectl delete kafka demo`; assert StatefulSet, Service, ConfigMap, Secret are garbage-collected within 60 seconds.

The existing diagnostics-on-failure block needs `kubectl get sts,svc,cm,secret -n default` rolled in.

### Out of test scope

- Multi-broker cluster (slice 20).
- Producing/consuming records through the brokered listener (deferred — JVM acceptance tests in core Crabka already exercise the wire path. The operator slice's job is to prove the pod runs; the wire path is a closed system that we don't need to re-test here).

---

## 9. File structure

```
crates/operator/src/
├── controller/kafka.rs         # REWRITTEN — full reconciler
├── crd/kafka.rs                # MODIFIED — new fields, replicas validation
├── config.rs                   # MODIFIED — default_broker_image
crates/operator/tests/
├── reconcile.rs                # MODIFIED — four new tests
crates/broker/src/
├── bin/broker.rs               # MODIFIED — CLI cluster_id + adv listener env
├── config.rs                   # MODIFIED — cluster_id field
crates/broker/tests/
├── cli_smoke.rs                # NEW — clap-only assertions
crates/raft/src/
├── controller.rs               # MODIFIED — pass cluster_id to CrabkaStateMachine
deploy/crds/
├── crabka.io_kafkas.yaml       # REGENERATED
charts/crabka-operator/
├── values.yaml                 # MODIFIED — brokerImage block
├── templates/deployment.yaml   # MODIFIED — --default-broker-image arg
├── templates/clusterrole.yaml  # MODIFIED — services/cm/secret/sts verbs
packaging/melange/
├── crabka-broker.yaml          # NEW
packaging/apko/
├── crabka-broker.yaml          # NEW
tools/
├── build-image.sh              # MODIFIED — build broker image too
.github/workflows/
├── operator-e2e.yml            # MODIFIED — build broker, real Kafka CR
```

Implementation plan target: **~12 tasks across 4 batches**.

- **Batch 1 (parallel):** T1 CRD schema + spec/status, T2 broker binary CLI extension + ControllerConfig::cluster_id, T3 broker packaging (melange/apko), T4 Helm chart RBAC + brokerImage values.
- **Batch 2 (parallel):** T5 pure `render_*` helpers + unit tests (depends on T1 only), T6 default_broker_image config plumbing (depends on T1).
- **Batch 3 (sequential):** T7 reconcile fn rewrite + status patching, T8 mocked-client reconcile tests, T9 broker-binary smoke tests.
- **Batch 4 (sequential):** T10 regen CRD YAML, T11 update operator-e2e workflow (build broker image, load into kind, real Kafka CR), T12 verify whole pipeline locally (cargo fmt/clippy/test, helm lint, helm template).

---

## 10. Open questions resolved

- **Should the broker image be one container with both init + main, or two images?** One. The same image runs `crabka format` as init and `crabka-broker` as main. Cuts pull cost in half and matches Strimzi.
- **Should the cluster-ID Secret be created via SSA or `if-not-exists`?** `if-not-exists` (server-side `create` then ignore `AlreadyExists`). SSA on a Secret with `data` would overwrite the generated UUID on every reconcile, which is wrong.
- **Where does the broker get its `broker_id`?** Hardcoded `0` for slice 19 (replicas=1). Slice 20 (KafkaNodePool) introduces ordinal-derived ids.
- **Why no `--advertised-listener` on the broker CLI today?** It exists as a CLI flag but not as an env var; slice 19 adds the env binding via clap's `env = "..."`. The operator passes it through `CRABKA_ADVERTISED_LISTENER`.
- **Why `podManagementPolicy: Parallel`?** With replicas=1 the policy is moot; setting it now means slice 20's multi-broker change doesn't touch the StatefulSet template.

---

## 11. Acceptance criteria

1. `cargo test -p crabka-operator` and `cargo test -p crabka-broker --bin crabka-broker` pass.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-operator` and `helm template ... | kubectl --dry-run=client apply -f -` pass.
4. `crabka-operator gen-crds` regenerates `deploy/crds/crabka.io_kafkas.yaml` with no drift (codegen-check CI green).
5. operator-e2e workflow: apply `Kafka demo` with `replicas: 1`; pod `demo-broker-0` reaches Ready; `crabka-broker --version` exec returns 0; log line `crabka-broker listening` appears.
6. `kubectl delete kafka demo` garbage-collects `Service`, `ConfigMap`, `Secret`, `StatefulSet` within 60 s.
