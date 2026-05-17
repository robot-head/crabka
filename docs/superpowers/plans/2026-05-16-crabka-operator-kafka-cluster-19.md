# Crabka Operator Slice 19 — `Kafka` CRD minimal (KRaft mixed-mode cluster)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Per CLAUDE.md, dispatch the batches in parallel within each batch. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace slice 17's placeholder `Kafka` CRD with a real schema and a reconciler that materializes a single-broker KRaft mixed-mode cluster: headless `Service`, `ConfigMap`, cluster-ID `Secret`, and a `StatefulSet` running the `crabka-broker` binary. The kind-cluster e2e is upgraded to actually run a broker pod and prove it reaches Ready.

**Spec:** [`docs/superpowers/specs/2026-05-16-crabka-operator-kafka-cluster-19-design.md`](../specs/2026-05-16-crabka-operator-kafka-cluster-19-design.md).

**Conventions:**

- All crates use `[lints] workspace = true`; clippy `pedantic` warn-by-default; `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` are CI gates.
- Commits follow conventional commits (`feat:`, `chore:`, `ci:`, `test:`).
- Per CLAUDE.md: no backwards-compatibility shims; Kafka wire-protocol bytes are the only invariant.

---

## Batch overview

| Batch | Tasks | File overlap | Parallel? |
|---|---|---|---|
| 1 | T1, T2, T3, T4 | disjoint | yes |
| 2 | T5, T6 | both touch `crates/operator` — T5 in `controller/kafka.rs`, T6 in `config.rs`, no overlap | yes |
| 3 | T7, T8, T9 | T7+T8 both touch `controller/kafka.rs` / `tests/reconcile.rs` → sequential; T9 disjoint (broker CLI tests) — pair T9 with T7 | T9 with T7; T8 after T7 |
| 4 | T10, T11, T12 | T10 regenerates CRD YAML, T11 modifies workflow, T12 is the verify step — T10+T11 disjoint, T12 must be last | T10 ‖ T11, then T12 |

---

## Task 1 — `Kafka` CRD schema (replicas, image, resources, status mirrors)

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`

- [ ] **Step 1: Extend `KafkaSpec`**

In `crates/operator/src/crd/kafka.rs`, replace `KafkaSpec` with:

```rust
use k8s_openapi::api::core::v1::ResourceRequirements;

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "Kafka",
    plural = "kafkas",
    singular = "kafka",
    shortname = "kk",
    namespaced,
    status = "KafkaStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaSpec {
    /// Crabka version label (informational; image tag governs the actual
    /// binary version).
    pub kafka_version: String,

    /// Number of broker replicas. Slice 19 supports `1` only;
    /// `KafkaNodePool` (slice 20) generalizes this.
    #[serde(default = "default_replicas")]
    #[schemars(range(min = 1, max = 1))]
    pub replicas: i32,

    /// Container image. Reconciler falls back to the operator's
    /// `--default-broker-image` flag if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Resource requests / limits applied to the broker container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
}

const fn default_replicas() -> i32 {
    1
}
```

- [ ] **Step 2: Extend `KafkaStatus`**

Replace `KafkaStatus` with:

```rust
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaStatus {
    /// Standard Kubernetes-style condition list.
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

- [ ] **Step 3: Update existing tests in the module**

The slice 17 `round_trips_through_json` test constructs a `KafkaSpec`; it must now include `replicas: 1, image: None, resources: None`. Update accordingly. The `crd_metadata_is_correct` test stays unchanged.

Add a new test:

```rust
#[test]
fn spec_defaults_replicas_to_one() {
    let json = r#"{"kafkaVersion":"0.1.1"}"#;
    let spec: KafkaSpec = serde_json::from_str(json).unwrap();
    assert_eq!(spec.replicas, 1);
    assert!(spec.image.is_none());
    assert!(spec.resources.is_none());
}
```

- [ ] **Step 4: Test**

```bash
cargo test -p crabka-operator --lib crd::
```

Expected: 3 tests pass.

---

## Task 2 — Broker CLI + `ControllerConfig::cluster_id`

**Files:**
- Modify: `crates/broker/src/bin/broker.rs`
- Modify: `crates/broker/src/config.rs`
- Modify: `crates/raft/src/controller.rs`
- Modify: `crates/raft/src/lib.rs` (if `ControllerConfig` is re-exported)

- [ ] **Step 1: `ControllerConfig` accepts a cluster id**

In `crates/raft/src/controller.rs`, add a `cluster_id: Option<Uuid>` field to `ControllerConfig` (default `None`). Wire it in `Controller::start`:

```rust
let state_machine = Arc::new(CrabkaStateMachine::new(
    config.cluster_id.unwrap_or_else(Uuid::nil),
));
```

The `Uuid::nil` fallback preserves every existing test path.

- [ ] **Step 2: `BrokerConfig` accepts a cluster id**

In `crates/broker/src/config.rs`, add `pub cluster_id: Option<uuid::Uuid>` to `BrokerConfig` with `None` default in both `Default` and `for_tests`. Update the existing `BrokerConfig` → `ControllerConfig` translation site (search for `ControllerConfig {` in `crates/broker/src/broker.rs`) to set `cluster_id: config.cluster_id`.

Update the unit tests in `config.rs` that construct `BrokerConfig` literally — pass `..BrokerConfig::default()` rather than enumerating every field.

- [ ] **Step 3: Broker CLI extension**

In `crates/broker/src/bin/broker.rs`:

```rust
#[derive(Debug, Parser)]
struct Args {
    // ... existing fields ...

    /// `host:port` to advertise to clients (defaults to `listen_addr`).
    /// Set via env `CRABKA_ADVERTISED_LISTENER` from the operator.
    #[arg(long, env = "CRABKA_ADVERTISED_LISTENER")]
    advertised_listener: Option<String>,

    /// Cluster UUID. Each broker in a cluster must share this value.
    /// Set via env `CRABKA_CLUSTER_ID` from the operator.
    #[arg(long, env = "CRABKA_CLUSTER_ID")]
    cluster_id: Option<uuid::Uuid>,
}
```

Then plumb `args.cluster_id` into the `BrokerConfig` constructor.

- [ ] **Step 4: Test**

```bash
cargo test -p crabka-broker --lib config::
cargo test -p crabka-raft --lib
cargo build -p crabka-broker --bin crabka-broker
```

Expected: no test failures, broker binary builds.

---

## Task 3 — `crabka-broker` melange + apko packaging

**Files:**
- Create: `packaging/melange/crabka-broker.yaml`
- Create: `packaging/apko/crabka-broker.yaml`
- Modify: `tools/build-image.sh`

- [ ] **Step 1: Read the operator packaging files**

Read `packaging/melange/crabka-operator.yaml` and `packaging/apko/crabka-operator.yaml` in full. The broker variants are mechanical copies with two differences: package name and binaries built.

- [ ] **Step 2: Create `packaging/melange/crabka-broker.yaml`**

Same structure as `crabka-operator.yaml` but the build step is:

```yaml
  - name: Build crabka-broker + crabka CLI
    runs: |
      export PATH="$HOME/.cargo/bin:$PATH"
      cargo build --release --bin crabka-broker -p crabka-broker
      cargo build --release --bin crabka -p crabka-cli
      install -D -m 0755 target/release/crabka-broker "${{targets.contextdir}}/usr/bin/crabka-broker"
      install -D -m 0755 target/release/crabka         "${{targets.contextdir}}/usr/bin/crabka"
```

Package name: `crabka-broker`. Everything else mirrors the operator config.

- [ ] **Step 3: Create `packaging/apko/crabka-broker.yaml`**

Mirror `packaging/apko/crabka-operator.yaml`. Swap the `packages:` list to reference `crabka-broker` (and keep the same wolfi-base + `ca-certificates-bundle` runtime). The `entrypoint.command` becomes `/usr/bin/crabka-broker`.

- [ ] **Step 4: Extend `tools/build-image.sh`**

Read the existing script first, then add a second melange+apko invocation block for `crabka-broker` after the operator block. Both apk packages produced into the same `packages/` directory; both apko outputs tagged `crabka-broker:e2e` / `crabka-operator:e2e`.

- [ ] **Step 5: Local smoke**

```bash
# Optional local proof (skip in CI):
# ./tools/build-image.sh
```

This task is verified end-to-end by Task 11's CI workflow; local builds are slow and not required.

---

## Task 4 — Helm chart RBAC + brokerImage values

**Files:**
- Modify: `charts/crabka-operator/values.yaml`
- Modify: `charts/crabka-operator/templates/clusterrole.yaml`
- Modify: `charts/crabka-operator/templates/deployment.yaml`

- [ ] **Step 1: `values.yaml` brokerImage block**

Insert above the existing `replicaCount:` block:

```yaml
brokerImage:
  repository: ghcr.io/robot-head/crabka-broker
  tag: "0.1.1"
  pullPolicy: IfNotPresent
```

- [ ] **Step 2: ClusterRole verbs**

Extend the existing `rules:` list in `templates/clusterrole.yaml`:

```yaml
  - apiGroups: [""]
    resources: ["services", "configmaps", "secrets"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["apps"]
    resources: ["statefulsets"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

(Keep the existing `crabka.io` / `apiextensions.k8s.io` / events rules.)

- [ ] **Step 3: Deployment args**

In `templates/deployment.yaml`, extend the operator container `args:` block:

```yaml
          args:
            - run
            - --default-broker-image={{ .Values.brokerImage.repository }}:{{ .Values.brokerImage.tag }}
            {{- if .Values.watchNamespaces }}
            - --watch-namespaces={{ join "," .Values.watchNamespaces }}
            {{- end }}
```

- [ ] **Step 4: Test**

```bash
helm lint charts/crabka-operator
helm template t charts/crabka-operator > /tmp/rendered.yaml
grep -E 'statefulsets|brokerImage' /tmp/rendered.yaml
```

Expected: helm lint clean; rendered output mentions `statefulsets` in the ClusterRole.

---

## Task 5 — Pure `render_*` helpers + unit tests

**Files:**
- Modify: `crates/operator/src/controller/kafka.rs`
- Modify: `crates/operator/Cargo.toml` (add `k8s-openapi` features for `core/v1` `apps/v1`)

This task introduces only the pure renderers and their unit tests. The reconcile fn itself stays the slice-17 stub until Task 7.

- [ ] **Step 1: Verify `k8s-openapi` features**

`crates/operator/Cargo.toml` already enables `k8s-openapi`. `core/v1` and `apps/v1` are gated behind kube version features; the workspace's `v1_30` feature already pulls them in. No change should be needed — confirm by `grep -r 'k8s_openapi::api::apps::v1' crates/operator` and ensure it compiles.

- [ ] **Step 2: Constants module-private**

At the top of `controller/kafka.rs`:

```rust
const BROKER_PORT: i32 = 9092;
const APP_LABEL: &str = "crabka-broker";
const DEFAULT_BROKER_IMAGE: &str = concat!("ghcr.io/robot-head/crabka-broker:", env!("CARGO_PKG_VERSION"));

fn common_labels(owner_name: &str, kafka_version: &str) -> std::collections::BTreeMap<String, String> {
    let mut m = std::collections::BTreeMap::new();
    m.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    m.insert("app.kubernetes.io/instance".into(), owner_name.into());
    m.insert("app.kubernetes.io/version".into(), kafka_version.into());
    m.insert("app.kubernetes.io/managed-by".into(), "crabka-operator".into());
    m
}

fn owner_ref(owner: &Kafka) -> Result<k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference, ReconcileError> {
    let uid = owner.metadata.uid.as_deref().ok_or(ReconcileError::MissingUid)?;
    Ok(k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
        api_version: "crabka.io/v1alpha1".into(),
        kind: "Kafka".into(),
        name: owner.metadata.name.clone().unwrap_or_default(),
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}
```

`ReconcileError::MissingUid` is a new variant.

- [ ] **Step 3: Render helpers**

Add four pure helpers below the constants:

```rust
pub(crate) fn render_service(owner: &Kafka) -> Result<Service, ReconcileError> { ... }
pub(crate) fn render_configmap(owner: &Kafka) -> Result<ConfigMap, ReconcileError> { ... }
pub(crate) fn render_secret(owner: &Kafka, cluster_id: uuid::Uuid) -> Result<Secret, ReconcileError> { ... }
pub(crate) fn render_statefulset(owner: &Kafka, broker_image: &str) -> Result<StatefulSet, ReconcileError> { ... }
```

Templates match section 4 of the spec. Notes:

- `render_secret` takes the UUID as a parameter so tests are deterministic; the reconcile fn calls `Uuid::new_v4()` at the call site only when creating a new Secret.
- `render_statefulset` builds the init container, main container, env vars, probes, volumeMounts, and volumes exactly as the spec template shows.
- Use `serde_json::from_value(json!(...))` for verbose embedded YAML structures (probes, security contexts) — the typed builder is verbose; embedded JSON keeps the helper readable. Type-check the round-trip in tests.

- [ ] **Step 4: Resource defaults**

```rust
fn default_resources() -> k8s_openapi::api::core::v1::ResourceRequirements {
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    let mut requests = std::collections::BTreeMap::new();
    requests.insert("cpu".into(), Quantity("100m".into()));
    requests.insert("memory".into(), Quantity("256Mi".into()));
    let mut limits = std::collections::BTreeMap::new();
    limits.insert("cpu".into(), Quantity("1000m".into()));
    limits.insert("memory".into(), Quantity("1Gi".into()));
    k8s_openapi::api::core::v1::ResourceRequirements {
        requests: Some(requests),
        limits: Some(limits),
        ..Default::default()
    }
}
```

- [ ] **Step 5: Tests**

In `crates/operator/src/controller/kafka.rs` `#[cfg(test)] mod tests`:

```rust
fn fixture(name: &str, replicas: i32) -> Kafka {
    let mut k = Kafka::new(name, KafkaSpec {
        kafka_version: "0.1.1".into(),
        replicas,
        image: None,
        resources: None,
    });
    k.metadata.namespace = Some("default".into());
    k.metadata.uid = Some("u-1".into());
    k
}

#[test]
fn render_service_clusterip_none_owner_ref_set() { ... }
#[test]
fn render_configmap_carries_owner_ref() { ... }
#[test]
fn render_secret_data_is_base64_uuid() { ... }
#[test]
fn render_statefulset_default_image() { ... }
#[test]
fn render_statefulset_user_image_override() { ... }
#[test]
fn render_statefulset_resources_default() { ... }
#[test]
fn render_statefulset_resources_user_override() { ... }
#[test]
fn render_statefulset_includes_cluster_id_env_from_secret() { ... }
```

Each test constructs `fixture("demo", 1)`, calls one renderer, and asserts on shape (port, owner refs, container counts, env-var names, image string).

- [ ] **Step 6: Test**

```bash
cargo test -p crabka-operator --lib controller::kafka::tests
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

Expected: 8 unit tests pass, clippy clean.

---

## Task 6 — `OperatorConfig::default_broker_image` plumbing

**Files:**
- Modify: `crates/operator/src/config.rs`

- [ ] **Step 1: Add the field**

```rust
/// Default broker image used when `Kafka.spec.image` is unset.
#[arg(long, env = "DEFAULT_BROKER_IMAGE")]
pub default_broker_image: Option<String>,
```

- [ ] **Step 2: Update existing tests**

The existing `cli_defaults_compute_cluster_scope` test will need a `default_broker_image: None` assertion after construction. Existing field-by-field construction sites (none in production code, but the test in `tests/reconcile.rs` does field-by-field) need the new field.

- [ ] **Step 3: Test**

```bash
cargo test -p crabka-operator --lib config::
```

Expected: 2 tests pass (existing 2, both updated).

---

## Task 7 — Reconcile fn rewrite

**Files:**
- Modify: `crates/operator/src/controller/kafka.rs`
- Modify: `crates/operator/src/context.rs` (if a new field is needed for the default image)

> **Note:** This task supersedes the slice-17 stub. After this task, `reconcile` actually applies four objects and patches status conditions reflecting StatefulSet rollout state.

- [ ] **Step 1: Extend `ReconcileError`**

```rust
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("Kafka resource missing uid (not yet admitted)")]
    MissingUid,
    #[error("spec.replicas={0} is unsupported in slice 19 (only 1 allowed)")]
    UnsupportedReplicas(i32),
}
```

- [ ] **Step 2: Reconcile fn body**

Replace the existing `reconcile` body. Pseudocode:

```rust
pub async fn reconcile(obj: Arc<Kafka>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    tracing::info!(%ns, %name, "reconciling Kafka");

    if obj.spec.replicas != 1 {
        patch_status(&ctx, &ns, &name, KafkaStatus {
            conditions: vec![condition("Ready", "False", "UnsupportedReplicaCount", &format!("spec.replicas must be 1 in slice 19, got {}", obj.spec.replicas))],
            replicas: None,
            ready_replicas: None,
        }).await?;
        return Ok(Action::await_change());
    }

    // 1. Service
    let svc = render_service(&obj)?;
    apply_object::<Service>(&ctx, &ns, &svc).await?;

    // 2. ConfigMap
    let cm = render_configmap(&obj)?;
    apply_object::<ConfigMap>(&ctx, &ns, &cm).await?;

    // 3. Secret — if-not-exists semantics
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let secret_name = format!("{}-cluster-id", name);
    let cluster_id = match secret_api.get_opt(&secret_name).await? {
        Some(existing) => uuid_from_secret(&existing)?,
        None => {
            let new_id = uuid::Uuid::new_v4();
            let s = render_secret(&obj, new_id)?;
            match secret_api.create(&PostParams::default(), &s).await {
                Ok(_) => new_id,
                Err(kube::Error::Api(e)) if e.code == 409 => {
                    // Lost the race; re-read.
                    let fetched = secret_api.get(&secret_name).await?;
                    uuid_from_secret(&fetched)?
                }
                Err(e) => return Err(e.into()),
            }
        }
    };

    // 4. StatefulSet
    let image = obj.spec.image.clone()
        .or_else(|| ctx.config.default_broker_image.clone())
        .unwrap_or_else(|| DEFAULT_BROKER_IMAGE.into());
    let sts = render_statefulset(&obj, &image)?;
    apply_object::<StatefulSet>(&ctx, &ns, &sts).await?;

    // 5. Reflect status
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let live = sts_api.get_opt(&format!("{}-broker", name)).await?;
    let (replicas, ready_replicas, ready, reason, message) = derive_status(live.as_ref(), obj.spec.replicas);
    patch_status(&ctx, &ns, &name, KafkaStatus {
        conditions: vec![condition("Ready", if ready { "True" } else { "False" }, reason, message)],
        replicas,
        ready_replicas,
    }).await?;
    let _ = cluster_id; // (already injected into the StatefulSet via env Secret ref)

    Ok(Action::requeue(Duration::from_secs(30)))
}
```

Helpers `apply_object`, `patch_status`, `uuid_from_secret`, `condition`, `derive_status` live in the same module. `apply_object` uses SSA (`Patch::Apply` with the operator field manager and `force=true`).

- [ ] **Step 3: Update `run` to own `StatefulSet`s**

```rust
Controller::new(api, watcher::Config::default())
    .owns::<StatefulSet>(Api::all(ctx.client.clone()), watcher::Config::default())
    .run(reconcile, error_policy, Arc::new(ctx))
    ...
```

- [ ] **Step 4: Test**

```bash
cargo test -p crabka-operator --lib
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

Expected: unit tests still pass, clippy clean. (Integration tests in `tests/reconcile.rs` are updated in Task 8.)

---

## Task 8 — Reconcile mocked-client tests

**Files:**
- Modify: `crates/operator/tests/reconcile.rs`

> **Sequencing:** Task 8 must run after Task 7 because it tests the new reconcile body. Do not start in parallel with Task 7.

- [ ] **Step 1: Generalize the mock harness**

Refactor the slice-17 single mock service into a shared helper that records requests and returns canned responses keyed by `(method, path-substring)`. Approximate signature:

```rust
struct MockResponses { rules: Vec<(http::Method, String, http::Response<Vec<u8>>)> }
fn mock_service(captured: Sender<Request<Bytes>>, responses: MockResponses) -> impl Service<Request<kube::client::Body>, ...> { ... }
```

The rules list matches in order. Default fallthrough: `404`.

- [ ] **Step 2: Test `applies_service_configmap_secret_statefulset_on_create`**

Preload responses for:
- `GET .../secrets/demo-cluster-id` → 404
- `POST .../secrets` → 201 (return a faked Secret with the requested data)
- `GET .../statefulsets/demo-broker` → 404
- `PATCH .../services/demo-broker-headless` → 200
- `PATCH .../configmaps/demo-broker-config` → 200
- `PATCH .../statefulsets/demo-broker` → 200
- `PATCH .../kafkas/demo/status` → 200

Drive `reconcile(Arc::new(kafka), Arc::new(ctx)).await.unwrap()`. Assert the captured request set contains exactly the 7 expected method+URI pairs.

- [ ] **Step 3: Test `status_ready_true_when_sts_ready`**

Same as above but `GET .../statefulsets/demo-broker` returns a StatefulSet with `status.readyReplicas == 1`. Capture the final status PATCH body and assert `"status":"True","reason":"Available"`.

- [ ] **Step 4: Test `status_ready_false_when_sts_partial`**

`readyReplicas == 0`. Assert `"status":"False","reason":"NoBrokersReady"` in the status PATCH.

- [ ] **Step 5: Test `validation_rejects_replicas_two`**

Construct a Kafka with `spec.replicas = 2`. Drive reconcile. Assert the status PATCH has `reason:"UnsupportedReplicaCount"` and that **no** Service / ConfigMap / Secret / StatefulSet PATCH was captured.

- [ ] **Step 6: Test**

```bash
cargo test -p crabka-operator --test reconcile
```

Expected: 4 new tests pass; the original slice-17 `reconcile_patches_status_ready_true` is updated to match the new flow (rename: `reconcile_status_ready_true_when_sts_ready`) — it is the same test as Step 3.

---

## Task 9 — Broker binary smoke tests

**Files:**
- Create: `crates/broker/tests/cli_smoke.rs`

> **Parallel with Task 7** — disjoint files.

- [ ] **Step 1: Create the test file**

```rust
use std::process::Command;

fn broker_bin() -> std::path::PathBuf {
    let exe = std::env::var_os("CARGO_BIN_EXE_crabka-broker")
        .expect("cargo provides CARGO_BIN_EXE_<bin> in test env");
    std::path::PathBuf::from(exe)
}

#[test]
fn help_mentions_cluster_id_and_advertised_listener() {
    let out = Command::new(broker_bin()).arg("--help").output().unwrap();
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let help = String::from_utf8(out.stdout).unwrap();
    assert!(help.contains("--cluster-id"), "help missing --cluster-id:\n{help}");
    assert!(help.contains("--advertised-listener"), "help missing --advertised-listener:\n{help}");
}

#[test]
fn version_returns_zero() {
    let out = Command::new(broker_bin()).arg("--version").output().unwrap();
    assert!(out.status.success());
}
```

- [ ] **Step 2: Test**

```bash
cargo test -p crabka-broker --test cli_smoke
```

Expected: 2 tests pass.

---

## Task 10 — Regenerate CRD YAML

**Files:**
- Modify: `deploy/crds/crabka.io_kafkas.yaml`

> **Parallel with Task 11** — disjoint files.

- [ ] **Step 1: Run the regen tool**

```bash
cargo run -p crabka-operator -- gen-crds deploy/crds
```

(Reference `tools/regen-crds.sh` — it wraps the same invocation.)

- [ ] **Step 2: Diff and commit the regenerated CRD**

```bash
git diff deploy/crds/crabka.io_kafkas.yaml
```

Expected diff: new schema fields (`replicas` with min/max=1, `image`, `resources`), expanded `status` (replicas, readyReplicas mirrors). The codegen-drift CI job (`.github/workflows/codegen-check.yml`) gates this — no drift on CI.

---

## Task 11 — Update operator-e2e workflow

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

> **Parallel with Task 10** — disjoint files.

- [ ] **Step 1: Build broker image alongside operator**

After the existing "Build crabka-operator apk + OCI image" step, add a parallel "Build crabka-broker apk + OCI image" block that invokes the broker melange + apko configs. Resulting tarball: `crabka-broker.tar`.

- [ ] **Step 2: Load broker image into kind**

Mirror the operator "Load operator image into kind" step:

```yaml
      - name: Load broker image into kind (via docker)
        run: |
          docker load -i crabka-broker.tar 2>&1 | tee /tmp/load-broker.log
          loaded=$(sed -n 's/^Loaded image: //p' /tmp/load-broker.log | head -1)
          if [ "$loaded" != "crabka-broker:e2e" ]; then
            docker tag "$loaded" crabka-broker:e2e
          fi
          kind load docker-image crabka-broker:e2e --name crabka-e2e
```

- [ ] **Step 3: Install chart with broker image**

```yaml
      - name: Install chart
        run: |
          kubectl create namespace crabka-operator
          helm install operator charts/crabka-operator \
            --namespace crabka-operator \
            --set image.repository=crabka-operator \
            --set image.tag=e2e \
            --set image.pullPolicy=IfNotPresent \
            --set brokerImage.repository=crabka-broker \
            --set brokerImage.tag=e2e \
            --set brokerImage.pullPolicy=IfNotPresent
```

- [ ] **Step 4: Replace the placeholder Kafka CR**

The `Apply a placeholder Kafka CR` step's body is already `spec: kafkaVersion: "0.1.1"` — that still parses against the new schema (`replicas` defaults to 1, others optional). Keep it as-is.

- [ ] **Step 5: Lengthen the Ready wait + smoke probe**

Replace the existing "Wait for Ready=True" step with:

```yaml
      - name: Wait for Ready=True
        run: |
          for i in $(seq 1 60); do
            status=$(kubectl get kafka demo -n default -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
            ready=$(kubectl get kafka demo -n default -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
            echo "attempt $i: status=$status readyReplicas=$ready"
            if [ "$status" = "True" ]; then
              exit 0
            fi
            sleep 5
          done
          echo "::error::Kafka 'demo' did not become Ready in 5 minutes"
          kubectl describe kafka demo -n default
          kubectl describe sts demo-broker -n default
          kubectl logs -n default demo-broker-0 -c broker --tail=200 || true
          exit 1

      - name: Smoke — broker binary launched in pod
        run: |
          kubectl exec -n default demo-broker-0 -c broker -- /usr/bin/crabka-broker --version
          kubectl logs -n default demo-broker-0 -c broker | grep -q 'crabka-broker listening'
```

- [ ] **Step 6: Garbage-collection probe**

```yaml
      - name: Garbage-collection on Kafka delete
        run: |
          kubectl delete kafka demo -n default --wait=false
          for i in $(seq 1 30); do
            remaining=$(kubectl get sts,svc,cm,secret -n default -l app.kubernetes.io/instance=demo -o name 2>/dev/null | wc -l)
            echo "attempt $i: remaining=$remaining"
            if [ "$remaining" = "0" ]; then exit 0; fi
            sleep 2
          done
          echo "::error::owned objects not GC'd within 60s"
          kubectl get sts,svc,cm,secret -n default -l app.kubernetes.io/instance=demo
          exit 1
```

- [ ] **Step 7: Diagnostics block**

Extend the existing `Cluster diagnostics on failure` block's `for section in` loop with:

```
              "kafka CR|kubectl get kafka demo -n default -o yaml" \
              "broker sts|kubectl get sts demo-broker -n default -o yaml" \
              "broker pod logs|kubectl logs -n default demo-broker-0 -c broker --tail=500 || true" \
              "broker init logs|kubectl logs -n default demo-broker-0 -c format --tail=500 || true" \
              "owned objects|kubectl get sts,svc,cm,secret -n default -l app.kubernetes.io/instance=demo -o yaml" \
```

---

## Task 12 — Final verification

**Files:** none (verification only).

> **Sequential — runs last.**

- [ ] **Step 1: Workspace-wide checks**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: green.

- [ ] **Step 2: Helm sanity**

```bash
helm lint charts/crabka-operator
helm template t charts/crabka-operator | head -200
```

Expected: lint clean, rendered output includes the broker image arg and the new RBAC rules.

- [ ] **Step 3: Drift check (manual local)**

```bash
cargo run -p crabka-operator -- gen-crds deploy/crds
git diff deploy/crds
```

Expected: no diff (the regen step in Task 10 already produced the committed file).

- [ ] **Step 4: Commit + push + PR**

```bash
git add -A
git status
# Inspect; then:
git commit -m "$(cat <<'EOF'
Slice 19: Operator — Kafka CRD minimal (KRaft mixed-mode cluster)

Replaces slice 17's placeholder Kafka CRD with a real schema and a
reconciler that materializes a single-broker KRaft mixed-mode cluster:
headless Service, ConfigMap, cluster-ID Secret, and a StatefulSet
running the crabka-broker binary. Status conditions reflect StatefulSet
rollout state.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
git push -u origin <branch>
gh pr create --title "..." --body "..."
```

The body recaps the spec scope, what's deferred to which slice, and confirms acceptance criteria.

---

## Acceptance criteria recap (spec section 11)

1. `cargo test -p crabka-operator` and `cargo test -p crabka-broker --bin crabka-broker` pass.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-operator` and `helm template … | kubectl --dry-run=client apply -f -` pass.
4. `crabka-operator gen-crds` regenerates the CRD with no drift.
5. operator-e2e: apply `Kafka demo` with `replicas: 1`; pod `demo-broker-0` reaches Ready within 5 min; `crabka-broker --version` exec returns 0; log line `crabka-broker listening` appears.
6. `kubectl delete kafka demo` garbage-collects Service/ConfigMap/Secret/StatefulSet within 60 s.
