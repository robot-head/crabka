# Crabka Operator Slice 20 — `KafkaNodePool` CRD

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Per CLAUDE.md, dispatch batches in parallel within each batch. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Introduce a `KafkaNodePool` CRD as the resource that owns broker `StatefulSet`s. The `Kafka` CR becomes a parent/coordinator that owns cluster-level objects (Service, Secret, ConfigMap) and aggregates per-pool status. Slice 20 is a **refactor only** — single-replica mixed-mode pools, identical observable behavior to slice 19.

**Spec:** [`docs/superpowers/specs/2026-05-17-crabka-operator-kafkanodepool-20-design.md`](../specs/2026-05-17-crabka-operator-kafkanodepool-20-design.md).

**Conventions:**
- `[lints] workspace = true`, clippy `pedantic` warn-by-default.
- `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` are CI gates.
- Per CLAUDE.md: greenfield — no backwards-compat shims; delete fields cleanly when they move.

---

## Batch overview

| Batch | Tasks | Files | Parallel? |
|---|---|---|---|
| 1 | T1, T2, T3 | disjoint (knp CRD; kafka CRD strip; chart RBAC) | yes |
| 2 | T4, T5 | both in `controller/`; T4 in new file `kafka_node_pool.rs`, T5 in `kafka.rs` | yes |
| 3 | T6, T7, T8 | T6+T7 share `controller/mod.rs` and `run.rs`; T8 splits `tests/reconcile.rs` | sequential within batch |
| 4 | T9, T10, T11 | T9 regenerates CRDs, T10 modifies workflow, T11 is verify | T9 ‖ T10; T11 last |

---

## Task 1 — `KafkaNodePool` CRD

**Files:**
- Create: `crates/operator/src/crd/kafka_node_pool.rs`
- Modify: `crates/operator/src/crd/mod.rs`

- [ ] **Step 1: Create `crates/operator/src/crd/kafka_node_pool.rs`**

```rust
use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A pool of nodes (pods) that share role + image + resources.
/// One StatefulSet per pool; pods are addressed via the shared
/// headless Service owned by the parent `Kafka`.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "KafkaNodePool",
    plural = "kafkanodepools",
    singular = "kafkanodepool",
    shortname = "knp",
    namespaced,
    status = "KafkaNodePoolStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaNodePoolSpec {
    /// Roles each node in this pool fulfills. Slice 20 supports only
    /// the union `{Controller, Broker}`.
    pub roles: Vec<NodeRole>,

    /// Number of pods. Slice 20 validation: must equal 1.
    #[serde(default = "default_replicas")]
    #[schemars(range(min = 1, max = 1))]
    pub replicas: i32,

    /// First node id. Pod ordinal `i` -> `node_id = nodeIdStart + i`.
    #[schemars(range(min = 0, max = 999_999))]
    pub node_id_start: i32,

    /// Container image. Falls back to operator `--default-broker-image`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Broker container resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
}

const fn default_replicas() -> i32 { 1 }

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
pub enum NodeRole { Controller, Broker }

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaNodePoolStatus {
    #[serde(default)]
    pub conditions: Vec<crate::crd::kafka::KafkaCondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt as _;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaNodePool::crd();
        assert_eq!(crd.spec.group, "crabka.io");
        assert_eq!(crd.spec.names.kind, "KafkaNodePool");
        assert_eq!(crd.spec.names.plural, "kafkanodepools");
        assert_eq!(crd.spec.names.short_names.as_deref(), Some(["knp".to_string()].as_slice()));
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn round_trips_through_json() {
        let pool = KafkaNodePool::new("brokers", KafkaNodePoolSpec {
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            replicas: 1,
            node_id_start: 0,
            image: None,
            resources: None,
        });
        let json = serde_json::to_string(&pool).unwrap();
        assert!(json.contains("\"nodeIdStart\""), "expected camelCase wire shape, got: {json}");
        assert!(json.contains("\"Controller\""), "roles serialized in UpperCamelCase, got: {json}");
        let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
        assert_eq!(back.spec, pool.spec);
    }

    #[test]
    fn spec_defaults_replicas_to_one() {
        let json = r#"{"roles":["Controller","Broker"],"nodeIdStart":0}"#;
        let spec: KafkaNodePoolSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.replicas, 1);
        assert!(spec.image.is_none());
    }
}
```

Note: the test for `short_names` may need a tweak depending on what `kube-rs` returns (`Option<Vec<String>>` vs `Vec<String>`); adjust to match.

- [ ] **Step 2: Re-export from `crd/mod.rs`**

```rust
pub mod kafka;
pub mod kafka_node_pool;

pub use kafka::{Kafka, KafkaCondition, KafkaSpec, KafkaStatus};
pub use kafka_node_pool::{KafkaNodePool, KafkaNodePoolSpec, KafkaNodePoolStatus, NodeRole};
```

- [ ] **Step 3: Test**

```bash
cargo test -p crabka-operator --lib crd::kafka_node_pool
cargo clippy -p crabka-operator --lib -- -D warnings
```

Expected: 3 tests pass.

---

## Task 2 — Strip `Kafka.spec` of pool-owned fields

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`

- [ ] **Step 1: Remove `replicas`, `image`, `resources` from `KafkaSpec`**

```rust
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(...)]
#[serde(rename_all = "camelCase")]
pub struct KafkaSpec {
    /// Crabka version label, propagated to all pool pods via the
    /// `app.kubernetes.io/version` label.
    pub kafka_version: String,
}
```

Drop the `default_replicas` helper. Drop the `k8s_openapi::api::core::v1::ResourceRequirements` import.

- [ ] **Step 2: Update existing tests**

`round_trips_through_json` no longer constructs `replicas`/`image`/`resources`. The test for `spec_defaults_replicas_to_one` is now invalid — replace it with:

```rust
#[test]
fn spec_only_carries_kafka_version() {
    let json = r#"{"kafkaVersion":"0.1.1"}"#;
    let spec: KafkaSpec = serde_json::from_str(json).unwrap();
    assert_eq!(spec.kafka_version, "0.1.1");
}
```

The `crd_metadata_is_correct` test stays as-is.

- [ ] **Step 3: Test**

```bash
cargo test -p crabka-operator --lib crd::kafka::
```

Expected: 3 tests pass (the controller + reconciler tests will fail until later tasks rewrite them — that's fine for this task in isolation; the workspace won't compile until Task 7 lands).

---

## Task 3 — ClusterRole verbs for `kafkanodepools`

**Files:**
- Modify: `charts/crabka-operator/templates/clusterrole.yaml`

- [ ] **Step 1: Append a rule**

```yaml
  - apiGroups: ["crabka.io"]
    resources: ["kafkanodepools", "kafkanodepools/status"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

Place after the existing `kafkas` rule for readability.

- [ ] **Step 2: Test**

```bash
helm lint charts/crabka-operator
helm template t charts/crabka-operator | grep -i kafkanodepools
```

Expected: lint clean; rendered output contains the new rule.

---

## Task 4 — Pool reconciler renderers + validation

**Files:**
- Create: `crates/operator/src/controller/kafka_node_pool.rs`

> **Sequencing:** must wait for Task 1 (the `KafkaNodePool` type) and Task 2 (the stripped `Kafka` type). Cannot start until both land. Per the batch table, T4 is in Batch 2.

The implementation mirrors the slice-19 `controller/kafka.rs` renderers but parameterized on the pool's spec instead of the Kafka's. Helpers introduced or reused:

- `pub(crate) fn render_statefulset(parent: &Kafka, pool: &KafkaNodePool, broker_image: &str) -> Result<StatefulSet, ReconcileError>` — name `<kafka>-<pool>`, owner-ref'd to the pool, `serviceName = <kafka>-broker-headless`, `replicas = pool.spec.replicas`, init script reads `NODE_ID_START` env + `$HOSTNAME` ordinal. Init creates `.node-id` in the data dir; main reads it. Labels include `app.kubernetes.io/instance = <kafka>` (matches the parent's Service selector) AND `crabka.io/pool = <pool>`.
- `pub(crate) fn validate(pool: &KafkaNodePool) -> Result<(), PoolValidationError>` — checks `roles`, `replicas`, `node_id_start`.
- `pub(crate) async fn reconcile(...)` — async runtime. Same shape as slice-19 reconcile fn, but operates on `KafkaNodePool` and reads its parent Kafka via `metadata.labels["crabka.io/cluster"]`.

- [ ] **Step 1: Drop common helpers into a shared module**

The slice-19 `controller/kafka.rs` defines `BROKER_PORT`, `APP_LABEL`, `DEFAULT_BROKER_IMAGE`, `common_labels`, `owner_ref`, `default_resources`, `condition`, `apply_object`, `patch_status`. Move these to a new `crates/operator/src/controller/common.rs` so both Kafka and KafkaNodePool reconcilers can share them. Re-export through `controller/mod.rs`.

Signatures of helpers that need to change:
- `pub(crate) fn owner_ref<T: kube::Resource<DynamicType = ()>>(obj: &T) -> Result<OwnerReference, ReconcileError>` — generic so both `Kafka` and `KafkaNodePool` can pass themselves as the owner.
- `pub(crate) fn common_labels(owner_kafka: &str, kafka_version: &str, pool: Option<&str>) -> BTreeMap<String, String>` — pool name is optional (Service/CM/Secret don't set it; StatefulSet pods do).

- [ ] **Step 2: Define `PoolValidationError`**

```rust
#[derive(Debug, thiserror::Error)]
pub enum PoolValidationError {
    #[error("spec.roles must equal {{Controller, Broker}}; got {0:?}")]
    RolesNotMixed(Vec<NodeRole>),
    #[error("spec.replicas={0} is unsupported in slice 20 (only 1 allowed)")]
    ReplicasNotOne(i32),
    #[error("spec.nodeIdStart={0} is out of range 0..=999999")]
    NodeIdOutOfRange(i32),
    #[error("metadata.labels.\"crabka.io/cluster\" missing")]
    MissingClusterLabel,
}

pub(crate) fn validate(pool: &KafkaNodePool) -> Result<(), PoolValidationError> {
    let roles: std::collections::HashSet<NodeRole> = pool.spec.roles.iter().copied().collect();
    let expected: std::collections::HashSet<NodeRole> =
        [NodeRole::Controller, NodeRole::Broker].into_iter().collect();
    if roles != expected {
        return Err(PoolValidationError::RolesNotMixed(pool.spec.roles.clone()));
    }
    if pool.spec.replicas != 1 {
        return Err(PoolValidationError::ReplicasNotOne(pool.spec.replicas));
    }
    if !(0..=999_999).contains(&pool.spec.node_id_start) {
        return Err(PoolValidationError::NodeIdOutOfRange(pool.spec.node_id_start));
    }
    Ok(())
}
```

Map each variant to a distinct condition reason in the reconciler:
- `RolesNotMixed` → `reason = "RolesNotMixed"`.
- `ReplicasNotOne` → `reason = "UnsupportedReplicaCount"`.
- `NodeIdOutOfRange` → `reason = "NodeIdOutOfRange"`.
- `MissingClusterLabel` → `reason = "MissingClusterLabel"`.

- [ ] **Step 3: Render helper**

Implement `render_statefulset` to produce the manifest shape from spec section 4. Notable details:
- The init container's `args` is a single shell script; it must derive `NODE_ID` from `$HOSTNAME` and write `.node-id` and `.formatted` markers.
- The main container's `command` is now `[/bin/sh, -c]` with an `args` script that reads `.node-id` and execs the broker. This drops the slice-19 direct-exec approach (no shell needed there); slice 20 needs shell because of the dynamic broker-id.
- Env var `NODE_ID_START` is literal (rendered from `pool.spec.node_id_start.to_string()`).
- `serviceName = format!("{}-broker-headless", parent_name)`.
- Owner-ref is the pool, not the parent Kafka.

- [ ] **Step 4: Reconcile fn**

```rust
pub async fn reconcile(pool: Arc<KafkaNodePool>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = pool.namespace().unwrap_or_else(|| "default".into());
    let name = pool.name_any();

    if let Err(e) = validate(&pool) {
        patch_status_for_pool(&ctx, &ns, &name, condition_for_validation_error(&e)).await?;
        return Ok(Action::await_change());
    }

    let kafka_name = pool.metadata.labels.as_ref()
        .and_then(|l| l.get("crabka.io/cluster").cloned())
        .ok_or(ReconcileError::PoolMissingClusterLabel)?;

    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let parent = match kafka_api.get_opt(&kafka_name).await? {
        Some(k) => k,
        None => {
            patch_status_for_pool(&ctx, &ns, &name, condition("Ready", "False", "ParentNotFound",
                &format!("Kafka '{kafka_name}' not found in namespace '{ns}'"))).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    let image = pool.spec.image.clone()
        .or_else(|| ctx.config.default_broker_image.clone())
        .unwrap_or_else(|| DEFAULT_BROKER_IMAGE.into());

    let sts = render_statefulset(&parent, &pool, &image)?;
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let sts_name = format!("{}-{}", kafka_name, name);
    apply_object(&sts_api, &sts_name, &sts).await?;

    let live = sts_api.get_opt(&sts_name).await?;
    let (replicas, ready_replicas, ready, reason, message) = derive_status(live.as_ref(), pool.spec.replicas);
    patch_status_for_pool(&ctx, &ns, &name, condition("Ready", if ready { "True" } else { "False" }, reason, &message)).await?;

    Ok(Action::requeue(Duration::from_secs(30)))
}
```

- [ ] **Step 5: Tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn parent_fixture(name: &str) -> Kafka { /* uid set */ }
    fn pool_fixture(name: &str, parent: &str, replicas: i32) -> KafkaNodePool { /* labels + uid set */ }

    #[test]
    fn render_statefulset_name_is_kafka_dash_pool() { ... }

    #[test]
    fn render_statefulset_service_name_is_shared_headless() { ... }

    #[test]
    fn render_statefulset_pod_labels_include_kafka_instance_and_pool_name() { ... }

    #[test]
    fn render_statefulset_init_script_uses_nodeidstart() {
        // Pool nodeIdStart=42; assert the rendered init args string
        // contains `NODE_ID_START=42` (via env) and the
        // `$((NODE_ID_START + ORDINAL))` shell expression.
    }

    #[test]
    fn validate_rejects_replicas_two() { ... }

    #[test]
    fn validate_rejects_controller_only_roles() { ... }

    #[test]
    fn validate_rejects_broker_only_roles() { ... }

    #[test]
    fn validate_rejects_negative_nodeidstart() { ... }
}
```

- [ ] **Step 6: Verify**

```bash
cargo build -p crabka-operator
cargo test -p crabka-operator --lib controller::kafka_node_pool::tests
cargo clippy -p crabka-operator --lib -- -D warnings
```

Expected: ~8 tests pass, clippy clean.

---

## Task 5 — Status aggregation helpers in `controller/kafka.rs`

**Files:**
- Modify: `crates/operator/src/controller/kafka.rs`

> **Parallel with Task 4** — disjoint files.

The slice-19 reconciler's StatefulSet rendering + status-from-sts logic moves out (to T4 / common module). What replaces it is a status aggregator that lists pools by label and rolls up.

- [ ] **Step 1: Move shared helpers to `controller/common.rs`**

Per Task 4 Step 1, helpers move into a new module. After T4 has created it, this task imports them.

- [ ] **Step 2: Drop StatefulSet rendering**

Remove `render_statefulset` and `default_resources` from `controller/kafka.rs` (T4 owns them now). Remove the StatefulSet-related code from `reconcile` — Step 4 below replaces it.

- [ ] **Step 3: New `aggregate_pool_status` pure helper**

```rust
pub(crate) struct ClusterRollup {
    pub replicas: i32,
    pub ready_replicas: i32,
    pub pool_count: usize,
}

pub(crate) fn aggregate_pool_status<'a>(
    pools: impl IntoIterator<Item = &'a KafkaNodePool>,
) -> ClusterRollup {
    let mut r = ClusterRollup { replicas: 0, ready_replicas: 0, pool_count: 0 };
    for pool in pools {
        r.pool_count += 1;
        let s = pool.status.as_ref();
        r.replicas      += s.and_then(|s| s.replicas).unwrap_or(0);
        r.ready_replicas += s.and_then(|s| s.ready_replicas).unwrap_or(0);
    }
    r
}

pub(crate) fn rollup_condition(rollup: &ClusterRollup) -> (bool, &'static str, String) {
    if rollup.pool_count == 0 {
        (false, "NoNodePools", "no KafkaNodePool with label crabka.io/cluster=<name>".into())
    } else if rollup.ready_replicas == rollup.replicas && rollup.replicas > 0 {
        (true, "Available", format!("{}/{} brokers ready across {} pool(s)",
            rollup.ready_replicas, rollup.replicas, rollup.pool_count))
    } else {
        (false, "PartiallyReady", format!("{}/{} brokers ready", rollup.ready_replicas, rollup.replicas))
    }
}
```

- [ ] **Step 4: Reconcile fn**

```rust
pub async fn reconcile(obj: Arc<Kafka>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();

    // 1. Service / ConfigMap (SSA) and Secret (if-not-exists).
    let svc = render_service(&obj)?;
    apply_object::<Service>(&ctx, &ns, &svc).await?;
    let cm = render_configmap(&obj)?;
    apply_object::<ConfigMap>(&ctx, &ns, &cm).await?;
    let _cluster_id = ensure_cluster_id_secret(&ctx, &ns, &name, &obj).await?;

    // 2. List pools by label.
    let pool_api: Api<KafkaNodePool> = Api::namespaced(ctx.client.clone(), &ns);
    let lp = kube::api::ListParams::default()
        .labels(&format!("crabka.io/cluster={name}"));
    let pools = pool_api.list(&lp).await?;

    // 3. Aggregate + patch status.
    let rollup = aggregate_pool_status(pools.iter());
    let (ready, reason, message) = rollup_condition(&rollup);
    let status = KafkaStatus {
        conditions: vec![condition("Ready", if ready { "True" } else { "False" }, reason, &message)],
        replicas: Some(rollup.replicas),
        ready_replicas: Some(rollup.ready_replicas),
    };
    patch_status(&ctx, &ns, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(30)))
}
```

- [ ] **Step 5: Update `run` to watch pools too**

```rust
let pools: Api<KafkaNodePool> = Api::all(ctx.client.clone());
Controller::new(api, watcher::Config::default())
    .owns::<Service>(svc_api.clone(), watcher::Config::default())
    // ConfigMap and Secret owned objects implicitly trigger reconciles via .owns ... (optional)
    .watches(pools, watcher::Config::default(), |pool| {
        // Map a pool change to a reconcile of its parent Kafka.
        pool.metadata.labels.as_ref()
            .and_then(|l| l.get("crabka.io/cluster"))
            .map(|kafka_name| ObjectRef::<Kafka>::new(kafka_name).within(pool.namespace().as_deref().unwrap_or("default")))
            .into_iter()
    })
    .run(reconcile, error_policy, Arc::new(ctx))
    ...
```

This causes `Kafka` reconciles to fire when any of its pools' statuses change.

- [ ] **Step 6: Tests**

```rust
#[test]
fn aggregate_status_no_pools_is_no_node_pools() {
    let r = aggregate_pool_status(std::iter::empty::<&KafkaNodePool>());
    let (ready, reason, _) = rollup_condition(&r);
    assert!(!ready);
    assert_eq!(reason, "NoNodePools");
}

#[test]
fn aggregate_status_partial_pool_is_partially_ready() { ... }

#[test]
fn aggregate_status_all_ready_pools_is_available() { ... }
```

- [ ] **Step 7: Verify**

```bash
cargo test -p crabka-operator --lib controller::kafka::
cargo clippy -p crabka-operator --lib -- -D warnings
```

Expected: existing render-tests stripped, new aggregate tests pass, clippy clean.

---

## Task 6 — Wire pool reconciler into `run.rs`

**Files:**
- Modify: `crates/operator/src/controller/mod.rs`
- Modify: `crates/operator/src/run.rs`

> **Sequencing:** must follow T4 + T5 (those land the new module names).

- [ ] **Step 1: Export the module**

```rust
// controller/mod.rs
pub mod common;
pub mod kafka;
pub mod kafka_node_pool;
```

- [ ] **Step 2: Spawn the second controller**

```rust
let kafka_handle = tokio::spawn({
    let ctx = ctx.clone();
    async move { controller::kafka::run(ctx).await }
});
let pool_handle = tokio::spawn({
    let ctx = ctx.clone();
    async move { controller::kafka_node_pool::run(ctx).await }
});

tokio::select! {
    res = health_handle => { ... },
    res = kafka_handle => { ... },
    res = pool_handle => { ... },
    () = shutdown_signal() => tracing::info!("shutdown signal received"),
}
```

- [ ] **Step 3: Verify**

```bash
cargo build -p crabka-operator
cargo run -p crabka-operator -- run --help
```

Expected: binary builds, `--help` still works.

---

## Task 7 — `Kafka` reconciler mocked-client tests (split)

**Files:**
- Delete: `crates/operator/tests/reconcile.rs`
- Create: `crates/operator/tests/reconcile_kafka.rs`
- Create: `crates/operator/tests/reconcile_pool.rs`
- Create: `crates/operator/tests/shared/mod.rs`

> **Sequencing:** must follow T5 (reconcile fn lands), T4 (pool reconcile fn lands).

- [ ] **Step 1: Extract `MockState` to `tests/shared/mod.rs`**

The slice-19 mock harness (`MockRule`, `MockState`, `happy_path_rules`, `mock_client`) is copied verbatim into a shared module. Each integration test includes it via:

```rust
#[path = "shared/mod.rs"]
mod shared;
```

This `#[path = ...]` trick is the standard way to share modules between separate `tests/` files.

- [ ] **Step 2: Rewrite the Kafka reconcile tests in `reconcile_kafka.rs`**

The slice-19 sequence drops the StatefulSet-related requests. New sequence:

1. PATCH services/<name>-broker-headless (SSA)
2. PATCH configmaps/<name>-broker-config (SSA)
3. GET secrets/<name>-cluster-id → 404
4. POST secrets → 201
5. GET kafkanodepools?labelSelector=crabka.io/cluster=<name> → 200 (return preloaded pool list)
6. PATCH kafkas/<name>/status (merge)

Tests:
- `kafka_applies_service_configmap_secret_no_statefulset`.
- `kafka_status_no_node_pools_when_list_empty` (pool list returns 0 items; reason=NoNodePools).
- `kafka_status_aggregates_pool_readyreplicas` (pool list returns one pool with readyReplicas=1).

- [ ] **Step 3: Pool reconcile tests in `reconcile_pool.rs`**

Sequence on the happy path:

1. GET kafkas/<parent-name> → 200 (return preloaded parent)
2. PATCH statefulsets/<parent>-<pool> (SSA)
3. GET statefulsets/<parent>-<pool>
4. PATCH kafkanodepools/<pool>/status (merge)

Tests:
- `pool_applies_statefulset_with_pool_name`.
- `pool_status_ready_when_sts_ready`.
- `pool_validation_rejects_replicas_two` (no StatefulSet PATCH).
- `pool_validation_rejects_missing_cluster_label`.
- `pool_status_parent_not_found` (GET kafkas/... → 404).

- [ ] **Step 4: Verify**

```bash
cargo test -p crabka-operator --tests
cargo clippy -p crabka-operator --tests -- -D warnings
```

Expected: ~8 integration tests pass.

---

## Task 8 — `Kafka.spec.image` migration in operator-e2e workflow

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

> **Parallel with Task 9 (CRD regen)** — disjoint files.

- [ ] **Step 1: Replace the placeholder Kafka CR step**

```yaml
      - name: Apply Kafka + KafkaNodePool
        run: |
          cat <<EOF | kubectl apply -f -
          apiVersion: crabka.io/v1alpha1
          kind: Kafka
          metadata:
            name: demo
            namespace: default
          spec:
            kafkaVersion: "0.1.1"
          ---
          apiVersion: crabka.io/v1alpha1
          kind: KafkaNodePool
          metadata:
            name: brokers
            namespace: default
            labels:
              crabka.io/cluster: demo
          spec:
            roles: [Controller, Broker]
            replicas: 1
            nodeIdStart: 0
          EOF
```

- [ ] **Step 2: Add the KafkaNodePool CRD install step**

The existing `Install CRDs` step becomes:

```yaml
      - name: Install CRDs
        run: |
          kubectl apply -f deploy/crds/crabka.io_kafkas.yaml
          kubectl apply -f deploy/crds/crabka.io_kafkanodepools.yaml
```

- [ ] **Step 3: Update pod name + Ready wait**

The expected pod name is now `demo-brokers-0` (was `demo-broker-0`). Update:
- The "Smoke" step's `kubectl exec` and `kubectl logs` commands.
- The "Wait for Ready=True" failure block's `kubectl describe sts demo-brokers` and `kubectl logs demo-brokers-0`.
- The "Garbage-collection on Kafka delete" step's poll target.

- [ ] **Step 4: Diagnostics block**

Add `kafkanodepool brokers` to the failure-diagnostics `for section in` loop:

```
              "knp|kubectl get knp -A -o yaml" \
              "broker sts|kubectl get sts demo-brokers -n default -o yaml" \
              "broker pod logs|kubectl logs -n default demo-brokers-0 -c broker --tail=500 || true" \
```

(The slice-19 entries that reference `demo-broker-0` should be renamed to `demo-brokers-0` and `demo-broker` to `demo-brokers`.)

---

## Task 9 — Regenerate CRDs

**Files:**
- Modify: `deploy/crds/crabka.io_kafkas.yaml` (loses 3 spec fields)
- Create: `deploy/crds/crabka.io_kafkanodepools.yaml`

> **Sequencing:** after T1+T2 land.

- [ ] **Step 1: Run gen-crds**

```bash
cargo run -p crabka-operator -- gen-crds deploy/crds
```

The tool writes both files. The slice-19 file shrinks; the new file is created.

- [ ] **Step 2: Diff for sanity**

```bash
git diff deploy/crds/
```

Expected:
- `crabka.io_kafkas.yaml` loses `replicas` (with min/max), `image`, `resources` from spec; description line updates.
- `crabka.io_kafkanodepools.yaml` is created with the slice-20 schema.

---

## Task 10 — gen-crds writes both CRDs

**Files:**
- Modify: `crates/operator/src/gen_crds.rs`

> **Sequencing:** after T1.

- [ ] **Step 1: Extend `write_all`**

```rust
pub fn write_all(out_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir)?;
    write_one::<Kafka>(out_dir)?;
    write_one::<KafkaNodePool>(out_dir)?;
    Ok(())
}
```

Update the test to assert both files exist:

```rust
#[test]
fn writes_kafka_and_pool_crd_files() {
    let dir = tempdir().unwrap();
    write_all(dir.path()).unwrap();
    assert!(dir.path().join("crabka.io_kafkas.yaml").exists());
    assert!(dir.path().join("crabka.io_kafkanodepools.yaml").exists());
}
```

---

## Task 11 — Final verification

**Files:** none (verify only).

> **Sequential — runs last.**

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `helm lint charts/crabka-operator`
- [ ] `cargo run -p crabka-operator -- gen-crds deploy/crds` → stable (no further diff)
- [ ] Commit, push, PR.

Commit message template:

```
Slice 20: Operator — KafkaNodePool CRD (single-pool refactor)

Introduces KafkaNodePool as the resource boundary for broker
StatefulSets, with Kafka demoted to a parent owning Service / Secret /
ConfigMap and aggregating per-pool status. Slice 20 is a refactor —
single-replica mixed-mode pools only; the architecture unlocks
multi-replica (20a), role separation (20b), and pod templates (20c).
```
