# Slice 20: Operator — `KafkaNodePool` CRD — Design

**Status:** Approved 2026-05-17.

**Goal:** Introduce a `KafkaNodePool` CRD as the resource that owns broker `StatefulSet`s, with the `Kafka` CR demoted to a parent/coordinator that owns cluster-level objects (`Service`, `Secret`, `ConfigMap`) and aggregates per-pool status. This is the architectural lift that unlocks multi-replica clusters (slice 20a), controller-only / broker-only role separation (slice 20b), pod templates (slice 20c — formerly slice 20 in the roadmap), and persistent storage (slice 24). Slice 20 itself is intentionally a **refactor only** — single-replica mixed-mode pools, identical observable behavior to slice 19.

---

## 1. Scope

### In

- New `KafkaNodePool` CRD (namespaced, `crabka.io/v1alpha1`) with:
  - `spec.roles: Vec<NodeRole>` — `Controller`, `Broker`, or both. Validation in slice 20: must be the set `{Controller, Broker}` (role separation arrives in slice 20b).
  - `spec.replicas: i32` — default `1`. Slice 20 validation: must equal `1` (multi-replica arrives in slice 20a once raft bootstrap-then-join is operator-orchestrated).
  - `spec.nodeIdStart: i32` — required, `0..=999999`. Pod ordinal `i` maps to `node_id = nodeIdStart + i`. Cross-pool collision detection is a slice-20a follow-up.
  - `spec.image: Option<String>` — defaults to the operator's `--default-broker-image`.
  - `spec.resources: Option<ResourceRequirements>` — broker container resources.
- New `KafkaNodePool` reconciler that owns one `StatefulSet` per pool, named `<kafka-name>-<pool-name>`. Status conditions reflect rollout state, mirroring the slice-19 pattern.
- `Kafka` reconciler is rewritten to:
  - No longer render the `StatefulSet` itself (pool reconciler owns it).
  - List `KafkaNodePool`s in the same namespace with label `crabka.io/cluster=<kafka-name>`, aggregate their `readyReplicas` into `Kafka.status`.
  - Continue to own the headless `Service`, the cluster-ID `Secret`, and the `ConfigMap`.
  - Reject `Kafka` CRs with zero matching pools (`Ready=False, reason=NoNodePools`).
- `Kafka.spec.replicas` field is **removed** (greenfield — no backwards-compat shim per CLAUDE.md). Users who upgrade from slice 19 must:
  - Replace `spec.replicas: 1` on their `Kafka` with a sibling `KafkaNodePool` named `<anything>` referencing the parent via `metadata.labels.crabka.io/cluster: <kafka-name>`.
  - `spec.image` and `spec.resources` move from `Kafka.spec` to `KafkaNodePool.spec`.
- One headless `Service` (`<kafka-name>-broker-headless`) per Kafka cluster. Selector remains label-based (`app.kubernetes.io/instance=<kafka-name>`) so any pool's pods are reachable.
- Pool's `StatefulSet.spec.serviceName = <kafka-name>-broker-headless` (cross-pool stable DNS).
- `kubectl` short name `knp` for `KafkaNodePool` to make `kubectl get knp` ergonomic.
- E2E: deploy `Kafka demo` + `KafkaNodePool brokers` (replicas=1, mixed roles, nodeIdStart=0). Pod becomes Ready exactly as in slice 19; `kubectl delete kafka demo` garbage-collects the pool, which cascades to its StatefulSet.

### Out (deferred)

| Concern | Slice |
|---|---|
| Multi-replica per pool (raft bootstrap-then-join wired through operator) | 20a |
| Controller-only / broker-only pools (broker role separation) | 20b |
| Pod templates (affinity, tolerations, labels, annotations) | 20c |
| Rolling restart on config drift | 21 |
| `NetworkPolicy` generation | 23 |
| Persistent storage (PVCs) | 24 |
| External listeners | 25–27 |
| Version upgrades | 28 |
| TLS/SASL listener config | 30–31 |
| `KafkaTopic` / `KafkaUser` CRDs | 35–36 |

### Constraints inherited from slice 19

- The `crabka-broker` binary still runs single-broker mixed-mode KRaft (`BootstrapMode::Bootstrap`, self-voter). The operator passes `--broker-id=$(NODE_ID)` (derived in the init script from `nodeIdStart`); broker still binds 9092 + 9093 and self-references in the quorum voter list.
- Cluster-ID Secret remains owned by the parent Kafka, not the pool — every pool in a cluster shares the same cluster id.
- Single-pool deployment is the only validated configuration; multi-pool support requires the broker CLI extensions and is deferred.

---

## 2. CRD shape

### `KafkaNodePool`

```rust
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
    /// the union `{Controller, Broker}`; role separation lands in slice 20b.
    pub roles: Vec<NodeRole>,

    /// Number of pods in this pool. Slice 20 supports `1` only;
    /// multi-replica arrives in slice 20a once raft bootstrap-then-join
    /// is operator-orchestrated.
    #[serde(default = "default_replicas")]
    #[schemars(range(min = 1, max = 1))]
    pub replicas: i32,

    /// First node id in this pool. Pod ordinal `i` maps to
    /// `node_id = nodeIdStart + i`. Operator does not yet enforce
    /// cross-pool uniqueness (slice 20a).
    #[schemars(range(min = 0, max = 999_999))]
    pub node_id_start: i32,

    /// Container image. Falls back to the operator's
    /// `--default-broker-image` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Broker container resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
}

const fn default_replicas() -> i32 { 1 }

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
pub enum NodeRole { Controller, Broker }
```

`KafkaNodePoolStatus` mirrors `KafkaStatus` from slice 19 (`conditions`, `replicas`, `ready_replicas`).

### `Kafka` (slice-19 diff)

- **Removed:** `spec.replicas`, `spec.image`, `spec.resources` (now on `KafkaNodePool`).
- **Retained:** `spec.kafkaVersion` (informational label, propagated to all pool pods as `app.kubernetes.io/version`).
- **Status:** retains `conditions`, `replicas`, `readyReplicas`. Pool aggregation rules:
  - `replicas = Σ pool.status.replicas`
  - `readyReplicas = Σ pool.status.readyReplicas`
  - `Ready=True, reason=Available` iff every pool's `readyReplicas == replicas` AND there is at least one pool.
  - `Ready=False, reason=NoNodePools` iff zero pools match the cluster label.
  - `Ready=False, reason=PartiallyReady` otherwise.

### Parent linkage

A `KafkaNodePool` belongs to a `Kafka` via:
- `metadata.labels["crabka.io/cluster"]` = parent Kafka name (required; absence → pool is ignored).
- Owner reference: `controller: true` from pool back to parent Kafka. `kubectl delete kafka demo` cascades to pools and (transitively) StatefulSets.

The pool reconciler's contract for orphan pools (label set but no matching Kafka in the namespace): patch `Ready=False, reason=ParentNotFound` and short-circuit. The operator does NOT delete orphans (user may be mid-typo).

---

## 3. Reconciler architecture

### Two reconcilers, one shared `Context`

- `controller::kafka::run` — watches `Kafka` and (via `.owns::<KafkaNodePool>(...)`) any pool whose Kafka changes.
- `controller::kafka_node_pool::run` — watches `KafkaNodePool` and (via `.owns::<StatefulSet>(...)`) the StatefulSet it spawns.

Both are spawned by `run::run` in parallel. They share a single `kube::Client` and the `Context` struct.

### `Kafka` reconcile flow

1. Server-side apply `Service` (`<name>-broker-headless`).
2. Server-side apply `ConfigMap` (`<name>-broker-config`).
3. If-not-exists create `Secret` (`<name>-cluster-id`). Same flow as slice 19.
4. List `KafkaNodePool`s in the same namespace by label selector `crabka.io/cluster=<name>`.
5. For each, ensure its `ownerReference` chain includes the parent Kafka (set via SSA on the pool — pool's controller owns its own status, parent owns its metadata).
6. Aggregate `pool.status.replicas` and `pool.status.readyReplicas` from all matching pools.
7. Compute `Ready` condition (rules above).
8. Patch `Kafka` status.
9. `Action::requeue(Duration::from_secs(30))`.

### `KafkaNodePool` reconcile flow

1. Validate `roles == {Controller, Broker}` and `replicas == 1` and `nodeIdStart in 0..=999_999`.
2. Read the parent Kafka name from `metadata.labels["crabka.io/cluster"]`. If absent → `Ready=False, reason=MissingClusterLabel`.
3. Look up the parent Kafka. If absent → `Ready=False, reason=ParentNotFound`.
4. SSA-apply the `StatefulSet` (named `<kafka>-<pool>`, owner-ref'd to the pool, NOT the parent Kafka — chained ownership).
5. Read the live StatefulSet, project `status.replicas` / `status.readyReplicas` into the pool's status conditions:
   - `readyReplicas == spec.replicas` → `Ready=True, reason=Available`.
   - `readyReplicas == 0` → `Ready=False, reason=NoBrokersReady`.
   - Otherwise → `Ready=False, reason=PartiallyReady`.
6. `Action::requeue(Duration::from_secs(30))`.

### Validation as a typed error

`PoolValidationError` enum in `controller::kafka_node_pool` with variants `RolesNotMixed`, `ReplicasNotOne(i32)`, `NodeIdOutOfRange(i32)`. The reconciler maps each to a distinct condition reason; tests assert exact reasons.

---

## 4. Rendered `StatefulSet` (per pool)

Largely identical to slice 19's, with three differences:

- **Name:** `<kafka>-<pool>` (e.g., `demo-brokers`).
- **Owner-ref:** the `KafkaNodePool`, not the Kafka.
- **Init container's `--broker-id`:** derived from `nodeIdStart + ordinal`, computed in the init script.
- **`serviceName`:** `<kafka>-broker-headless` (the shared headless service owned by the parent Kafka).
- **Pod labels:**
  - `app.kubernetes.io/name = crabka-broker`
  - `app.kubernetes.io/instance = <kafka>` (matches the shared headless `Service` selector)
  - `app.kubernetes.io/version = <Kafka.spec.kafkaVersion>`
  - `crabka.io/pool = <pool>` (selector for future pool-specific queries)

Init container shell:

```sh
set -eu
ORDINAL="${HOSTNAME##*-}"
NODE_ID=$((NODE_ID_START + ORDINAL))
if [ ! -f /var/lib/crabka/data/.formatted ]; then
  /usr/bin/crabka format --log-dir /var/lib/crabka/data --cluster-id "$CRABKA_CLUSTER_ID"
  touch /var/lib/crabka/data/.formatted
fi
echo "$NODE_ID" > /var/lib/crabka/data/.node-id
```

The main container reads `.node-id` and substitutes it into the broker args via a small entrypoint shell:

```sh
exec /usr/bin/crabka-broker \
  --listen-addr=0.0.0.0:9092 \
  --log-dir=/var/lib/crabka/data \
  --broker-id="$(cat /var/lib/crabka/data/.node-id)"
```

This requires `busybox` in the broker image (already present from slice 19 hotfix).

Env vars on both init and main:
- `NODE_ID_START` (literal int from pool spec; baked into the StatefulSet template at render time).
- `CRABKA_CLUSTER_ID` (from the shared cluster-id Secret).
- `CRABKA_ADVERTISED_LISTENER` = `$(POD_NAME).<kafka>-broker-headless.$(POD_NAMESPACE).svc.cluster.local:9092`.

---

## 5. CRD YAML, RBAC, Helm chart

### CRD YAML

`deploy/crds/crabka.io_kafkanodepools.yaml` is regenerated from the Rust types via `crabka-operator gen-crds`. The slice-19 `deploy/crds/crabka.io_kafkas.yaml` also regenerates (the spec lost three fields).

### `ClusterRole`

`charts/crabka-operator/templates/clusterrole.yaml` gains:

```yaml
  - apiGroups: ["crabka.io"]
    resources: ["kafkanodepools", "kafkanodepools/status"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

The existing `kafkas` + Service/ConfigMap/Secret/StatefulSet rules from slice 19 stay.

### Helm values

No new values — pool image / resources are set by the user on each `KafkaNodePool` CR, not the chart.

---

## 6. Testing

### Unit tests

`crates/operator/src/crd/kafka_node_pool.rs`:
- `crd_metadata_is_correct` (group/kind/plural/shortname/version).
- `round_trips_through_json` (`Vec<NodeRole>` serializes as `["Controller","Broker"]`).
- `spec_defaults_replicas_to_one`.

`crates/operator/src/controller/kafka_node_pool.rs`:
- `render_statefulset_name_is_kafka_dash_pool`.
- `render_statefulset_service_name_is_shared_headless`.
- `render_statefulset_pod_labels_include_kafka_instance_and_pool_name`.
- `render_statefulset_init_script_uses_nodeidstart`.
- `validate_rejects_replicas_two`.
- `validate_rejects_controller_only_roles`.
- `validate_rejects_negative_nodeidstart`.

`crates/operator/src/controller/kafka.rs`:
- `aggregate_status_no_pools_is_no_node_pools`.
- `aggregate_status_partial_pool_is_partially_ready`.
- `aggregate_status_all_ready_pools_is_available`.

### Mocked-client reconcile tests

`crates/operator/tests/reconcile_kafka.rs` (new file, split for clarity):
- `kafka_applies_service_configmap_secret_only` — assert NO StatefulSet PATCH (pools handle that).
- `kafka_status_no_node_pools_when_list_empty`.
- `kafka_status_aggregates_pool_readyreplicas`.

`crates/operator/tests/reconcile_pool.rs` (new):
- `pool_applies_statefulset_with_pool_name`.
- `pool_status_ready_when_sts_ready`.
- `pool_validation_rejects_replicas_two`.
- `pool_validation_rejects_missing_cluster_label`.

The slice-19 `tests/reconcile.rs` is split into these two new files. Mock-harness `MockState` lives in a small shared module.

### Broker binary

No CLI changes in slice 20. Existing `tests/cli_smoke.rs` still applies unmodified.

### E2E (kind)

`.github/workflows/operator-e2e.yml` is updated to apply both a `Kafka` and a `KafkaNodePool`:

```yaml
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata: { name: demo, namespace: default }
spec: { kafkaVersion: "0.1.1" }
---
apiVersion: crabka.io/v1alpha1
kind: KafkaNodePool
metadata:
  name: brokers
  namespace: default
  labels: { crabka.io/cluster: demo }
spec:
  roles: [Controller, Broker]
  replicas: 1
  nodeIdStart: 0
```

Assertions:
- `Kafka demo` reaches `Ready=True`.
- `KafkaNodePool brokers` reaches `Ready=True` independently.
- Pod `demo-brokers-0` runs (broker StatefulSet renamed from slice-19's `demo-broker-0`).
- `crabka-broker --version` exec returns 0 and `crabka-broker listening` appears in logs (same smoke as slice 19).
- `kubectl delete kafka demo` cascades to the pool, which cascades to the StatefulSet. After 60 s, no `kafkanodepool`, no `statefulset`, no `service`, no `configmap`, no `secret` with the cluster label remain.

The CRD-install step gains `deploy/crds/crabka.io_kafkanodepools.yaml`.

---

## 7. File structure

```
crates/operator/src/
├── crd/kafka.rs                       # MODIFIED — drop replicas/image/resources
├── crd/kafka_node_pool.rs             # NEW
├── crd/mod.rs                         # MODIFIED — pub use KafkaNodePool
├── controller/kafka.rs                # REWRITTEN — pool-aware aggregator
├── controller/kafka_node_pool.rs      # NEW
├── controller/mod.rs                  # MODIFIED — pub mod kafka_node_pool
├── run.rs                             # MODIFIED — spawn both controllers
├── gen_crds.rs                        # MODIFIED — also write knp CRD
crates/operator/tests/
├── reconcile.rs                       # DELETED
├── reconcile_kafka.rs                 # NEW
├── reconcile_pool.rs                  # NEW
├── shared/mod.rs                      # NEW — MockState extracted
deploy/crds/
├── crabka.io_kafkas.yaml              # REGENERATED
├── crabka.io_kafkanodepools.yaml      # NEW
charts/crabka-operator/templates/
├── clusterrole.yaml                   # MODIFIED — knp verbs
.github/workflows/
├── operator-e2e.yml                   # MODIFIED — apply KafkaNodePool + assert
```

Implementation plan target: **~11 tasks across 4 batches**.

- **Batch 1 (parallel):** T1 CRD `KafkaNodePool`, T2 strip `Kafka.spec` fields, T3 ClusterRole + RBAC.
- **Batch 2 (parallel):** T4 pool render helpers + unit tests, T5 status aggregation helpers + unit tests.
- **Batch 3 (sequential):** T6 pool reconciler + run wiring, T7 Kafka reconciler rewrite (depends on T4+T5), T8 split tests into reconcile_kafka.rs / reconcile_pool.rs.
- **Batch 4 (parallel + final):** T9 regen both CRDs, T10 e2e workflow updates, T11 final verification.

---

## 8. Open questions resolved

- **Should the pool's StatefulSet have a parent reference all the way to Kafka?** No — chained owner refs (pool owns sts, kafka owns pool) keep the deletion cascade simple and let `kubectl delete kafkanodepool <pool>` drop a pool cleanly without touching siblings.
- **Cross-pool node-id uniqueness?** Deferred to slice 20a. Single-pool is the only supported configuration; collisions can't occur.
- **What happens if a user changes `spec.nodeIdStart`?** The StatefulSet's env var is re-rendered; the init script reformats the data dir (slice 20: `emptyDir`, so reformatting on pod restart is harmless). When persistent storage arrives in slice 24, the operator will reject `nodeIdStart` mutations.
- **One service or one per pool?** One per Kafka. Cross-pool DNS lookups stay simple, and the slice-19 service shape is preserved.

---

## 9. Acceptance criteria

1. `cargo test -p crabka-operator` green (existing + new tests across `reconcile_kafka.rs` and `reconcile_pool.rs`).
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-operator` passes.
4. `crabka-operator gen-crds` is stable (both `kafkas` and `kafkanodepools` CRDs regen with no further drift).
5. operator-e2e workflow: apply `Kafka demo` + `KafkaNodePool brokers`; both reach `Ready=True`; `demo-brokers-0` pod is Ready; cascade-delete clears all owned objects within 60 s.
