# Slice 24: Operator — Persistent storage on `KafkaNodePool` — Design

**Status:** Approved 2026-05-17.

**Goal:** Replace slice 19/20's `emptyDir` with a real PVC-backed storage option so a Kafka cluster survives pod restarts and pod replacements. Introduce `KafkaNodePool.spec.storage` as a tagged-union over `Ephemeral` (current behavior, kept as a default) and `PersistentClaim` (single PVC per pod with optional `class` and `deleteClaim`). Monotonic resize is allowed; type and class changes are rejected.

---

## 1. Scope

### In

- `KafkaNodePool.spec.storage: Option<Storage>` with `Storage` a tagged-union enum (`type` discriminator) over:
  - `Ephemeral` — `emptyDir: {}` volume on the StatefulSet (preserves slice 19/20 defaults). The variant carries no fields.
  - `PersistentClaim { size: String, class: Option<String>, deleteClaim: bool }` — single PVC per pod via the StatefulSet's `volumeClaimTemplates`.
- Renderer switches between `volumes: [{emptyDir}]` (current) and a `volumeClaimTemplates: [...]` entry on the StatefulSet. Pod-template `volumeMounts` reference the same `data` name in both cases — no change to mount points.
- When `type == PersistentClaim`, the rendered StatefulSet sets `persistentVolumeClaimRetentionPolicy.{whenDeleted: <deleteClaim ? Delete : Retain>, whenScaled: Retain}`. The `whenScaled` value is always `Retain` in slice 24 (multi-replica scale-down is a slice-20a concern).
- Static validation: `size` parses as a positive K8s `Quantity` (binary suffixes `Ki/Mi/Gi/Ti/Pi/Ei` and decimal `K/M/G/T/P/E`); a small in-tree parser lives in `controller/common.rs`.
- Monotonic-resize validation (compares spec against the live StatefulSet's existing `volumeClaimTemplates`): rejects `Ephemeral ↔ PersistentClaim` switches, rejects `class` changes, rejects size *decreases*. Allows size *increases* (operator patches the StatefulSet template; K8s decides whether the storageClass actually supports expansion — failures surface as `Ready=False` via the bubbled-up API error).
- PVC labels inherit the pool's pod labels (including `app.kubernetes.io/instance=<kafka>`) so the existing slice-20 GC label selector still works on PVCs.
- E2E (kind): `local-path-provisioner` (kind's default storageClass) is used implicitly via `class: None`. Smoke step asserts the PVC reaches `Bound`; GC step asserts PVCs are cleaned up when `deleteClaim: true` and the cluster is deleted.

### Out (deferred)

| Concern | Slice |
|---|---|
| JBOD (multiple PVCs per pod, `KIP-113` log-dir reassignment) | 46 (operator surface) / 45 (broker side) |
| `KafkaNodePool.status.storage` mirror (`pvc.status.{phase, capacity}`) | future |
| Cluster-level `Kafka.spec.storage` default | future (operator slices haven't needed it yet) |
| In-place size *decrease* / class change orchestration (data migration) | not on roadmap |
| Volume snapshot / restore | not on roadmap |
| `accessModes` beyond `ReadWriteOnce` | future |
| Per-pod heterogeneous storage (e.g., log-dir on SSD, meta on HDD) | depends on slice 45 |
| Status condition for "resize in progress" | future |

### Constraints inherited from slice 20

- `KafkaNodePool` is the only writer of broker `StatefulSet`s (SSA field manager `crabka-operator`).
- Slice 20 invariants stay: replicas = 1, roles = {Controller, Broker}.
- Slice 21's `crabka.io/config-hash` pod-template annotation still rolls pods on `spec.config` change; slice 24 changes are independent (volume swap rolls the pod via template diff naturally).

---

## 2. CRD shape

### `Storage` enum

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type")]
pub enum Storage {
    Ephemeral,
    PersistentClaim(PersistentClaimSpec),
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersistentClaimSpec {
    /// K8s `Quantity` (e.g., `"10Gi"`, `"500Mi"`).
    pub size: String,
    /// Storage class name. `None` = cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    /// `true` → `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`.
    /// Default `false` (Retain) is the safe option.
    #[serde(default)]
    pub delete_claim: bool,
}
```

`#[serde(tag = "type")]` produces flat YAML (Strimzi-shape):

```yaml
spec:
  storage:
    type: PersistentClaim
    size: 10Gi
    class: fast-ssd          # optional
    deleteClaim: false       # optional, default Retain
```

For `Ephemeral`:

```yaml
spec:
  storage:
    type: Ephemeral
```

### `KafkaNodePoolSpec` extension

```rust
/// Storage configuration. `None` = emptyDir (slice 19/20 default).
#[serde(default, skip_serializing_if = "Option::is_none")]
pub storage: Option<Storage>,
```

A field-absent `spec.storage` is semantically identical to `spec.storage: {type: Ephemeral}`.

---

## 3. Renderer changes

`crates/operator/src/controller/kafka_node_pool.rs::render_statefulset`:

1. **Volume selection.** Replace today's hard-coded `volumes: [{name: data, emptyDir: {}}]` with:
   - `None | Some(Storage::Ephemeral)` → unchanged (emptyDir, no `volumeClaimTemplates`).
   - `Some(Storage::PersistentClaim(pc))` → drop the `data` entry from `volumes`, add a `volumeClaimTemplates` entry:
     ```yaml
     metadata:
       name: data
       labels: <same labels as the pod template>
     spec:
       accessModes: [ReadWriteOnce]
       resources:
         requests:
           storage: <pc.size>
       storageClassName: <pc.class or omit>
     ```
     The pod-template `volumeMounts` stay `[{name: data, mountPath: /var/lib/crabka/data}]`.
   - PVC labels include `app.kubernetes.io/instance=<kafka>` (matches slice-20 GC selector) plus `app.kubernetes.io/name=crabka-broker` and `crabka.io/pool=<pool>`. K8s' StatefulSet controller already propagates labels from the `volumeClaimTemplates.metadata.labels` block onto the bound PVC.

2. **PVC retention policy.** Only emitted when `type == PersistentClaim`:
   ```yaml
   spec:
     persistentVolumeClaimRetentionPolicy:
       whenDeleted: <Delete | Retain>
       whenScaled: Retain
   ```

3. **No other StatefulSet field changes.** Pod template, init container, main container, ports, probes — all unchanged.

The renderer remains pure: no I/O, no allocations beyond what `serde_json` emits.

---

## 4. Validation

### Static validation (no I/O)

Runs after the slice-20 validation (`roles`, `replicas`, `nodeIdStart`, `crabka.io/cluster` label). Variants added to `PoolValidationError`:

| Variant | Trigger | Condition `reason` |
|---|---|---|
| `StorageSizeInvalid(String)` | `pc.size` doesn't parse as a `Quantity` | `StorageSizeInvalid` |
| `StorageSizeNegativeOrZero(String)` | parses but ≤ 0 | `StorageSizeInvalid` |

### Monotonic-storage validation (one StatefulSet GET)

Compares desired `Storage` against the existing StatefulSet's `volumeClaimTemplates`. Runs *after* the slice-20 parent-Kafka lookup but *before* SSA-applying the StatefulSet. Helper:

```rust
fn validate_storage_change(
    desired: Option<&Storage>,
    observed: Option<&Storage>,
) -> Result<(), PoolValidationError>;
```

The `observed` side is derived from the live StatefulSet:
- If `volumeClaimTemplates` is empty → `observed = Some(Storage::Ephemeral)`.
- If there's a `data` template → reconstruct `PersistentClaim` with `size = template.spec.resources.requests.storage`, `class = template.spec.storageClassName`, `delete_claim` derived from `persistentVolumeClaimRetentionPolicy.whenDeleted == Delete`.

Rejection variants:

| Variant | Trigger | Condition `reason` |
|---|---|---|
| `StorageTypeChanged { from, to }` | `Ephemeral ↔ PersistentClaim` switch | `StorageImmutable` |
| `StorageClassChanged { from, to }` | `pc.class` value changed | `StorageImmutable` |
| `StorageShrinkNotAllowed { current, desired }` | `pc.size` decreased | `StorageImmutable` |

When no live StatefulSet exists yet (first reconcile), monotonic validation is a no-op — any storage spec is accepted.

When `pc.size` increased: pass-through. The operator patches the StatefulSet's `volumeClaimTemplates.spec.resources.requests.storage`. K8s decides whether the storageClass supports expansion; if it doesn't, the SSA patch returns an API error. The error bubbles through the normal `ReconcileError::Kube` path → `error_policy` re-queue + log. No special `PoolValidationError` variant — the SSA error message is descriptive enough on its own.

`deleteClaim` mutations are *allowed* — they only affect the StatefulSet's retention-policy field, which K8s lets us patch freely. `validate_storage_change` rejects only the three immutable mutations (`type`, `class`, size-decrease); any other delta on `PersistentClaim` (i.e., `deleteClaim` flip or size increase) is acceptable and falls through to the SSA-apply path.

### Validation ordering in `reconcile`

1. Slice-20 spec validation (roles, replicas, nodeIdStart, label).
2. **NEW:** Static `Storage` validation (`parse_quantity`).
3. Parent Kafka lookup (existing slice-20 path).
4. Image resolution (spec → operator default).
5. **NEW:** Live StatefulSet GET (added pre-apply specifically for the monotonic check). Returns `None` on first-reconcile.
6. **NEW:** Monotonic-storage validation (`validate_storage_change` against the GET'd live StatefulSet's `volumeClaimTemplates`).
7. SSA-apply StatefulSet.
8. Status read (existing slice-20 GET; reused to capture rolled-out replicas).
9. Status patch.

Steps 5 and 8 are two separate GETs against the same StatefulSet object per tick. Slice 24 accepts this overhead (~1 extra round-trip) rather than threading the pre-apply state through to step 8 — the additional I/O is cheap relative to the SSA-apply, and the data freshness matters more for the status mirror.

---

## 5. Quantity parser

The workspace doesn't currently depend on a K8s `Quantity` parser library. A ~50-line in-tree parser in `controller/common.rs` covers the cases the operator needs:

```rust
/// Parse a K8s `Quantity` string into a comparable integer (bytes).
/// Returns `Err` on invalid format or non-positive value.
pub(crate) fn parse_quantity(s: &str) -> Result<i128, &'static str>;
```

Accepts:
- Binary suffixes `Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei` (1 Ki = 1024 bytes).
- Decimal suffixes `K`, `M`, `G`, `T`, `P`, `E` (1 K = 1000 bytes).
- Bare integers (no suffix → bytes).
- Integer or decimal mantissa (`1.5Gi`).
- Rejects scientific notation, negative numbers, empty strings.

`i128` so `1.5Pi` fits comfortably with headroom for arithmetic. The returned value is byte-count-equivalent; two `Quantity` strings compare equal iff their byte counts match (so `1Gi != 1G`, but `1Gi == 1024Mi`). Comparison is the only operation slice 24 needs — we do NOT round-trip the parsed value back to a string.

Unit tests cover binary suffixes, decimal suffixes, parse rejections, zero / negative rejection.

---

## 6. Testing

### Unit tests

**`crd::kafka_node_pool::tests`** (3 new):
- `storage_ephemeral_round_trips_through_json`.
- `storage_persistent_claim_round_trips_through_json` (full body with size, class, deleteClaim).
- `spec_defaults_storage_to_none`.

**`controller::common::tests`** (4 new):
- `quantity_parse_binary_suffixes` (`"10Gi"`, `"512Mi"`, `"1Ki"`).
- `quantity_parse_decimal_suffixes` (`"10G"`, `"500M"`).
- `quantity_parse_rejects_garbage` (`"banana"`, `""`, `"1.5x"`).
- `quantity_parse_zero_and_negative_are_errors` (`"0"`, `"-10Gi"`).

**`controller::kafka_node_pool::tests`** (~8 new):
- `render_statefulset_emptydir_when_storage_none`.
- `render_statefulset_emptydir_when_storage_ephemeral`.
- `render_statefulset_volume_claim_template_when_persistent`.
- `render_statefulset_no_storage_class_when_class_absent`.
- `render_statefulset_storage_class_propagates_when_set`.
- `render_statefulset_retention_policy_delete_when_delete_claim_true`.
- `render_statefulset_retention_policy_retain_when_delete_claim_false`.
- `validate_storage_change_rejects_type_switch` / `_rejects_class_change` / `_rejects_shrink` / `_allows_grow` / `_allows_deleteclaim_flip`.

### Integration tests (`tests/reconcile_pool.rs`)

The slice-20 mock harness's StatefulSet GET body needs `volumeClaimTemplates` support. Add two tests:

- `pool_persistent_claim_renders_volume_claim_template` — apply with `storage: {type: PersistentClaim, size: 10Gi}`. Capture the SSA PATCH body, assert it contains a `volumeClaimTemplates` entry with the requested size + `accessModes: [ReadWriteOnce]`. Assert NO `volumes[name=data].emptyDir` entry exists.
- `pool_storage_shrink_is_rejected` — preload a StatefulSet GET response whose `volumeClaimTemplates.spec.resources.requests.storage` is `10Gi`; drive reconcile with `5Gi`; assert status PATCH `reason: StorageImmutable` and NO StatefulSet PATCH was captured.

The existing 5 slice-20 pool tests stay green (default `storage = None` → emptyDir, identical render output).

### E2E (`.github/workflows/operator-e2e.yml`)

Three changes:

1. **Apply manifest** — replace the existing `KafkaNodePool brokers` spec body to include storage:
   ```yaml
   spec:
     roles: [Controller, Broker]
     replicas: 1
     nodeIdStart: 0
     storage:
       type: PersistentClaim
       size: 1Gi
       deleteClaim: true        # so the GC step doesn't leave PVCs behind
     template:
       # ...existing block unchanged...
   ```

2. **New smoke step** (after `Smoke — broker binary launched in pod`, before `Smoke — config change rolls broker pod`):
   ```yaml
   - name: Smoke — broker bound a PersistentVolumeClaim
     run: |
       set -e
       # PVC name pattern: `<volumeClaimTemplate.name>-<sts>-<ordinal>`
       phase=$(kubectl get pvc data-demo-brokers-0 -n default -o jsonpath='{.status.phase}')
       [ "$phase" = "Bound" ] || { echo "::error::PVC not Bound: $phase"; exit 1; }
       cap=$(kubectl get pvc data-demo-brokers-0 -n default -o jsonpath='{.status.capacity.storage}')
       echo "PVC bound, capacity=$cap"
       instance_label=$(kubectl get pvc data-demo-brokers-0 -n default -o jsonpath='{.metadata.labels.app\.kubernetes\.io/instance}')
       [ "$instance_label" = "demo" ] || { echo "::error::PVC missing instance label: $instance_label"; exit 1; }
   ```

3. **Extend GC probe** — include PVCs in the labeled-resource count:
   ```bash
   pvcs=$(kubectl get pvc -n default -l app.kubernetes.io/instance=demo -o name 2>/dev/null | wc -l)
   # ...
   if [ "$knp" = "0" ] && [ "$owned" = "0" ] && [ "$pvcs" = "0" ]; then exit 0; fi
   ```

   On failure, the diagnostics block already runs `kubectl get sts,svc,cm,secret`; extend it to also dump `kubectl get pvc -n default`.

### Note on the slice-21 rolling-restart e2e step

The slice-21 step patches `Kafka.spec.config.log.retention.hours` and asserts the pod's UID changes. With persistent storage attached, the new pod re-binds the same PVC (StatefulSet semantics — same ordinal, same PVC name). The pod still gets a new UID; the assertion is unchanged. The PVC's `Bound` status persists across the roll.

---

## 7. File structure

```
crates/operator/src/
├── crd/kafka_node_pool.rs       # MODIFIED — Storage enum + PersistentClaimSpec
├── controller/common.rs         # MODIFIED — parse_quantity + tests
├── controller/kafka_node_pool.rs # MODIFIED — renderer + validate_storage_change + tests
crates/operator/tests/
├── reconcile_pool.rs            # MODIFIED — 2 new integration tests + extended SS GET mock
deploy/crds/
├── crabka.io_kafkanodepools.yaml # REGENERATED
.github/workflows/
├── operator-e2e.yml             # MODIFIED — PVC smoke + extended GC probe
```

Implementation plan target: **~6 tasks across 3 batches.**

- **Batch 1 (parallel):** T1 CRD types (`Storage`, `PersistentClaimSpec`); T2 `parse_quantity` helper in `common.rs`.
- **Batch 2 (sequential):** T3 renderer changes (volume + retention) in `kafka_node_pool.rs`; T4 validation (static + monotonic) in `kafka_node_pool.rs` — same file, must be sequential.
- **Batch 3 (parallel + final):** T5 regen CRD YAML + extend e2e workflow; T6 final verification (fmt / clippy / tests / helm lint / drift).

---

## 8. Acceptance criteria

1. `cargo test -p crabka-operator` green (existing 55 + ~17 new = ~72 tests).
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-operator` passes (no chart changes expected).
4. CRD regen stable for both `kafkas` and `kafkanodepools`.
5. operator-e2e (kind):
   - `Kafka demo` + `KafkaNodePool brokers` with `storage.type=PersistentClaim, size=1Gi, deleteClaim=true` becomes `Ready=True`.
   - PVC `data-demo-brokers-0` reaches `Bound` and carries `app.kubernetes.io/instance=demo` label.
   - Slice-21 rolling-restart smoke still passes with the same PVC binding.
   - `kubectl delete kafka demo` GCs the PVC alongside everything else within 60 s.

---

## 9. Open questions resolved

- **One field or two for retention?** One. `deleteClaim: bool` (Strimzi-compatible) maps to `whenDeleted: <Delete | Retain>`. `whenScaled` stays `Retain` until slice 20a multi-replica matters.
- **Where does `spec.storage` live?** `KafkaNodePool` only — matches slice 20's per-pool pattern. Cluster-level `Kafka.spec.storage` defaults can come later if a real use case demands it.
- **Default variant when field is absent?** `Ephemeral`. Preserves slice 19/20 behavior; existing dev YAMLs keep working.
- **Why a custom quantity parser instead of a crate?** No workspace dep for it today; the parsing surface we need is ~50 lines and the parser is internal. Adding a transitive dep for one comparison isn't worth it.
- **Why no `status.storage` mirror?** Slice scope. The bound-PVC state is observable via `kubectl get pvc`; surfacing it through `KafkaNodePool.status` is a discrete enhancement that doesn't unblock anything else.
- **Why allow `class` change-rejection rather than orchestrating a re-bind?** A `class` change implies a different provisioner, possibly a different backing technology. Safe orchestration is data-migration territory and out of scope for any near-term slice.
