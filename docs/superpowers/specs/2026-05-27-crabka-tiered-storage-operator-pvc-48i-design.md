# Crabka tiered storage 48i — Operator PVC rendering for local-tier directory

**Date:** 2026-05-27
**Status:** Slice design. Follows slice 48h
(`Kafka.spec.tieredStorage.metadataManager`, #230). Part of the
KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Why this exists

Slice 48g shipped the `Kafka.spec.tieredStorage` surface and wired the
broker's `[remote_storage]` block, but the local-tier directory was
forced to `emptyDir` because metadata loss on pod restart was the
limiting factor: the `InmemoryRemoteLogMetadataManager` lost its
state every restart, so persisting segment bytes on a PVC would only
delay the data loss by one cycle.

Slice 48h (operator surface for `TopicBasedRemoteLogMetadataManager`)
closed that gap. Tier metadata now survives pod restarts; the
segment bytes can survive too if the broker's `remote_log_storage_dir`
is on a PersistentVolumeClaim.

The CRD field for declaring this — `TieredStorage.persistence:
Option<TieredStoragePersistence>` — is already in place
(`crates/operator/src/crd/kafka.rs:153`), along with shape validation.
**This slice's only job is the operator rendering** that turns that
field into a `volumeClaimTemplate`, plus one missing CRD field
(`delete_claim`) to control PVC retention.

## What already exists (do NOT redo)

- `TieredStorage.persistence: Option<TieredStoragePersistence>` —
  declared in `crates/operator/src/crd/kafka.rs:153`.
- `TieredStoragePersistence { size, class }` — declared in the same
  file. Doc comment marks it "Slice 48i".
- `TieredStorage::validate()` — enforces "persistence is only valid
  with `type=Local`" and "`persistence.size` is required". Lives in
  `crates/operator/src/crd/kafka.rs:255`.
- The `tier-storage` `emptyDir` volume + per-broker mount at
  `/var/lib/crabka/remote` — emitted unconditionally by
  `crates/operator/src/controller/kafka_node_pool.rs` (around line
  636 for the volume, around line 422 for the mount). Slice 48i must
  replace the emptyDir with a `volumeClaimTemplate` when
  `persistence` is `Some`, but the mount stays as-is.

## Goal

When a user declares:

```yaml
spec:
  tieredStorage:
    type: Local
    persistence:
      size: 100Gi
      class: fast-ssd
      deleteClaim: false
```

…the operator renders:

- A `volumeClaimTemplates` entry named `tier-storage` with the
  requested size and `storageClassName`.
- No `emptyDir` for `tier-storage` (would collide with the PVC name).
- A reasonable `persistentVolumeClaimRetentionPolicy` whose
  `whenDeleted` matches the `delete_claim` rules below.

When `persistence` is `None` (the 48g default), the operator behavior
is unchanged: `tier-storage` is an `emptyDir`.

## Non-goals

- **S3 backend.** S3 sends segment bytes straight to the bucket; no
  local dir to back. Existing validation rejects `persistence` under
  `type=S3`.
- **Per-broker / per-pool override.** Tiered storage is a cluster-wide
  property in 48g+; persistence stays cluster-wide too. Per-pool
  override is a separate slice.
- **Migrating an existing emptyDir to a PVC.** Switching the field on
  a running cluster requires a rolling restart, and the first PVC
  boot is empty. Users who care must drain via
  `local.retention.bytes=0` first. Documented; not automated.

## CRD change (small)

Add `delete_claim` to `TieredStoragePersistence`:

```rust
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TieredStoragePersistence {
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    /// `true` → `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`.
    /// Must match the parent `KafkaNodePool.spec.storage.deleteClaim`
    /// when both PVCs are present (K8s StatefulSets have a single
    /// set-wide retention policy with no per-template override).
    /// Validated at reconcile time; mismatch surfaces as
    /// `TieredStorageInvalid`.
    #[serde(default)]
    pub delete_claim: bool,
}
```

Default `false` matches the safer "Retain" semantics already used by
`KafkaNodePool::Storage::PersistentClaim`.

## Validation change (small)

Extend the existing `TieredStorage::validate()` (`crates/operator/src/
crd/kafka.rs:255`). The new check can't run there — it needs the
parent `KafkaNodePool.spec.storage.delete_claim` value — so it lives
in the reconciler instead.

New site: `crates/operator/src/controller/kafka_node_pool.rs`,
during the per-pool render. Pseudocode:

```rust
if let Some(persistence) = parent.spec.tiered_storage.as_ref().and_then(|t| t.persistence.as_ref()) {
    let data_delete_claim = match pool.spec.storage.as_ref() {
        Some(Storage::PersistentClaim(pc)) => Some(pc.delete_claim),
        Some(Storage::Jbod(j))             => Some(j.delete_claim),
        _                                   => None,
    };
    if let Some(dc) = data_delete_claim
        && dc != persistence.delete_claim
    {
        return Err(ReconcileError::TieredStorageInvalid(format!(
            "tiered storage persistence.deleteClaim={} but pool {} storage.deleteClaim={}; \
             K8s StatefulSets have a single set-wide PVC retention policy — these must match",
            persistence.delete_claim, pool_name, dc,
        )));
    }
}
```

When the pool has `Storage::Ephemeral` (no data PVC), the tier PVC's
`deleteClaim` is unconstrained — there's no data-PVC retention to
collide with.

## Operator rendering changes

`crates/operator/src/controller/kafka_node_pool.rs`. Three sites change:

1. **The volumes function that emits the `tier-storage` emptyDir**
   (around line 624–640). When `parent.spec.tiered_storage.persistence
   .is_some()`, skip the emptyDir push. The volume comes from the
   StatefulSet's `volumeClaimTemplates` instead.

2. **The volumeClaimTemplates builder** (around line 490–660). When
   `parent.spec.tiered_storage.persistence` is `Some(p)`, append:

   ```json
   {
     "metadata": { "name": "tier-storage" },
     "spec": {
       "accessModes": ["ReadWriteOnce"],
       "resources": { "requests": { "storage": "<p.size>" } },
       "storageClassName": "<p.class>"   // omitted when None
     }
   }
   ```

   Matches the existing `data` PVC template's shape (already produced
   for `Storage::PersistentClaim` / `Storage::Jbod`).

3. **`render_pvc_retention_policy`** (line 657). The existing function
   returns `None` for `Storage::Ephemeral`, meaning no retention
   policy is emitted at all. If the pool is `Ephemeral` but
   `persistence` is `Some`, we now have a PVC (the tier one) and must
   emit a retention policy. Extend the function:

   ```rust
   fn render_pvc_retention_policy(
       storage: Option<&Storage>,
       tier_persistence: Option<&TieredStoragePersistence>,
   ) -> Option<serde_json::Value> {
       let delete_claim = match storage {
           Some(Storage::PersistentClaim(pc)) => pc.delete_claim,
           Some(Storage::Jbod(j))             => j.delete_claim,
           _ => match tier_persistence {
               Some(p) => p.delete_claim,
               None    => return None,   // no PVCs at all
           },
       };
       Some(json!({
           "whenDeleted": if delete_claim { "Delete" } else { "Retain" },
           "whenScaled":  "Retain",
       }))
   }
   ```

   We trust the validation: when both `storage` and
   `tier_persistence` are present, the two `delete_claim` flags are
   already known to match, so the pool's `delete_claim` is the right
   value to use regardless.

## Migration

Pre-48i `Kafka` CRs without `persistence` continue to get `emptyDir`.
No migration. The field is opt-in.

If a user flips `persistence` from absent → present on a running
cluster, the StatefulSet update recreates the pods. The first time
the pod boots with the PVC, the directory is empty (matching the
prior emptyDir state). No data migration in either direction.

## Error handling

| Failure                                                | Surface                              |
|--------------------------------------------------------|--------------------------------------|
| `kind=S3` with `persistence` set (already validated)   | `TieredStorageInvalid` (existing)    |
| `persistence.size` empty (already validated)           | `TieredStorageInvalid` (existing)    |
| `persistence.deleteClaim` ≠ pool data PVC's            | `TieredStorageInvalid` (new)         |
| `persistence.size` malformed (e.g. `"abc"`)            | K8s API server rejects PVC apply     |
| StorageClass doesn't exist                             | Native k8s `Pending` PVC condition   |

## Testing

- **Unit (`crates/operator/src/crd/kafka.rs`)**: new round-trip test
  for `delete_claim` serialization; existing `persistence_*` tests
  still pass.
- **Unit (`crates/operator/src/controller/kafka_node_pool.rs`)**:
  - With `persistence = None`: StatefulSet has the
    `tier-storage` emptyDir; no `tier-storage` volumeClaimTemplate.
    Existing behavior; assert it still holds.
  - With `persistence = Some` and pool `Storage::PersistentClaim`
    (matching `delete_claim`): StatefulSet has a `tier-storage`
    volumeClaimTemplate of the right size/class; no `tier-storage`
    emptyDir.
  - With `persistence = Some` and pool `Storage::Ephemeral`:
    StatefulSet has the `tier-storage` volumeClaimTemplate AND a
    `persistentVolumeClaimRetentionPolicy` (the pool-only path used
    to return None for the policy).
  - With `persistence.delete_claim = true` and pool
    `Storage::PersistentClaim { delete_claim: false }`: reconcile
    returns `TieredStorageInvalid` with the mismatch message.
- **Kind e2e (optional)**: a single test in
  `.github/workflows/operator-e2e.yml`'s existing tiered-storage job
  that creates a `Kafka` CR with `persistence`, produces records,
  deletes a broker pod, asserts the pod is recreated with its PVC
  bound and tier data still readable. Defer to a follow-up PR if
  this grows.

## Implementation order

1. Add `delete_claim` to `TieredStoragePersistence` (CRD).
2. Add the `delete_claim` mismatch validation in the pool reconciler.
3. Extend the volumes function to skip `tier-storage` emptyDir when
   `persistence` is `Some`.
4. Extend the volumeClaimTemplates builder to emit `tier-storage` when
   `persistence` is `Some`.
5. Extend `render_pvc_retention_policy` to consider `persistence`.
6. Unit tests (4 cases per the table above).

## Risk register

- **K8s set-wide retention policy.** Documented above; mismatch
  validation rejects inconsistent CRs. A future `StatefulSet` API
  extension could permit per-template retention; until then, the
  documented constraint stands.
- **`persistence` field already in the CRD without rendering.** A
  user could set the field today and observe no effect (the operator
  silently keeps the emptyDir). 48i closes this. No deprecation
  warning needed (greenfield, no users).
- **PVC migration from emptyDir.** Not supported. Documented as a
  non-goal; the `local.retention.bytes=0` drain workflow exists for
  users who care.
