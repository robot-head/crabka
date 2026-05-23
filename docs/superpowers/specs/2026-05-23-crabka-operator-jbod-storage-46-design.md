# Slice 46: Operator — JBOD in `KafkaNodePool.spec.storage` — Design

**Status:** Approved 2026-05-23.

**Goal:** Surface slice 45's broker-side JBOD (multi-log-dir) capability
through the operator. Add a `Jbod` variant to `KafkaNodePool.spec.storage`
that materializes **multiple PVCs per pod** — one persistent volume per
JBOD disk — and wires the broker to spread partition data across them via
the `CRABKA_EXTRA_LOG_DIRS` env var (slice 45). Phase 8, the operator
surface for the core JBOD work landed in slice 45.

---

## 1. Scope

### In

- New `Storage::Jbod(JbodSpec)` enum variant alongside `Ephemeral` and
  `PersistentClaim`. `JbodSpec` carries a non-empty `volumes: Vec<JbodVolume>`
  list plus a JBOD-level `deleteClaim: bool`. Each `JbodVolume` has a unique
  `id: i32`, a `size: String` (K8s `Quantity`), and an optional
  `class: Option<String>`.
- Renderer materializes **one `volumeClaimTemplate` per JBOD volume** plus the
  matching pod `volumeMounts`, and sets `CRABKA_EXTRA_LOG_DIRS` on the broker
  container so the broker treats every disk as a log dir.
- Static validation: volumes non-empty, ids unique, every `size` a positive
  `Quantity`.
- Monotonic validation against the live `StatefulSet`'s
  `volumeClaimTemplates`: rejects storage-type switches
  (`Ephemeral`/`PersistentClaim`/`Jbod`), JBOD volume-set changes
  (add/remove deferred — see Out), per-volume `class` changes, and per-volume
  size shrinks. Size grows pass through.
- CRD YAML regenerated; one e2e (kind) applying a 2-disk JBOD pool.

### Out (deferred)

| Concern | Slice |
|---|---|
| Adding / removing JBOD volumes on a live pool (data rebalance across a changed disk set) | future — needs KIP-113 intra-broker moves (`AlterReplicaLogDirs`, slice 45b) |
| Per-volume `deleteClaim` (K8s `persistentVolumeClaimRetentionPolicy` is StatefulSet-wide, not per-template) | not on roadmap — one JBOD-level `deleteClaim` instead |
| Ephemeral volumes inside a JBOD set (Strimzi allows; niche) | future |
| `KafkaNodePool.status.storage` PVC mirror | future (same as slice 24) |
| Operator-driven log-dir balancing across disks | rebalancer territory |

### Semantics

- **The lowest-`id` volume is the primary.** It keeps the slice-24 PVC name
  `data` and mount path `/var/lib/crabka/data`, so it holds the
  `__cluster_metadata` raft log (slice 45 keeps metadata on the primary
  `log_dir`) and reuses the existing init container, cluster-level broker TOML
  (`log_dir = "/var/lib/crabka/data"`), and main script **unchanged**.
- **Every non-primary volume `id = N`** gets PVC template `data-{N}` mounted at
  `/var/lib/crabka/data-{N}`, and is passed to the broker via
  `CRABKA_EXTRA_LOG_DIRS` (comma-joined, sorted by id). The broker (slice 45)
  splits the env value on commas and spreads partitions across
  `[/var/lib/crabka/data] + extras` by least-loaded placement.
- **No broker / init-script / main-script / cluster-TOML change.** JBOD is
  entirely a `KafkaNodePool`-reconciler concern: extra `volumeClaimTemplates`,
  extra `volumeMounts` on the broker container, and one extra env var. This is
  the same self-contained boundary slice 24 established.
- **Retention is set-wide.** `persistentVolumeClaimRetentionPolicy.whenDeleted`
  on a `StatefulSet` applies to *all* its `volumeClaimTemplates`; K8s offers no
  per-template retention. So JBOD exposes a single `deleteClaim` covering every
  disk (diverging from Strimzi's per-volume flag — Crabka is Strimzi-shaped,
  not -compatible, and delegates PVC GC to K8s rather than managing it itself).

---

## 2. CRD shape

```rust
#[serde(tag = "type")]
#[schemars(schema_with = "storage_schema")]
pub enum Storage {
    Ephemeral,
    PersistentClaim(PersistentClaimSpec),
    Jbod(JbodSpec),
}

#[serde(rename_all = "camelCase")]
pub struct JbodSpec {
    /// One persistent volume per JBOD disk. Non-empty; ids unique.
    pub volumes: Vec<JbodVolume>,
    /// `true` → StatefulSet `persistentVolumeClaimRetentionPolicy.whenDeleted:
    /// Delete` for *every* JBOD PVC (K8s retention is set-wide). Default
    /// `Retain`.
    #[serde(default)]
    pub delete_claim: bool,
}

#[serde(rename_all = "camelCase")]
pub struct JbodVolume {
    /// Stable disk id. Lowest id is the primary (metadata) disk. Used to
    /// derive the PVC name / mount path for non-primary disks.
    pub id: i32,
    /// K8s `Quantity` (e.g., `"100Gi"`). Validated at reconcile time.
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
}
```

Wire shape (flat tagged discriminator, Strimzi-ish):

```yaml
spec:
  storage:
    type: Jbod
    deleteClaim: false
    volumes:
      - id: 0
        size: 100Gi
      - id: 1
        size: 100Gi
        class: fast-ssd
```

The hand-rolled `storage_schema` gains `"Jbod"` in the `type` enum and a
`volumes` array property + a `deleteClaim` boolean (kube-rs 3.x's structural
rewriter can't emit the schemars tagged-union output — same reason slice 24
hand-rolled it).

---

## 3. Renderer

`render_statefulset` already routes storage through `render_storage` (pod
volumes + `volumeClaimTemplates`) and `render_pvc_retention_policy`. JBOD
extends both, plus `render_broker_container` for the extra mounts + env.

Volumes are **sorted by id** before rendering so output is deterministic
regardless of YAML order. Let `primary = volumes_sorted[0]`,
`extras = volumes_sorted[1..]`.

1. **`render_storage`** (JBOD arm):
   - Pod `volumes`: the usual `broker-config` / `broker-runtime` / CA / TLS
     secret volumes (unchanged). No explicit `data` entry (the STS controller
     mounts each `volumeClaimTemplate` automatically).
   - `volumeClaimTemplates`: one per volume.
     - primary → name `data`, mount `/var/lib/crabka/data`.
     - extra id N → name `data-{N}`, mount `/var/lib/crabka/data-{N}`.
     - each: `accessModes: [ReadWriteOnce]`, `resources.requests.storage =
       size`, optional `storageClassName`, labels = pod labels (GC selector).
2. **`render_broker_container`**: for each extra volume, add a
   `volumeMount {name: data-{N}, mountPath: /var/lib/crabka/data-{N}}`, and add
   env `CRABKA_EXTRA_LOG_DIRS = "/var/lib/crabka/data-{N1},/var/lib/crabka/data-{N2},…"`
   (extras only, sorted by id). The init container is untouched (it only
   formats the primary metadata dir).
3. **`render_pvc_retention_policy`** (JBOD arm): `whenDeleted = Delete` iff
   `delete_claim`, `whenScaled: Retain` — identical to the PersistentClaim
   shape, applied set-wide.

The renderer stays pure.

---

## 4. Validation

### Static (`validate`)

| Variant | Trigger |
|---|---|
| `JbodNoVolumes` | `volumes` empty |
| `JbodDuplicateVolumeId(i32)` | two volumes share an `id` |
| `StorageSizeInvalid(size, why)` | any volume `size` not a positive `Quantity` (reused) |

### Monotonic (`validate_storage_change`, one STS GET)

The pre-apply GET now hands **all** `volumeClaimTemplates` to the validator
(slice 24 passed only the `data` template). `storage_kind` gains `"Jbod"`.

- **Type change** (`Ephemeral`/`PersistentClaim`/`Jbod` kind differs) →
  `StorageTypeChanged`.
- **JBOD ↔ JBOD:**
  - Observed identities: `data` → *primary slot*; `data-{N}` → id N.
  - Desired identities: lowest id → *primary slot*; the rest → their id.
  - Non-primary id set must match (`JbodVolumesImmutable` otherwise) — this also
    rejects primary reassignment (changing which id is lowest) and add/remove,
    all of which would re-point or orphan a PVC and lose data. Deferred to a
    future slice once intra-broker moves exist.
  - Per matched slot: `class` change → `StorageClassChanged`; size decrease →
    `StorageShrinkNotAllowed`; grow → ok.

First reconcile (no live STS) accepts any spec.

---

## 5. Testing

**`crd::kafka_node_pool::tests`:** JBOD round-trips through JSON (flat tagged
shape, `volumes` array, `deleteClaim`).

**`controller::kafka_node_pool::tests`:**
- `render_statefulset_jbod_renders_one_pvc_per_volume` (names `data` +
  `data-1`; sizes/classes).
- `render_statefulset_jbod_primary_is_lowest_id` (primary keeps `data` /
  `/var/lib/crabka/data`).
- `render_statefulset_jbod_sets_extra_log_dirs_env` (env value lists the extra
  mount paths, sorted, primary excluded).
- `render_statefulset_jbod_mounts_extra_volumes` (broker container mounts
  `data-1`).
- `render_statefulset_jbod_retention_policy_{delete,retain}`.
- `validate_rejects_jbod_empty_volumes` / `_duplicate_ids` / `_bad_size`.
- `validate_storage_change` JBOD: rejects type switch into/out of JBOD,
  rejects volume-set change, rejects per-volume shrink/class change, allows
  per-volume grow.

**`tests/reconcile_pool.rs`:** `pool_jbod_renders_multiple_volume_claim_templates`
— apply a 2-volume JBOD pool, capture the SSA PATCH, assert two
`volumeClaimTemplates` (`data`, `data-1`) and the `CRABKA_EXTRA_LOG_DIRS` env.
The shared `fake_sts_body_with_storage` helper grows a JBOD-aware variant for a
shrink-rejection integration test.

**E2E (`operator-e2e.yml`):** a dedicated short-lived `Kafka` + JBOD
`KafkaNodePool` (2×1Gi, `deleteClaim: true`); assert both PVCs
(`data-…-0`, `data-1-…-0`) reach `Bound`, the broker pod is `Ready`, and both
disks appear as log dirs. GC step already counts PVCs by label.

---

## 6. File structure

```
crates/operator/src/
├── crd/kafka_node_pool.rs        # MODIFIED — Jbod variant + JbodSpec/JbodVolume + schema + tests
├── crd/mod.rs                    # MODIFIED — re-export JbodSpec, JbodVolume
├── controller/kafka_node_pool.rs # MODIFIED — render_storage/broker_container/retention + validate(+monotonic) + tests
crates/operator/tests/
├── reconcile_pool.rs             # MODIFIED — JBOD render + shrink integration tests
├── shared/mod.rs                 # MODIFIED — JBOD-aware fake STS helper
deploy/crds/
├── crabka.io_kafkanodepools.yaml # REGENERATED
.github/workflows/
├── operator-e2e.yml              # MODIFIED — JBOD pool e2e
```

No broker, init-script, main-script, or cluster-level ConfigMap/TOML change.

---

## 7. Acceptance criteria

1. `cargo test -p crabka-operator` green (existing + JBOD unit/integration).
2. `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo fmt --check`.
3. CRD regen stable (`tools/regen-crds.sh` leaves no diff after commit).
4. `helm lint charts/crabka-operator` passes.
5. operator-e2e (kind): JBOD pool becomes `Ready=True`; both PVCs `Bound`;
   broker reports both disks as log dirs; PVCs GC'd on cluster delete.
