# Slice 28: Operator — Version upgrades — Design

**Date:** 2026-05-22
**Status:** Approved, ready for implementation plan.

**Goal:** Give operators a Strimzi-shaped way to declare the Kafka
*metadata version* (the KRaft analog of `inter.broker.protocol.version`),
have the operator validate version/metadata-version compatibility, render
the resolved metadata version into broker config, and roll the cluster one
node at a time on a version change. Closes the Phase-3 "Version upgrades"
roadmap item.

This is **pure operator work**. The broker has no runtime
`metadata.version` feature (the `UpdateFeatures` codec exists but no broker
handler consumes it); the resolved metadata version is rendered into the
broker's `[server_properties]` table, which the broker treats as inert —
the same pattern slices 21 and 25 used for opaque config. The operator owns
the orchestration and safety logic.

---

## 1. Background: how Kafka/Strimzi handle version upgrades

In KRaft mode there are two independent versions:

- **Binary version** — the Kafka release running in the container. In
  Strimzi this is `Kafka.spec.kafka.version`; it selects the image.
- **Metadata version** (`metadata.version`, a.k.a. KRaft feature level) —
  the on-quorum feature level. The runtime analog of the old ZK-era
  `inter.broker.protocol.version`. In Strimzi this is
  `Kafka.spec.kafka.metadataVersion`.

The safe upgrade dance:

1. Bump the **binary** version; the operator rolls every node to the new
   image one at a time while keeping `metadata.version` pinned at the old
   value.
2. Once every node runs the new binary, bump `metadata.version`. This is a
   one-way step on the quorum.

The safe **downgrade window**: a binary may only be downgraded as far as
the finalized `metadata.version` it still understands. Once
`metadata.version` is finalized at `F`, the binary can never drop below
`F`, and `metadata.version` itself cannot be lowered below `F`.

Crabka is KRaft-only (a non-goal of the operator is ZK-mode anything), so
we model **`metadata.version` only** — there is no
`inter.broker.protocol.version` / `log.message.format.version` ZK lineage.

## 2. Scope

### In

- New optional field `Kafka.spec.metadataVersion: Option<String>`
  (Strimzi-shaped). When unset, it **tracks** the binary version
  (`spec.kafkaVersion`'s `major.minor`). When set, it pins the metadata
  version (the safe two-step path).
- A `version` module: parse `X`, `X.Y`, `X.Y.Z` and `X.Y-IVn`
  IBP-suffixed strings; compare by `(major, minor)`; resolve the desired
  metadata version; and validate compatibility.
- Validation surfaced as a new `KafkaVersionValid` status condition. On a
  validation failure the operator does **not** roll: it skips injecting the
  new metadata version into the ConfigMap, does not advance the config
  hash, does not advance `status.metadataVersion`, and requeues — exactly
  the "surface the error and wait" posture slice-25 listener validation
  uses.
- The resolved metadata version is rendered into the broker ConfigMap as
  `metadata.version = "<X.Y>"` inside the per-broker TOML
  `[server_properties]` table (broker-inert today).
- An **explicit** `spec.metadataVersion` pin participates in the slice-21
  config hash, so changing the pin rolls the cluster. A *defaulted*
  metadata version does not enter the hash (a binary bump already rolls via
  the pod-template image change), preserving the slice-24 empty-hash
  collapse.
- **Ordered, one-node-at-a-time rollout** across pools. The Kafka
  reconciler already lists every sibling pool (with status) and patches
  each pool's `crabka.io/config-hash` label; slice 28 changes only the
  *value* written per pool so an established cluster advances one pool at a
  time, gated on the previous pool reaching Ready. No new API requests.
- `KafkaStatus` gains `kafkaVersion` (echo of the spec) and
  `metadataVersion` (the operator-finalized value), so the finalized
  metadata version is observable and drives the downgrade-window check on
  the next reconcile.

### Out (deferred)

| Concern | Why |
|---|---|
| Broker actually enforcing `metadata.version` (feature levels, `UpdateFeatures` handler) | A large Crabka-core slice; the roadmap classifies slice 28 as pure operator. |
| `inter.broker.protocol.version` / `log.message.format.version` | ZK-era only; Crabka is KRaft-only. |
| A version → image-tag mapping in the operator | Image resolution stays `pool.spec.image > operator default > built-in`; CI/operators set version-tagged images. `kafkaVersion` remains the declared label + the metadata-version default source. |
| Cross-pool ISR-aware ordering / draining each node via `ControlledShutdown` before roll | Slice 22's `controlled_shutdown` is broker-side; wiring it into the rollout gate is a follow-up. The gate here orders by node id and waits for Ready. |
| Multi-replica pools | Slice-20 single-replica invariant stands. |

### Constraints

- Slice-20 invariants stay (single-replica mixed pools).
- The slice-24 empty-hash collapse stays byte-identical (defaulted
  metadata version must not change the hash).
- Initial cluster bring-up must **not** be gated one-at-a-time: a KRaft
  controller quorum needs every controller node up together to form, so
  ordered gating engages only for an *established, uniform* cluster
  transitioning to a new hash.

---

## 3. CRD shape

`crates/operator/src/crd/kafka.rs`:

```rust
// KafkaSpec
/// KRaft metadata version (the runtime analog of
/// `inter.broker.protocol.version`). When unset, tracks `kafkaVersion`'s
/// `major.minor`. When set, pins the metadata version for the safe
/// two-step upgrade. Validated against `kafkaVersion` and the finalized
/// `status.metadataVersion`; an invalid value surfaces
/// `KafkaVersionValid=False` and blocks the roll.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub metadata_version: Option<String>,

// KafkaStatus
/// Echo of `spec.kafkaVersion`, for observability.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub kafka_version: Option<String>,
/// The operator-finalized metadata version. Advances only when version
/// validation passes; drives the downgrade-window check next reconcile.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub metadata_version: Option<String>,
```

`KafkaStatus` already derives `Default`. The `KafkaVersionValid` condition
slots into the existing `Vec<KafkaCondition>`.

---

## 4. Version model — `crates/operator/src/version.rs`

```rust
pub struct KafkaVersion { major: u32, minor: u32, patch: u32 }
impl KafkaVersion {
    pub fn parse(s: &str) -> Result<Self, VersionError>; // strips `-IVn`
    pub fn metadata_key(&self) -> (u32, u32);            // (major, minor)
    pub fn short(&self) -> String;                       // "X.Y"
}

pub enum VersionOutcome {
    Valid { resolved_metadata: String },        // canonical "X.Y"
    Invalid { reason: VersionReason, message: String },
}
pub enum VersionReason { InvalidVersion, MetadataVersionTooHigh, MetadataVersionDowngrade }

pub fn evaluate(
    kafka_version: &str,
    spec_metadata_version: Option<&str>,
    finalized_metadata_version: Option<&str>,
) -> VersionOutcome;
```

`evaluate` rules (each maps to a `KafkaVersionValid=False` reason):

1. **InvalidVersion** — `kafka_version` (and `spec_metadata_version`, if
   present) must parse.
2. Resolve: `resolved = spec_metadata_version` (canonicalized to `X.Y`) if
   set, else `kafka_version`'s `X.Y`.
3. **MetadataVersionTooHigh** — `resolved.metadata_key()` must be
   `<= kafka_version.metadata_key()`. You cannot finalize a metadata
   version newer than the running binary.
4. **MetadataVersionDowngrade** — when `finalized_metadata_version` is set
   and parses, `resolved.metadata_key()` must be `>=` it. Metadata
   versions never go backward.

Rules 3 + 4 together are the **downgrade-window enforcement**: the binary
is `>= resolved >= finalized`, so a binary can never drop below the
finalized metadata version, and metadata never regresses.

`finalized_metadata_version` is read from `obj.status.metadata_version` —
already on the watched object, so no extra API request.

---

## 5. Reconciler wiring — `controller/kafka.rs`

Near the listener-validation block:

```rust
let finalized = obj.status.as_ref().and_then(|s| s.metadata_version.as_deref());
let version_outcome = crate::version::evaluate(
    &obj.spec.kafka_version, obj.spec.metadata_version.as_deref(), finalized);
let (version_cond, resolved_metadata, explicit_pin) = match &version_outcome {
    Valid { resolved_metadata } => (
        condition("KafkaVersionValid", "True", "Valid",
            &format!("kafkaVersion {} metadata.version {}", spec.kafka_version, resolved_metadata)),
        Some(resolved_metadata.clone()),
        // only an explicit, valid pin enters the hash
        obj.spec.metadata_version.clone(),
    ),
    Invalid { reason, message } => (
        condition("KafkaVersionValid", "False", reason.as_str(), message),
        None, None),
};
```

- `combined_config_hash(&spec, ca_cert, explicit_pin.as_deref())` — the
  4th argument is the explicit metadata pin (or `None`).
- `render_configmap(.., resolved_metadata.as_deref())` injects
  `metadata.version` into `[server_properties]` only when valid.
- `KafkaStatus.kafka_version = Some(spec.kafka_version.clone())`.
- `KafkaStatus.metadata_version = resolved_metadata.or(finalized)` —
  advances when valid, holds the last finalized value when invalid.
- `KafkaVersionValid` is pushed onto the conditions vec.

`adopt_pools` is changed to plan an ordered rollout (see §6) instead of
writing the same hash to every pool.

## 6. Ordered rollout — pure `plan_rollout`

```rust
/// Per-pool target config hash for an ordered, one-at-a-time rollout.
/// `pools` are pre-sorted by (node_id_start, name).
pub(crate) fn plan_rollout(
    pools: &[PoolRolloutState],   // { name, current_hash: Option<String>, ready: bool }
    desired: &str,
) -> Vec<(String /*name*/, String /*target hash*/)>;
```

Decision:

- If any pool has no current hash, or there is more than one distinct
  *non-desired* hash among pools → **bring-up / recovery**: every pool gets
  `desired` (parallel — lets a KRaft quorum form). This is also the
  single-pool first-reconcile path.
- If every pool already carries `desired` → no-op (all `desired`).
- Otherwise (established cluster, current hashes ⊆ `{old, desired}`,
  transitioning) → **ordered roll**: walk pools in order; a pool is
  *converged* when it already carries `desired` AND is Ready. Advance the
  first non-converged pool to `desired`; every later pool keeps its current
  hash until the earlier pools converge.

`adopt_pools` builds `PoolRolloutState` from the listed pool objects
(`metadata.labels["crabka.io/config-hash"]`, `status.ready_replicas`),
sorts by `(node_id_start, name)`, calls `plan_rollout`, and patches each
pool with its planned target hash + owner-ref — same one-PATCH-per-pool
shape as today, so the FIFO-mock request sequences are unchanged.

---

## 7. Broker ConfigMap rendering

`render_configmap` gains a `metadata_version: Option<&str>` parameter. When
`Some`, it merges two synthetic entries into the per-broker
`server_properties` before calling `render_broker_toml`:

```
metadata.version = "<X.Y>"
```

(Operator-owned key; if a user also sets it in `spec.config`, the
operator's value wins.) `render_broker_toml` is unchanged — it already
emits whatever `server_properties` it is handed into the `[server_properties]`
table, which the broker parses and ignores.

---

## 8. Testing

### Unit (`version.rs`)
- parse: `3.7`, `3.7.1`, `3.7-IV2`, bare `3`, junk → error.
- `evaluate`: default-tracks-binary; explicit pin ≤ binary ok; pin > binary
  → `MetadataVersionTooHigh`; metadata below finalized → `MetadataVersionDowngrade`;
  binary below finalized (auto-track downgrade) → `MetadataVersionDowngrade`;
  unparseable → `InvalidVersion`.

### Unit (`controller::kafka` / `common`)
- `plan_rollout`: bring-up (None hashes) → all desired; single pool roll;
  established multi-pool advances one at a time; gated pool not-ready holds
  later pools; converged prefix continues; all-desired no-op; messy
  multi-hash → all desired.
- `combined_config_hash`: explicit pin changes the hash; defaulted (None
  4th arg) preserves the slice-24 collapse.

### Integration (`tests/reconcile_kafka.rs`)
- ConfigMap PATCH carries `metadata.version` in a broker TOML when a valid
  spec is applied (default + explicit pin).
- `metadataVersion` greater than `kafkaVersion` → `KafkaVersionValid=False,
  reason=MetadataVersionTooHigh`; ConfigMap omits the key; status
  `metadataVersion` not advanced.
- status PATCH carries `kafkaVersion` + `metadataVersion`.

### E2E (`operator-e2e.yml`)
- A probe in the existing rolling-restart job: apply `spec.metadataVersion:
  "0.1"`, wait Ready; bump to a higher pin, observe the pod roll and the
  rendered ConfigMap carry the new `metadata.version`; apply an invalid
  (too-high) pin and assert `KafkaVersionValid=False` with the pod **not**
  rolled.

---

## 9. Files

```
crates/operator/src/
├── version.rs                      # NEW — KafkaVersion + evaluate
├── lib.rs                          # MODIFIED — pub mod version
├── crd/kafka.rs                    # MODIFIED — spec.metadataVersion, status fields
├── controller/common.rs           # MODIFIED — combined_config_hash arg, render_configmap arg, plan_rollout
├── controller/kafka.rs            # MODIFIED — evaluate wiring, status, ordered adopt_pools
crates/operator/tests/reconcile_kafka.rs   # MODIFIED — new cases + literal updates
deploy/crds/crabka.io_kafkas.yaml          # REGENERATED
.github/workflows/operator-e2e.yml         # MODIFIED — version-upgrade probe
docs/superpowers/plans/2026-05-22-crabka-operator-version-upgrades-28.md  # plan
```

Adding a `KafkaSpec` field touches every `KafkaSpec { .. }` literal
(≈23 sites, mostly test fixtures): each gets `metadata_version: None`.

## 10. Acceptance criteria

1. `cargo test -p crabka-operator` green (existing + new).
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `cargo fmt --check` clean.
4. `tools/regen-crds.sh` produces no diff after commit.
5. `helm lint charts/crabka-operator` passes.
6. operator-e2e version-upgrade probe: a `metadataVersion` pin bump rolls
   the pod and renders the new `metadata.version`; an invalid pin sets
   `KafkaVersionValid=False` without rolling.
