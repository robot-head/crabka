# Crabka tiered storage 48g — Operator CRD surface (design)

**Date:** 2026-05-26
**Status:** Slice design. Follows slice 48e (remote retention + partition
delete). Part of the KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Make Crabka's KIP-405 tiered-storage stack (slices 48a–e) operator-
addressable: an operator declares tiered storage on the `Kafka` CR, and
every broker pod boots with the local-tier RSM enabled and a writable
directory mounted at the path the broker writes to.

After 48g, the smallest viable tiered-storage cluster is one annotation:

```yaml
spec:
  tieredStorage:
    type: local
```

…which sets `Kafka.spec.config["remote.storage.enable"]=true` per topic
(existing pass-through, no operator work) and turns the broker-wide RSM
on automatically.

## What 48a–e already provide

- **Broker config:** `[remote_storage] storage_dir = "<path>"` populates
  `BrokerConfig.remote_log_storage_dir`. When set, `Broker::start`
  constructs a shared `LocalTieredStorage` + `InmemoryRemoteLogMetadataManager`
  pair and spawns the copy / retention task. When unset, every tiered
  path is a no-op.
- **Per-topic enablement:** `remote.storage.enable` /
  `local.retention.{ms,bytes}` are already valid topic configs that flow
  through the existing `KafkaTopic.spec.config` map → broker
  `IncrementalAlterConfigs` (slice 35).

So 48g's job is exactly **cluster-level enablement** — render
`[remote_storage]` into each broker's TOML, mount the directory, and
nothing else.

## Non-goals (deferred to 48f / later)

- **PersistentClaim for the local-tier directory.** 48g uses `emptyDir`
  only. With `LocalTieredStorage` and `InmemoryRemoteLogMetadataManager`
  both losing state on pod restart, a PVC would only delay the data
  loss by one restart — the metadata is gone either way until 48f
  ships `TopicBasedRemoteLogMetadataManager`. PVC support pairs
  naturally with 48f; cleanest follow-up.
- **Object-store RSM types** (`type: s3` / `gcs` / `azure`). The CRD
  reserves the `type` discriminator so they can be added without a
  breaking change.
- **Per-pool tiered-storage overrides.** Tiered storage is a
  cluster-wide property; 48g keeps it on the `Kafka` CR.
- **Status condition for tiered storage state.** A simple presence
  check on the `[remote_storage]` block is enough for 48g; a richer
  per-broker "tier reachable" condition can land with 48f.

## CRD surface (`crates/operator/src/crd/kafka.rs`)

```rust
pub struct KafkaSpec {
    // ... existing fields ...

    /// Slice 48g (KIP-405): cluster-wide tiered storage. When `Some`,
    /// every broker pod boots with the local-tier RSM enabled, an
    /// `emptyDir` mounted at `/var/lib/crabka/remote` (the broker's
    /// `remote_log_storage_dir`), and `[remote_storage]` rendered in
    /// the broker TOML. Per-topic enablement is unchanged
    /// (`KafkaTopic.spec.config["remote.storage.enable"] = "true"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tiered_storage: Option<TieredStorage>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TieredStorage {
    /// Tiered-storage backend. Only `Local` is supported in 48g — the
    /// future `S3` / `Gcs` / `Azure` variants extend this enum without
    /// breaking the wire shape.
    #[serde(rename = "type")]
    pub kind: TieredStorageType,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TieredStorageType {
    /// On-pod filesystem store via `LocalTieredStorage` (the reference
    /// RSM from slice 48a). Data lives at `/var/lib/crabka/remote` on
    /// the broker pod and does **not** survive pod restarts in 48g
    /// (emptyDir + in-memory RLMM).
    Local,
}
```

A single discriminator-only struct is forward-compatible: when an
S3 variant lands, `TieredStorage` grows a sibling `s3:
S3StorageSpec` field gated by the new `Type::S3` discriminator. The
hand-rolled-schema dance from `KafkaNodePool.spec.storage` is not
required here because there are no per-variant fields yet.

## Reconciler changes

### `render_broker_toml`
(`crates/operator/src/controller/listeners.rs`)

Accept a new `tiered_storage: Option<&TieredStorage>` argument. When
`Some(t)` with `t.kind == Local`, emit a top-level `[remote_storage]`
block:

```toml
[remote_storage]
storage_dir = "/var/lib/crabka/remote"
```

Placed after `[server_properties]` and before `[authorization]` (the
existing block order). The path is operator-owned — operators do not
configure it.

### `render_configmap`
(`crates/operator/src/controller/common.rs`)

Plumb `owner.spec.tiered_storage.as_ref()` into the per-broker
`render_broker_toml` call.

### `kafka_node_pool.rs::reconcile`

After resolving `parent: Kafka` (existing code path), read
`parent.spec.tiered_storage`. When `Some`:

- Add an `emptyDir` pod volume named `tier-storage`.
- Add a writable `volumeMount` `{ name: "tier-storage", mountPath:
  "/var/lib/crabka/remote" }` to the `broker` container.

When `None`: neither volume nor mount is rendered (byte-identical pod
template — no spurious roll on non-tiered clusters).

The slice-21 config hash already covers the broker TOML changes; the
StatefulSet's `spec.template.metadata.annotations["crabka.io/config-hash"]`
flips when `[remote_storage]` is added, triggering a rolling restart.

## Test plan

### CRD round-trip (`crates/operator/src/crd/kafka.rs`)

- `tiered_storage_round_trips_through_json` — `Some(TieredStorage {
  kind: Local })` ↔ `{"tieredStorage":{"type":"Local"}}`.
- `tiered_storage_omitted_when_none` — JSON doesn't contain
  `tieredStorage` when the field is `None`.
- `minimum_required_spec_parses_without_tiered_storage` — the existing
  minimum-spec test must keep passing (regression guard for default
  behavior).

### `render_broker_toml` (`crates/operator/src/controller/listeners.rs`)

- `render_broker_toml_emits_remote_storage_when_tiered_local` — when
  `tiered_storage = Some(local)`, the rendered TOML contains the
  `[remote_storage]` block with the canonical path.
- `render_broker_toml_omits_remote_storage_when_tiered_none` —
  rendered TOML has no `[remote_storage]` block (byte-equal regression
  guard).

### `render_configmap` (`crates/operator/src/controller/common.rs`)

- `configmap_includes_remote_storage_block_when_tiered_local` —
  smoke-test the full `Kafka` → `ConfigMap` path.

### `kafka_node_pool` (`crates/operator/src/controller/kafka_node_pool.rs`)

- `pod_template_mounts_tier_storage_emptydir_when_tiered_set` —
  given a parent `Kafka` with `tieredStorage`, the rendered
  `StatefulSet` carries a `tier-storage` `emptyDir` volume and the
  broker container has the matching read-write `volumeMount`.
- `pod_template_omits_tier_storage_when_tiered_none` — non-tiered
  parent has neither (byte-equal pre-48g render).

### `gen_crds` snapshot

The CRD YAML snapshot (`deploy/crds/crabka.io_kafkas.yaml`) regenerates
to include the `tieredStorage` schema. Adding the field is a
non-breaking schema addition — the YAML grows but is not invalidated.

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-operator`
- `cargo test -p crabka-broker -p crabka-log -p crabka-remote-storage`
  (no regressions)
- `cargo run --bin gen-crds` regenerates
  `deploy/crds/crabka.io_kafkas.yaml`; the YAML is committed.
- No drift on other CRDs.
