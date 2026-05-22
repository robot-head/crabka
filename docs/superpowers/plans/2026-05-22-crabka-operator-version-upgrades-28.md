# Slice 28: Operator — Version upgrades — Implementation plan

Design: `docs/superpowers/specs/2026-05-22-crabka-operator-version-upgrades-28-design.md`.

## Batches

### Batch 1 (parallel — disjoint files)

- **T1 — version model.** New `crates/operator/src/version.rs`
  (`KafkaVersion::parse`, `metadata_key`, `short`; `evaluate` with the four
  rules) + `pub mod version;` in `lib.rs`. Full unit tests in-module.
- **T2 — KafkaSpec/KafkaStatus literal sweep.** Add
  `metadata_version: None,` to every `KafkaSpec { .. }` literal in test
  fixtures that this slice does not otherwise edit
  (`controller/topic.rs`, `controller/kafka_node_pool.rs`,
  `controller/listeners.rs`, `controller/network_policy.rs`,
  `controller/metrics.rs`, and `tests/reconcile_*.rs`). (CRD field itself
  added in T3.)

### Batch 2 (sequential — shared files, depends on T1+T2)

- **T3 — CRD.** `crd/kafka.rs`: add `spec.metadata_version`,
  `status.kafka_version`, `status.metadata_version`; update the in-file
  literals + round-trip tests.
- **T4 — common.rs.** `combined_config_hash` 4th arg (explicit pin,
  empty-collapse preserved); `render_configmap` `metadata_version` arg
  (merge `metadata.version` into server_properties); `plan_rollout` +
  `PoolRolloutState` with unit tests; update hash call sites/tests.
- **T5 — kafka.rs reconciler.** Wire `version::evaluate`, the
  `KafkaVersionValid` condition, status `kafkaVersion`/`metadataVersion`,
  the `render_configmap`/`combined_config_hash` call updates, and the
  ordered `adopt_pools` via `plan_rollout`.

### Batch 3 (sequential)

- **T6 — tests.** New integration cases in `tests/reconcile_kafka.rs`.
- **T7 — regen + e2e + status.** `tools/regen-crds.sh`; operator-e2e probe;
  STATUS.md entry; full `cargo test`/`clippy`/`fmt`.

## Notes

- No new API requests in reconcile: `evaluate` reads `obj.status`;
  `plan_rollout` uses already-listed pools. FIFO-mock sequences unchanged.
- Greenfield: update existing tests freely; no back-compat shims.
