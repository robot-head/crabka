# Gres Compute Registry Reader Policy

**Goal:** Remove the remaining hardcoded Kafka fetch policy from live Gres compute registry readers and route the already validated, Kafka-owned `RegistryPolicy` through every production read path.

**Constraints:** Add no new configuration surface or dependency. Reuse `RegistryPolicy`; its values already come from the Kafka CRD and from exact CLI/environment bindings. Preserve in-memory behavior and Kafka security forwarding. Use TDD, keep the change minimal, and run all Cargo commands with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

## Task 1: Route the shared policy through every live reader

**Files:**

- Modify: `crates/gres/src/lib.rs`
- Modify only as required by the public test seam: `crates/gres/tests/runtime.rs`

- [x] Add failing tests proving a non-default policy reaches tenant-config loads and split-operation fetch settings.
- [x] Extend `TenantConfigLoader::load_tenant_config` to receive `&RegistryPolicy`; prove `load_substrate_tenant_record` forwards `ServeArgs.registry.policy()`.
- [x] Store/forward `config.registry_policy` in ordinary and must-activate `LiveRangeRegistrySource` paths.
- [x] Pass the same policy into split-operation discovery and use its `fetch_max_wait_ms()` and `fetch_partition_max_bytes()` getters.
- [x] Remove `TENANT_CONFIG_FETCH_MAX_WAIT_MS` and `TENANT_CONFIG_FETCH_PARTITION_MAX_BYTES`; leave zero production references.
- [x] Preserve security, client IDs, topic/partition protocol invariants, and in-memory behavior.
- [x] Run focused tests, full `crabka-gres` nextest, strict all-target/all-feature Clippy, nightly formatting, and `git diff --check`.
- [x] Commit only implementation files.

## Task 2: Independent review and audit closure

- [x] Review specification compliance and code quality independently.
- [x] Remediate all findings with a fresh implementer and re-review.
- [x] Confirm the runtime-value scanner has no remaining Gres compute fetch defaults outside `RegistryPolicy`.
- [x] Record exact verification evidence and the next owner: compute checkpoint precedence.
