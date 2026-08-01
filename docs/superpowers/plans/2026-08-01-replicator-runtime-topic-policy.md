# Replicator Runtime and Topic Policy Implementation Plan

**Goal:** Configure all replicator-owned production timing, retry, batching,
transport, and topic-creation policy while preserving source partition shape.

**Architecture:** Flatten one validated `ReplicatorRuntimePolicy` into the
binary CLI/environment surface and thread it through existing owners. Retain
source partition counts during discovery rather than exposing topology as a
knob.

**Tech Stack:** Rust, Clap, crabka-units, refined_type, Kafka admin/client APIs.

### Task 1: Validated process policy

- [x] Add failing default, override, environment-precedence, and invalid-value tests.
- [x] Add UOM timing/transport fields, positive counts, and refined replication factors.
- [x] Validate retry ordering and preserve every existing default.
- [x] Run focused tests and strict replicator Clippy; commit.

### Task 2: Admin, source, and drain policy

- [ ] Pass topic timeout, client transport, source poll, and drain settings through shared helpers.
- [ ] Pass data/internal replication factors to the correct topic owners.
- [ ] Preserve default wrappers for library callers.
- [ ] Add focused behavior tests; run strict replicator Clippy; commit.

### Task 3: Worker and background cadence policy

- [ ] Pass build retry, commit, batch, supervisor, heartbeat, and checkpoint values through existing parameters.
- [ ] Add focused default/override and retry-bound tests.
- [ ] Run affected tests and strict replicator Clippy; commit.

### Task 4: Source partition topology

- [ ] Retain source topic partition counts during metadata discovery.
- [ ] Create target data topics with source counts and configured replication factor.
- [ ] Prove records retain their source partition and internal topics remain single-partition.
- [ ] Run replicator all-target tests and strict Clippy; commit.

### Task 5: Closure

- [ ] Run replicator all-target tests.
- [ ] Run workspace all-target check and strict warnings-as-errors Clippy.
- [ ] Run nightly formatting and `git diff --check`.
- [ ] Update `docs/configuration-audit.md` with the surface and evidence.
- [ ] Commit closure documents; leave the broader repository goal active.
