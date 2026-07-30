# Client Resource Policy Deployment Propagation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Expose one validated Kafka client queue/frame policy per deployed
process and the approved isolated-fetch minima, preserving every current
default and forwarding values to all private components.

**Architecture:** Each binary owns raw CLI/environment values, validates them
before startup I/O using client-core types, then passes typed values through
existing config structs. Libraries remain environment-free. Private secondary
clients reuse their process policy rather than adding component-specific
settings.

**Tech Stack:** Rust, Clap, existing environment parsing helpers,
`crabka-units`, `refined_type`, Cargo tests.

## Global Constraints

- Defaults remain queue `64`, frame maximum `100MiB`, and fetch minimum `1B`.
- CLI suffixes are `client-dispatch-queue-capacity` and `client-frame-max`.
- Environment suffixes are `CLIENT_DISPATCH_QUEUE_CAPACITY` and
  `CLIENT_FRAME_MAX`, under each binary's established prefix.
- Frame and fetch inputs use human `ByteSize` syntax. Never add raw byte-count
  flags for dimensioned values.
- Validate before DNS, listener bind, file mutation, or broker I/O.
- One process gets one queue/frame pair. Private writer, reader, coordinator,
  retry, reconnect, and recovery clients reuse it.
- Only approved isolated-fetch owners get fetch-minimum settings.
- Keep the fixed `100MiB` client-core frame security ceiling non-configurable.
- Preserve the four unrelated untracked plans dated `2026-07-28`.
- Run Cargo with
  `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

## Approved Surfaces

| Process | Queue/frame environment prefix | Extra fetch minimum |
|---|---|---|
| bench-driver | `CRABKA_BENCH_DRIVER_` | none |
| broker | `CRABKA_BROKER_` | none |
| gres | `CRABKA_GRES_` | `FDW_FETCH_MIN`, `WAL_RECOVERY_FETCH_MIN`, `REGISTRY_READER_FETCH_MIN` |
| grpc-gateway | `CRABKA_GRPC_GATEWAY_` | none |
| metrics | `CRABKA_METRICS_` | none |
| metrics-service | `CRABKA_METRICS_SERVICE_` | none |
| observability-demo-app | `CRABKA_DEMO_` | `STREAMS_FETCH_MIN` |
| profiles | `CRABKA_PROFILES_` | none |
| rebalancer | `CRABKA_REBALANCER_` | none |
| replicator | `CRABKA_REPLICATOR_` | none |
| schema-registry | `CRABKA_SCHEMA_REGISTRY_` | none |
| traces | `CRABKA_TRACES_` | none |

If a binary's checked-in parser uses a different established prefix, preserve
that prefix and update this table/audit rather than introducing an alias.

---

### Task 1: Add Shared Parser-Test Conventions Without a Shared Parser

**Files:**
- Inspect: existing parser helpers in the listed binaries
- Modify: no shared crate unless an identical existing helper already owns
  `ByteSize` environment parsing

- [ ] Inventory existing CLI/environment precedence tests and human-UOM parsers.
- [ ] Reuse those local patterns. Do not create a cross-binary config framework.
- [ ] For every process, require tests for default, environment, CLI precedence,
  zero queue, over-ceiling frame, and fractional/non-finite frame where its
  input path can represent them.

---

### Task 2: Propagate Policy Through Producer-Only Deployments

**Files:**
- Modify: `crates/bench-driver/src/main.rs`
- Modify: `crates/bench-driver/src/workload.rs`
- Modify: `crates/metrics/src/bin/crabka-metrics.rs`
- Modify: `crates/metrics-service/src/main.rs`
- Modify: `crates/profiles/src/bin/crabka-profiles.rs`
- Modify: `crates/traces/src/bin/crabka-traces.rs`
- Modify: focused parser/propagation tests beside each owner

- [ ] Add failing parser precedence and invalid-input tests for each binary.
- [ ] Add the two raw CLI/environment values using the binary's existing
  parser pattern.
- [ ] Validate into `ConnectionDispatchQueueCapacity` and `ClientFrameMax`
  before constructing work.
- [ ] Apply the pair to every `Producer::builder()` in the process, including
  workload or role-specific helpers.
- [ ] Re-scan each crate for `Producer::builder()` and prove every production
  hit consumes the process policy.
- [ ] Run each package's all-target tests and commit one coherent package (or
  tightly related small group) at a time.

Suggested commit subjects:

```text
feat(bench-driver): expose client resource policy
feat(metrics): expose client resource policy
feat(profiles): expose client resource policy
feat(traces): expose client resource policy
```

---

### Task 3: Propagate Policy Through Rebalancer

**Files:**
- Modify: `crates/rebalancer/src/bin/rebalancer.rs`
- Modify: `crates/rebalancer/src/ingest/admin_client.rs`
- Modify: `crates/rebalancer/src/executor/client_impl.rs`
- Modify: `crates/rebalancer/src/state_topic/producer.rs`
- Modify: `crates/rebalancer/src/state_topic/loader.rs`
- Modify: the existing rebalancer config structs and tests

- [ ] Write failing tests proving one non-default pair reaches ingest,
  executor, state-topic producer, and state-topic loader clients.
- [ ] Parse and validate the process pair before startup I/O.
- [ ] Store it once in the existing rebalancer config and forward it to all
  five client-owning paths.
- [ ] Verify reconnect/reload paths retain the pair.
- [ ] Run `cargo test -p crabka-rebalancer --all-targets --locked` and commit.

---

### Task 4: Propagate Policy Through gRPC Gateway

**Files:**
- Modify: `crates/grpc-gateway/src/bin/gateway.rs`
- Modify: `crates/grpc-gateway/src/produce.rs`
- Modify: `crates/grpc-gateway/src/dedup/membership.rs`
- Modify: `crates/grpc-gateway/src/dedup/store.rs`
- Modify: `crates/grpc-gateway/src/dedup/mod.rs`
- Modify: focused parser and role/path tests

- [ ] Add failing tests for default/env/CLI precedence and propagation to
  direct production plus every dedup producer.
- [ ] Validate once at gateway startup and store the typed pair in its existing
  application configuration.
- [ ] Forward the same pair to all `Producer::builder()` calls.
- [ ] Run `cargo test -p crabka-grpc-gateway --all-targets --locked` and commit.

---

### Task 5: Propagate Policy Through Schema Registry

**Files:**
- Modify: `crates/schema-registry/src/bin/schema-registry.rs`
- Modify: `crates/schema-registry/src/kafkastore/writer.rs`
- Modify: `crates/schema-registry/src/kafkastore/reader.rs`
- Modify: the existing Kafka-store configuration and tests

- [ ] Write failing tests proving the writer producer and reader
  `ConnectionOptions` receive one non-default pair.
- [ ] Add process CLI/environment parsing and early validation.
- [ ] Carry typed values through the existing Kafka-store config into writer,
  reader, and reader reconstruction paths.
- [ ] Keep unrelated HTTP `reqwest::Client` builders out of scope.
- [ ] Run `cargo test -p crabka-schema-registry --all-targets --locked` and
  commit.

---

### Task 6: Propagate Policy Through Replicator and Remote Storage

**Files:**
- Modify: `crates/replicator/src/main.rs`
- Modify: `crates/replicator/src/tasks/checkpoint.rs`
- Modify: replicator config/tests
- Modify: `crates/remote-storage-topic/src/kafka_log.rs`
- Modify: the existing caller/config that constructs `KafkaLog`

- [ ] Trace ownership before editing: determine whether every
  `remote-storage-topic` production client is owned exclusively by broker,
  replicator, or another listed deployment.
- [ ] Write failing propagation tests at each real deployment owner.
- [ ] Add no standalone environment parsing to `remote-storage-topic`; accept
  typed values from its owner.
- [ ] Ensure its producer, metadata client, and raw connection all reuse the
  pair.
- [ ] Run affected package tests and commit by owner.

---

### Task 7: Propagate Policy Through Broker

**Files:**
- Modify: `crates/broker/src/bin/broker.rs`
- Modify: broker runtime config carrying outbound client policy
- Modify: `crates/raft/src/network.rs`
- Modify: `crates/raft/src/controller.rs`
- Modify: broker-owned remote-storage and internal client construction paths
- Modify: focused broker/raft parser and propagation tests

- [ ] Write failing parser tests and non-default propagation tests for broker
  inter-broker, controller/raft, and broker-owned remote-storage clients.
- [ ] Validate the process pair before listener bind.
- [ ] Carry typed policy through existing broker and controller configs.
- [ ] Do not expose broker-side accepted-frame settings under these client
  names; this pair applies only to outbound Kafka client connections.
- [ ] Re-scan broker/raft production sites and document fixed protocol/test
  defaults separately.
- [ ] Run broker and raft all-target tests, including the secured inter-broker
  test, then commit.

---

### Task 8: Propagate Gres Process Policy and Fetch Minima

**Files:**
- Modify: `crates/gres/src/lib.rs`
- Modify: existing Gres CLI/environment parser tests
- Modify: construction of `KafkaFdw`, `LiveRecoveryConfig`, and
  `RegistryPolicy`

- [ ] Add failing tests for:
  - queue/frame defaults, environment, CLI precedence, and invalid values;
  - `fdw-fetch-min`;
  - `wal-recovery-fetch-min`;
  - `registry-reader-fetch-min`; and
  - existing role restrictions.
- [ ] Validate all raw values before listener bind.
- [ ] Apply the process queue/frame pair to FDW, WAL recovery, and registry
  owners using the library setters added in the previous phase.
- [ ] Apply each fetch minimum only to its named owner.
- [ ] Reject supplied role-specific settings when that role cannot consume
  them, preserving existing Gres validation behavior.
- [ ] Ensure Gres-local direct `ConnectionOptions` and `IsolatedFetch` sites
  reuse the corresponding typed policies.
- [ ] Run `cargo test -p crabka-gres --all-targets --locked` and the affected
  Gres library packages, then commit.

---

### Task 9: Propagate Observability Demo Streams Policy

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Modify: role/config tests

- [ ] Add the process queue/frame pair plus
  `--streams-fetch-min` / `CRABKA_DEMO_STREAMS_FETCH_MIN`.
- [ ] Validate before role startup.
- [ ] Pass values to `KafkaStreams::builder()` only for the Stream role.
- [ ] Preserve or add explicit rejection when the fetch-minimum is supplied to
  a role without streams.
- [ ] Run package all-target tests and commit.

---

### Task 10: Audit Deployment Ownership and Verify

- [ ] Re-run:

```bash
rg -n 'Client::builder\(|Producer::builder\(|ConnectionOptions \{|IsolatedFetch \{' \
  crates --glob '*.rs'
```

- [ ] Every production hit must now be:
  - fed by a deployed process's typed policy;
  - a CRD rendering path deferred to the final plan; or
  - fixed protocol/test behavior documented in the audit.
- [ ] Update `docs/configuration-audit.md` with exact CLI/environment names and
  remaining CRD-only work.
- [ ] Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
```

- [ ] Run affected package all-target tests after the final formatting pass.
- [ ] Commit the audit:

```bash
git add docs/configuration-audit.md
git commit -m "docs(config): record client deployment policy"
```

After this plan passes, write the final Kafka/Gres CRD schema and rendered
argument plan. Do not run `cargo clean` until that CRD phase and the final
whole-repo audit are complete.
