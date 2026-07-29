# Branch-Wide UOM Configuration Boundaries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convert every dimensioned configuration boundary added on
`configuration_expose` from unit-suffixed primitives to explicit UOM values,
with zero unresolved branch-diff audit results.

**Architecture:** Reuse `crabka-units` parsing, human serde adapters, and UOM
types at CLI, environment, file-config, CRD, Compose, and manifest boundaries.
Lower quantities once into refined protocol integers where exact wire domains
require them. Migrate by owner so each commit is independently testable.

**Tech Stack:** Rust 2024, Clap, Serde, Schemars, `uom` through
`crabka-units`, `refined_type`, Kubernetes CRDs, Docker Compose.

## Global Constraints

- Preserve every existing physical default.
- Require explicit units on every nonzero dimensioned operator value.
- CLI overrides environment.
- Remove unit suffixes from branch-added names; add no compatibility aliases.
- Keep counts, IDs, offsets, booleans, enums, paths, addresses, timestamps, and
  Kubernetes-native probe fields primitive.
- Keep protocol and format invariants non-configurable.
- Reject invalid signs, nonfinite values, wrong dimensions, fractional
  protocol units, and overflow before external I/O.
- Use `refined_type` only for dimensionless or lowered primitive newtypes.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; add `--locked` to lock-aware
  commands.
- Preserve unrelated work and stage only task paths.

## Audit Baseline

Snapshot:

```text
merge-base: 1d171e99ac73cebdb944479d0d249b816e55a454
audited head: dfb46b262173bb933f95130bc4ebd5b363f2b68b
branch commits: 361
changed files: 908
unique branch-added environment variables: 244
dimensioned boundary appearances: 192
already UOM-compliant appearances: 20
raw migration candidates: 172
```

Before each batch, rerun the focused branch-diff inventory against the recorded
merge-base. Reclassify renamed or deleted entries rather than assuming the
initial count remains stable.

## Task 1: Share validated UOM boundary parsers

**Files:**

- `crates/units/src/parse.rs`
- `crates/units/src/lib.rs`
- `crates/units/src/serde_units.rs` only if an adapter is missing
- `crates/grpc-gateway/src/config_value.rs`
- `crates/grpc-gateway/src/bin/gateway.rs`
- `crates/schema-registry/src/config_value.rs`
- `crates/schema-registry/src/bin/schema-registry.rs`

- [ ] Add failing `crabka-units` tests for reusable positive/nonnegative parsers
  over `Time`, `ByteSize`, `ByteRate`, and `Ratio`.

- [ ] Prove:

  - explicit units parse;
  - bare nonzero values and wrong dimensions fail;
  - zero follows the positive/nonnegative variant;
  - negative, `NaN`, and infinity fail; and
  - returned values are the UOM quantity, not a wrapper primitive.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-units config_quantity --locked
```

- [ ] Add only the shared helpers actually required by the owner inventory.
  Build them from the existing `parse::{time, byte_size, byte_rate, ratio}`
  functions; do not add a configuration framework.

- [ ] Replace gateway-local duplicate CLI parsers where the shared helper is
  identical. Convert schema-registry `PositiveTime` and `PositiveSize` CLI
  wrappers to direct UOM fields if doing so deletes code without weakening
  validation. Retain refined protocol integer types.

- [ ] Verify:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test --locked -p crabka-units -p crabka-grpc-gateway \
  -p crabka-schema-registry --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --locked -p crabka-units -p crabka-grpc-gateway \
  -p crabka-schema-registry --all-targets -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

- [ ] Commit:

```bash
git add crates/units crates/grpc-gateway crates/schema-registry Cargo.lock
git commit -m "feat(units): validate config quantities"
```

## Task 2: Migrate admin UI and operator runtime settings

**Files:**

- `crates/admin-ui/src/config.rs`
- admin UI callers and tests selected by `rg`
- `crates/operator/src/config.rs`
- operator controller callers and tests selected by `rg`
- checked-in deployment/config examples containing the renamed variables

- [ ] Add failing CLI/environment tests for:

```text
CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT=1MiB
CRABKA_ADMIN_UI_SESSION_TTL=8h
CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT=30s
PGDOG_RELOAD_BACKOFF=100ms
PGDOG_RELOAD_REQUEUE=15s
PGDOG_ADMIN_TIMEOUT=20s
PGDOG_TRANSITION_POLL=1m
CONTROLLER_ERROR_REQUEUE=15s
```

- [ ] Prove defaults, CLI-over-environment precedence, explicit-unit
  enforcement, positivity, and protocol lowering for the topic-admin timeout.

- [ ] Replace the admin UI time/size primitive wrappers with direct `Time` and
  `ByteSize`. Keep monotonic-clock representability and Kafka `i32`
  millisecond checks at their use sites.

- [ ] Replace operator runtime millisecond wrappers with `Time`. Rename Rust
  fields, flags, variables, manifests, and tests without aliases.

- [ ] Verify both packages, strict Clippy, help output, formatting, and lockfile
  stability.

- [ ] Commit:

```bash
git add crates/admin-ui crates/operator deploy demo docs Cargo.lock
git commit -m "feat(config): use UOM for admin runtime"
```

Stage only paths actually changed; do not add unrelated documentation plans.

## Task 3: Migrate bench-driver and observability-demo boundaries

**Files:**

- `crates/bench-driver/src/main.rs`
- `crates/bench-driver/src/workload.rs`
- bench manifests/config/tests selected by `rg`
- `crates/observability-demo-app/src/main.rs`
- `crates/observability-demo-app/tests`
- `demo/observability/docker-compose.yml`

- [ ] Add failing tests for unit-bearing bench settings:

```text
BENCH_PROMETHEUS_REQUEST_TIMEOUT=5s
BENCH_PRODUCER_REQUEST_TIMEOUT=5s
BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT=30s
BENCH_CONSUMER_REQUEST_TIMEOUT=5s
BENCH_CONSUMER_BUILD_INITIAL_BACKOFF=100ms
BENCH_CONSUMER_BUILD_MAX_BACKOFF=2s
BENCH_CONSUMER_POLL_TIMEOUT=500ms
BENCH_CONSUMER_POLL_ERROR_BACKOFF=100ms
BENCH_SAMPLE_INTERVAL=1s
```

- [ ] Replace time-suffixed bench newtypes with direct `Time` where no
  primitive protocol domain is being modeled. Preserve retry-policy ordering
  and deadline checks.

- [ ] Add failing demo tests and migrate:

  - consumer leave and metadata-refresh timeouts;
  - streams DNS, poll, commit, rebalance, leave-heartbeat, and join-backoff
    settings; and
  - streams state-store cache maximum to `ByteSize`.

- [ ] Keep queue capacity and all other counts refined/dimensionless.

- [ ] Update Compose and benchmark manifests to explicit units, verify rendered
  defaults and overrides, then run affected tests and strict Clippy.

- [ ] Commit:

```bash
git add crates/bench-driver crates/observability-demo-app demo deploy docs Cargo.lock
git commit -m "feat(config): use UOM for bench and demo"
```

## Task 4: Migrate traces and profiles boundaries

**Files:**

- `crates/blockstore/src/index_snapshot.rs`
- `crates/blockstore/src/reader.rs`
- `crates/client-consumer/src/consumer.rs`
- `crates/traces/src/bin/crabka-traces.rs`
- `crates/traces/src/querier/store.rs`
- `crates/profiles/src/bin/crabka-profiles.rs`
- `crates/profiles/src/blockbuilder.rs`
- `crates/observability-demo-app/tests/observability_demo_config.rs`
- `demo/observability/docker-compose.yml`

- [ ] Add failing CLI/environment tests for:

```text
CRABKA_TRACES_WAL_FETCH_MAX=2MiB
CRABKA_TRACES_WAL_FETCH_PARTITION_MAX=256KiB
CRABKA_TRACES_INDEX_SNAPSHOT_MAX=256MiB
CRABKA_TRACES_BLOCK_READ_MAX=1GiB
CRABKA_TRACES_SCAN_CONCAT_MAX=1.5GB
CRABKA_PROFILES_WAL_FETCH_MAX=2MiB
CRABKA_PROFILES_WAL_FETCH_PARTITION_MAX=256KiB
CRABKA_PROFILES_INDEX_SNAPSHOT_MAX=256MiB
CRABKA_PROFILES_WAL_POLL_TIMEOUT=500ms
```

- [ ] Make CLI fields `ByteSize` or `Time`. Remove the recently added
  primitive-backed size wrappers where they have no protocol role.

- [ ] Keep classic consumer fetch newtypes only as exact positive-`i32` Kafka
  lowering types. Add `TryFrom<ByteSize>` or equivalent checked constructors
  and reject fractional bytes or overflow before consumer construction.

- [ ] Keep the 1.5-GB scan-concatenation ceiling invariant while storing the
  configured cap as `ByteSize`.

- [ ] Add the approved profiles WAL poll timeout to block-builder, querier, and
  query-frontend paths as `Time`.

- [ ] Update only the demo services that own each setting. Verify exact
  defaults, overrides, help entries, and the existing exact-cap behavior.

- [ ] Run all-target tests and strict Clippy for blockstore, consumer, traces,
  profiles, and demo.

- [ ] Commit:

```bash
git add crates/blockstore crates/client-consumer crates/traces crates/profiles \
  crates/observability-demo-app demo/observability Cargo.lock
git commit -m "feat(observability): use UOM config values"
```

## Task 5: Migrate broker time boundaries

**Files:**

- `crates/broker/src/bin/broker.rs`
- `crates/broker/src/file_config.rs`
- `crates/broker/src/config.rs`
- `crates/broker/src/config_value.rs`
- broker callers/tests selected by the focused inventory
- `crates/operator/src/crd/kafka.rs`
- Kafka controller/config rendering and tests
- `deploy/crds/kafka.yaml`
- broker examples and deployment manifests

- [ ] Generate a focused list of every branch-added broker field or variable
  ending in `_ms`, `_secs`, or `_seconds`, plus semantic names containing
  timeout, interval, delay, deadline, backoff, TTL, linger, or window.

- [ ] Add table-driven failing boundary tests covering each name and every
  distinct validation domain:

  - positive arbitrary `Time`;
  - nonnegative `Time`;
  - whole-millisecond `i32` protocol time;
  - whole-millisecond `i64` time; and
  - ordered min/max pairs.

- [ ] Convert `RuntimeArgs` to direct `Time` and unitless flags/environment
  names. Replace `PositiveMillis` use at the CLI boundary.

- [ ] Convert branch-added human file-config fields to `Time` with human serde
  adapters and unitless keys. Preserve compatibility-only external Kafka
  record fields as documented exceptions.

- [ ] Convert time fields in `BrokerTuning` to `Time` string-valued CRD fields.
  Update controller rendering to unitless environment names and human values.

- [ ] Preserve runtime `BrokerConfig` UOM values and existing cross-field
  validation. Lower only at protocol/verified arithmetic seams.

- [ ] Regenerate the Kafka CRD with:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run --locked -p crabka-operator -- gen-crds deploy/crds
```

- [ ] Verify broker/operator all-target tests, strict Clippy, generated CRD
  cleanliness, representative config-file round trips, help output, and
  manifests.

- [ ] Commit:

```bash
git add crates/broker crates/operator deploy/crds deploy docs Cargo.lock
git commit -m "feat(broker): use UOM time configuration"
```

## Task 6: Migrate broker size, rate, and ratio boundaries

Use the same Task 5 files and inventory.

- [ ] Add table-driven failing tests for every branch-added tunable byte cap,
  buffer, chunk, body, snapshot, fetch, and output bound.

- [ ] Convert CLI/environment, file-config, and `BrokerTuning` values to
  `ByteSize` with unitless names and human CRD/config serialization.

- [ ] Convert throughput values to `ByteRate` and decompression proportions to
  `Ratio`. Keep entry capacities and offset windows dimensionless.

- [ ] Preserve exact `i32`, `u32`, and `usize` constraints at socket, Kafka
  protocol, allocator, and collection seams. Reject fractional bytes and
  overflow during config validation.

- [ ] Regenerate CRDs and verify broker/operator tests, strict Clippy, config
  round trips, help, manifests, formatting, and lockfile changes.

- [ ] Commit:

```bash
git add crates/broker crates/operator deploy/crds deploy docs Cargo.lock
git commit -m "feat(broker): use UOM size configuration"
```

## Task 7: Migrate Gres time boundaries

**Files:**

- `crates/gres/src/lib.rs`
- `crates/cli/src/gres.rs`
- `crates/gres-activator/src/main.rs`
- `crates/gres-loadtest/src/main.rs`
- `crates/gres-control` policy and controller seams
- `crates/operator/src/crd/gres.rs`
- `crates/operator/src/crd/kafka.rs` Gres registry policy
- Gres operator controllers/tests
- `deploy/crds/gres.yaml`
- Gres manifests/examples/docs selected by `rg`

- [ ] Inventory every branch-added Gres `_MS` boundary once, deduplicating the
  shared registry settings repeated across binaries.

- [ ] Add failing tests for unit-bearing:

  - registry topic, retry, fetch-wait, and DNS times;
  - activator polling and cold-start timeout;
  - PgDog cold-start, idle, suspension, and lifetime policy;
  - WAL recovery/admin/producer timeouts and backoffs;
  - checkpoint/delete/poll and suspension polling;
  - local-vacuum cadence; and
  - range-zero follower polling.

- [ ] Convert CLI and runtime boundary fields to `Time`, remove suffixes from
  environment/flag names, and reuse the same registry policy conversion in all
  binaries.

- [ ] Convert Gres and Kafka Gres-registry CRD time fields to human `Time`
  strings. Preserve Kubernetes probe seconds and absolute timestamps.

- [ ] Lower whole-millisecond Kafka fields through existing refined policy
  constructors and reject fractional/overflow values.

- [ ] Regenerate CRDs, render manifests, and run all-target tests plus strict
  Clippy for Gres, CLI, activator, loadtest, Gres control, and operator.

- [ ] Commit:

```bash
git add crates/gres crates/cli crates/gres-activator crates/gres-loadtest \
  crates/gres-control crates/operator deploy/crds deploy docs Cargo.lock
git commit -m "feat(gres): use UOM time configuration"
```

## Task 8: Migrate Gres size boundaries

Use the Task 7 owner files.

- [ ] Add failing tests for unit-bearing fetch caps, producer batches,
  checkpoint totals/parts, and Gres sizing thresholds.

- [ ] Convert every tunable size boundary to `ByteSize`, remove `_BYTES` from
  branch-added external names, and use human CRD/config serialization.

- [ ] Keep frame counts, key budgets, retries, replication factors, and record
  counts dimensionless.

- [ ] Lower Kafka fetch/batch sizes to exact positive `i32`/`usize` values only
  at policy or protocol seams.

- [ ] Regenerate CRDs and repeat the Task 7 package/manifests quality gates.

- [ ] Commit:

```bash
git add crates/gres crates/cli crates/gres-activator crates/gres-loadtest \
  crates/gres-control crates/operator deploy/crds deploy docs Cargo.lock
git commit -m "feat(gres): use UOM size configuration"
```

## Task 9: Close schema-registry, gateway, and remaining CRD gaps

**Files:**

- `crates/operator/src/crd/schema_registry.rs`
- `crates/operator/src/crd/grpc_gateway.rs`
- `crates/operator/src/crd/kafka.rs`
- relevant controllers/tests
- `crates/schema-registry`
- `crates/grpc-gateway`
- generated CRDs and examples

- [ ] Convert remaining schema-registry runtime CRD millisecond and byte fields
  to `Time`/`ByteSize` human strings with unitless field names.

- [ ] Convert gateway basis-point configuration to `Ratio` where it is an
  operator-selected proportion. Keep Kubernetes health-check seconds primitive.

- [ ] Verify controller-rendered environment names match the already-unitized
  binaries and values render with explicit units.

- [ ] Remove superseded wrappers, conversion helpers, and primitive schema
  ranges only when no caller remains.

- [ ] Regenerate CRDs; run schema-registry, gateway, Kafka/Gres CRD, and
  operator tests plus strict Clippy.

- [ ] Commit:

```bash
git add crates/schema-registry crates/grpc-gateway crates/operator \
  deploy/crds deploy docs Cargo.lock
git commit -m "feat(operator): use UOM runtime fields"
```

## Task 10: Branch-wide deployment and documentation reconciliation

- [ ] Search every changed file from the recorded merge-base for old
  unit-suffixed names. Update remaining Compose, Kubernetes, shell, CI,
  examples, specs, plans, and audit references that describe the live
  configuration contract.

- [ ] Do not rewrite historical prose that only records a superseded audit
  count unless it would mislead a current operator. Mark deliberate historical
  references as such.

- [ ] Render every checked-in Compose file changed by the branch and run
  operator manifest/CRD generation checks.

- [ ] Verify `--help` once per changed binary and assert each renamed flag
  appears exactly once.

- [ ] Commit:

```bash
git add demo deploy docs crates
git commit -m "docs(config): reconcile UOM names"
```

## Task 11: Zero-unresolved audit and final verification

- [ ] Recreate the branch diff from the recorded merge-base and inventory all
  added configuration definitions:

```bash
base=1d171e99ac73cebdb944479d0d249b816e55a454
git diff --unified=0 "$base"..HEAD -- \
  '*.rs' '*.toml' '*.yaml' '*.yml' '*.json' '*.md'
```

- [ ] Search names and field types for:

```text
_MS
_MILLIS
_SECONDS
_SECS
_BYTES
_BYTES_PER_SEC
timeout
interval
delay
deadline
backoff
linger
window
ttl
rate
ratio
Time
ByteSize
ByteRate
Frequency
Ratio
```

- [ ] Classify every result as UOM configuration flow, dimensionless,
  external-contract exception, invariant, historical documentation, or
  unresolved. Record every exception by field and reason. Require zero
  unresolved results.

- [ ] Update `docs/configuration-audit.md` with:

  - baseline and final counts;
  - owner-by-owner migration summary;
  - renamed public surfaces;
  - exceptions;
  - exact focused searches; and
  - verification evidence.

- [ ] Run owner-combined tests after each batch, then the final workspace gates:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test --workspace --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run --locked -p crabka-operator -- gen-crds /tmp/crabka-uom-crds
diff -ru /tmp/crabka-uom-crds deploy/crds
git diff --check
git diff -- Cargo.lock
tools/audit-runtime-values.sh
```

- [ ] Run all changed Compose rendering checks and representative default and
  override renders for every renamed environment variable family.

- [ ] Commit the audit:

```bash
git add docs/configuration-audit.md
git commit -m "docs(audit): record branch-wide UOM boundaries"
```

- [ ] Fetch `origin/configuration_expose`. Rebase only if it advanced, rerun
  affected verification after any conflict, push, fetch again, and verify local
  and remote HEADs match.

## Review Checkpoints

Stop for review after Tasks 1, 4, 6, 9, and 11. Each checkpoint reports:

- migrated owner and exact external renames;
- tests and strict Clippy evidence;
- remaining raw candidate count;
- exception count; and
- lockfile/CRD changes.
