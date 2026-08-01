# Gres Registry Runtime Configuration Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to implement this plan task by
> task.

**Goal:** Make the shared Gres Kafka registry policy explicit, validated, and
consistent for every production creator/reader so the first caller cannot
silently defeat later configuration.

**Architecture:** `crabka-gres-control` owns one small `RegistryPolicy` value
and stores it in each `Registry`. The cluster-wide Kubernetes source of truth
is `Kafka.spec.gresRegistry`, because `__gres_tenants` belongs to a Kafka
cluster and may be shared by several `Gres` fleets. Standalone binaries expose
the same fields through CLI options backed by environment variables. The
operator passes the effective Kafka policy to activators, computes, and its
cached tenant-control handle; cache entries are replaced when policy changes.

**Tech Stack:** Rust 2024, `refined_type` 0.6, Clap, kube/schemars CRDs,
Cargo nextest.

## Constraints

- Preserve defaults: replication factor `1`, topic-create timeout `15000ms`,
  reader retry `250ms`, fetch wait `500ms`, and fetch partition maximum
  `1048576` bytes.
- New validated newtypes use `refined_type`; never use `Refined::unsafe_new`.
- One registry policy owns registry replication. Tenant WAL replication must
  never be passed to registry-topic creation.
- `Kafka.spec.gresRegistry` is authoritative for operator-managed workloads.
  Remove the newly introduced activator-scoped replication field rather than
  retaining two conflicting v1alpha1 sources.
- Every standalone production caller receives CLI options with environment
  backing. CLI wins over environment.
- Preserve registry topic name, one partition, compact cleanup, partition
  zero, read-committed isolation, Kafka error code 36, transactional/client
  identities, idempotence, and `Acks::All` as compatibility/integrity
  invariants.
- Preserve per-tenant config-topic replication as tenant WAL policy. It is
  distinct from the shared registry policy.
- Use `assert2::assert!`; add no lint suppressions or dependencies beyond the
  workspace-owned `refined_type` and existing test helpers.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

## Effective Settings

| Setting | CLI/environment suffix | Kafka CRD field | Default | Constraint |
|---|---|---|---:|---|
| registry replication | `REGISTRY_REPLICATION_FACTOR` | `spec.gresRegistry.replicationFactor` | 1 | `1..=32767` |
| topic-create timeout | `REGISTRY_TOPIC_CREATE_TIMEOUT_MS` | `spec.gresRegistry.topicCreateTimeoutMs` | 15000 | positive `i32` |
| reader retry backoff | `REGISTRY_READER_RETRY_BACKOFF_MS` | `spec.gresRegistry.readerRetryBackoffMs` | 250 | positive `u64` |
| fetch max wait | `REGISTRY_FETCH_MAX_WAIT_MS` | `spec.gresRegistry.fetchMaxWaitMs` | 500 | positive `i32` |
| fetch partition bytes | `REGISTRY_FETCH_PARTITION_MAX_BYTES` | `spec.gresRegistry.fetchPartitionMaxBytes` | 1048576 | positive `i32` |

Every standalone binary uses the exact common names
`CRABKA_GRES_REGISTRY_REPLICATION_FACTOR`,
`CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT_MS`,
`CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF_MS`,
`CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT_MS`, and
`CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX_BYTES`. The operator renders the
activator/compute process arguments explicitly from the Kafka CR.

---

### Task 1: Centralize and Validate Registry Policy

**Files:**

- Modify: `crates/gres-control/Cargo.toml`
- Modify: `crates/gres-control/src/registry.rs`
- Modify: `crates/gres-control/src/lib.rs`
- Modify: affected `gres-control` tests

- [x] **Step 1: Add failing policy and propagation tests**

Cover:

- exact defaults;
- zero rejection for every positive field;
- replication bounds `1..=32767`;
- custom topic-create timeout reaches `AdminClient::create_topics`;
- custom fetch wait/bytes reach foreground refresh and the background reader;
- custom retry backoff reaches every reader failure branch; and
- an existing registry topic whose observed replication differs from policy
  returns an explicit immutable-policy mismatch instead of silently accepting
  the first creator's value.

Introduce the minimum fake/seam needed to observe request values. Do not build
a general client abstraction.

- [x] **Step 2: Add `RegistryPolicy`**

Add one public, cloneable/equatable policy with a validating constructor and
default. Use `refined_type` rules at construction; store ordinary validated
scalars/durations so consumers do not depend on generic refined internals.
Export the small refined, `FromStr`-capable scalar wrappers; do not add Clap to
`gres-control`.

Make the explicit API:

```rust
Registry::connect_with_policy(bootstrap, policy)
Registry::connect(bootstrap) // compatibility shorthand using Default
Registry::policy()
Registry::ensure_topic()     // no replication argument
```

Store the policy in `Registry` and thread it through topic creation, refresh,
and `spawn_reader`. Keep the shorthand only for tests and callers not yet
migrated in this task; the final audit must find no production use that hides
runtime policy. After topic creation or `TOPIC_ALREADY_EXISTS`, inspect
metadata and reject a nonzero observed replica count that differs from policy.
Replica reassignment is out of scope; silent mismatch is not.

Removing the `ensure_topic` parameter is verified by compilation and the
Task 4 semantic/`rg` audit. Do not add compile-fail tooling solely to make an
API-shape test red.

- [x] **Step 3: Separate shared and per-tenant replication**

Change every shared registry ensure to use the stored registry replication.
Keep `upsert_tenant_config(record, record.wal_replication)` unchanged for the
per-tenant compacted config topic. Add a regression test proving WAL
replication never reaches `__gres_tenants`.

- [x] **Step 4: Verify Task 1**

Run focused `gres-control` tests, strict all-target Clippy, nightly formatting,
and `git diff --check`.

---

### Task 2: Expose Standalone Process Inputs

**Files:**

- Modify: `crates/gres-activator/src/config_value.rs`
- Modify: `crates/gres-activator/src/main.rs`
- Modify: `crates/gres/Cargo.toml`
- Modify: `crates/gres/src/lib.rs`
- Modify: `crates/cli/Cargo.toml`
- Modify: `crates/cli/src/gres.rs`
- Modify: `crates/gres-loadtest/src/main.rs`
- Modify: `crates/gres-loadtest/src/runner.rs`
- Modify: `crates/gres-loadtest/src/cluster.rs`
- Modify: focused tests in those crates

- [x] **Step 1: Add failing parsing/precedence tests**

For activator, compute, main CLI, and loadtest, cover:

- exact defaults;
- environment-only parsing;
- CLI-over-environment precedence;
- zero/overflow rejection; and
- conversion into an equal `RegistryPolicy`.

Use each binary's existing environment prefix. Do not introduce a
configuration framework.

- [x] **Step 2: Add the five flattened options**

Reuse the exported validated scalar wrappers from `gres-control`, with one
thin flattened Clap struct local to each binary as its command shape requires;
avoid four validation implementations and do not make the library depend on
Clap. Enable Clap's `env` feature in `gres` and `cli`. Every option has one of
the exact `CRABKA_GRES_REGISTRY_*` names above. Thread the resulting policy
into every production `Registry` construction:

- activator startup;
- compute lifecycle startup and live split-intent authority;
- `crabka gres` registry-backed commands; and
- loadtest provisioning.

For loadtest, carry policy from its CLI through `RunConfig`,
`ClusterOptions`, and every `NodeSpec`, then render the five options when
spawning each `crabka-gres` process. Provisioning and spawned readers must use
the same policy. Hidden/test harness call sites may use
`RegistryPolicy::default()`.

- [x] **Step 3: Verify Task 2**

Run focused tests for all four process surfaces, each binary's `--help`, strict
all-target Clippy for affected packages, nightly formatting, and
`git diff --check`.

---

### Task 3: Add the Kafka CRD Source of Truth

**Files:**

- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/crd/mod.rs`
- Modify: `crates/operator/src/controller/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify: `crates/operator/src/context.rs`
- Modify: operator reconciliation/context/CRD tests
- Regenerate: `deploy/crds/crabka.io_kafkas.yaml`
- Regenerate: `deploy/crds/crabka.io_greses.yaml`

- [x] **Step 1: Add failing CRD and first-creator tests**

Cover:

- Kafka CRD JSON/YAML roundtrip and OpenAPI bounds;
- absent block yields exact defaults;
- invalid values fail before child API/network writes;
- a missing referenced Kafka CR causes no activator or other child write;
- activator Deployment receives all five effective settings;
- compute Deployment receives all five effective settings;
- GresTenant reconcile and cleanup pass the referenced Kafka policy;
- a cached control handle is reused for equal policy and replaced for changed
  policy;
- same-named Kafka resources in different namespaces never share a cached
  control handle;
- construction does not hold the cache-map mutex across network awaits; and
- WAL replication cannot alter the registry policy.

- [x] **Step 2: Add `Kafka.spec.gresRegistry`**

Add an optional typed `GresRegistrySpec` with the five fields and schema
bounds. Convert it once to `RegistryPolicy` through refined validation.
Remove `GresActivatorSpec.registry_replication_factor` and its generated
`Gres` schema entry; it is the wrong owner and the API is v1alpha1.

- [x] **Step 3: Thread policy through operator paths**

The Gres controller already loads the referenced Kafka; use its effective
policy when rendering the activator. Remove its synthesized-bootstrap/default
policy fallback: the referenced Kafka CR must exist before any activator or
other Gres child is applied, otherwise an early default-policy pod can win
topic creation. The GresTenant controller separately loads the referenced Gres
and Kafka; use the same policy when rendering compute workloads and extend
`ReadyTenant`/cleanup inputs so `Context::gres_control_for` receives it.

Key the cache by `(namespace, kafka_name)`, not cluster name alone, and store
bootstrap plus policy with the handle. For equal bootstrap/policy, return the
handle. For changed input, build a replacement outside the map lock, then
install it with a short second lock. Do not hold the map mutex while connecting
or ensuring the topic.

Change `KafkaGresControl::replace_tenant_if_version` to call argument-free
`ensure_topic()` and retain tenant WAL replication only for
`upsert_tenant_config`.

- [x] **Step 4: Generate and verify**

Generate all CRDs to a temporary directory and require an exact recursive diff
against `deploy/crds`. Run focused operator/Gres/activator tests, strict
all-target Clippy for affected packages, nightly formatting, and
`git diff --check`.

---

### Task 4: Audit and Close the Registry Sub-slice

**Files:**

- Modify: `docs/configuration-audit.md`
- Modify: this plan
- Modify only code/tests if the semantic audit finds a missed registry-owned
  policy

- [x] **Step 1: Run the scanner and semantic audit**

Run `tools/audit-runtime-values.sh`. Inspect every production registry
construction and every numeric/string literal in `gres-control::registry`.
Classify candidates as configurable, derived, test-only, or fixed invariant.

Required production callers:

- activator;
- operator Gres/GresTenant/context;
- compute lifecycle and split-intent authority;
- `crabka gres`;
- loadtest.

- [x] **Step 2: Prove end-to-end ownership**

Verify:

- one Kafka CR is the operator-managed source of truth;
- every first creator uses the same effective policy;
- a missing Kafka CR cannot launch a default-policy activator;
- no production `Registry::connect` silently selects defaults;
- no `ensure_topic` accepts a replication argument;
- an already-created topic with conflicting replication fails explicitly;
- operator cache identity includes namespace and Kafka name;
- loadtest provisioner and every spawned compute receive equal policy;
- tenant WAL replication remains separate; and
- all five standalone settings are visible in help with environment bindings.

- [x] **Step 3: Run closure gates**

Run affected full nextest suites, strict all-target Clippy, all relevant help
commands, nightly formatting, exact all-CRD generation, the runtime-value
scanner, and `git diff --check`.

- [x] **Step 4: Document evidence and remaining work**

Record exact scanner counts, classifications, tests, and commands in
`docs/configuration-audit.md`. Keep PgDog pool policy and Gres-controller
timing Pending for the following front-door slice.
