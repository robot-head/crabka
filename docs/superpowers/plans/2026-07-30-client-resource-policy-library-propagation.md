# Client Resource Policy Library Propagation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Carry client-core's validated connection queue/frame policy and
isolated-fetch minimum through every higher-level library that owns Kafka
clients, without adding environment or CLI parsing.

**Architecture:** Each library stores the already-validated
`ConnectionDispatchQueueCapacity`, `ClientFrameMax`, and, where applicable,
`FetchMinBytes` values at its existing configuration boundary. Every primary,
coordinator, retry, reconnect, reader, and recovery client receives the same
owner policy. Compatibility constructors select the current typed defaults.

**Tech Stack:** Rust, `refined_type`, `crabka-units`, Bon builders, Cargo tests.

## Global Constraints

- Preserve defaults exactly: dispatch queue `64`, frame maximum `100MiB`, and
  isolated-fetch minimum `1B`.
- Use the public validated types from `crabka-client-core`; do not duplicate
  validation or lower UOM values early.
- Libraries do not read environment variables and do not invent deployment
  ownership.
- Secondary clients must reuse the owner's values; never silently fall back to
  defaults in coordinator, retry, recovery, or reconnect paths.
- Keep convenience constructors source-compatible where practical and have
  them select typed defaults.
- Preserve the four unrelated untracked plans dated `2026-07-28`.
- Run Cargo with
  `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

---

### Task 1: Propagate Connection Policy Through `crabka-client-producer`

**Files:**
- Modify: `crates/client-producer/src/builder.rs`
- Modify: `crates/client-producer/src/producer.rs`
- Modify: `crates/client-producer/src/transport.rs`
- Modify: `crates/client-producer/src/transactional.rs`

**Interfaces:**
- Producer builder accepts raw `dispatch_queue_capacity: usize` and
  `frame_max: ByteSize`, preserving client-core defaults.
- `Producer` stores the two validated client-core policy values.
- Main, transaction-coordinator, and group-coordinator clients all receive the
  stored policy.

- [ ] **Step 1: Write failing propagation tests**

Add focused builder tests that use small non-default values and assert the
resulting producer stores them. Add a coordinator-client seam test (using the
existing test transport or a private options helper) proving both transaction
and group coordinator options reuse those stored values.

- [ ] **Step 2: Verify the tests fail**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer --lib connection_resource_policy --locked
```

Expected: compilation fails because the producer builder and stored producer
state do not expose the policy.

- [ ] **Step 3: Validate once in the producer builder**

Accept the raw values at `Producer::builder()`, construct
`ConnectionDispatchQueueCapacity` and `ClientFrameMax` before any DNS or broker
I/O, and pass the typed values into the producer state and initial
`Client::builder()`.

- [ ] **Step 4: Reuse policy for secondary clients**

Apply the stored policy to:

- the transaction coordinator built by `init_transactions`;
- the group coordinator built by `send_offsets_to_transaction`; and
- transport/client reconstruction paths in `transport.rs` and
  `transactional.rs`.

Factor one small private helper that applies both values to a client builder if
that avoids repeating the pair. Do not introduce a new public abstraction.

- [ ] **Step 5: Run package tests and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer --all-targets --locked
git add crates/client-producer
git commit -m "feat(producer): carry client resource policy"
```

---

### Task 2: Give `crabka-client-admin` an Explicit Connection Policy

**Files:**
- Modify: `crates/client-admin/src/lib.rs`

**Interfaces:**
- Existing convenience constructors retain default `ConnectionOptions`.
- Explicit constructors accept a complete `ConnectionOptions` value and use it
  unchanged for every broker connection.

- [ ] **Step 1: Write a failing non-default-options test**

Extend the existing `custom_admin_options` coverage with non-default queue and
frame values. Assert that the admin connection factory receives exactly those
typed values instead of rebuilding default options.

- [ ] **Step 2: Verify failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-admin --lib custom_admin_options --locked
```

- [ ] **Step 3: Preserve complete options**

Adjust the explicit admin construction path to store/clone the complete
`ConnectionOptions`. Keep `connect` and other convenience entry points as
wrappers using `ConnectionOptions::default()` plus their existing identity,
timeout, and security overrides.

- [ ] **Step 4: Run package tests and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-admin --all-targets --locked
git add crates/client-admin/src/lib.rs
git commit -m "feat(admin): preserve client resource policy"
```

---

### Task 3: Propagate One Policy Through Every Streams Client

**Files:**
- Modify: `crates/client-streams/src/membership/client.rs`
- Modify: `crates/client-streams/src/runtime/io_broker.rs`
- Modify: the existing streams runtime/config builder that calls these entry
  points, as identified by
  `rg -n 'StreamsMembership::builder|io_broker::build|build_eos' crates/client-streams`
- Modify: focused tests beside those owners

**Interfaces:**
- Streams configuration carries one typed queue/frame pair.
- Streams configuration separately carries one typed `FetchMinBytes`.
- Membership, coordinator, metadata, fetch, producer, offset, EOS, restore, and
  test-driver clients all receive the same owner values.

- [ ] **Step 1: Inventory every construction path**

```bash
rg -n 'Client::builder\(|Producer::builder\(|ConnectionOptions \{|IsolatedFetch \{' \
  crates/client-streams --glob '*.rs'
```

Record every production hit in the test name or a short comment so new
secondary-client paths cannot be missed.

- [ ] **Step 2: Write failing policy-flow tests**

Add:

- a membership test proving both the initial and coordinator clients receive a
  non-default queue/frame pair;
- an I/O builder test proving metadata, raw fetch, producer, and offset clients
  all receive the pair in both regular and EOS builds; and
- a fetch request test proving a non-default `FetchMinBytes` reaches
  `IsolatedFetch`.

Prefer extracting pure private option-building helpers over adding network
tests solely for inspection.

- [ ] **Step 3: Verify failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-streams --lib connection_resource_policy --locked
```

- [ ] **Step 4: Carry typed values from the existing streams owner**

Add typed fields/defaults at the streams configuration boundary and thread them
through `StreamsMembership`, `io_broker::build`, `build_eos`, and every helper
they call. Apply `FetchMinBytes` only to the isolated-fetch path.

Do not add environment parsing or a streams-specific wrapper type.

- [ ] **Step 5: Re-run the inventory and tests**

The inventory must show every production client/producer/options/fetch literal
receives the propagated policy or is an explicit compatibility/test default.

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-streams --all-targets --locked
git add crates/client-streams
git commit -m "feat(streams): carry client resource policy"
```

---

### Task 4: Add FDW Scan-Owned Connection and Fetch Policy

**Files:**
- Modify: `crates/gres-fdw/src/source.rs`
- Modify: the existing FDW connection/config owner located from
  `rg -n 'ConnProfile|fetch_budgets|connection_options' crates/gres-fdw/src`
- Modify: focused tests in `crates/gres-fdw/src/source.rs`

**Interfaces:**
- FDW configuration carries typed queue/frame values into
  `connection_options`.
- FDW scan policy carries typed `FetchMinBytes` into every isolated fetch.

- [ ] **Step 1: Write failing propagation tests**

Extend the existing connection-option and fetch-budget tests with non-default
queue/frame/fetch-min values. Assert the exact typed values at the
`ConnectionOptions` and `IsolatedFetch` construction seams.

- [ ] **Step 2: Verify failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-fdw --lib source::tests --locked
```

- [ ] **Step 3: Thread policy without parsing**

Store the three typed values in the existing FDW owner and use them in
`connection_options` and the fetch loop. Keep existing entry points defaulted;
deployment parsing belongs to the later deployment plan.

- [ ] **Step 4: Test and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-fdw --all-targets --locked
git add crates/gres-fdw
git commit -m "feat(fdw): carry client resource policy"
```

---

### Task 5: Propagate WAL Recovery Client Policy

**Files:**
- Modify: `crates/gres-substrate/src/recovery.rs`
- Modify: focused recovery tests

**Interfaces:**
- `LiveRecoveryConfig` stores a typed queue/frame pair and fetch minimum.
- WAL admin, producer, replay connection, and every reconstructed recovery
  client reuse the same connection pair.
- WAL replay `IsolatedFetch` uses the stored fetch minimum.

- [ ] **Step 1: Write failing config and propagation tests**

Add non-default values to a `LiveRecoveryConfig` and assert:

- `wal_admin_connection_options` preserves queue/frame;
- replay connection options preserve queue/frame;
- the producer builder receives queue/frame; and
- the generated isolated fetch contains the configured minimum.

- [ ] **Step 2: Verify failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate --lib recovery::tests::client_resource_policy --locked
```

- [ ] **Step 3: Implement typed setters and forwarding**

Add narrow `with_connection_resource_policy` and `with_fetch_min` methods (or
extend an existing cohesive recovery policy setter if one already owns these
values). Default them to the client-core types. Apply them at every
`ConnectionOptions`, `Producer::builder`, and `IsolatedFetch` construction in
`recovery.rs`.

- [ ] **Step 4: Test and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate --all-targets --locked
git add crates/gres-substrate/src/recovery.rs crates/gres-substrate/tests
git commit -m "feat(substrate): carry recovery client policy"
```

---

### Task 6: Propagate Registry Reader and Writer Policy

**Files:**
- Modify: `crates/gres-control/src/registry.rs`

**Interfaces:**
- `RegistryPolicy` owns one typed queue/frame pair shared by registry admin,
  writer, and reader connections.
- `RegistryPolicy` owns a typed reader `FetchMinBytes`.
- Reader reconnects reuse the same values.

- [ ] **Step 1: Write failing policy tests**

Extend the existing registry default/replacement tests with non-default
queue/frame/fetch-min values. Add assertions at the writer producer, admin
`ConnectionOptions`, reader `ConnectionOptions`, reconnect, and
`IsolatedFetch` seams.

- [ ] **Step 2: Verify failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control --lib registry::tests::registry_client_resource_policy --locked
```

- [ ] **Step 3: Extend `RegistryPolicy` and forward it everywhere**

Use client-core validated types directly. Preserve `Registry::connect` and
`RegistryPolicy::default`. Ensure the background reader captures the complete
policy so reconnects cannot reintroduce defaults.

- [ ] **Step 4: Test and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control --all-targets --locked
git add crates/gres-control/src/registry.rs
git commit -m "feat(gres-control): carry registry client policy"
```

---

### Task 7: Audit Remaining Library Construction Sites

**Files:**
- Modify: any library-only owner reported by the inventory that was not covered
  above
- Modify: `docs/configuration-audit.md`

- [ ] **Step 1: Inventory the workspace**

```bash
rg -n 'Client::builder\(|Producer::builder\(|ConnectionOptions \{|IsolatedFetch \{' \
  crates --glob '*.rs'
```

Classify every production hit as:

- covered by a typed higher-level policy;
- a deployment boundary intentionally deferred to the next plan; or
- a compatibility/test path explicitly selecting defaults.

- [ ] **Step 2: Fix missed library-owned paths test-first**

For each missed library owner, add one failing propagation test, carry the two
connection values and optional fetch minimum from its deployment owner, and
commit the smallest coherent package change.

- [ ] **Step 3: Update the audit**

Document library propagation closure and list only deployment/CRD work as
remaining. Include the exact inventory command and explain any explicit
default-only compatibility path.

- [ ] **Step 4: Run the phase gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-admin --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-streams --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-fdw --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
```

- [ ] **Step 5: Commit the audit**

```bash
git add docs/configuration-audit.md
git commit -m "docs(config): record client policy propagation"
```

After this plan passes, write separate executable plans for:

1. deployment CLI/environment ownership; and
2. Kafka/Gres CRD schema and rendered arguments.
