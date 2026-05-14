# Bulletproof EOS sub-slice 10b follow-up — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the slice-10b deferrals: re-enable the 4 rust integration tests + 3 JVM acceptance tests that were marked `#[ignore]` / `--skip` because `Broker::start`'s hardcoded 5s openraft `election_timeout` forced a 10s `leader_lease`, blocking new-leader election until after the producer's 10s timeout.

**Architecture:** Three independent changes, each its own commit. (1) Add `controller_election_timeout` and `controller_heartbeat_interval` to `BrokerConfig` (defaults unchanged) and plumb through to `crabka_raft::ControllerConfig`. (2) Hoist `start_n_node_with_retry` + ephemeral-port (bind-and-drop) helper into a shared `tests/support/mod.rs` and use it from all four multi-broker test files. (3) Un-ignore the deferred tests; remove `--skip` flags from CI.

**Tech Stack:** Rust 1.95.0; existing `tokio` + `openraft` + `crabka-raft`; no new dependencies.

**Reference spec:** [`docs/superpowers/specs/2026-05-13-crabka-bulletproof-eos-10b-followup-design.md`](../specs/2026-05-13-crabka-bulletproof-eos-10b-followup-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/bulletproof-eos-10b-followup` (already created). Implementation runs on `feature/bulletproof-eos-10b-followup` once this plan's PR merges.

---

## File structure

```
crates/broker/src/
├── config.rs                      # MODIFIED — 2 new fields, Default + for_tests update
└── broker.rs                      # MODIFIED — pass new fields to ControllerConfig (replace hardcoded 5s/500ms)

crates/broker/tests/
├── support/
│   └── mod.rs                     # NEW — start_n_node_with_retry + bind_and_drop_ports + init_tracing
├── durability.rs                  # MODIFIED — `mod support;`, replace boot_three_node, un-#[ignore]
├── leader_election.rs             # MODIFIED — `mod support;`, replace boot_three_node, un-#[ignore] × 3
├── replication.rs                 # MODIFIED — `mod support;`, drop inline start_n_node, un-#[ignore] × 2
├── quorum.rs                      # MODIFIED — `mod support;`, drop inline start_n_node helper
└── jvm_acceptance.rs              # MODIFIED — short raft timings on slice-10b multi-broker tests

.github/workflows/ci.yml           # MODIFIED — drop `--skip acks_all_durability`, etc.
```

The support module follows Cargo convention: `tests/support/mod.rs` is treated as a non-binary submodule (vs. `tests/support.rs` which Cargo would compile as its own test binary). Each test file declares `mod support;` to pull it in.

---

## Phase 1 — Plumb raft timings through `BrokerConfig`

### Task 1: Add `controller_election_timeout` + `controller_heartbeat_interval` to `BrokerConfig`

**Files:**
- Modify: `crates/broker/src/config.rs`

- [ ] **Step 1: Write a failing unit test for the new defaults**

Add to the existing `#[cfg(test)] mod tests` block in `crates/broker/src/config.rs`:

```rust
    #[test]
    fn defaults_use_conservative_raft_timings() {
        let c = BrokerConfig::default();
        assert_eq!(c.controller_election_timeout, std::time::Duration::from_secs(5));
        assert_eq!(
            c.controller_heartbeat_interval,
            std::time::Duration::from_millis(500)
        );
    }

    #[test]
    fn for_tests_uses_short_raft_timings_for_fast_failover() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        // Short enough that a 3-broker test can detect a dead leader and
        // re-elect within a few hundred ms — the deferred slice-10b tests
        // need failover well under their 10s producer timeout.
        assert!(c.controller_election_timeout <= std::time::Duration::from_millis(750));
        assert!(c.controller_heartbeat_interval <= std::time::Duration::from_millis(200));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p crabka-broker --lib config::tests::defaults_use_conservative_raft_timings
cargo test -p crabka-broker --lib config::tests::for_tests_uses_short_raft_timings_for_fast_failover
```

Expected: both FAIL with "no field `controller_election_timeout`".

- [ ] **Step 3: Add the fields + update `Default` and `for_tests`**

In `crates/broker/src/config.rs`, add to the struct (just after `replica_lag_time_max_ms`):

```rust
    /// Openraft election timeout (sets `election_timeout_min`; max is 2×).
    /// Indirectly sets `leader_lease = election_timeout_max` inside
    /// openraft's engine — peers refuse to grant a new leader's vote
    /// until the lease expires, so this is also the lower bound on how
    /// fast a 3-broker cluster can recover from a dead controller leader.
    /// Default 5s (conservative; avoids split-vote on slow runners).
    pub controller_election_timeout: std::time::Duration,

    /// Openraft heartbeat interval. Default 500ms. Should be ≤
    /// `controller_election_timeout / 3` per raft consensus norms.
    pub controller_heartbeat_interval: std::time::Duration,
```

Update `for_tests` to add the two fields just before the closing brace of the struct literal:

```rust
            heartbeat_interval_ms: 200,
            heartbeat_timeout_ms: 2_000,
            replica_lag_time_max_ms: 2_000,
            // Short timings: single-node tests don't need quorum so split-vote
            // isn't a risk; multi-broker tests use these (via the shared
            // `support::start_n_node_with_retry` helper) so failover from a
            // dead controller leader completes well under the producer's
            // 10s timeout. The factor of ~10× vs. production defaults
            // is what makes `acks_all_completes_via_isr_shrink_when_follower_dead`
            // pass within its 5s assertion window.
            controller_election_timeout: std::time::Duration::from_millis(500),
            controller_heartbeat_interval: std::time::Duration::from_millis(100),
```

Update `Default` to add the two fields just before the closing brace:

```rust
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p crabka-broker --lib config::tests
```

Expected: PASS (all 4 config tests, including the 2 new ones).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/config.rs
git commit -m "feat(broker): expose openraft timings on BrokerConfig

Previously hardcoded in Broker::start as 5s/500ms. Production defaults
unchanged; for_tests uses 500ms/100ms so multi-broker failover tests
recover from a dead controller leader within their producer timeout.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: Wire the new fields into `Broker::start`

**Files:**
- Modify: `crates/broker/src/broker.rs:224-237`

- [ ] **Step 1: Update the `ControllerConfig` literal in `Broker::start`**

Replace the existing block in `crates/broker/src/broker.rs` (currently lines 224-237):

```rust
        let controller_cfg = crabka_raft::ControllerConfig {
            node_id: config.node_id,
            voters: config.controller_quorum_voters.clone(),
            controller_listen_addr: config.controller_listen_addr,
            log_dir: config.log_dir.join("__cluster_metadata"),
            // Aggressive defaults (1s / 200ms) split-vote on slow CI runners
            // when our hand-rolled wire's RPC round-trip exceeds the
            // election-timeout window. 5s/500ms keeps elections deterministic
            // for multi-node startups without making the single-node path
            // perceptibly slower.
            election_timeout: std::time::Duration::from_secs(5),
            heartbeat_interval: std::time::Duration::from_millis(500),
            client_id: format!("crabka-broker-{}-controller", config.broker_id),
        };
```

with:

```rust
        let controller_cfg = crabka_raft::ControllerConfig {
            node_id: config.node_id,
            voters: config.controller_quorum_voters.clone(),
            controller_listen_addr: config.controller_listen_addr,
            log_dir: config.log_dir.join("__cluster_metadata"),
            // Sourced from `BrokerConfig` — see the docstrings there for
            // the production-vs-test tradeoff. Crucially this also sets
            // openraft's `leader_lease` to `election_timeout × 2`, which
            // is the floor on how fast a 3-broker cluster can elect a
            // replacement when the controller leader dies.
            election_timeout: config.controller_election_timeout,
            heartbeat_interval: config.controller_heartbeat_interval,
            client_id: format!("crabka-broker-{}-controller", config.broker_id),
        };
```

- [ ] **Step 2: Run the broker test suite to verify nothing regressed**

```bash
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --test unit
```

Expected: both PASS (95+ lib tests, 21+ unit tests). No new failures.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "feat(broker): Broker::start reads raft timings from BrokerConfig

Drop hardcoded election_timeout=5s / heartbeat_interval=500ms and
read from config.controller_election_timeout / .controller_heartbeat_interval.
Defaults via BrokerConfig::default() are unchanged, so production
behavior is identical.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Phase 2 — Shared test-support module

### Task 3: Create `tests/support/mod.rs` with shared cluster helpers

**Files:**
- Create: `crates/broker/tests/support/mod.rs`

- [ ] **Step 1: Write the module**

```rust
//! Shared helpers for multi-broker integration tests.
//!
//! Each `tests/*.rs` integration-test crate that needs a 3-broker
//! cluster declares `mod support;` and reaches in for `start_n_node_with_retry`.
//! Cargo treats `tests/support/mod.rs` (rather than `tests/support.rs`) as
//! a non-binary submodule, so it doesn't get compiled as its own test
//! crate.

#![cfg(not(target_os = "windows"))]
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use tempfile::TempDir;

use crabka_broker::{Broker, BrokerConfig, BrokerError, BrokerHandle};

/// Lazily-initialized tracing subscriber so `RUST_LOG=...` works in
/// integration tests. Safe to call multiple times; `try_init` is a no-op
/// after the first success.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Reserve `n` pairs of ephemeral loopback ports (client + controller per
/// broker) via the bind-and-drop trick: bind a `TcpListener` on
/// `127.0.0.1:0`, read its assigned port, then drop the listener. The OS
/// won't immediately reuse the port for another bind, so we can pass it
/// to `Broker::start` and the broker re-binds it on the same address.
///
/// Avoids the Linux TIME_WAIT trap that fixed ports hit when multiple
/// tests in the same binary boot 3-broker clusters back-to-back.
pub async fn bind_and_drop_ports(n: usize) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    let mut client_addrs = Vec::with_capacity(n);
    let mut controller_addrs = Vec::with_capacity(n);
    for _ in 0..n {
        let cl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        client_addrs.push(cl.local_addr().unwrap());
        let ct = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        controller_addrs.push(ct.local_addr().unwrap());
        drop((cl, ct));
    }
    (client_addrs, controller_addrs)
}

/// Build a `BrokerConfig` for broker `i` (0-indexed) in an `n`-broker
/// cluster using the supplied ephemeral port lists + voter map. All
/// callers want the same `BrokerConfig::for_tests`-style short raft
/// timings; this helper centralizes the boilerplate so individual tests
/// don't drift on field values when `BrokerConfig` grows.
pub fn broker_config(
    i: usize,
    client_addrs: &[SocketAddr],
    controller_addrs: &[SocketAddr],
    voters: &[(u64, SocketAddr)],
    log_dir: &std::path::Path,
) -> BrokerConfig {
    let listen = client_addrs[i];
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.node_id = u64::try_from(i + 1).unwrap();
    cfg.controller_listen_addr = controller_addrs[i];
    cfg.controller_quorum_voters = voters.to_vec();
    cfg
}

/// Boot an `n`-broker cluster with ephemeral ports + short raft timings.
/// Brokers spawn concurrently because a single broker blocks waiting for
/// quorum during initial leader election.
///
/// Returns `(handle, config, tempdir)` triples preserving spawn order;
/// `cluster[0]` is broker_id 1.
pub async fn start_n_node(
    n: u64,
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    init_tracing();

    let n_usize = usize::try_from(n).unwrap();
    let (client_addrs, controller_addrs) = bind_and_drop_ports(n_usize).await;
    let voters: Vec<(u64, SocketAddr)> = (0..n)
        .map(|i| (i + 1, controller_addrs[i as usize]))
        .collect();

    let mut spawned = Vec::with_capacity(n_usize);
    let mut metas: Vec<(TempDir, BrokerConfig)> = Vec::with_capacity(n_usize);
    for i in 0..n_usize {
        let dir = TempDir::new().unwrap();
        let cfg = broker_config(i, &client_addrs, &controller_addrs, &voters, dir.path());
        let cfg_clone = cfg.clone();
        spawned.push(tokio::spawn(async move { Broker::start(cfg_clone).await }));
        metas.push((dir, cfg));
    }

    let mut out = Vec::with_capacity(n_usize);
    for (j, (dir, cfg)) in spawned.into_iter().zip(metas) {
        let h = j.await.expect("broker spawn join")?;
        out.push((h, cfg, dir));
    }
    Ok(out)
}

/// Retry `start_n_node` up to 3 times. Short raft timings occasionally
/// split-vote on slow runners; a fresh tempdir + port set on retry
/// clears the openraft state and usually succeeds within 2 attempts.
pub async fn start_n_node_with_retry(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last_err = None;
    for attempt in 1..=3 {
        match start_n_node(n).await {
            Ok(cluster) => return cluster,
            Err(e) => {
                tracing::warn!(attempt, error = %e, "cluster start failed; retrying");
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("cluster start failed after 3 attempts; last error: {last_err:?}");
}

/// Poll every broker's controller image until each one sees `n`
/// brokers registered. Required before any test that needs the
/// partition's replica set to include all `n` nodes (CreateTopics
/// reads `image.brokers()` to pick replicas; a race here silently
/// degrades to a smaller replica set).
pub async fn wait_for_all_brokers_registered(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    n: usize,
) {
    let deadline = std::time::Instant::now() + Duration::from_mins(2);
    loop {
        let mut all = true;
        for (h, _, _) in cluster {
            if h.broker_count().await < n {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "brokers didn't converge on {n}-broker view within 2 min"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cargo build --tests -p crabka-broker
```

Expected: builds clean. The module isn't yet referenced by any test, so this is a syntactic check only.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/tests/support/mod.rs
git commit -m "test(broker): add shared support module for multi-broker tests

Hoists start_n_node_with_retry + the bind-and-drop ephemeral-port helper
out of replication.rs/quorum.rs so slice-10b helpers in durability.rs
and leader_election.rs can share it. Files using fixed ports
(11_092..=12_293) hit Linux TIME_WAIT when run back-to-back; the
ephemeral pattern avoids that.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 4: Switch `replication.rs` to use `support` (and drop its inline copy)

**Files:**
- Modify: `crates/broker/tests/replication.rs`

- [ ] **Step 1: Confirm the failing tests still fail (TDD baseline)**

These are the tests we re-ignored last commit; un-ignoring won't happen until Phase 3. For now, just confirm the file compiles and existing tests pass:

```bash
cargo test -p crabka-broker --test replication 2>&1 | tail -5
```

Expected: PASS (all currently-enabled tests; the 2 #[ignored] ones aren't run).

- [ ] **Step 2: Add the `mod support;` declaration**

In `crates/broker/tests/replication.rs`, just below the existing `use` block at the top (after the imports), add:

```rust
mod support;
```

- [ ] **Step 3: Replace the inline `start_n_node` / `start_n_node_with_retry` / `init_tracing` with re-exports from `support`**

Delete the current `fn init_tracing`, `async fn start_n_node`, and `async fn start_n_node_with_retry` from `replication.rs` (lines ~55-139 in the current file). Replace the corresponding call sites:

Search/replace within `replication.rs`:

- `init_tracing()` → `support::init_tracing()`
- `start_n_node(` → `support::start_n_node(`
- `start_n_node_with_retry(` → `support::start_n_node_with_retry(`

- [ ] **Step 4: Run replication tests to verify behavior is unchanged**

```bash
cargo test -p crabka-broker --test replication 2>&1 | tail -5
```

Expected: PASS (same count as Step 1). The 2 `#[ignore]`d tests are still ignored — Phase 3 will un-ignore them.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/tests/replication.rs
git commit -m "test(broker): replication.rs uses shared support module

Drops the inline start_n_node helper. Behavior unchanged; the
#[ignore]d tests stay ignored until Phase 3.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 5: Switch `quorum.rs` to use `support`

**Files:**
- Modify: `crates/broker/tests/quorum.rs`

- [ ] **Step 1: Confirm quorum tests pass as baseline**

```bash
cargo test -p crabka-broker --test quorum 2>&1 | tail -5
```

Expected: PASS.

- [ ] **Step 2: Add `mod support;` declaration to `quorum.rs`**

At the top of `crates/broker/tests/quorum.rs`, after the existing `use` block, add:

```rust
mod support;
```

- [ ] **Step 3: Delete the inline helpers and update call sites**

Open `crates/broker/tests/quorum.rs` and:

1. Delete the function definitions `fn init_tracing`, `async fn start_n_node`, and `async fn start_n_node_with_retry` (these are the verbatim copies that replication.rs originally cribbed from — both will now live in `support`).
2. Search/replace within the same file:
   - `init_tracing()` → `support::init_tracing()`
   - `start_n_node(` → `support::start_n_node(`
   - `start_n_node_with_retry(` → `support::start_n_node_with_retry(`
3. If any test in quorum.rs unpacks `(h, bootstrap_str, dir)` from the old return type, replace with `(h, cfg, dir)` and adjust the test body to read `cfg.listen_addr.to_string()` (or `cfg.advertised_listener.clone()`) where it previously used the string.

- [ ] **Step 4: Run quorum tests**

```bash
cargo test -p crabka-broker --test quorum 2>&1 | tail -5
```

Expected: PASS (same count as Step 1).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/tests/quorum.rs
git commit -m "test(broker): quorum.rs uses shared support module

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 6: Switch `durability.rs` to use `support` (replaces fixed-port `boot_three_node`)

**Files:**
- Modify: `crates/broker/tests/durability.rs`

- [ ] **Step 1: Confirm durability tests still pass as baseline**

```bash
cargo test -p crabka-broker --test durability 2>&1 | tail -5
```

Expected: PASS (3 enabled, 2 ignored — the 1 we just ignored + the pre-existing read_committed one).

- [ ] **Step 2: Add `mod support;` declaration**

At the top of `crates/broker/tests/durability.rs`, after the existing `use` block, add:

```rust
mod support;
```

- [ ] **Step 3: Delete the inline `boot_three_node` + `wait_for_all_three_brokers`**

In `crates/broker/tests/durability.rs`, delete the existing `async fn boot_three_node()` (~lines 318-368) and `async fn wait_for_all_three_brokers(...)` (~lines 329-353 in the post-#[ignore] version).

These are replaced by `support::start_n_node_with_retry` and `support::wait_for_all_brokers_registered`.

- [ ] **Step 4: Update the `acks_all_completes_via_isr_shrink_when_follower_dead` test body to use the support helpers**

Replace the entire test body. Find:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Slice-10b leaves a race we haven't fully closed: ...
#[ignore = "flaky on Linux/macOS CI; tracked under slice-10b follow-up"]
async fn acks_all_completes_via_isr_shrink_when_follower_dead() {
    let _ = tracing_subscriber::fmt()
        ...
        .try_init();
    let (mut cluster, bootstrap_1) = boot_three_node().await;
    wait_for_all_three_brokers(&cluster).await;
    create_topic(&cluster[0].0, &bootstrap_1, "shrink", 3).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let dead = cluster.pop().expect("3rd broker");
    dead.0.shutdown().await;
    let start = Instant::now();
    ...
}
```

Replace with (note: still `#[ignore]` in this task; Phase 3 un-ignores):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "flaky on Linux/macOS CI; tracked under slice-10b follow-up"]
async fn acks_all_completes_via_isr_shrink_when_follower_dead() {
    support::init_tracing();
    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let bootstrap_1 = cluster[0].1.listen_addr.to_string();
    create_topic(&cluster[0].0, &bootstrap_1, "shrink", 3).await;
    // Give follower replicators a moment to spawn and start fetching.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Kill broker 3 — its absence forces ISR to shrink within
    // replica_lag_time_max_ms (2s on CI), unblocking the acks=-1 produce.
    let dead = cluster.pop().expect("3rd broker");
    dead.0.shutdown().await;

    let start = Instant::now();
    let offset = produce_acks(&bootstrap_1, "shrink", &["x", "y", "z"], -1, 10_000)
        .await
        .expect("acks=-1 success after shrink");
    let elapsed = start.elapsed();
    assert_eq!(offset, 0);
    assert!(
        elapsed >= Duration::from_millis(1_500),
        "expected to wait for ISR shrink (~2s); took {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "shrink + completion should be well under 5s; took {elapsed:?}"
    );
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
```

- [ ] **Step 5: Run durability tests to verify nothing regressed**

```bash
cargo test -p crabka-broker --test durability 2>&1 | tail -5
```

Expected: PASS — same passing count as Step 1 (the failover test is still `#[ignore]`d).

- [ ] **Step 6: Verify the test signature passes clippy + fmt**

```bash
cargo clippy -p crabka-broker --tests 2>&1 | tail -5
cargo fmt --all -- --check
```

Expected: both clean.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/tests/durability.rs
git commit -m "test(broker): durability.rs uses shared support module

Drops the inline fixed-port boot_three_node (11_092..=11_293) in
favor of support::start_n_node_with_retry's ephemeral ports. Also
collapses the bespoke wait_for_all_three_brokers into the shared
support helper.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 7: Switch `leader_election.rs` to use `support`

**Files:**
- Modify: `crates/broker/tests/leader_election.rs`

- [ ] **Step 1: Confirm leader_election tests still pass as baseline**

```bash
cargo test -p crabka-broker --test leader_election 2>&1 | tail -5
```

Expected: PASS for the 1 non-ignored test (`produce_during_leader_failover`); 3 ignored.

- [ ] **Step 2: Add `mod support;` to `leader_election.rs`**

At the top of `crates/broker/tests/leader_election.rs`, after the existing `use` block, add:

```rust
mod support;
```

- [ ] **Step 3: Delete the inline `boot_three_node` + `wait_for_all_three_brokers`**

In `crates/broker/tests/leader_election.rs`, delete:

- `async fn boot_three_node()` (~lines 36-82)
- `async fn wait_for_all_three_brokers(cluster: ...)` (~lines 84-103)

- [ ] **Step 4: Update all four test bodies to use the support helpers**

Each of `broker_death_elects_new_leader`, `acks_all_completes_after_isr_shrink`, `isr_expand_on_catchup`, and `produce_during_leader_failover` currently does:

```rust
    let (mut cluster, bootstrap_1) = boot_three_node().await;
    wait_for_all_three_brokers(&cluster).await;
```

Replace each instance with:

```rust
    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let bootstrap_1 = cluster[0].1.listen_addr.to_string();
```

Note: `support::start_n_node_with_retry` returns `Vec<(BrokerHandle, BrokerConfig, TempDir)>` (3-tuple including BrokerConfig), while the inline helper returned `Vec<(BrokerHandle, String, TempDir)>`. Search for any `(h, bootstrap, dir)` or `(h, _, _)` destructuring on the cluster and confirm it still type-checks; the indices line up.

- [ ] **Step 5: Run leader_election tests to verify**

```bash
cargo test -p crabka-broker --test leader_election 2>&1 | tail -5
```

Expected: PASS for the same set as Step 1. The 3 ignored tests stay ignored — Phase 3 un-ignores them.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/tests/leader_election.rs
git commit -m "test(broker): leader_election.rs uses shared support module

Replaces fixed ports (12_092..=12_293) + per-file boot helpers with
support::start_n_node_with_retry. Same TIME_WAIT fix as durability.rs.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Phase 3 — Re-enable the deferred tests

### Task 8: Un-ignore `durability::acks_all_completes_via_isr_shrink_when_follower_dead`

**Files:**
- Modify: `crates/broker/tests/durability.rs`

- [ ] **Step 1: Remove the `#[ignore]` + obsolete comment block**

Find:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "flaky on Linux/macOS CI; tracked under slice-10b follow-up"]
async fn acks_all_completes_via_isr_shrink_when_follower_dead() {
```

Replace with:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_completes_via_isr_shrink_when_follower_dead() {
```

- [ ] **Step 2: Run the test on Linux to verify it passes**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && git pull origin plan/bulletproof-eos-10b-followup 2>&1 | tail -3; cargo test -p crabka-broker --test durability acks_all_completes_via_isr_shrink_when_follower_dead -- --nocapture 2>&1 | tail -15"
```

Expected: PASS. If `git pull` of the plan branch isn't possible (subagent might not have access), instead `cp` the files into the WSL working copy and run.

The test's `elapsed` should be in the 2-4s range — well under the 5s assertion ceiling and well over the 1.5s floor.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/tests/durability.rs
git commit -m "test(broker): un-ignore acks_all_completes_via_isr_shrink_when_follower_dead

Short raft timings + ephemeral ports landed in Phases 1-2 fix the
underlying issue (10s leader_lease blocked failover beyond the 10s
producer timeout). Test now completes in ~2-4s.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 9: Un-ignore the 3 `leader_election.rs` tests

**Files:**
- Modify: `crates/broker/tests/leader_election.rs`

- [ ] **Step 1: Remove `#[ignore]` from `broker_death_elects_new_leader`**

Find:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Slice-10b leader-election propagation isn't yet reliable within
// ... follow-up will fix.
#[ignore = "flaky on CI; tracked under slice-10b follow-up"]
async fn broker_death_elects_new_leader() {
```

Replace with:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_death_elects_new_leader() {
```

- [ ] **Step 2: Remove `#[ignore]` from `acks_all_completes_after_isr_shrink`**

Same pattern: drop the multi-line comment + `#[ignore]` attribute.

- [ ] **Step 3: Remove `#[ignore]` from `isr_expand_on_catchup`**

Same pattern.

- [ ] **Step 4: Run all four leader_election tests on Linux**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo test -p crabka-broker --test leader_election -- --nocapture 2>&1 | tail -15"
```

Expected: 4 passed, 0 failed.

If any fail, the most likely culprit is still-stale `boot_three_node` callers or a missed `support::` prefix. Re-run with `RUST_LOG=openraft=info,crabka_raft=info,crabka_broker::isr_maintenance=info` to confirm leader-election timing is correct (election should complete in well under 1s with the 500ms `controller_election_timeout`).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/tests/leader_election.rs
git commit -m "test(broker): un-ignore 3 leader_election failover tests

Short raft timings unblock leader election after broker death;
ephemeral ports unblock isr_expand_on_catchup's second 3-broker boot.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 10: Un-ignore the 2 `replication.rs` tests

**Files:**
- Modify: `crates/broker/tests/replication.rs`

- [ ] **Step 1: Remove `#[ignore]` from `replication_factor_three_propagates_to_all_followers`**

Find:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Re-ignored: re-enabled in 29d86c7 ...
#[ignore = "follower replicators intermittently stall on Linux CI; slice-10b follow-up will fix"]
async fn replication_factor_three_propagates_to_all_followers() {
```

Replace with:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_factor_three_propagates_to_all_followers() {
```

- [ ] **Step 2: Remove `#[ignore]` from `out_of_range_truncates_and_recovers`**

Same pattern.

- [ ] **Step 3: Run all replication tests on Linux**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo test -p crabka-broker --test replication -- --nocapture 2>&1 | tail -15"
```

Expected: all replication tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/replication.rs
git commit -m "test(broker): un-ignore 2 slice-10a replication tests

These were originally re-enabled in 29d86c7 under the slice-10b ISR
expectation; the real fix was the raft timing (Phases 1-2).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 11: Verify full broker test suite green on Linux

**Files:** none

- [ ] **Step 1: Run the entire broker test suite on Linux**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo test -p crabka-broker 2>&1 | tail -30"
```

Expected: all tests pass (0 failed; some still `#[ignored]` only when they require Docker — i.e., the JVM acceptance tests, which are gated by `#[ignore = \"requires Docker\"]` and only run under the dedicated CI job).

- [ ] **Step 2: Run on Windows host to confirm the cfg-gating still excludes the right tests**

```bash
cargo test -p crabka-broker 2>&1 | tail -10
```

Expected: PASS on Windows. The Linux/macOS-only tests are excluded by `#![cfg(not(target_os = "windows"))]`.

- [ ] **Step 3: Verify clippy + fmt are clean**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | head -5
```

Expected: both clean.

---

## Phase 4 — JVM acceptance follow-up

### Task 12: Drop `--skip` flags from CI

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Remove the `--skip` arguments**

In `.github/workflows/ci.yml`, find the `broker-jvm-acceptance` job's run step:

```yaml
      - run: |
          cargo test -p crabka-broker --test jvm_acceptance -- \
            --ignored --nocapture --test-threads=1 \
            --skip acks_all_durability \
            --skip acks_all_survives_leader_crash \
            --skip three_node_replication_byte_compare
```

Replace with:

```yaml
      - run: cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1
```

And drop the now-stale "# --skip filters out..." comment block immediately above it (the `# --test-threads=1: ...` comment stays).

- [ ] **Step 2: Verify locally that the 3 previously-skipped JVM tests pass**

These tests require Docker. If Docker isn't running on WSL, start it (or run on a host with Docker available):

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo test -p crabka-broker --test jvm_acceptance acks_all_durability -- --ignored --nocapture 2>&1 | tail -20"
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo test -p crabka-broker --test jvm_acceptance acks_all_survives_leader_crash -- --ignored --nocapture 2>&1 | tail -20"
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo test -p crabka-broker --test jvm_acceptance three_node_replication_byte_compare -- --ignored --nocapture 2>&1 | tail -20"
```

Expected: each PASSES within ~2-3 minutes.

If `acks_all_durability` or `acks_all_survives_leader_crash` still hits the producer timeout, double-check that `crates/broker/tests/jvm_acceptance.rs` builds its `BrokerConfig` literals via `BrokerConfig::for_tests` (which now includes the short raft timings) OR explicitly sets the new fields. The `three_node_*` helpers in `jvm_acceptance.rs` currently construct `BrokerConfig` field-by-field; they need an update too — see Task 13.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci(jvm-acceptance): drop --skip for slice-10b multi-broker tests

Short raft timings (BrokerConfig.controller_election_timeout) + ephemeral
ports unblock the 3 tests that were skipped in PR #73.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 13: Update `jvm_acceptance.rs` cluster helpers to set short raft timings

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Find the literal `BrokerConfig` constructions**

Each of `three_node_jvm_round_trip`, `three_node_replication_byte_compare`, `acks_all_durability`, and `acks_all_survives_leader_crash` constructs `BrokerConfig` field-by-field — search:

```bash
grep -n "BrokerConfig {" crates/broker/tests/jvm_acceptance.rs
```

For each match, the literal looks like:

```rust
let cfg = BrokerConfig {
    broker_id: ...,
    listen_addr: ...,
    advertised_listener: ...,
    log_dir: ...,
    log_config: ...,
    node_id: ...,
    controller_listen_addr: ...,
    controller_quorum_voters: voters.clone(),
    heartbeat_interval_ms: 3_000,
    heartbeat_timeout_ms: 9_000,
    replica_lag_time_max_ms: 30_000,
};
```

The compiler will already require the two new fields after Task 1 — without them the build fails. The plan: add them with **short** timings on each match, mirroring `BrokerConfig::for_tests`:

```rust
    heartbeat_interval_ms: 3_000,
    heartbeat_timeout_ms: 9_000,
    replica_lag_time_max_ms: 30_000,
    controller_election_timeout: std::time::Duration::from_millis(500),
    controller_heartbeat_interval: std::time::Duration::from_millis(100),
};
```

- [ ] **Step 2: Verify the file compiles**

```bash
cargo build --tests -p crabka-broker 2>&1 | tail -5
```

Expected: clean (note: `jvm_acceptance.rs` has the same `#![cfg(not(target_os = "windows"))]` gating, so this must be run on Linux or macOS — use WSL for the local verification).

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo build --tests -p crabka-broker 2>&1 | tail -5"
```

- [ ] **Step 3: Run the 3 previously-skipped JVM tests on Linux (with Docker)**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo test -p crabka-broker --test jvm_acceptance --ignored -- --nocapture --test-threads=1 2>&1 | tail -30"
```

Expected: all 9 JVM acceptance tests pass; total runtime ~15-20 minutes.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(broker): jvm_acceptance.rs sets short raft timings

The four 3-broker JVM acceptance helpers construct BrokerConfig
field-by-field; after the new controller_election_timeout +
controller_heartbeat_interval fields land, set both to the same
short values BrokerConfig::for_tests uses so the slice-10b multi-broker
tests recover from a dead leader within the producer timeout.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Phase 5 — Closeout

### Task 14: Update STATUS.md / docstrings

**Files:**
- Modify: `STATUS.md`
- Modify: `crates/broker/src/lib.rs` (module docstring, if it references slice-10b deferrals)

- [ ] **Step 1: Update STATUS.md**

Append a "slice-10b follow-up" entry under the existing slice 10b row. If `STATUS.md` doesn't currently mention the deferrals, no change is needed — the README's slice-10b bullet is enough.

If you find a "slice 10b — deferred:" line, replace it with:

```
- **Slice 10b follow-up** — closed the openraft `leader_lease` race:
  `BrokerConfig.controller_election_timeout` is now the dominant
  failover knob (production default 5s; tests use 500ms), and
  multi-broker tests share a single bind-and-drop port helper to
  avoid Linux TIME_WAIT. The 4 rust + 3 JVM acceptance tests
  deferred in PR #73 are now green.
```

- [ ] **Step 2: Check `crates/broker/src/lib.rs` for stale "see slice-10b follow-up" comments**

```bash
grep -n "slice-10b follow-up\|slice 10b follow-up\|10b.*follow" crates/broker/src/*.rs crates/broker/tests/*.rs
```

For each hit, remove the comment if the referenced issue is now closed; otherwise update to point at the merged follow-up PR.

- [ ] **Step 3: Commit**

```bash
git add STATUS.md crates/broker/src/lib.rs crates/broker/src/**.rs crates/broker/tests/**.rs
git commit -m "docs: mark slice-10b follow-up complete

Removes stale 'tracked under slice-10b follow-up' breadcrumbs now
that the underlying raft-timing fix landed.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 15: Final acceptance gate

**Files:** none

- [ ] **Step 1: Full workspace test, Linux**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo test --workspace 2>&1 | tail -20"
```

Expected: all green; no failures.

- [ ] **Step 2: Full workspace test, Windows**

```bash
cargo test --workspace 2>&1 | tail -10
```

Expected: all green (cross-platform tests pass; Linux/macOS-only tests excluded via cfg).

- [ ] **Step 3: Workspace clippy + fmt**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | head -5
```

Expected: both clean.

- [ ] **Step 4: Open the PR**

```bash
git push -u origin feature/bulletproof-eos-10b-followup
gh pr create --title "Slice 10b follow-up: re-enable deferred ISR/failover tests" \
  --body "$(cat <<'EOF'
## Summary

- Plumb openraft `election_timeout` + `heartbeat_interval` through `BrokerConfig` (defaults unchanged for prod safety; tests use 500ms/100ms).
- Hoist `start_n_node_with_retry` + bind-and-drop port helper into a shared `tests/support/mod.rs`.
- Re-enable the 4 rust + 3 JVM acceptance tests deferred in PR #73.

## Root cause

`Broker::start` hardcoded `election_timeout = 5s`, which made openraft's `leader_lease = 10s`. When the controller leader died, peers refused new elections for 10 seconds — exceeding the 10s producer timeout. Everything downstream (ISR shrink, HW advance, AlterPartition commit) waited on that election that never came.

## Test plan

- [ ] `cargo test --workspace` green on ubuntu, macos, windows
- [ ] `cargo test -p crabka-broker --test jvm_acceptance --ignored` green in <30 min
- [ ] No `#[ignore]` annotations claim slice-10b follow-up anymore

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed.

---

## Self-review

After completing all 15 tasks, verify against the spec sections:

| Spec section | Implemented in tasks |
|--------------|----------------------|
| Goal: re-enable 4 rust + 3 JVM tests | Tasks 8, 9, 10, 12, 13 |
| Background: root-cause raft `leader_lease` | Task 1 (docstring) |
| Arch: configurable raft timings on BrokerConfig | Tasks 1, 2 |
| Arch: boot retry for slice-10b helpers | Tasks 3, 6, 7 |
| Arch: ephemeral ports | Tasks 3, 6, 7 |
| Test plan: local WSL all green | Tasks 11, 15 |
| Test plan: CI all green | Task 15 |
| Out of scope: `Raft::trigger_election` integration | Not addressed — by design |

If any test still fails after Task 15, the most likely cause is a missed `BrokerConfig` literal in `jvm_acceptance.rs` — re-grep `grep -n "BrokerConfig {" crates/broker/tests/jvm_acceptance.rs` and verify each has the two new fields.
