# Deterministic raft cluster bootstrap — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace simultaneous static-init with explicit bootstrap-then-join so a 3-broker cluster forms deterministically on cold boot, eliminating the openraft cold-boot split-vote that's been forcing `start_n_node_with_retry` and `--skip` flags.

**Architecture:** Add `BootstrapMode { Bootstrap, Join, Rejoin }` to `crabka_raft::ControllerConfig` (and mirror onto `crabka_broker::BrokerConfig`). `Bootstrap` calls `raft.initialize` with `{(self, self_addr)}` — a singleton voter that self-elects trivially. `Join` skips `initialize` entirely; its raft engine sits in Learner state until the bootstrap broker calls `add_learner` + `change_membership` (the API shipped in PR #75). `Rejoin` skips `initialize` because the on-disk raft log already encodes membership.

**Tech Stack:** Rust 1.95.0; openraft 0.9.24; existing `BrokerHandle::{add_learner, change_membership}` from PR #75.

**Reference spec:** [`docs/superpowers/specs/2026-05-14-crabka-bootstrap-then-join-design.md`](../specs/2026-05-14-crabka-bootstrap-then-join-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/raft-bootstrap-join` (already created). Implementation runs on `feature/raft-bootstrap-join` off `main`.

---

## File structure

```
crates/raft/src/
├── config.rs                    # MODIFIED — BootstrapMode enum + ControllerConfig field
├── error.rs                     # MODIFIED — add RaftError::Startup(String) variant
└── controller.rs                # MODIFIED — bootstrap-mode match in Controller::start

crates/broker/src/
├── config.rs                    # MODIFIED — BootstrapMode field on BrokerConfig (re-exports raft enum)
├── lib.rs                       # MODIFIED — re-export BootstrapMode
├── bin/broker.rs                # MODIFIED — set Bootstrap on the CLI literal
└── broker.rs                    # MODIFIED — pass bootstrap_mode through to ControllerConfig

crates/broker/tests/
├── support/mod.rs               # MODIFIED — start_n_node uses bootstrap-then-join + broker_config takes mode
├── jvm_acceptance.rs            # MODIFIED — 6 BrokerConfig literals get explicit modes; multi-broker tests bootstrap-then-join
└── leader_election.rs           # MODIFIED — reborn BrokerConfig literal sets Rejoin

.github/workflows/ci.yml         # MODIFIED — drop 4 --skip flags
```

Eight `BrokerConfig` literal sites need the new field (verified via `grep -rn "BrokerConfig {" crates --include='*.rs'`). Two `ControllerConfig` literal sites need the new field. Multiple `BrokerConfig::for_tests` callers automatically pick up `Bootstrap` mode (the for_tests default is the single-broker-safe choice).

---

## Phase 1 — Add `BootstrapMode` to `crabka_raft`

### Task 1: Define `BootstrapMode` and add it to `ControllerConfig`

**Files:**
- Modify: `crates/raft/src/config.rs`
- Modify: `crates/raft/src/error.rs`

- [ ] **Step 1: Add `RaftError::Startup` variant**

Open `crates/raft/src/error.rs`. Add a new variant to the `RaftError` enum, just before the existing `#[error("controller shut down")] Shutdown`:

```rust
    #[error("startup misconfiguration: {0}")]
    Startup(String),
```

(The existing `#[non_exhaustive]` on the enum means callers won't be broken by the addition.)

- [ ] **Step 2: Verify the error change builds**

```bash
cargo build -p crabka-raft 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 3: Define `BootstrapMode` in `config.rs`**

Open `crates/raft/src/config.rs`. Replace the entire file contents with:

```rust
//! Construction-time config for `Controller::start`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::types::NodeId;

/// Bootstrap orchestration for a freshly-formatted controller node.
///
/// Openraft 0.9 lacks pre-vote (KIP-595's equivalent), so simultaneous
/// `raft.initialize(full_voter_set)` on multiple brokers can split-vote
/// indefinitely on cold boot. This enum lets the operator (or test harness)
/// pick a deterministic boot order:
///
/// 1. One broker boots with `Bootstrap` — it initializes as the sole voter
///    in a singleton cluster and self-elects on the first election timeout.
/// 2. Remaining brokers boot with `Join` — they don't initialize, so they
///    don't race to elect. The bootstrap broker brings them in via
///    [`crate::ControllerHandle::add_learner`] +
///    [`crate::ControllerHandle::change_membership`].
/// 3. After the initial format, restarted brokers use `Rejoin` — their
///    on-disk raft log already carries the membership and openraft replays
///    it during `Raft::new`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapMode {
    /// Cold-boot the first voter of a fresh cluster. `Controller::start`
    /// calls `raft.initialize({(self.node_id, self.controller_listen_addr)})`,
    /// producing a singleton-voter cluster that elects this broker as
    /// leader on its first timeout.
    Bootstrap,

    /// Cold-boot a subsequent voter. `Controller::start` skips `initialize`
    /// and the raft engine sits in Learner state waiting for the bootstrap
    /// broker to add it via `add_learner` + `change_membership`.
    Join,

    /// Restart a previously-formatted broker. The on-disk raft log encodes
    /// the cluster's current membership; `Controller::start` skips
    /// `initialize` and openraft replays existing state during `Raft::new`.
    Rejoin,
}

#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub node_id: NodeId,
    pub voters: Vec<(NodeId, SocketAddr)>,
    pub controller_listen_addr: SocketAddr,
    pub log_dir: PathBuf,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub client_id: String,
    pub bootstrap_mode: BootstrapMode,
}

impl ControllerConfig {
    #[must_use]
    pub fn for_tests(node_id: NodeId, log_dir: PathBuf) -> Self {
        Self {
            node_id,
            voters: vec![(node_id, "127.0.0.1:0".parse().expect("static"))],
            controller_listen_addr: "127.0.0.1:0".parse().expect("static"),
            log_dir,
            election_timeout: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(200),
            client_id: "crabka-controller-test".into(),
            bootstrap_mode: BootstrapMode::Bootstrap,
        }
    }
}
```

- [ ] **Step 4: Re-export `BootstrapMode` from `crates/raft/src/lib.rs`**

Open `crates/raft/src/lib.rs` and find the existing `pub use config::ControllerConfig;` (or similar). Add:

```rust
pub use config::BootstrapMode;
```

If the existing line is a glob (`pub use config::*;`), the export is already there — skip.

- [ ] **Step 5: Verify build**

```bash
cargo build -p crabka-raft 2>&1 | tail -5
```

This will fail — the two `ControllerConfig { ... }` literal sites in `crates/broker` are missing the new field. We'll fix those in Task 2 + 4. For now, confirm the failure is what we expect:

```
error[E0063]: missing field `bootstrap_mode` in initializer of `ControllerConfig`
```

That's the expected failure. Don't fix it here.

- [ ] **Step 6: Commit Phase 1's raft-side change**

```bash
git add crates/raft/src/config.rs crates/raft/src/error.rs crates/raft/src/lib.rs
git commit -m "feat(raft): BootstrapMode enum on ControllerConfig

Three modes: Bootstrap (singleton-voter init on first broker of a fresh
cluster), Join (skip initialize, wait for add_learner), Rejoin (restart
of a previously-formatted broker; on-disk log replays membership).
Also adds RaftError::Startup for misconfiguration surfaces in
Controller::start.

Caller-side wiring lands in the next commits.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: Implement `bootstrap_mode` match in `Controller::start`

**Files:**
- Modify: `crates/raft/src/controller.rs` (the "5. First-boot bootstrap" block, around line 422)

- [ ] **Step 1: Replace the simultaneous-init block**

Open `crates/raft/src/controller.rs`. Find the block that starts with:

```rust
// 5. First-boot bootstrap. Only attempt `initialize` when the log
//    is empty — restarting a node that has already participated
//    must NOT re-seed the cluster.
if log_store.last_log_id().await.is_none() {
    let members: BTreeMap<NodeId, Node> = config
        .voters
        ...
    if let Err(e) = raft.initialize(members).await {
        warn!(error = ?e, "raft initialize returned error (likely already-initialized); continuing");
    }
}
```

Replace the entire `// 5. First-boot bootstrap.` block (the comment + the `if log_store.last_log_id().await.is_none() { ... }` body) with:

```rust
        // 5. First-boot orchestration. The bootstrap_mode tells us which
        //    role this broker plays in cluster formation. Misuse is fatal —
        //    a Bootstrap on top of an existing log would re-seed the
        //    cluster, and a Rejoin on an empty log would never converge.
        let log_is_empty = log_store.last_log_id().await.is_none();
        match (config.bootstrap_mode, log_is_empty) {
            (BootstrapMode::Bootstrap, true) => {
                // Singleton-voter init. We become leader on our first
                // election timeout, no contention, no split-vote.
                let self_node = openraft::BasicNode {
                    addr: config.controller_listen_addr.to_string(),
                };
                let members: BTreeMap<NodeId, Node> =
                    [(config.node_id, self_node)].into_iter().collect();
                raft.initialize(members).await.map_err(|e| {
                    RaftError::Openraft(format!("bootstrap initialize: {e:?}"))
                })?;
            }
            (BootstrapMode::Join, true) => {
                // Don't initialize. Raft engine sits in Learner state
                // until the bootstrap broker's add_learner reaches us.
            }
            (BootstrapMode::Rejoin, false) => {
                // Existing log carries membership; openraft replayed it
                // during Raft::new. Nothing to do.
            }
            (BootstrapMode::Bootstrap, false) => {
                return Err(RaftError::Startup(
                    "Bootstrap mode requires empty raft log; existing log indicates an already-initialized broker — use Rejoin".into(),
                ));
            }
            (BootstrapMode::Rejoin, true) => {
                return Err(RaftError::Startup(
                    "Rejoin mode requires non-empty raft log; this broker has no on-disk state — use Bootstrap or Join".into(),
                ));
            }
            (BootstrapMode::Join, false) => {
                return Err(RaftError::Startup(
                    "Join mode requires empty raft log; this broker has on-disk state — use Rejoin".into(),
                ));
            }
        }
```

The `BootstrapMode` is now visible via `crate::config::BootstrapMode` or the `use crate::BootstrapMode;` re-export — add whichever import is needed at the top of the file (look for the existing `use crate::config::...` or `use crate::types::...` imports and add `use crate::config::BootstrapMode;` next to them).

- [ ] **Step 2: Verify the raft crate builds**

```bash
cargo build -p crabka-raft 2>&1 | tail -5
```

If `BTreeMap` or `Node` aren't already imported in this file, add `use std::collections::BTreeMap;` and check that `openraft::BasicNode` is reachable (the original code constructed it inline). Build until clean.

Expected outcome: `crabka-raft` builds clean. `crabka-broker` still fails because its two `ControllerConfig` literals are missing the field — that's fine, we'll fix in Task 4.

- [ ] **Step 3: Commit**

```bash
git add crates/raft/src/controller.rs
git commit -m "feat(raft): Controller::start branches on bootstrap_mode

Bootstrap (empty log) initializes a singleton-voter cluster. Join
(empty log) skips initialize so the engine sits in Learner state.
Rejoin (non-empty log) skips initialize because openraft replayed
the persisted membership. Every (mode, log-state) combination outside
those three returns RaftError::Startup with a message that points the
caller at the correct mode.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Phase 2 — Plumb `BootstrapMode` through `crabka_broker`

### Task 3: Add `bootstrap_mode` to `BrokerConfig`

**Files:**
- Modify: `crates/broker/src/config.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Add the field and re-export**

Open `crates/broker/src/config.rs`. Add at the top of the file (with the existing `use` block):

```rust
pub use crabka_raft::BootstrapMode;
```

Add the field to the `BrokerConfig` struct, immediately after `controller_heartbeat_interval`:

```rust
    /// How this broker participates in cluster formation. See
    /// [`crabka_raft::BootstrapMode`] for the trade-offs. The first broker
    /// of a fresh multi-broker cluster uses `Bootstrap`; subsequent brokers
    /// use `Join`; a restart of any previously-formatted broker uses
    /// `Rejoin`. Single-broker setups always use `Bootstrap`.
    pub bootstrap_mode: BootstrapMode,
```

In `for_tests`, add the field at the end of the struct literal:

```rust
            controller_election_timeout: std::time::Duration::from_millis(500),
            controller_heartbeat_interval: std::time::Duration::from_millis(100),
            bootstrap_mode: BootstrapMode::Bootstrap,
```

In `Default`, add the field at the end:

```rust
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: BootstrapMode::Bootstrap,
```

- [ ] **Step 2: Re-export `BootstrapMode` from the broker crate root**

Open `crates/broker/src/lib.rs`. Find the existing `pub use config::{BrokerConfig, ...};` (or `pub use config::BrokerConfig;`) and extend it to include `BootstrapMode`:

```rust
pub use config::{BrokerConfig, BootstrapMode};
```

If multiple `pub use` lines exist for `config`, add a separate `pub use config::BootstrapMode;` line.

- [ ] **Step 3: Add a unit test that asserts the default mode**

In `crates/broker/src/config.rs`, add to the existing `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn defaults_use_bootstrap_mode() {
        let c = BrokerConfig::default();
        assert_eq!(c.bootstrap_mode, BootstrapMode::Bootstrap);
    }

    #[test]
    fn for_tests_uses_bootstrap_mode() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert_eq!(c.bootstrap_mode, BootstrapMode::Bootstrap);
    }
```

- [ ] **Step 4: Build (will still fail until Task 4)**

```bash
cargo build -p crabka-broker 2>&1 | tail -10
```

Expect failures in `broker.rs` and the test files for missing `bootstrap_mode` field in the `ControllerConfig` / `BrokerConfig` struct literals. Tasks 4 and 5 fix those.

Don't commit yet — Task 4 has dependent changes that must land in the same commit to keep `cargo build` green.

---

### Task 4: Plumb through `Broker::start` and fix existing `ControllerConfig` / `BrokerConfig` literals

**Files:**
- Modify: `crates/broker/src/broker.rs` (around line 272, where `ControllerConfig { ... }` is built)
- Modify: `crates/broker/src/bin/broker.rs` (the single CLI `BrokerConfig` literal)
- Modify: `crates/broker/src/coordinator/bootstrap.rs` (the test-only `ControllerConfig` literal around line 193)
- Modify: `crates/broker/tests/leader_election.rs:288` (the reborn-broker `BrokerConfig` literal)
- Modify: `crates/broker/tests/jvm_acceptance.rs` (six `BrokerConfig` literals — line numbers shift after first edit; use grep)

- [ ] **Step 1: Pass `bootstrap_mode` through `Broker::start`**

Open `crates/broker/src/broker.rs`. Find the `ControllerConfig { ... }` literal around line 272 (the block that copies BrokerConfig timing fields into ControllerConfig). Add to the struct literal, alongside the other fields:

```rust
            bootstrap_mode: config.bootstrap_mode,
```

- [ ] **Step 2: Update the CLI binary**

Open `crates/broker/src/bin/broker.rs`. Find the `BrokerConfig { ... }` literal around line 57. Add to the struct literal (the CLI binary always single-broker-bootstraps):

```rust
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
```

- [ ] **Step 3: Update the test-only ControllerConfig in coordinator/bootstrap.rs**

Open `crates/broker/src/coordinator/bootstrap.rs`. Find the `ControllerConfig { ... }` literal around line 193 (inside a test). Add:

```rust
            bootstrap_mode: crabka_raft::BootstrapMode::Bootstrap,
```

- [ ] **Step 4: Update the reborn-broker literal in leader_election.rs**

Open `crates/broker/tests/leader_election.rs`. Find the `BrokerConfig { ... }` literal around line 288 (inside the `isr_expand_on_catchup` test, where the reborn broker is constructed). Add:

```rust
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
```

(The reborn broker is a fresh single-voter cluster that the test then folds back into the real cluster via `add_learner` + `change_membership` — same pattern as Bootstrap.)

- [ ] **Step 5: Update all six BrokerConfig literals in jvm_acceptance.rs**

Open `crates/broker/tests/jvm_acceptance.rs`. Run a grep first to confirm the line numbers (they're approximate from the spec):

```bash
grep -nE "BrokerConfig \{$|crabka_broker::BrokerConfig \{$" crates/broker/tests/jvm_acceptance.rs
```

There should be 6 hits. For each, add a `bootstrap_mode` field. Locations and modes:

1. Line ~65 (the `start_host_broker` helper, single-broker): `BootstrapMode::Bootstrap`
2. Line ~477 (`three_node_jvm_round_trip`, broker `i`): use `if i == 0 { BootstrapMode::Bootstrap } else { BootstrapMode::Join }`. The field expression in the struct literal is:

```rust
            bootstrap_mode: if i == 0 {
                crabka_broker::BootstrapMode::Bootstrap
            } else {
                crabka_broker::BootstrapMode::Join
            },
```

3. Line ~710 (`three_node_replication_byte_compare`, broker `i`): same pattern as 2.
4. Line ~943 (`transactional_console_producer_eos`, broker `i`): same pattern as 2.
5. Line ~1105 (`acks_all_durability`, broker `i`): same pattern as 2.
6. Line ~1254 (`acks_all_survives_leader_crash`, broker `i`): same pattern as 2.

Each multi-broker test boots `for i in 0..3` and constructs a `BrokerConfig` per `i`; the conditional sets broker 0 as Bootstrap and brokers 1, 2 as Join.

- [ ] **Step 6: Build to verify all literals compile**

```bash
cargo build --tests -p crabka-broker 2>&1 | tail -5
```

Expected: clean. If any literal is still missing the field, the compiler points at it — add `bootstrap_mode: BootstrapMode::Bootstrap` (or `Join`, depending on context) and rebuild.

- [ ] **Step 7: Verify the unit tests pass**

```bash
cargo test -p crabka-broker --lib 2>&1 | tail -10
```

Expected: all tests pass, including the two new `defaults_use_bootstrap_mode` + `for_tests_uses_bootstrap_mode` cases.

- [ ] **Step 8: Verify clippy + fmt**

```bash
cargo clippy -p crabka-broker --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | head -3
```

Both clean.

- [ ] **Step 9: Commit Tasks 3 + 4 together**

```bash
git add crates/broker/src/config.rs crates/broker/src/lib.rs crates/broker/src/broker.rs crates/broker/src/bin/broker.rs crates/broker/src/coordinator/bootstrap.rs crates/broker/tests/leader_election.rs crates/broker/tests/jvm_acceptance.rs
git commit -m "feat(broker): plumb BootstrapMode through BrokerConfig

Adds bootstrap_mode to BrokerConfig (default + for_tests both set
Bootstrap). Broker::start passes it through to ControllerConfig. All 8
existing literal sites get an explicit mode:

- CLI binary: Bootstrap (single-broker production)
- jvm_acceptance.rs (6 sites): broker 0 Bootstrap, brokers 1/2 Join
- leader_election.rs reborn broker: Bootstrap (singleton then merged)
- coordinator/bootstrap.rs test ControllerConfig: Bootstrap

Bootstrap-then-join wiring on the test side lands in the next task.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Phase 3 — Test helpers use bootstrap-then-join

### Task 5: Rewrite `support::start_n_node`

**Files:**
- Modify: `crates/broker/tests/support/mod.rs`

- [ ] **Step 1: Update `broker_config` to take a mode**

Find the existing `pub fn broker_config(...)` in `tests/support/mod.rs`. Add a `mode` parameter and propagate it. Replace the function with:

```rust
pub fn broker_config(
    i: usize,
    client_addrs: &[SocketAddr],
    controller_addrs: &[SocketAddr],
    voters: &[(u64, SocketAddr)],
    log_dir: &std::path::Path,
    mode: BootstrapMode,
) -> BrokerConfig {
    let listen = client_addrs[i];
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.node_id = u64::try_from(i + 1).unwrap();
    cfg.controller_listen_addr = controller_addrs[i];
    cfg.controller_quorum_voters = voters.to_vec();
    cfg.bootstrap_mode = mode;
    cfg
}
```

Add `BootstrapMode` to the imports at the top of `support/mod.rs`:

```rust
use crabka_broker::{Broker, BrokerConfig, BrokerError, BrokerHandle, BootstrapMode};
```

- [ ] **Step 2: Rewrite `start_n_node` for bootstrap-then-join**

Replace the existing `pub async fn start_n_node(...)` with:

```rust
pub async fn start_n_node(
    n: u64,
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    init_tracing();

    let n_usize = usize::try_from(n).unwrap();
    let (client_addrs, controller_addrs) = bind_and_drop_ports(n_usize).await;
    let voters: Vec<(u64, SocketAddr)> = (0..n)
        .map(|i| (i + 1, controller_addrs[i as usize]))
        .collect();

    // Phase 1: bootstrap broker 0 alone. Initializes as singleton voter,
    // becomes leader on first election timeout (no contention).
    let dir0 = TempDir::new().unwrap();
    let cfg0 = broker_config(0, &client_addrs, &controller_addrs, &voters, dir0.path(), BootstrapMode::Bootstrap);
    let broker0 = Broker::start(cfg0.clone()).await?;

    // Phase 2: spawn brokers 1..n in Join mode. Their Broker::start
    // blocks on watch_leader; we'll add_learner + change_membership below
    // to make them part of the cluster.
    let mut join_handles = Vec::with_capacity(n_usize.saturating_sub(1));
    let mut join_metas = Vec::with_capacity(n_usize.saturating_sub(1));
    for i in 1..n_usize {
        let dir = TempDir::new().unwrap();
        let cfg = broker_config(i, &client_addrs, &controller_addrs, &voters, dir.path(), BootstrapMode::Join);
        let cfg_clone = cfg.clone();
        join_handles.push(tokio::spawn(async move { Broker::start(cfg_clone).await }));
        join_metas.push((dir, cfg));
    }

    // Phase 3: add each Join broker as a learner, then promote them all
    // to voters in a single change_membership. The bootstrap broker
    // replicates the existing log to each follower as part of add_learner.
    for i in 1..n_usize {
        broker0
            .add_learner(u64::try_from(i + 1).unwrap(), controller_addrs[i])
            .await?;
    }
    let target_voters: std::collections::BTreeSet<u64> =
        (1..=u64::try_from(n_usize).unwrap()).collect();
    broker0.change_membership(target_voters).await?;

    // Now join brokers' watch_leader fires and Broker::start returns.
    let mut out: Vec<(BrokerHandle, BrokerConfig, TempDir)> = Vec::with_capacity(n_usize);
    out.push((broker0, cfg0, dir0));
    for (h, (dir, cfg)) in join_handles.into_iter().zip(join_metas) {
        let broker = h.await.expect("broker spawn join")?;
        out.push((broker, cfg, dir));
    }
    Ok(out)
}
```

- [ ] **Step 3: Keep `start_n_node_with_retry` as a thin alias**

The retry helper is still useful for transient network failures (port collisions on extra-busy CI runners, etc.), but it no longer needs to defend against split-vote. Leave its existing implementation alone — the underlying `start_n_node` is now deterministic, so retries should rarely fire.

- [ ] **Step 4: Build and test**

```bash
cargo build --tests -p crabka-broker 2>&1 | tail -5
```

Expected: clean. The call sites that pass `mode` are all internal to `support/mod.rs` (only `start_n_node` calls `broker_config` now).

- [ ] **Step 5: Run the rust integration suite on Linux via WSL**

```bash
git push -u origin feature/raft-bootstrap-join
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && git fetch origin --quiet && git checkout feature/raft-bootstrap-join 2>&1 | tail -2 && git pull origin feature/raft-bootstrap-join 2>&1 | tail -2 && timeout 600 cargo test -p crabka-broker -- --test-threads=1 2>&1 | grep -E 'test result|FAILED' | tail -20"
```

Expected: all rust integration tests still pass. The bootstrap-then-join path should be visibly faster (3-broker cluster reaches first leader in <1s rather than 5-30s observed today).

- [ ] **Step 6: Commit**

```bash
git add crates/broker/tests/support/mod.rs
git commit -m "test(support): start_n_node uses bootstrap-then-join

Phase 1: bootstrap broker 0 as a singleton voter. Phase 2: spawn
brokers 1..n in Join mode (their Broker::start blocks waiting for a
leader). Phase 3: bootstrap broker calls add_learner per joiner, then
change_membership to promote them all. No concurrent elections, no
split-vote, deterministic boot.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Phase 4 — JVM acceptance tests bootstrap-then-join

### Task 6: Migrate the four multi-broker JVM acceptance tests

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

The four multi-broker tests (`three_node_jvm_round_trip`, `three_node_replication_byte_compare`, `acks_all_durability`, `acks_all_survives_leader_crash` — `transactional_console_producer_eos` is env-var-gated and outside this slice) currently `for i in 0..3 { tokio::spawn(Broker::start(cfg)) }` and then join all three. Task 4 already gave each cfg the right `BootstrapMode` (broker 0 Bootstrap, others Join), so the only remaining work is sequencing the spawn + `add_learner` + `change_membership`.

- [ ] **Step 1: Identify the pattern in `three_node_jvm_round_trip`**

```bash
grep -nA80 "async fn three_node_jvm_round_trip" crates/broker/tests/jvm_acceptance.rs | head -100
```

Find the block that constructs the 3 BrokerConfigs in a loop and spawns them. There's a `let mut spawns = Vec::with_capacity(3);` followed by `for i in 0..3 { ... spawns.push(tokio::spawn(...)) }`, then `for sp in spawns ... cluster.push(sp.await.expect("spawn"))`.

We replace this with the same bootstrap-then-join pattern as `support::start_n_node`.

- [ ] **Step 2: Replace the spawn loop in `three_node_jvm_round_trip`**

Find the spawn loop (the `let mut spawns = ...` + `for i in 0..3 { ... }` block) and the subsequent collect into `cluster`. Replace both with:

```rust
    // Bootstrap-then-join: start broker 0 alone (it self-elects as a
    // singleton voter), then start brokers 1, 2 in Join mode and bring
    // them into the cluster via add_learner + change_membership. Avoids
    // openraft's cold-boot split-vote risk.
    let mut tempdirs = Vec::with_capacity(3);
    let mut cluster: Vec<(crabka_broker::BrokerHandle, tempfile::TempDir)> = Vec::with_capacity(3);

    // Broker 0 (Bootstrap).
    let dir0 = tempfile::tempdir().expect("tempdir");
    let cfg0 = BrokerConfig {
        broker_id: 1,
        listen_addr: format!("0.0.0.0:{}", client_ports[0]).parse().expect("static addr"),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: 1,
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0]).parse().expect("static addr"),
        controller_quorum_voters: voters.clone(),
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: std::time::Duration::from_secs(5),
        controller_heartbeat_interval: std::time::Duration::from_millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
    };
    let broker0 = crabka_broker::Broker::start(cfg0).await.expect("broker start");

    // Brokers 1, 2 (Join).
    let mut join_spawns = Vec::with_capacity(2);
    for i in 1..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i]).parse().expect("static addr"),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: LogConfig::default(),
            node_id: u64::try_from(i + 1).unwrap(),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i]).parse().expect("static addr"),
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: crabka_broker::BootstrapMode::Join,
        };
        tempdirs.push(dir);
        join_spawns.push(tokio::spawn(async move {
            crabka_broker::Broker::start(cfg).await.expect("broker start")
        }));
    }

    // Bring the join brokers into the cluster: add as learners, then
    // promote to voters in one change_membership.
    let voter_addr = |i: usize| -> std::net::SocketAddr {
        format!("127.0.0.1:{}", controller_ports[i]).parse().expect("static addr")
    };
    broker0.add_learner(2, voter_addr(1)).await.expect("add_learner 2");
    broker0.add_learner(3, voter_addr(2)).await.expect("add_learner 3");
    broker0.change_membership([1u64, 2u64, 3u64].into_iter().collect()).await.expect("promote join brokers to voters");

    // Join brokers' watch_leader fires and Broker::start returns.
    cluster.push((broker0, dir0));
    for (spawn, dir) in join_spawns.into_iter().zip(tempdirs) {
        cluster.push((spawn.await.expect("spawn"), dir));
    }
```

Notice: `voter_addr` uses `127.0.0.1` rather than `0.0.0.0`. Openraft connects to the addr from the membership; we need a routable one. Brokers bind on `0.0.0.0` (any interface) but the leader's openraft contacts them via `127.0.0.1:port`.

Wait — that's wrong. We pass the original `controller_listen_addr` (which is `0.0.0.0:port`) into the voter map. openraft uses *that* string to dial. `0.0.0.0` is a bind-only address and isn't valid for connecting. Let me double-check…

Actually look at the existing test code: the existing voters map uses `format!("127.0.0.1:{}", controller_ports[i])`, NOT the broker's listen_addr. The bind addr is `0.0.0.0:port` (for accepting), the voter addr is `127.0.0.1:port` (for dialing). So `voter_addr` here matches.

- [ ] **Step 3: Apply the same pattern to the other three multi-broker tests**

`three_node_replication_byte_compare`, `acks_all_durability`, `acks_all_survives_leader_crash` — each follows the same structure. For each, replace the `for i in 0..3 { tokio::spawn(...) }` spawn loop + collect with the Bootstrap-broker-0, Join-brokers-1..n, add_learner, change_membership flow shown above. The only test-specific differences are:

- The `heartbeat_*_ms` and `replica_lag_time_max_ms` values (some tests use 3000/9000/30_000, `acks_all_survives_leader_crash` uses 200/2_000/2_000 — preserve each test's existing values when rewriting the cfg literal).
- The `controller_election_timeout` and `controller_heartbeat_interval` values (preserve each test's existing values — 5s/500ms for the non-failover tests, 500ms/100ms for `acks_all_survives_leader_crash`).
- The variable name for the cluster tuple (`cluster` everywhere, but `acks_all_survives_leader_crash` later does `cluster.remove(leader_idx)` so the type signature stays).

Don't blindly copy-paste — preserve each test's timing constants. The bootstrap-then-join *sequencing* is what changes, not the per-broker config.

- [ ] **Step 4: Build and verify compile**

```bash
cargo build --tests -p crabka-broker 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 5: Run the JVM acceptance suite on Linux (requires Docker)**

```bash
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(broker): JVM-acceptance multi-broker tests use bootstrap-then-join

The four multi-broker JVM acceptance tests (three_node_jvm_round_trip,
three_node_replication_byte_compare, acks_all_durability, and
acks_all_survives_leader_crash) replace their for-loop static-init
spawn pattern with the same bootstrap-then-join sequence as
support::start_n_node. Cold-boot election now reaches a leader within
~election_timeout_min (sub-second for most tests).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
git push origin feature/raft-bootstrap-join

wsl -d Ubuntu -- bash -c "cd ~/git/crabka && git fetch origin --quiet && git pull origin feature/raft-bootstrap-join 2>&1 | tail -2 && timeout 1500 cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1 2>&1 | tail -25"
```

Expected: all 9 JVM acceptance tests pass within ~25 minutes.

If a specific test fails: read its panic message, capture the relevant log section (election traces, AlterPartition activity, broker shutdown order), and either iterate or report DONE_WITH_CONCERNS with the specific diagnostic.

---

## Phase 5 — Drop the CI `--skip` flags

### Task 7: Remove `--skip` filters from `broker-jvm-acceptance`

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Replace the multi-line run step**

Find the `broker-jvm-acceptance` job's `run` block, which currently looks like:

```yaml
      # --skip the 4 multi-broker JVM acceptance tests: ...
      - run: |
          cargo test -p crabka-broker --test jvm_acceptance -- \
            --ignored --nocapture --test-threads=1 \
            --skip acks_all_durability \
            --skip acks_all_survives_leader_crash \
            --skip three_node_jvm_round_trip \
            --skip three_node_replication_byte_compare
```

Replace with:

```yaml
      # --test-threads=1: tests bind the Rust broker to a fixed host port
      # (so the `docker run --add-host` Kafka tools have a known bootstrap
      # target). Parallel execution would hit `AddrInUse`.
      - run: cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1
```

- [ ] **Step 2: Commit and push**

```bash
git add .github/workflows/ci.yml
git commit -m "ci(jvm-acceptance): drop --skip; full 9-test suite

Bootstrap-then-join in jvm_acceptance.rs's multi-broker tests
eliminates the cold-boot split-vote that was forcing these skips.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
git push origin feature/raft-bootstrap-join
```

---

## Phase 6 — Closeout

### Task 8: Final acceptance + PR

**Files:** none

- [ ] **Step 1: Full workspace test on Windows**

```bash
cargo test --workspace 2>&1 | tail -10
```

Expected: all green.

- [ ] **Step 2: Full workspace test on Linux**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && timeout 600 cargo test --workspace -- --test-threads=1 2>&1 | grep -E 'test result|FAILED' | tail -25"
```

Expected: all green. The 3-broker rust integration tests (`isr_expand_on_catchup`, `broker_death_elects_new_leader`, etc.) should complete noticeably faster.

- [ ] **Step 3: Workspace clippy + fmt**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | head -3
```

Both clean.

- [ ] **Step 4: Open the PR**

```bash
gh pr create --title "Deterministic raft bootstrap: bootstrap-then-join eliminates cold-boot split-vote" --body "$(cat <<'EOF'
## Summary

- Add `BootstrapMode { Bootstrap, Join, Rejoin }` to `crabka_raft::ControllerConfig` (mirrored on `BrokerConfig`).
- `Controller::start` branches on the mode: Bootstrap initializes a singleton-voter cluster, Join skips initialize and waits for `add_learner`, Rejoin replays the existing on-disk log.
- `tests/support/mod.rs::start_n_node` and all four multi-broker JVM acceptance tests use the bootstrap-then-join pattern. No more cold-boot split-vote.
- Drop the four `--skip` flags from `broker-jvm-acceptance` CI.

## Why

openraft 0.9 lacks pre-vote (KIP-595). With simultaneous static `initialize(full_voter_set)`, three brokers each pick a once-randomized election timeout and end up firing concurrent elections — votes split, retry, split again. We've observed >2-min boot failures on ubuntu-latest. Bootstrap-then-join sidesteps this entirely: one broker initializes alone (trivial self-election), the rest receive `AppendEntries` from the leader instead of trying to elect themselves.

Matches Kafka KRaft's operational semantics (all membership changes operator-driven) — just orchestrated differently at cold-boot because we can't lean on pre-vote yet.

## Test plan

- [ ] `cargo test --workspace` green on ubuntu/macos/windows
- [ ] `cargo test -p crabka-broker --test jvm_acceptance --ignored` runs all 9 tests, all pass
- [ ] No `--skip` filters in `broker-jvm-acceptance`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)" 2>&1 | tail -3
```

Report the PR URL.

---

## Self-review

| Spec section | Implemented in tasks |
|--------------|----------------------|
| `BootstrapMode` enum on ControllerConfig | Task 1 |
| `RaftError::Startup` variant | Task 1 |
| Controller::start mode-match | Task 2 |
| BrokerConfig field + Default/for_tests + re-export | Task 3 |
| Broker::start passes mode through | Task 4 |
| All 8 existing BrokerConfig/ControllerConfig literal sites | Task 4 |
| support::start_n_node uses bootstrap-then-join | Task 5 |
| jvm_acceptance.rs 4 multi-broker tests | Task 6 |
| CI --skip flags removed | Task 7 |
| Acceptance: workspace test, clippy, fmt, PR | Task 8 |
| Unit test: default mode is Bootstrap | Task 3 |
| Unit test: Bootstrap on non-empty log errors | (added below) |

A spec requirement was missed: "New unit test: `Controller::start` returns `Startup` error if `Bootstrap` mode given a non-empty raft log." Adding as Task 1.5.

### Task 1.5: Unit test for invalid (mode, log-state) combos

**File:** `crates/raft/src/controller.rs` (new test in the existing `#[cfg(test)] mod tests` block, or create one if absent)

- [ ] **Step 1: Add the test**

In `crates/raft/src/controller.rs`, find an existing test block or add one at the bottom of the file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn bootstrap_on_non_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        // First boot: bootstrap fresh.
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("first bootstrap ok");
        ctrl.shutdown().await;

        // Second boot: log is non-empty, Bootstrap must error.
        let cfg2 = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let err = Controller::start(cfg2).await.expect_err("Bootstrap on existing log must error");
        assert!(matches!(err, RaftError::Startup(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn rejoin_on_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Rejoin,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let err = Controller::start(cfg).await.expect_err("Rejoin on empty log must error");
        assert!(matches!(err, RaftError::Startup(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn join_on_empty_log_starts_in_learner_state() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Join,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("Join on empty log starts ok");
        // Without an external add_learner the watch_leader stays None.
        assert!(ctrl.watch_leader().borrow().is_none());
        ctrl.shutdown().await;
    }
}
```

- [ ] **Step 2: Run the new tests**

```bash
cargo test -p crabka-raft --lib 2>&1 | tail -10
```

Expected: 3 new tests pass.

- [ ] **Step 3: Commit (folds into Task 2's commit if executed back-to-back, otherwise its own)**

If folded into Task 2's commit, `git add` the controller.rs change before the commit. Otherwise:

```bash
git add crates/raft/src/controller.rs
git commit -m "test(raft): Controller::start guards against invalid (mode, log) combos

Bootstrap on a non-empty log refuses (would re-seed an existing
cluster). Rejoin on an empty log refuses (would never converge). Join
on an empty log starts fine and leaves watch_leader at None until an
external add_learner brings the node into the cluster.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```
