# Raft controller-quorum membership change — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose manual `change_membership` + `add_learner` on `ControllerHandle` (and a thin `BrokerHandle` wrapper) so tests and operators can mutate the openraft voter set at runtime. Close the 3 still-deferred slice-10b tests using this API + a multi-broker JVM bootstrap fix.

**Architecture:** Forward openraft's `Raft::change_membership(ReplaceAllVoters)` and `Raft::add_learner(blocking=true)` calls through `ControllerHandle`. Map errors into `crabka_raft::RaftError`. No auto-removal on broker death — matches Kafka KRaft. The 3 deferred integration tests get explicit `change_membership` / `add_learner` calls + multi-broker JVM bootstrap.

**Tech Stack:** Rust 1.95.0; openraft 0.9.24 (already in workspace); existing `crabka_raft` controller infra.

**Reference spec:** [`docs/superpowers/specs/2026-05-14-crabka-raft-membership-design.md`](../specs/2026-05-14-crabka-raft-membership-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/raft-membership` (already created). Implementation runs on `feature/raft-membership` once this plan's PR merges.

---

## File structure

```
crates/raft/src/
└── controller.rs                    # MODIFIED — change_membership + add_learner public methods, voter cache update

crates/broker/src/
└── broker.rs                        # MODIFIED — change_membership + add_learner wrappers on BrokerHandle

crates/broker/tests/
├── leader_election.rs               # MODIFIED — un-#[ignore] isr_expand_on_catchup + use new API
└── jvm_acceptance.rs                # MODIFIED — un-skip 2 tests; multi-bootstrap producer args

.github/workflows/ci.yml             # MODIFIED — drop --skip flags
```

No new files. Everything is targeted additions to existing modules.

---

## Phase 1 — `change_membership` on `ControllerHandle`

### Task 1: Add `ControllerHandle::change_membership(new_voters)`

**Files:**
- Modify: `crates/raft/src/controller.rs` (add a new method to `impl ControllerHandle`)

- [ ] **Step 1: Add the method**

Open `crates/raft/src/controller.rs`. Find the existing `impl ControllerHandle` block — locate the `pub async fn submit_change` method (around line 87 currently) and add the following method immediately after `submit_change`:

```rust
    /// Mutate the openraft voter set. `new_voters` is the **complete** desired set
    /// (not a delta). Any voter in the current set but not in `new_voters` is
    /// removed entirely (`retain=false`). Any voter in `new_voters` that isn't
    /// already in the cluster must have been registered via [`Self::add_learner`]
    /// first — openraft refuses to promote unknown ids.
    ///
    /// Two-phase joint config: openraft commits a joint membership log entry
    /// (old ∪ new), then a uniform log entry (new only). If the leader crashes
    /// between the two, the cluster is left in joint config and a future call
    /// completes the transition.
    ///
    /// # Errors
    ///
    /// - `RaftError::NotLeader` if this node isn't the openraft leader.
    /// - `RaftError::ChangeRejected` if openraft rejects (e.g., the new voter set
    ///   would leave the cluster without quorum, or a promoted node isn't a learner).
    /// - `RaftError::Shutdown` if the raft engine has been shut down.
    pub async fn change_membership(
        &self,
        new_voters: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), RaftError> {
        use openraft::error::ClientWriteError;
        use openraft::error::RaftError as ORE;
        match self.raft.change_membership(new_voters, false).await {
            Ok(_) => Ok(()),
            Err(ORE::APIError(ClientWriteError::ForwardToLeader(f))) => Err(RaftError::NotLeader {
                current_leader: f.leader_id,
            }),
            Err(ORE::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(RaftError::ChangeRejected(format!("{e:?}")))
            }
            Err(e) => Err(RaftError::Openraft(format!("{e:?}"))),
        }
    }
```

The `ChangeMembershipError` enum covers cases like `EmptyMembership`, `LearnerNotFound`, `LearnerNotFoundForNode`, etc. — we collapse them all into a single `ChangeRejected` string variant; callers don't need to discriminate.

- [ ] **Step 2: Verify the file compiles**

```bash
cargo build -p crabka-raft 2>&1 | tail -5
```

Expected: clean build.

- [ ] **Step 3: Verify clippy + fmt**

```bash
cargo clippy -p crabka-raft --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | head -5
```

Expected: both clean.

- [ ] **Step 4: Commit**

```bash
git add crates/raft/src/controller.rs
git commit -m "feat(raft): ControllerHandle::change_membership for manual voter-set changes

Wraps openraft's Raft::change_membership(ReplaceAllVoters, retain=false).
Caller passes the complete desired voter set; voters not in it are
removed entirely. Errors map to RaftError::NotLeader / ChangeRejected.

Matches Kafka KRaft's manual operator-driven membership semantics. No
auto-removal of dead brokers; the surviving raft cluster keeps the dead
voter (with the usual AppendEntries log spam) until an operator or test
calls this API.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: Add `ControllerHandle::add_learner(node_id, addr)`

**Files:**
- Modify: `crates/raft/src/controller.rs` (add a method directly after `change_membership`)

- [ ] **Step 1: Add the method**

Add this method immediately after `change_membership` in the `impl ControllerHandle` block:

```rust
    /// Register a non-voting raft learner at `addr` with id `node_id`. Blocks
    /// until the leader has replicated up to its current commit index to the
    /// new node (so a subsequent [`Self::change_membership`] promotion won't
    /// stall waiting for catch-up). Pair with [`Self::change_membership`] to
    /// turn a learner into a voter:
    ///
    /// ```ignore
    /// controller.add_learner(4, "127.0.0.1:9094".parse().unwrap()).await?;
    /// controller.change_membership([1, 2, 3, 4].into_iter().collect()).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// - `RaftError::NotLeader` if this node isn't the openraft leader.
    /// - `RaftError::ChangeRejected` if openraft rejects (e.g., the learner
    ///   never catches up within openraft's internal deadline).
    /// - `RaftError::Shutdown` if the raft engine has been shut down.
    pub async fn add_learner(
        &self,
        node_id: NodeId,
        addr: SocketAddr,
    ) -> Result<(), RaftError> {
        use openraft::error::{ClientWriteError, RaftError as ORE};
        let node = openraft::BasicNode {
            addr: addr.to_string(),
        };
        match self.raft.add_learner(node_id, node, true).await {
            Ok(_) => Ok(()),
            Err(ORE::APIError(ClientWriteError::ForwardToLeader(f))) => Err(RaftError::NotLeader {
                current_leader: f.leader_id,
            }),
            Err(ORE::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(RaftError::ChangeRejected(format!("{e:?}")))
            }
            Err(e) => Err(RaftError::Openraft(format!("{e:?}"))),
        }
    }
```

- [ ] **Step 2: Verify build, clippy, fmt**

```bash
cargo build -p crabka-raft 2>&1 | tail -5
cargo clippy -p crabka-raft --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | head -5
```

All three: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/raft/src/controller.rs
git commit -m "feat(raft): ControllerHandle::add_learner for two-phase voter add

Wraps openraft's Raft::add_learner(blocking=true). Pair with
change_membership to add a brand-new node to the voter set: first
register as a learner so openraft replicates the log to it, then
promote via change_membership. Blocking=true so the subsequent
promotion doesn't stall waiting for catch-up.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Phase 2 — `BrokerHandle` wrappers

### Task 3: Add `BrokerHandle::change_membership` and `add_learner`

**Files:**
- Modify: `crates/broker/src/broker.rs` (extend the existing `impl BrokerHandle` block with two new methods)

- [ ] **Step 1: Find the right insertion point**

Search `crates/broker/src/broker.rs` for the existing `impl BrokerHandle` block — there's an existing `pub async fn broker_count` method and a `pub async fn shutdown` method. Add the two new methods immediately after `broker_count` (or anywhere in the block; keep them grouped).

- [ ] **Step 2: Add the methods**

```rust
    /// Manually mutate the openraft voter set on this broker's controller.
    /// `new_voters` is the complete desired set (not a delta). Callers must
    /// invoke this on the broker that's currently the openraft leader, or
    /// the call returns [`RaftError::NotLeader`]. See
    /// [`crabka_raft::ControllerHandle::change_membership`] for full semantics.
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft errors as
    /// [`BrokerError::Replication`]. The error message includes the openraft
    /// detail (rejection reason, leader hint, etc.).
    pub async fn change_membership(
        &self,
        new_voters: std::collections::BTreeSet<crabka_raft::NodeId>,
    ) -> Result<(), BrokerError> {
        self._broker
            .controller
            .change_membership(new_voters)
            .await
            .map_err(|e| BrokerError::Replication(format!("change_membership: {e}")))
    }

    /// Register a non-voting openraft learner at `addr`. Blocks until the
    /// leader has caught the new node up to the current commit index.
    /// Subsequent [`Self::change_membership`] promotes the learner to a voter.
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft errors as [`BrokerError::Replication`].
    pub async fn add_learner(
        &self,
        node_id: crabka_raft::NodeId,
        addr: std::net::SocketAddr,
    ) -> Result<(), BrokerError> {
        self._broker
            .controller
            .add_learner(node_id, addr)
            .await
            .map_err(|e| BrokerError::Replication(format!("add_learner: {e}")))
    }
```

- [ ] **Step 3: Verify build, clippy, fmt**

```bash
cargo build -p crabka-broker 2>&1 | tail -5
cargo clippy -p crabka-broker --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | head -5
```

All clean.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "feat(broker): BrokerHandle::{change_membership,add_learner}

Thin wrappers around the new crabka_raft methods so tests and library
consumers don't need to reach into the raft crate. Errors map to
BrokerError::Replication with the raft detail preserved.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Phase 3 — Re-enable `isr_expand_on_catchup`

### Task 4: Use `add_learner` + `change_membership` in `isr_expand_on_catchup`

**Files:**
- Modify: `crates/broker/tests/leader_election.rs`

The test currently re-binds broker 3 at the same address after shutdown and waits for ISR to expand back to {1,2,3}. Openraft on the survivors keeps trying to replicate to the dead node 3 indefinitely (because node 3 remains in the voter set with stale state), and the reborn node 3 — with a fresh raft log — can't replay the existing committed log naturally. The fix is to make the membership change explicit:

1. **Before kill:** call `change_membership` on the openraft leader to remove node 3 from the voter set. Openraft now stops replicating to node 3.
2. **Kill node 3.**
3. **Reboot node 3 with a fresh TempDir** at the same addr (or a fresh ephemeral port — either works once node 3 is out of the voter set).
4. **After reboot:** call `add_learner` to register the new node-3 instance and replicate the committed log into it.
5. **Then:** call `change_membership` to promote node 3 back to a voter.
6. **Assert:** the partition's ISR converges to {1,2,3} (existing assertion in the test).

- [ ] **Step 1: Locate the test**

In `crates/broker/tests/leader_election.rs`, find the existing `isr_expand_on_catchup` test. It's currently marked:

```rust
// Hangs on Linux: openraft on the surviving leader spams AppendEntries
// to the killed broker 3 indefinitely, and the "reborn broker" pattern
// re-binds the old controller addr but openraft on the survivors
// already considers node 3 a phantom replica it can't reach. Needs a
// proper raft membership change (Raft::change_membership) to remove
// the dead node before reborn, or a different test design.
#[ignore = "raft thrashes when target=3 stays in voter set after kill; needs membership-change wire"]
async fn isr_expand_on_catchup() {
```

- [ ] **Step 2: Read the existing test body to understand the current cluster setup**

```bash
grep -nA 60 "async fn isr_expand_on_catchup" crates/broker/tests/leader_election.rs | head -100
```

You'll see:
- `let mut cluster = support::start_n_node_with_retry(3).await;`
- The test pops broker 3 and `dead.0.shutdown().await;`
- The test then calls `Broker::start(cfg)` to reborn broker 3 at the same controller addr (`dead_voters` voter map kept the original port).

We're going to insert two helper calls — one before the kill, one after the reboot — and remove the obsolete `#[ignore]` + comment block.

- [ ] **Step 3: Replace the test body with the membership-aware flow**

Open `crates/broker/tests/leader_election.rs`. Replace the entire `isr_expand_on_catchup` function (the `#[ignore]` attribute, the comment block above it, and the function body) with:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isr_expand_on_catchup() {
    let _g = cluster_lock().lock().await;
    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let bootstrap_1 = cluster[0].1.listen_addr.to_string();

    create_topic(&cluster[0].0, &bootstrap_1, "expand", 3).await;

    // 1. Find the current openraft leader so we can drive membership changes on it.
    let leader_idx = find_controller_leader(&cluster).await;
    let leader_node_id = cluster[leader_idx].1.node_id;
    eprintln!("CRABKA[test] controller leader is node_id={leader_node_id}");

    // 2. Remove node 3 from the voter set BEFORE kill. The leader's openraft
    //    commits a joint config then a uniform config; after the second commit
    //    survivors stop replicating to node 3.
    cluster[leader_idx]
        .0
        .change_membership([1u64, 2u64].into_iter().collect())
        .await
        .expect("remove node 3 from voter set");

    // 3. Capture node 3's addr for the reborn broker, then kill.
    let dead_listen_addr = cluster[2].1.listen_addr;
    let dead_controller_addr = cluster[2].1.controller_listen_addr;
    let (dead_h, _dead_cfg, _dead_dir) = cluster.remove(2);
    dead_h.shutdown().await;

    // 4. Reboot node 3 with a fresh TempDir + same controller addr. The
    //    BrokerConfig's voter map still lists all three voters; that's fine
    //    because we'll re-add node 3 explicitly.
    let reborn_dir = TempDir::new().unwrap();
    let voters = vec![
        (1u64, cluster[0].1.controller_listen_addr),
        (2u64, cluster[1].1.controller_listen_addr),
        (3u64, dead_controller_addr),
    ];
    let reborn_cfg = BrokerConfig {
        broker_id: 3,
        listen_addr: dead_listen_addr,
        advertised_listener: dead_listen_addr.to_string(),
        log_dir: reborn_dir.path().to_path_buf(),
        log_config: Default::default(),
        node_id: 3,
        controller_listen_addr: dead_controller_addr,
        controller_quorum_voters: voters,
        heartbeat_interval_ms: 200,
        heartbeat_timeout_ms: 2_000,
        replica_lag_time_max_ms: 2_000,
        controller_election_timeout: Duration::from_millis(500),
        controller_heartbeat_interval: Duration::from_millis(100),
    };
    let reborn = Broker::start(reborn_cfg).await.expect("reborn node 3");
    eprintln!("CRABKA[test] reborn node 3 started");

    // 5. Re-find the controller leader (broker_death_elects_new_leader proves it
    //    might have changed during the joint-config commit).
    let leader_idx = find_controller_leader(&cluster).await;

    // 6. Register reborn node 3 as a learner; openraft will replicate the
    //    committed log to it. Then promote it back to a voter.
    cluster[leader_idx]
        .0
        .add_learner(3, dead_controller_addr)
        .await
        .expect("add reborn node 3 as learner");
    cluster[leader_idx]
        .0
        .change_membership([1u64, 2u64, 3u64].into_iter().collect())
        .await
        .expect("promote reborn node 3 to voter");

    // 7. Wait for the partition's ISR to expand back to {1, 2, 3}.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut expanded = false;
    while Instant::now() < deadline {
        let client = Client::builder()
            .bootstrap(bootstrap_1.clone())
            .build()
            .await
            .unwrap();
        let resp = client
            .send(MetadataRequest {
                topics: Some(vec![MetadataRequestTopic {
                    name: Some("expand".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .await
            .expect("metadata");
        if let Some(t) = resp
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some("expand"))
            && let Some(p) = t.partitions.first()
            && p.isr_nodes.len() == 3
        {
            expanded = true;
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert!(expanded, "ISR did not expand back to 3 within 10s");

    reborn.shutdown().await;
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
```

- [ ] **Step 4: Add the `find_controller_leader` helper**

The test body above references `find_controller_leader`. Add this helper just above the `isr_expand_on_catchup` test (or anywhere in the file outside a test function):

```rust
/// Poll the cluster until exactly one broker reports itself as the openraft
/// controller leader. Returns the cluster index of that broker (0-based).
async fn find_controller_leader(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for (i, (h, cfg, _)) in cluster.iter().enumerate() {
            if h.controller_leader_id().await == Some(cfg.node_id) {
                return i;
            }
        }
        assert!(
            Instant::now() <= deadline,
            "no controller leader found within 10s"
        );
        sleep(Duration::from_millis(50)).await;
    }
}
```

- [ ] **Step 5: Push and run on Linux**

```bash
git add crates/broker/tests/leader_election.rs
git commit -m "test(broker): isr_expand_on_catchup uses change_membership

Was hanging because openraft on the survivors kept replicating to the
dead node 3 forever after the test reborn-ed it at the same address.
Now the test explicitly removes node 3 before kill (change_membership)
and re-adds it after reboot (add_learner + change_membership) — the
manual KRaft membership-change flow.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
git push origin feature/raft-membership
```

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && git fetch origin --quiet && git checkout feature/raft-membership 2>&1 | tail -2 && git pull origin feature/raft-membership 2>&1 | tail -2 && timeout 240 cargo test -p crabka-broker --test leader_election isr_expand_on_catchup -- --nocapture 2>&1 | tail -20"
```

Expected: test passes (ISR converges to 3 within 10s).

If the test fails:
- If `change_membership` returns `NotLeader`: the test's `find_controller_leader` didn't find the right broker. Add `eprintln!` on `cluster[i].0.controller_leader_id().await` to see what each broker reports.
- If `add_learner` hangs / times out: the reborn broker's openraft isn't accepting the connection. Check the reborn broker's controller-listen log. Most likely a port-binding race; ensure no other broker is on the same addr.
- If ISR doesn't expand: re-check `assert!(expanded, ...)` message — it should print the ISR snapshot. If ISR is e.g. {1,2} forever, the AlterPartition expand path needs revisiting (out of this slice's scope).

---

## Phase 4 — Fix JVM acceptance tests' bootstrap

### Task 5: Multi-broker bootstrap for `acks_all_survives_leader_crash`

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs` (only the `acks_all_survives_leader_crash` function)

The test currently sets `bootstrap_1 = host.docker.internal:{client_ports[0]}` and passes that as `--bootstrap-server` to the JVM producer. After the test kills broker 1, the producer can't bootstrap because its only known broker is dead — it retries indefinitely. Apache Kafka producers accept a comma-separated bootstrap list; passing all 3 lets the producer find a survivor.

- [ ] **Step 1: Find the relevant block**

```bash
grep -nB1 -A3 "bootstrap_1.*client_ports\[0\]" crates/broker/tests/jvm_acceptance.rs | head -30
```

Locate the `bootstrap_1` definition inside `acks_all_survives_leader_crash` (NOT the one in `acks_all_durability` or `three_node_jvm_round_trip`).

- [ ] **Step 2: Add a multi-broker bootstrap variable**

Just below the `bootstrap_1 = ...` line, add:

```rust
    // Multi-broker bootstrap so the JVM producer can find a survivor when
    // broker 1 (the partition leader) is killed mid-burst. Without this the
    // producer hangs on bootstrap because its only known broker is dead.
    let bootstrap_all = format!(
        "host.docker.internal:{},host.docker.internal:{},host.docker.internal:{}",
        client_ports[0], client_ports[1], client_ports[2],
    );
```

- [ ] **Step 3: Switch the producer to use the multi-bootstrap**

Find the producer-spawn `Command::new("docker")` block and locate the `--bootstrap-server {bootstrap_1}` substring inside the formatted shell string. Replace `{bootstrap_1}` with `{bootstrap_all}`. The block becomes:

```rust
    let producer_child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "bash",
            "-c",
            &format!(
                "for i in $(seq 1 100); do echo \"crash-msg-$i\"; done | \
                 kafka-console-producer \
                   --bootstrap-server {bootstrap_all} \
                   --topic {TOPIC} \
                   --request-required-acks -1 \
                   --request-timeout-ms 30000"
            ),
        ])
```

The `kafka-topics --create`, `kafka-topics --describe`, and `kafka-console-consumer` invocations in the same test already use `bootstrap_1` for a single broker — those are fine to leave on `bootstrap_1` because they run before the kill. Don't change them.

- [ ] **Step 4: Build (Linux) to verify the test still compiles**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo build --tests -p crabka-broker 2>&1 | tail -5"
```

Expected: clean.

- [ ] **Step 5: Commit (do not push yet — we want to combine with Task 6)**

```bash
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(broker): acks_all_survives_leader_crash uses multi-broker bootstrap

The JVM producer's --bootstrap-server was a single host:port pointing
at the broker the test then killed. Kafka producers without a fallback
bootstrap retry forever on NETWORK_EXCEPTION (their default
retries=Integer.MAX_VALUE). Passing all 3 brokers lets the producer
find a survivor and continue produces against the new partition leader.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 6: Multi-broker bootstrap for `three_node_replication_byte_compare`

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs` (only the `three_node_replication_byte_compare` function)

This test sends bursts against a 3-broker cluster and compares the resulting partition bytes for parity with a JVM reference. It was on the `--skip` list because it was hanging — same root cause class (single bootstrap broker can become unavailable mid-test if openraft churn breaks the listener, even without explicit kill). Switch to multi-bootstrap as the simpler fix.

- [ ] **Step 1: Locate the `bootstrap_1` in `three_node_replication_byte_compare`**

```bash
grep -nE "async fn three_node_replication_byte_compare|let bootstrap_1" crates/broker/tests/jvm_acceptance.rs | head -10
```

Identify the `bootstrap_1` line inside the `three_node_replication_byte_compare` function body (NOT the one in `three_node_jvm_round_trip` or other tests).

- [ ] **Step 2: Add the multi-bootstrap variable**

Just after the `bootstrap_1 = ...` line, add:

```rust
    let bootstrap_all = format!(
        "host.docker.internal:{},host.docker.internal:{},host.docker.internal:{}",
        client_ports[0], client_ports[1], client_ports[2],
    );
```

- [ ] **Step 3: Replace `{bootstrap_1}` with `{bootstrap_all}` inside the kafka-console-producer call only**

Locate the `kafka-console-producer` invocation in this test (there's exactly one). Replace the `--bootstrap-server {bootstrap_1}` arg with `--bootstrap-server {bootstrap_all}`. Leave other tool calls (`kafka-topics`, `kafka-console-consumer`) on `bootstrap_1`.

- [ ] **Step 4: Build (Linux)**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && cargo build --tests -p crabka-broker 2>&1 | tail -5"
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(broker): three_node_replication_byte_compare uses multi-broker bootstrap

Same fix as acks_all_survives_leader_crash: pass all 3 brokers as
bootstrap so the JVM producer can find a survivor through any
transient broker unavailability during the test's 3-broker churn.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Phase 5 — Drop CI `--skip` flags

### Task 7: Remove the JVM acceptance `--skip` arguments

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Replace the multi-line `--skip` invocation**

In `.github/workflows/ci.yml`, find the `broker-jvm-acceptance` job's `run` step. It currently looks like:

```yaml
      # --skip filters out 2 acceptance tests that kill a broker mid-run:
      # ...
      - run: |
          cargo test -p crabka-broker --test jvm_acceptance -- \
            --ignored --nocapture --test-threads=1 \
            --skip acks_all_survives_leader_crash \
            --skip three_node_replication_byte_compare
```

Replace the entire block (the `--skip filters out…` comment + the multi-line `run:` body) with:

```yaml
      # --test-threads=1: tests bind the Rust broker to a fixed host port
      # (so the `docker run --add-host` Kafka tools have a known bootstrap
      # target). Parallel execution would hit `AddrInUse`.
      - run: cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1
```

(The `--test-threads=1` comment is preserved; only the `--skip` rationale block is dropped.)

- [ ] **Step 2: Commit and push everything**

```bash
git add .github/workflows/ci.yml
git commit -m "ci(jvm-acceptance): drop --skip; all 9 multi-broker tests run

The membership-change wire (this PR) + multi-broker bootstrap fixes in
the previous two commits unblock the two acceptance tests we were
skipping. CI now runs the full jvm_acceptance suite.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
git push origin feature/raft-membership
```

---

## Phase 6 — Closeout

### Task 8: Final acceptance gate + PR

**Files:** none

- [ ] **Step 1: Full workspace test on Windows**

```bash
cargo test --workspace 2>&1 | tail -10
```

Expected: all green.

- [ ] **Step 2: Full broker suite on Linux**

```bash
wsl -d Ubuntu -- bash -c "cd ~/git/crabka && timeout 600 cargo test -p crabka-broker -- --test-threads=1 2>&1 | grep -E 'test result|FAILED' | tail -20"
```

Expected: every `test result` line ends in `0 failed`. `isr_expand_on_catchup` is now in the pass count, not the ignored count.

- [ ] **Step 3: Workspace clippy + fmt**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | head -5
```

Both clean.

- [ ] **Step 4: Open the PR**

```bash
gh pr create --title "Manual raft membership change + close last 3 slice-10b tests" \
  --body "$(cat <<'EOF'
## Summary

- Expose `ControllerHandle::change_membership(new_voters)` and
  `ControllerHandle::add_learner(node_id, addr)`, with thin
  `BrokerHandle` wrappers. Matches Kafka KRaft's manual membership
  semantics: no auto-removal of dead brokers.
- Un-ignore `leader_election::isr_expand_on_catchup`; the test now
  explicitly removes node 3 before kill and re-adds it (as learner then
  voter) after reboot.
- Multi-broker `--bootstrap-server` for the 2 still-skipped JVM
  acceptance tests so the JVM producer can find a survivor through any
  transient broker unavailability.
- Drop `--skip` flags from broker-jvm-acceptance CI.

## Test plan

- [ ] `cargo test --workspace` green on ubuntu/macos/windows
- [ ] `cargo test -p crabka-broker --test jvm_acceptance --ignored` green
      (no `--skip` filter)
- [ ] No `#[ignore]` annotations claim slice-10b follow-up anymore

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Report the PR URL.

---

## Self-review

| Spec section | Implemented in tasks |
|--------------|----------------------|
| Goal: 3 deferred tests passing | Tasks 4, 5, 6, 7 |
| `ControllerHandle::change_membership` | Task 1 |
| `ControllerHandle::add_learner` | Task 2 |
| `BrokerHandle` wrappers | Task 3 |
| `isr_expand_on_catchup` uses new API | Task 4 |
| JVM bootstrap fix | Tasks 5, 6 |
| CI `--skip` removed | Task 7 |
| Acceptance: workspace test, clippy, fmt | Task 8 |
| Out of scope: admin RPC, auto-remove, KIP-853 | (correctly absent) |

If `change_membership` returns `NotLeader` in any test, the most likely cause is that the test called it on a non-leader broker — re-check `find_controller_leader` ran first.
