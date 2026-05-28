# KIP-392 Fetch-From-Follower / Rack-Aware Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a consumer advertising `client.rack` be redirected by the partition leader to a same-rack follower replica and read committed records from it.

**Architecture:** The Fetch wire fields (`rack_id`, `preferred_read_replica`) and broker `rack` metadata already exist. Add (1) a pure `ReplicaSelector` the leader runs to populate `preferred_read_replica`, (2) follower high-watermark propagation so a follower can bound consumer reads, (3) two broker config knobs, and (4) a long-poll wake fix. The follower stores the leader-reported HW into the existing `ReplicaState.hw`, so the read path (`do_read`) needs no changes.

**Tech Stack:** Rust, tokio, the crabka broker/protocol/metadata crates. Tests are `#[test]` / `#[tokio::test]` unit tests plus a multi-broker integration test.

**Reference spec:** `docs/superpowers/specs/2026-05-28-crabka-kip-392-fetch-from-follower-design.md`

**Execution batching (per CLAUDE.md — dispatch disjoint-file tasks in parallel):**
- **Batch 1 (parallel):** Task 1 (`replica_selector.rs` + `lib.rs`), Task 2 (`partition.rs`).
- **Batch 2 (parallel, after Batch 1):** Task 3 (`config.rs` + `file_config.rs`, needs Task 1's enum), Task 4 (`replicator.rs`, needs Task 2's method).
- **Batch 3 (parallel, after Batch 2):** Task 5 (`broker.rs`, needs Task 3's config field), Task 6 (`handlers/fetch.rs`, needs Task 1 + Task 3).
- **Batch 4:** Task 7 (integration test, needs everything).

Run `cargo fmt` before every commit — CI gates on `cargo fmt --check`.

---

### Task 1: `ReplicaSelector` module

**Files:**
- Create: `crates/broker/src/replica_selector.rs`
- Modify: `crates/broker/src/lib.rs:158` (add module declaration next to `replica_state`)

- [ ] **Step 1: Write the failing tests**

Create `crates/broker/src/replica_selector.rs` with the full module below. It contains the production code AND the tests; write it all in one shot since the tests reference the public items.

```rust
//! KIP-392 replica selection. The partition leader runs `select` on every
//! consumer Fetch that carries a `client.rack` (`rack_id`) and reports the
//! chosen node id in `FetchResponse.preferred_read_replica`. Returning `-1`
//! means "no preference — read from the leader".

/// One replica's view as the leader sees it, for selection purposes.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaView {
    /// Wire replica id (broker node id as `i32`).
    pub node_id: i32,
    /// The broker's configured rack, if any.
    pub rack: Option<String>,
    /// Whether this replica is currently in the ISR.
    pub in_isr: bool,
}

/// Which built-in selector the broker uses. Maps to Kafka's
/// `replica.selector.class`, but as a native enum (Crabka does not load
/// JVM classes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplicaSelectorKind {
    /// Always read from the leader. Default.
    #[default]
    Leader,
    /// Prefer a same-rack in-sync replica when the client advertises a rack.
    RackAware,
}

impl ReplicaSelectorKind {
    /// Parse the `replica.selector` config value. Accepts `"leader"` and
    /// `"rack-aware"`. Returns `Err(value)` on anything else.
    pub fn from_config_str(s: &str) -> Result<Self, String> {
        match s.trim() {
            "leader" => Ok(Self::Leader),
            "rack-aware" => Ok(Self::RackAware),
            other => Err(other.to_string()),
        }
    }

    /// Choose the preferred read replica. Returns a node id, or `-1` for
    /// "no preference — use the leader".
    pub(crate) fn select(
        self,
        client_rack: Option<&str>,
        leader_id: i32,
        replicas: &[ReplicaView],
    ) -> i32 {
        match self {
            Self::Leader => -1,
            Self::RackAware => {
                let Some(rack) = client_rack.filter(|r| !r.is_empty()) else {
                    return -1;
                };
                let winner = replicas
                    .iter()
                    .filter(|r| r.in_isr && r.rack.as_deref() == Some(rack))
                    .min_by_key(|r| r.node_id);
                match winner {
                    Some(r) if r.node_id != leader_id => r.node_id,
                    _ => -1,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(node_id: i32, rack: &str, in_isr: bool) -> ReplicaView {
        ReplicaView {
            node_id,
            rack: Some(rack.to_string()),
            in_isr,
        }
    }

    #[test]
    fn parse_known_values() {
        assert_eq!(
            ReplicaSelectorKind::from_config_str("leader"),
            Ok(ReplicaSelectorKind::Leader)
        );
        assert_eq!(
            ReplicaSelectorKind::from_config_str("rack-aware"),
            Ok(ReplicaSelectorKind::RackAware)
        );
        assert!(ReplicaSelectorKind::from_config_str("bogus").is_err());
    }

    #[test]
    fn leader_kind_always_returns_minus_one() {
        let replicas = [view(1, "a", true), view(2, "b", true)];
        assert_eq!(
            ReplicaSelectorKind::Leader.select(Some("b"), 1, &replicas),
            -1
        );
    }

    #[test]
    fn rack_aware_picks_same_rack_isr_member() {
        let replicas = [view(1, "a", true), view(2, "b", true), view(3, "b", true)];
        // leader is node 1 (rack a); client in rack b -> lowest-id same-rack
        // ISR member is node 2.
        assert_eq!(
            ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas),
            2
        );
    }

    #[test]
    fn rack_aware_none_when_client_rack_missing() {
        let replicas = [view(1, "a", true), view(2, "b", true)];
        assert_eq!(ReplicaSelectorKind::RackAware.select(None, 1, &replicas), -1);
        assert_eq!(
            ReplicaSelectorKind::RackAware.select(Some(""), 1, &replicas),
            -1
        );
    }

    #[test]
    fn rack_aware_none_when_no_same_rack_replica() {
        let replicas = [view(1, "a", true), view(2, "a", true)];
        assert_eq!(
            ReplicaSelectorKind::RackAware.select(Some("z"), 1, &replicas),
            -1
        );
    }

    #[test]
    fn rack_aware_ignores_non_isr_same_rack_replica() {
        let replicas = [view(1, "a", true), view(2, "b", false)];
        // Node 2 is same-rack but out of ISR -> no redirect.
        assert_eq!(
            ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas),
            -1
        );
    }

    #[test]
    fn rack_aware_none_when_only_same_rack_replica_is_leader() {
        let replicas = [view(1, "b", true), view(2, "a", true)];
        // Client rack b matches only the leader (node 1) -> stay on leader.
        assert_eq!(
            ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas),
            -1
        );
    }
}
```

- [ ] **Step 2: Add the module declaration**

In `crates/broker/src/lib.rs`, directly below the `pub(crate) mod replica_state;` line (currently line 158), add:

```rust
pub mod replica_selector;
```

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p crabka-broker replica_selector`
Expected: all `replica_selector::tests::*` pass.

- [ ] **Step 4: Format and commit**

```bash
cargo fmt
git add crates/broker/src/replica_selector.rs crates/broker/src/lib.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): KIP-392 ReplicaSelector (leader + rack-aware)"
```

---

### Task 2: `Partition::set_follower_hw`

**Files:**
- Modify: `crates/broker/src/partition.rs` (add method in the `impl Partition` block, near `high_watermark` ~line 402; add test in the `#[cfg(test)] mod tests` block ~line 715)

- [ ] **Step 1: Write the failing test**

Add this test inside `crates/broker/src/partition.rs`'s `mod tests` block (before the closing `}` at line 715). It appends a batch directly to the log so `log_end_offset` advances, then exercises clamp / advance-only / notify behavior.

```rust
    #[tokio::test]
    async fn set_follower_hw_clamps_advances_and_notifies() {
        use crabka_protocol::records::{Attributes, Record, RecordBatch};

        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let hw_advance_notify = Arc::new(Notify::new());
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state: Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            hw_advance_notify: hw_advance_notify.clone(),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };

        // Append a 3-record batch so log_end_offset() == 3.
        let mut batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default(),
            last_offset_delta: 2,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: (0..3)
                .map(|i| Record {
                    attributes: 0,
                    offset_delta: i,
                    timestamp_delta: 0,
                    key: None,
                    value: Some(bytes::Bytes::from_static(b"v")),
                    headers: vec![],
                })
                .collect(),
        };
        p.log.lock().expect("log mutex").append(&mut batch).expect("append");
        assert_eq!(p.log_end_offset(), 3);

        // reported_hw below log_end: stored verbatim, notify fires.
        // A `Notified` future does not register with the `Notify` until it is
        // first polled, and `notify_waiters()` only wakes already-registered
        // waiters — so poll once (Pending) to register BEFORE advancing HW.
        let waiter = hw_advance_notify.notified();
        tokio::pin!(waiter);
        assert!(
            futures_util::poll!(&mut waiter).is_pending(),
            "waiter registers on first poll"
        );
        p.set_follower_hw(2).await;
        assert_eq!(p.high_watermark().await, 2);
        assert!(
            futures_util::poll!(&mut waiter).is_ready(),
            "notify should fire when HW advances"
        );

        // reported_hw above log_end: clamped to log_end (3).
        p.set_follower_hw(100).await;
        assert_eq!(p.high_watermark().await, 3);

        // reported_hw below current HW: no regression.
        p.set_follower_hw(1).await;
        assert_eq!(p.high_watermark().await, 3);
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-broker set_follower_hw_clamps_advances_and_notifies`
Expected: FAIL to compile — `no method named set_follower_hw`.

- [ ] **Step 3: Implement `set_follower_hw`**

Add this method to the `impl Partition` block in `crates/broker/src/partition.rs`, immediately after the `high_watermark` method (after line 402):

```rust
    /// KIP-392: record the high watermark the leader reported in a follower
    /// Fetch response, so consumer reads served from this follower are bounded
    /// correctly. Clamps to the local log end (never expose records we have not
    /// replicated yet) and only advances `hw` (HW is monotonic). Fires
    /// `hw_advance_notify` when it advances so a consumer parked at the old HW
    /// wakes.
    pub async fn set_follower_hw(&self, reported_hw: i64) {
        let log_end = self.log_end_offset();
        let new_hw = reported_hw.min(log_end);
        let advanced = {
            let mut st = self.replica_state.lock().await;
            if new_hw > st.hw {
                st.hw = new_hw;
                true
            } else {
                false
            }
        };
        if advanced {
            self.hw_advance_notify.notify_waiters();
        }
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-broker set_follower_hw_clamps_advances_and_notifies`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt
git add crates/broker/src/partition.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): KIP-392 Partition::set_follower_hw for follower reads"
```

---

### Task 3: Broker config knobs (`broker.rack`, `replica.selector`)

**Files:**
- Modify: `crates/broker/src/config.rs` (add two fields to `BrokerConfig`; set them in `for_tests` and `Default`; add a `validate` test)
- Modify: `crates/broker/src/file_config.rs` (add `rack` + `replica_selector` to `FileConfig`; parse in `apply_to`)

- [ ] **Step 1: Write the failing test (config defaults)**

Add to the `#[cfg(test)] mod tests` block in `crates/broker/src/config.rs` (before its closing `}` at line 890):

```rust
    #[test]
    fn rack_and_selector_default_off() {
        let c = BrokerConfig::default();
        assert_eq!(c.rack, None);
        assert_eq!(
            c.replica_selector,
            crate::replica_selector::ReplicaSelectorKind::Leader
        );
        let t = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert_eq!(t.rack, None);
        assert_eq!(
            t.replica_selector,
            crate::replica_selector::ReplicaSelectorKind::Leader
        );
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-broker rack_and_selector_default_off`
Expected: FAIL to compile — `no field rack on BrokerConfig`.

- [ ] **Step 3: Add the fields to `BrokerConfig`**

In `crates/broker/src/config.rs`, add these two fields to the `BrokerConfig` struct (place them after the `cluster_id` field, ~line 120):

```rust
    /// KIP-392: this broker's rack identifier (`broker.rack`). Reported in
    /// its `BrokerRegistrationRecord` and used by the leader's rack-aware
    /// replica selector. `None` (default) means no rack.
    pub rack: Option<String>,

    /// KIP-392: which replica selector the leader runs to populate
    /// `FetchResponse.preferred_read_replica` for rack-aware consumers.
    /// Default `Leader` (never redirect).
    pub replica_selector: crate::replica_selector::ReplicaSelectorKind,
```

- [ ] **Step 4: Initialize the fields in `for_tests` and `Default`**

In `crates/broker/src/config.rs`, in `for_tests` (after `cluster_id: None,` ~line 427) add:

```rust
            rack: None,
            replica_selector: crate::replica_selector::ReplicaSelectorKind::Leader,
```

In `Default` (after `cluster_id: None,` ~line 621) add the identical two lines:

```rust
            rack: None,
            replica_selector: crate::replica_selector::ReplicaSelectorKind::Leader,
```

- [ ] **Step 5: Run the config test to verify it passes**

Run: `cargo test -p crabka-broker rack_and_selector_default_off`
Expected: PASS.

- [ ] **Step 6: Write the failing test (file config parsing)**

Add to the `#[cfg(test)] mod tests` block in `crates/broker/src/file_config.rs`:

```rust
    #[test]
    fn apply_to_parses_rack_and_replica_selector() {
        use crate::replica_selector::ReplicaSelectorKind;
        let src = r#"
broker_id = 0
rack = "az-1"
replica_selector = "rack-aware"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse");
        let mut broker = BrokerConfig::default();
        cfg.apply_to(&mut broker).expect("apply");
        assert_eq!(broker.rack.as_deref(), Some("az-1"));
        assert_eq!(broker.replica_selector, ReplicaSelectorKind::RackAware);
    }

    #[test]
    fn apply_to_rejects_unknown_replica_selector() {
        let src = r#"
broker_id = 0
replica_selector = "nonsense"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse");
        let mut broker = BrokerConfig::default();
        assert!(cfg.apply_to(&mut broker).is_err());
    }
```

- [ ] **Step 7: Run the file_config tests to verify they fail**

Run: `cargo test -p crabka-broker apply_to_parses_rack_and_replica_selector apply_to_rejects_unknown_replica_selector`
Expected: FAIL to compile — `no field rack on FileConfig`.

- [ ] **Step 8: Add the fields to `FileConfig`**

In `crates/broker/src/file_config.rs`, add to the `FileConfig` struct (after the `broker_id` field, ~line 46):

```rust
    /// KIP-392: this broker's rack id. Maps to `BrokerConfig::rack`.
    #[serde(default)]
    pub rack: Option<String>,

    /// KIP-392: replica selector name (`"leader"` | `"rack-aware"`).
    /// Maps to `BrokerConfig::replica_selector`.
    #[serde(default)]
    pub replica_selector: Option<String>,
```

- [ ] **Step 9: Parse the fields in `apply_to`**

In `crates/broker/src/file_config.rs`, inside `apply_to` (the body starting ~line 483), add (after the `broker_id` handling block, ~line 489):

```rust
        if let Some(rack) = self.rack {
            cfg.rack = Some(rack);
        }
        if let Some(sel) = self.replica_selector {
            cfg.replica_selector =
                crate::replica_selector::ReplicaSelectorKind::from_config_str(&sel).map_err(
                    |bad| FileConfigError::InvalidConfig(format!("unknown replica_selector: {bad}")),
                )?;
        }
```

(`FileConfigError::InvalidConfig(String)` is the existing ad-hoc validation-error variant — confirmed in this file's `enum FileConfigError`.)

- [ ] **Step 10: Run the file_config tests to verify they pass**

Run: `cargo test -p crabka-broker apply_to_parses_rack_and_replica_selector apply_to_rejects_unknown_replica_selector`
Expected: PASS.

- [ ] **Step 11: Format and commit**

```bash
cargo fmt
git add crates/broker/src/config.rs crates/broker/src/file_config.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): KIP-392 broker.rack + replica.selector config"
```

---

### Task 4: Replicator captures leader-reported HW

**Files:**
- Modify: `crates/broker/src/replicator.rs:312-335` (the `codes::NONE` branch of `handle_response`)

- [ ] **Step 1: Update the `codes::NONE` branch to call `set_follower_hw`**

In `crates/broker/src/replicator.rs`, replace the body of the `codes::NONE =>` arm (currently lines 313-334) with the version below. The change: after the optional `replicate_batch`, unconditionally record the leader-reported HW (`part_resp.high_watermark`) on the local partition — even when no batch was returned, so a caught-up follower still tracks the advancing HW.

```rust
        codes::NONE => {
            let Some(entry) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition)) else {
                warn!(topic = %cfg.topic, partition = cfg.partition,
                    "replicator: local partition vanished between fetches");
                return LoopAction::Continue;
            };
            if let Some(batch) = part_resp.records.as_ref().and_then(|p| p.as_v2()) {
                // Capture byte count before the move into replicate_batch
                // so the metrics update only fires on a successful append.
                let batch_bytes = batch.encoded_len();
                if let Err(e) = entry.value().replicate_batch(batch.clone()).await {
                    warn!(error = %e, topic = %cfg.topic, partition = cfg.partition,
                        "replicator: replicate_batch failed");
                } else {
                    cfg.metrics.record_replication_in(
                        &cfg.topic,
                        cfg.partition,
                        u64::try_from(batch_bytes).unwrap_or(0),
                    );
                }
            }
            // KIP-392: record the leader's high watermark so consumer reads
            // served from this follower are bounded correctly. Done on every
            // successful response, including empty ones.
            entry
                .value()
                .set_follower_hw(part_resp.high_watermark)
                .await;
            LoopAction::Continue
        }
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: builds clean. (Behavior is covered by the Task 7 integration test; no unit test here — the replicator drives a live connection.)

- [ ] **Step 3: Format and commit**

```bash
cargo fmt
git add crates/broker/src/replicator.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): KIP-392 follower records leader HW from Fetch responses"
```

---

### Task 5: Self-registration carries the rack

**Files:**
- Modify: `crates/broker/src/broker.rs:956` (the `rack: None` in the self-registration `BrokerRegistrationRecord`)

- [ ] **Step 1: Use the configured rack**

In `crates/broker/src/broker.rs`, in the `BrokerRegistrationRecord` literal at ~line 947, replace:

```rust
                    rack: None,
```

with:

```rust
                    rack: config.rack.clone(),
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: builds clean. (Rack round-trip through Metadata responses is exercised by the Task 7 integration test.)

- [ ] **Step 3: Format and commit**

```bash
cargo fmt
git add crates/broker/src/broker.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): KIP-392 register broker.rack in BrokerRegistrationRecord"
```

---

### Task 6: Leader populates `preferred_read_replica` + long-poll wake fix

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs` (set `preferred_read_replica` in the pending-resolution success branch ~line 354; add `hw_advance_notify` to the consumer long-poll wait set in `long_poll_then_reread` ~line 1032)

- [ ] **Step 1: Populate `preferred_read_replica` in the success branch**

In `crates/broker/src/handlers/fetch.rs`, find the final successful `pending.push(PendingRead { ... })` at ~line 354 (the one whose `partition: part_opt` is `Some` and `out.error_code` is unset). Immediately BEFORE that `pending.push`, insert the block below. It runs only for consumer fetches that advertise a rack, looks the partition up in the already-loaded metadata `image` (bound at line 159), builds `ReplicaView`s joining each replica to its broker rack, and asks the configured selector.

```rust
            // KIP-392: for a consumer fetch advertising client.rack, ask the
            // configured replica selector which replica it should prefer to
            // read from, and report it in `preferred_read_replica`. The field
            // only encodes at Fetch v11+ (where `rack_id` first appears), so
            // older clients are unaffected. `-1` (the default) means
            // "use the leader".
            if !is_follower_fetch && !req.rack_id.is_empty() {
                if let Some(pr) = image.partition(&topic_name, idx) {
                    let isr: std::collections::HashSet<crabka_raft::NodeId> =
                        pr.isr.iter().copied().collect();
                    let views: Vec<crate::replica_selector::ReplicaView> = pr
                        .replicas
                        .iter()
                        .map(|&nid| crate::replica_selector::ReplicaView {
                            node_id: i32::try_from(nid).unwrap_or(-1),
                            rack: image.broker(nid).and_then(|b| b.rack.clone()),
                            in_isr: isr.contains(&nid),
                        })
                        .collect();
                    let leader_id = i32::try_from(pr.leader).unwrap_or(-1);
                    out.preferred_read_replica = broker.config.replica_selector.select(
                        Some(req.rack_id.as_str()),
                        leader_id,
                        &views,
                    );
                }
            }
```

NOTE on imports: `crabka_raft::NodeId` and `std::collections::HashSet` are referenced fully-qualified above, so no new `use` is required. Confirm `image` (the `controller.current_image()` binding at line 159) is in scope at this point — it is, since the pending loop is below line 159.

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: builds clean.

- [ ] **Step 3: Add `hw_advance_notify` to the consumer long-poll wait set**

In `crates/broker/src/handlers/fetch.rs`, in `long_poll_then_reread` (~line 1032), replace the `notifies` construction:

```rust
    let notifies: Vec<Arc<Notify>> = pending
        .iter()
        .filter_map(|p| p.partition.as_ref().map(|part| part.append_notify.clone()))
        .collect();
```

with a version that also waits on each readable partition's `hw_advance_notify` for **consumer** fetches (so a consumer parked at a follower's HW wakes when that HW advances; follower fetches keep append-only waits):

```rust
    let mut notifies: Vec<Arc<Notify>> = Vec::new();
    for p in pending.iter() {
        if let Some(part) = p.partition.as_ref() {
            notifies.push(part.append_notify.clone());
            // KIP-392: a consumer reading from a follower becomes unblocked
            // when the follower's HW advances (via set_follower_hw), not only
            // on raw append. Follower (inter-broker) fetches don't need this.
            if !p.is_follower_fetch {
                notifies.push(part.hw_advance_notify.clone());
            }
        }
    }
```

- [ ] **Step 4: Verify it compiles and existing fetch tests still pass**

Run: `cargo test -p crabka-broker --lib handlers::fetch`
Expected: PASS (no regressions in existing fetch handler unit tests).

- [ ] **Step 5: Format and commit**

```bash
cargo fmt
git add crates/broker/src/handlers/fetch.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): KIP-392 leader populates preferred_read_replica + follower long-poll wake"
```

---

### Task 7: End-to-end integration test

**Files:**
- Create: `crates/broker/tests/kip_392_fetch_from_follower.rs`

This test is the success criterion: a consumer in the follower's rack is redirected to the follower and reads the records from it.

- [ ] **Step 1: Learn the multi-broker test harness conventions**

The shared harness is `crates/broker/tests/support/mod.rs`. Read it plus `crates/broker/tests/replication.rs` (a 2+ broker produce/replicate test) end-to-end before writing. Key helpers already there:
- `support::start_n_node_with_retry(n)` → `Vec<(BrokerHandle, BrokerConfig, TempDir)>` — starts an n-broker cluster.
- `support::broker_config(i, ...)` — builds broker `i`'s `BrokerConfig` (for_tests-style timings).
- `support::wait_for_all_brokers_registered(&cluster)`.

Identify from `replication.rs`:
- how a topic is created with replication factor 2 and how the test waits for the follower to enter the ISR / replicate (look for ISR / high-watermark polling helpers),
- how it issues raw protocol requests (Produce / Fetch) against a specific broker's listener.

**Per-broker rack/selector:** `start_n_node_with_retry` builds configs internally, so to set distinct `rack` + `replica_selector = RackAware` per broker you will likely need to either (a) add an optional config-customizer parameter / a sibling `start_n_node_with` helper to `support/mod.rs`, or (b) start the brokers with a bespoke loop in this test that calls `support::broker_config(i, ...)`, sets `cfg.rack` / `cfg.replica_selector`, and starts each `Broker`. Pick whichever matches the harness's existing extension style; if you extend `support/mod.rs`, keep the existing `start_n_node` signature working for other tests.

- [ ] **Step 2: Write the integration test**

Create `crates/broker/tests/kip_392_fetch_from_follower.rs`. Adapt the cluster/produce/fetch helpers to the conventions found in Step 1; the assertions below are the contract this test must verify. Structure:

```rust
//! KIP-392 end-to-end: a consumer advertising `client.rack` matching a
//! follower's rack is redirected to that follower and reads committed records
//! from it.

// <imports + the multi-broker support module used by sibling tests>

#[tokio::test]
async fn consumer_in_follower_rack_is_redirected_and_reads_from_follower() {
    // 1. Start 2 brokers in DIFFERENT racks, both with replica_selector = RackAware:
    //      broker 1: rack = "rack-a"  (will be leader)
    //      broker 2: rack = "rack-b"  (will be follower)
    //    Set on each broker's BrokerConfig:
    //      cfg.rack = Some("rack-a"/"rack-b".into());
    //      cfg.replica_selector = ReplicaSelectorKind::RackAware;
    //
    // 2. Create topic "t" with 1 partition, replication factor 2.
    //    Wait until partition (t,0) has leader = broker 1 and isr = {1,2}.
    //
    // 3. Produce N (e.g. 5) records to the leader (broker 1) with acks=all,
    //    so they are committed (HW advances on the leader).
    //
    // 4. Wait until broker 2 (the follower) has replicated to the HW. Poll
    //    its local partition's high_watermark (or just retry the follower
    //    fetch in step 6 until it returns the records).
    //
    // 5. Send a consumer Fetch to the LEADER (broker 1) at offset 0 with
    //    rack_id = "rack-b" and replica_id = -1. Assert the response's
    //    partition `preferred_read_replica == 2` (the follower in rack-b).
    //
    // 6. Send a consumer Fetch to the FOLLOWER (broker 2) at offset 0 with
    //    rack_id = "rack-b", replica_id = -1. Assert it returns the produced
    //    records (record count == N), bounded by the follower's HW
    //    (error_code == NONE, records present).
    //
    // 7. Sanity: a consumer Fetch to the leader with rack_id = "rack-a"
    //    (same rack as leader) yields preferred_read_replica == -1.
}
```

When sending the raw Fetch in steps 5-7, negotiate a Fetch version >= 11 so `rack_id` is sent and `preferred_read_replica` is decoded. Build the `FetchRequest` like the replicator does (see `crates/broker/src/replicator.rs:260`) but with `replica_id = -1`, `replica_state` left default, and `rack_id` set.

- [ ] **Step 3: Run the integration test**

Run: `cargo test -p crabka-broker --test kip_392_fetch_from_follower`
Expected: PASS. If step 6 flakes on timing, add a bounded retry loop around the follower fetch waiting for the records to appear (the follower may not have advanced its HW the instant the leader committed).

- [ ] **Step 4: Run the full broker test suite to check for regressions**

Run: `cargo test -p crabka-broker`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt
git add crates/broker/tests/kip_392_fetch_from_follower.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): KIP-392 end-to-end fetch-from-follower integration test"
```

---

## Final verification

- [ ] **Workspace build + clippy + fmt check + full tests**

```bash
cargo fmt --check
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker
```
Expected: all clean / green. Fix any clippy findings before considering the work done.
