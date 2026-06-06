# Crabka gRPC Gateway P3b — Gateway→Gateway Forwarding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete active-active dedup by routing a keyed `Send` whose dedup-partition this replica does *not* own to the replica that *does* own it (instead of returning `Unavailable`), via a membership-topic routing table and an internal gateway→gateway HTTP forward.

**Architecture:** Each replica publishes `{advertised_addr, owned, epoch}` (keyed by a per-process `node_id`) to a compacted **single-partition** membership topic `__crabka_grpc_gateway_membership` on every dedup-assignment change. Every replica tails the whole topic (a unique consumer group per process ⇒ all partitions assigned ⇒ a broadcast read) into a `dedup_partition → owner_addr` routing table, breaking stale-owner ties by record offset. The produce core, for a non-owned keyed record, forwards it as JSON over HTTP to the owner's `/internal/v1/forward` endpoint (a dedicated internal protocol on the gateway's own listener, distinct from the public Connect `Send` API). The owner produces it **locally** (it owns the partition) and returns the result. Single replica owning all partitions ⇒ no forwarding ⇒ identical to P3a.

**Tech Stack:** Rust (edition 2024, `unsafe_code` forbid), `reqwest` (JSON client), `axum` (internal route), the native `crabka-client-{producer,consumer,admin}` crates, `serde_json` wire. Broker is NEVER modified.

**Out of scope (later phases):** caller-identity forwarding (P5), TLS on the forward channel (P4 — forward over plain `http://` for now), telemetry `gateway_forward_total` (P8). Batch-by-owner coalescing (optimization, not correctness).

---

## Execution constraints (every task)

- **Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25`. Subagent shells reset cwd to the MAIN repo — prefix every Bash with `cd /Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25 && ...`, use `git -C <worktree>`.
- **Branch:** `claude/gateway-p3b`, **stacked on `claude/gateway-p3`** (P3a, PR #401 — unmerged; P3b depends on its `run_ownership`/`owns`/`Unavailable`). The P3b PR bases on #401, or rebases onto `main` if #401 merges first. Assert `git -C <worktree> rev-parse --abbrev-ref HEAD` == `claude/gateway-p3b` before every commit (else STOP → BLOCKED).
- **Git identity:** commit with `git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit ...` (never `git config`). Stage `Cargo.lock` if it changes.
- **Each task ends GREEN:** `cargo test -p crabka-grpc-gateway`, `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`, `cargo fmt --check -p crabka-grpc-gateway`.
- Tasks are **sequential** (each layer uses the previous layer's types; shared files). Dispatch one implementer at a time.

## File map

- Create: `crates/grpc-gateway/src/dedup/membership.rs` — `NodeInfo`, `MembershipStore` (routing + offset tiebreak), `run_membership` consumer, `MembershipPublisher`.
- Create: `crates/grpc-gateway/src/forward.rs` — `Forwarder` (reqwest client), wire types, `forward_router` (`/internal/v1/forward`).
- Create: `crates/grpc-gateway/tests/membership.rs` — `run_membership` routing/tiebreak (T2).
- Create: `crates/grpc-gateway/tests/forwarding.rs` — multi-replica forwarding (T6).
- Modify: `Cargo.toml` (reqwest), `src/error.rs` (`Forward`), `src/dedup/topic.rs` (`ensure_membership_topic`), `src/dedup/mod.rs` (`pub mod membership;` + `owns`/`partition_for_key`), `src/dedup/store.rs` (membership publish on assignment change), `src/produce.rs` (`produce`/`produce_local`/`with_forwarding`), `src/lib.rs` (`pub mod forward;`), `src/config.rs` (fields), `src/bin/gateway.rs` (wiring), `src/state.rs` (none — `AppState` unchanged), `tests/wire.rs` (`GatewayConfig` literal gains fields).

---

## Task 1: Deps + `Forward` error + membership-topic ensure

**Files:** Modify `crates/grpc-gateway/Cargo.toml`, `crates/grpc-gateway/src/error.rs`, `crates/grpc-gateway/src/dedup/topic.rs`.

- [ ] **Step 1: Add `reqwest` to the gateway crate.** In `crates/grpc-gateway/Cargo.toml` under `[dependencies]` (after `tokio-util`), matching the repo convention (see `crates/schema-registry/Cargo.toml`):

```toml
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls"] }
```

- [ ] **Step 2: Add the `Forward` error variant.** In `src/error.rs`, add to `GatewayError` (after `Unavailable`):

```rust
    #[error("forward to owner failed: {0}")]
    Forward(String),
```

- [ ] **Step 3: Add `ensure_membership_topic`.** Append to `src/dedup/topic.rs`. Membership is **compact-only** (latest record per node persists; tombstone removes), **single partition** (total publish order ⇒ exact offset tiebreak in the router):

```rust
/// Idempotently create the compacted, single-partition membership topic.
/// `cleanup.policy=compact` (no delete) keeps one live record per node until a
/// tombstone supersedes it. Single partition ⇒ all publishes are totally
/// ordered, so the routing table's offset tiebreak is exact.
pub async fn ensure_membership_topic(
    bootstrap: &str,
    name: &str,
    replication: i16,
) -> Result<(), GatewayError> {
    let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
    let mut admin = AdminClient::connect(&addrs)
        .await
        .map_err(|e| GatewayError::Other(format!("admin connect: {e}")))?;

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact".to_string());
    configs.insert("min.cleanable.dirty.ratio".to_string(), "0.01".to_string());
    configs.insert("segment.ms".to_string(), "60000".to_string());

    create_with_rf(&mut admin, name, 1, replication, &configs).await
}
```

(`create_with_rf`, `AdminClient`, `BTreeMap`, the `INVALID_REPLICATION_FACTOR`/`TOPIC_ALREADY_EXISTS` handling are already in this file — reuse them.)

- [ ] **Step 4: Gates + commit.** Run `cargo build -p crabka-grpc-gateway` (pulls reqwest), then the three gates (all green — additions are unused-but-`pub`, no warnings). Commit:

```bash
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(gateway): reqwest dep, Forward error, ensure_membership_topic"
```

---

## Task 2: Membership store, consumer, and publisher

**Files:** Create `crates/grpc-gateway/src/dedup/membership.rs`; modify `crates/grpc-gateway/src/dedup/mod.rs` (`pub mod membership;`); create `crates/grpc-gateway/tests/membership.rs`.

- [ ] **Step 1: Create `src/dedup/membership.rs`** with the full contents:

```rust
//! Gateway membership + owner-routing for active-active forwarding.
//!
//! Each replica publishes `{advertised_addr, owned, epoch}` (keyed by a
//! per-process `node_id`) to the compacted, single-partition membership topic
//! on every dedup-assignment change. Every replica tails the whole topic — a
//! unique consumer group per process ⇒ it is the sole member ⇒ assigned all
//! partitions ⇒ a broadcast read — into a `dedup_partition → owner_addr`
//! routing table. A crashed node's stale ownership record cannot shadow the
//! live owner: the table breaks ties by record offset, and the topic's single
//! partition makes those offsets a total order, so the most-recent claim wins.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use serde::{Deserialize, Serialize};

use crate::error::GatewayError;

/// One replica's published membership (value; key = `node_id`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub advertised_addr: String,
    pub owned: Vec<u32>,
    pub epoch: u64,
}

struct NodeEntry {
    info: NodeInfo,
    /// Membership-topic offset of this node's latest record (recency tiebreak).
    offset: i64,
}

/// Materialized membership + the derived `partition → owner_addr` routing table.
pub struct MembershipStore {
    nodes: RwLock<HashMap<String, NodeEntry>>,
    routing: RwLock<HashMap<u32, String>>,
}

impl MembershipStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            routing: RwLock::new(HashMap::new()),
        }
    }

    /// Owner advertised-addr for dedup-partition `p`, if any replica claims it.
    #[must_use]
    pub fn owner_of(&self, p: u32) -> Option<String> {
        self.routing.read().expect("routing lock").get(&p).cloned()
    }

    fn apply(&self, node_id: String, info: Option<NodeInfo>, offset: i64) {
        {
            let mut nodes = self.nodes.write().expect("nodes lock");
            match info {
                Some(info) => {
                    nodes.insert(node_id, NodeEntry { info, offset });
                }
                None => {
                    nodes.remove(&node_id);
                }
            }
        }
        self.rebuild();
    }

    /// Rebuild `partition → owner_addr`: for each partition, the claimant whose
    /// record has the highest offset (most recent publish) wins.
    fn rebuild(&self) {
        let nodes = self.nodes.read().expect("nodes lock");
        let mut best: HashMap<u32, (i64, String)> = HashMap::new();
        for entry in nodes.values() {
            for &p in &entry.info.owned {
                let slot = best.entry(p).or_insert((i64::MIN, String::new()));
                if entry.offset >= slot.0 {
                    *slot = (entry.offset, entry.info.advertised_addr.clone());
                }
            }
        }
        *self.routing.write().expect("routing lock") =
            best.into_iter().map(|(p, (_, addr))| (p, addr)).collect();
    }

    /// Tail the membership topic into the routing table until `shutdown`.
    /// `group` MUST be unique per process (node-scoped) so this replica is the
    /// sole member and is assigned every partition (a broadcast read). Closes
    /// the consumer on exit so the coordinator + group member don't leak.
    pub async fn run_membership(
        self: Arc<Self>,
        bootstrap: String,
        client_id: String,
        membership_topic: String,
        group: String,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), GatewayError> {
        let mut consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id(client_id)
            .group_id(group)
            .subscribe(vec![membership_topic])
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .assignor(crabka_client_consumer::Assignor::CooperativeSticky)
            .build()
            .await?;

        let mut poll_err: Option<GatewayError> = None;
        loop {
            let batch = tokio::select! {
                () = shutdown.cancelled() => break,
                b = consumer.poll(Duration::from_millis(500)) => match b {
                    Ok(batch) => batch,
                    Err(e) => { poll_err = Some(e.into()); break; }
                },
            };
            for r in batch {
                let Some(key_bytes) = r.key else { continue };
                let node_id = String::from_utf8_lossy(&key_bytes).into_owned();
                match r.value {
                    None => self.apply(node_id, None, r.offset),
                    // Skip malformed records; never kill the loop.
                    Some(v) => {
                        if let Ok(info) = serde_json::from_slice::<NodeInfo>(&v) {
                            self.apply(node_id, Some(info), r.offset);
                        }
                    }
                }
            }
        }

        let _ = consumer.close().await;
        match poll_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl Default for MembershipStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Publishes this replica's membership on each dedup-assignment change.
pub struct MembershipPublisher {
    producer: Producer,
    node_id: String,
    advertised_addr: String,
    membership_topic: String,
    epoch: AtomicU64,
}

impl MembershipPublisher {
    /// Build the publisher's idempotent producer.
    pub async fn new(
        bootstrap: &str,
        client_id: &str,
        node_id: String,
        advertised_addr: String,
        membership_topic: String,
    ) -> Result<Self, GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .enable_idempotence(true)
            .acks(Acks::All)
            .build()
            .await?;
        Ok(Self {
            producer,
            node_id,
            advertised_addr,
            membership_topic,
            epoch: AtomicU64::new(0),
        })
    }

    /// Publish the current owned set (bumps `epoch`). Keyed by `node_id` so the
    /// compacted topic keeps exactly one live record per replica.
    pub async fn publish(&self, owned: &HashSet<u32>) -> Result<(), GatewayError> {
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst);
        let mut owned: Vec<u32> = owned.iter().copied().collect();
        owned.sort_unstable();
        let info = NodeInfo {
            advertised_addr: self.advertised_addr.clone(),
            owned,
            epoch,
        };
        let rec = ProducerRecord {
            topic: self.membership_topic.clone(),
            partition: None,
            key: Some(Bytes::from(self.node_id.clone().into_bytes())),
            value: Some(Bytes::from(serde_json::to_vec(&info)?)),
            headers: vec![],
            timestamp_ms: None,
        };
        self.producer
            .send(rec)
            .await
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?
            .map_err(GatewayError::Producer)?;
        Ok(())
    }
}
```

- [ ] **Step 2: Wire the module.** In `src/dedup/mod.rs`, add after `pub mod store;`:

```rust
pub mod membership;
```

- [ ] **Step 3: Create `tests/membership.rs`** — prove `run_membership` materializes routing and the offset tiebreak picks the most recent claimant:

```rust
//! `run_membership` builds the `partition → owner_addr` routing table from the
//! membership topic, and a later claim of the same partition (higher offset)
//! supersedes an earlier one.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_grpc_gateway::config::GatewayConfig;
use crabka_grpc_gateway::dedup::membership::{MembershipStore, NodeInfo};
use crabka_grpc_gateway::dedup::topic::ensure_membership_topic;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const TOPIC: &str = "__crabka_grpc_gateway_membership";

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn publish(producer: &Producer, node_id: &str, info: &NodeInfo) {
    let rec = ProducerRecord {
        topic: TOPIC.to_string(),
        partition: None,
        key: Some(Bytes::from(node_id.as_bytes().to_vec())),
        value: Some(Bytes::from(serde_json::to_vec(info).unwrap())),
        headers: vec![],
        timestamp_ms: None,
    };
    producer.send(rec).await.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_membership_builds_routing_with_offset_tiebreak() {
    let (broker, bootstrap, _dir) = boot().await;
    ensure_membership_topic(&bootstrap, TOPIC, 1).await.unwrap();

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .client_id("memb-test".into())
        .enable_idempotence(true)
        .acks(Acks::All)
        .build()
        .await
        .unwrap();

    // node-a owns {0,1}; node-b owns {2,3}.
    publish(&producer, "node-a", &NodeInfo { advertised_addr: "addr-a".into(), owned: vec![0, 1], epoch: 0 }).await;
    publish(&producer, "node-b", &NodeInfo { advertised_addr: "addr-b".into(), owned: vec![2, 3], epoch: 0 }).await;
    // Later, node-b also claims partition 1 (ownership moved off node-a): higher
    // offset ⇒ wins for partition 1.
    publish(&producer, "node-b", &NodeInfo { advertised_addr: "addr-b".into(), owned: vec![1, 2, 3], epoch: 1 }).await;

    let store = Arc::new(MembershipStore::new());
    let token = CancellationToken::new();
    let h = tokio::spawn(store.clone().run_membership(
        bootstrap.clone(),
        "memb-reader".into(),
        TOPIC.into(),
        "memb-reader-unique-1".into(),
        token.clone(),
    ));

    let mut ok = false;
    for _ in 0..80 {
        if store.owner_of(0).as_deref() == Some("addr-a")
            && store.owner_of(1).as_deref() == Some("addr-b") // tiebreak: latest claim
            && store.owner_of(2).as_deref() == Some("addr-b")
            && store.owner_of(3).as_deref() == Some("addr-b")
        {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(ok, "routing table did not converge with the offset tiebreak");

    // Sanity: an unclaimed partition has no owner.
    assert_eq!(store.owner_of(7), None);
    let _ = GatewayConfig::DEDUP_TOPIC_REPLICATION; // touch the type (lint hygiene)

    token.cancel();
    let _ = h.await;
    broker.shutdown().await;
}
```

- [ ] **Step 4: Gates + commit.** Run the membership test (`cargo test -p crabka-grpc-gateway --test membership`) + full gates. Commit `feat(gateway): membership store, run_membership consumer, publisher`.

---

## Task 3: Publish membership on assignment change + engine ownership accessors

**Files:** Modify `crates/grpc-gateway/src/dedup/store.rs`, `crates/grpc-gateway/src/dedup/mod.rs`.

- [ ] **Step 1: Add a membership-publisher slot to `DedupStore`.** In `src/dedup/store.rs`: add the import `use std::sync::OnceLock;` (with the other `std::sync` imports) and a field on `DedupStore`:

```rust
    /// Optional membership publisher; set by the binary before `run_ownership`
    /// starts. `None` in single-owner/unit contexts ⇒ no publishing (P3a behavior).
    membership: OnceLock<Arc<crate::dedup::membership::MembershipPublisher>>,
```

Initialize it in `new` (`membership: OnceLock::new(),`), and add a setter:

```rust
    /// Install the membership publisher. Call before spawning `run_ownership`
    /// so the first assignment is published.
    pub fn set_membership(&self, publisher: Arc<crate::dedup::membership::MembershipPublisher>) {
        let _ = self.membership.set(publisher);
    }
```

- [ ] **Step 2: Publish on each assignment change.** In `run_ownership`, the assignment-change branch currently ends:

```rust
                current.clone_from(&assigned);
                *self.owned.write().expect("owned lock") = assigned;
                self.warm.store(false, Ordering::SeqCst);
                empty_polls = 0;
```

Append (publishing `&current`, which now equals the new assignment — `assigned` was moved into `owned`):

```rust
                if let Some(publisher) = self.membership.get() {
                    if let Err(e) = publisher.publish(&current).await {
                        tracing::warn!(error = %e, "membership publish failed");
                    }
                }
```

- [ ] **Step 3: Add `DedupEngine` ownership accessors.** In `src/dedup/mod.rs`, inside `impl DedupEngine`, add:

```rust
    /// The dedup partition a key hashes to (for routing decisions).
    #[must_use]
    pub fn partition_for_key(&self, key: &str) -> u32 {
        partition_for(key, self.partitions)
    }

    /// True if this replica currently owns dedup-partition `p`.
    #[must_use]
    pub fn owns(&self, p: u32) -> bool {
        self.store.owns(p)
    }
```

- [ ] **Step 4: Gates + commit.** All existing tests still pass (P3a `run_ownership` callers don't set membership ⇒ `OnceLock` empty ⇒ no publish ⇒ unchanged). Commit `feat(gateway): publish membership on assignment change + engine owns/partition_for_key`.

---

## Task 4: Forwarder client, internal endpoint, and produce routing

**Files:** Create `crates/grpc-gateway/src/forward.rs`; modify `crates/grpc-gateway/src/lib.rs` (`pub mod forward;`), `crates/grpc-gateway/src/produce.rs`.

- [ ] **Step 1: Create `src/forward.rs`:**

```rust
//! Internal gateway→gateway forwarding: the owner-routing client plus the
//! `/internal/v1/forward` endpoint that receives a forwarded record and
//! produces it LOCALLY (the receiver is the partition's owner).
//!
//! Transport is plain JSON over HTTP on the gateway's own listener (TLS is a
//! later phase). This INTERNAL protocol is deliberately separate from the
//! public Connect `Send` API so the two evolve independently.

use std::sync::Arc;

use axum::routing::post;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};

use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{GatewayRecord, RecordOutcome};

/// Wire form of a forwarded record (bytes as JSON arrays — no extra deps).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
    pub idempotency_key: Option<String>,
}

impl ForwardRecord {
    fn from_record(r: &GatewayRecord) -> Self {
        Self {
            topic: r.topic.clone(),
            key: r.key.as_ref().map(|b| b.to_vec()),
            value: r.value.to_vec(),
            headers: r.headers.iter().map(|(k, v)| (k.clone(), v.to_vec())).collect(),
            partition: r.partition,
            timestamp_ms: r.timestamp_ms,
            idempotency_key: r.idempotency_key.clone(),
        }
    }

    fn into_record(self) -> GatewayRecord {
        GatewayRecord {
            topic: self.topic,
            key: self.key.map(bytes::Bytes::from),
            value: bytes::Bytes::from(self.value),
            headers: self.headers.into_iter().map(|(k, v)| (k, bytes::Bytes::from(v))).collect(),
            partition: self.partition,
            timestamp_ms: self.timestamp_ms,
            idempotency_key: self.idempotency_key,
        }
    }
}

/// Wire form of a forward result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardResult {
    pub partition: i32,
    pub offset: i64,
    pub deduplicated: bool,
    /// Present when the owner could not produce; `retriable` ⇒ the origin maps
    /// it back to `Unavailable` and retries / re-resolves.
    pub error: Option<ForwardError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardError {
    pub message: String,
    pub retriable: bool,
}

/// reqwest client that forwards a record to the owning replica.
pub struct Forwarder {
    http: reqwest::Client,
}

impl Forwarder {
    #[must_use]
    pub fn new() -> Self {
        Self { http: reqwest::Client::new() }
    }

    /// POST the record to `owner_addr`'s internal forward endpoint. Transport
    /// failures and owner-`retriable` errors become `Unavailable` so the origin
    /// retries / re-resolves to the (possibly new) owner.
    pub async fn forward(
        &self,
        owner_addr: &str,
        rec: &GatewayRecord,
    ) -> Result<RecordOutcome, GatewayError> {
        let url = format!("http://{owner_addr}/internal/v1/forward");
        let body = ForwardRecord::from_record(rec);
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|_| GatewayError::Unavailable)?;
        if !resp.status().is_success() {
            return Err(GatewayError::Unavailable);
        }
        let result: ForwardResult = resp
            .json()
            .await
            .map_err(|e| GatewayError::Forward(format!("decode forward result: {e}")))?;
        match result.error {
            None => Ok(RecordOutcome {
                partition: result.partition,
                offset: result.offset,
                deduplicated: result.deduplicated,
            }),
            Some(e) if e.retriable => Err(GatewayError::Unavailable),
            Some(e) => Err(GatewayError::Forward(e.message)),
        }
    }
}

impl Default for Forwarder {
    fn default() -> Self {
        Self::new()
    }
}

/// The `/internal/v1/forward` route. Mount alongside the Connect + health
/// routers on the gateway listener.
#[must_use]
pub fn forward_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/internal/v1/forward", post(forward_handler))
        .layer(Extension(state))
}

/// Receiver side: produce LOCALLY (no further forwarding — this replica owns
/// the partition; `produce_local` returns `Unavailable` if it just lost it,
/// which the origin retries). Never re-forwards, so there are no forward loops.
async fn forward_handler(
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<ForwardRecord>,
) -> Json<ForwardResult> {
    let rec = req.into_record();
    match state.produce.produce_local(rec).await {
        Ok(o) => Json(ForwardResult {
            partition: o.partition,
            offset: o.offset,
            deduplicated: o.deduplicated,
            error: None,
        }),
        Err(e) => {
            let retriable = matches!(e, GatewayError::Unavailable);
            Json(ForwardResult {
                partition: -1,
                offset: -1,
                deduplicated: false,
                error: Some(ForwardError { message: e.to_string(), retriable }),
            })
        }
    }
}
```

- [ ] **Step 2: Wire the module.** In `src/lib.rs`, add `pub mod forward;` to the module list (after `pub mod error;` keeps alpha-ish order; any position compiles).

- [ ] **Step 3: Split produce into forwarding `produce` + local `produce_local`.** In `src/produce.rs`:

Add imports near the top:

```rust
use crate::dedup::membership::MembershipStore;
use crate::forward::Forwarder;
```

Add the forwarding context + field. Change the struct to:

```rust
pub struct ProduceCore {
    producer: Arc<Producer>,
    codec: Arc<dyn RecordCodec>,
    dedup: Option<Arc<crate::dedup::DedupEngine>>,
    forwarding: Option<Forwarding>,
}

struct Forwarding {
    membership: Arc<MembershipStore>,
    forwarder: Arc<Forwarder>,
    self_addr: String,
}
```

In `new`, add `forwarding: None,` to the constructed `Self`. Add the builder (after `with_dedup`):

```rust
    /// Enable active-active forwarding: non-owned keyed records route to the
    /// owner named by the membership routing table.
    #[must_use]
    pub fn with_forwarding(
        mut self,
        membership: Arc<MembershipStore>,
        forwarder: Arc<Forwarder>,
        self_addr: String,
    ) -> Self {
        self.forwarding = Some(Forwarding { membership, forwarder, self_addr });
        self
    }
```

Replace the existing `produce` method with the forwarding entry point **plus** a renamed local path (the old body becomes `produce_local`):

```rust
    /// Public produce entry point. A keyed record whose dedup-partition this
    /// replica does not own is forwarded to the owner (per the membership
    /// routing table); everything else is produced locally.
    pub async fn produce(&self, rec: GatewayRecord) -> Result<RecordOutcome, GatewayError> {
        // Resolve the route without holding a borrow of `rec` across its move.
        let forward_addr: Option<String> =
            match (&self.dedup, &self.forwarding, &rec.idempotency_key) {
                (Some(dedup), Some(fwd), Some(key)) => {
                    let p = dedup.partition_for_key(key);
                    if dedup.owns(p) {
                        None
                    } else {
                        match fwd.membership.owner_of(p) {
                            Some(addr) if addr == fwd.self_addr => None,
                            Some(addr) => Some(addr),
                            None => return Err(GatewayError::Unavailable),
                        }
                    }
                }
                _ => None,
            };

        match forward_addr {
            Some(addr) => {
                let fwd = self.forwarding.as_ref().expect("route implies forwarding");
                fwd.forwarder.forward(&addr, &rec).await
            }
            None => self.produce_local(rec).await,
        }
    }

    /// Local produce (NO forwarding): keyed → dedup engine (owner/warm gate),
    /// unkeyed → plain idempotent producer. Used by the public path when this
    /// replica owns the key, and by the internal forward endpoint.
    pub async fn produce_local(&self, rec: GatewayRecord) -> Result<RecordOutcome, GatewayError> {
        let value = self.codec.encode_value(&rec.topic, rec.value.clone());
        match (&self.dedup, &rec.idempotency_key) {
            (Some(dedup), Some(_key)) => dedup.dedup_produce(&rec, value).await,
            _ => self.produce_plain(&rec, value).await,
        }
    }
```

(`produce_plain` and `to_producer_record` are unchanged. `handlers::send` and `streaming::send_stream`/`subscribe` keep calling `state.produce.produce(...)` — now forwarding-aware — with no change.)

- [ ] **Step 4: Gates + commit.** Existing tests still pass (no `with_forwarding` ⇒ `produce` falls straight to `produce_local`, identical to before). Commit `feat(gateway): Forwarder client + /internal/v1/forward + produce forwarding routing`.

---

## Task 5: Config fields + binary wiring

**Files:** Modify `crates/grpc-gateway/src/config.rs`, `crates/grpc-gateway/src/bin/gateway.rs`, `crates/grpc-gateway/tests/wire.rs`.

- [ ] **Step 1: Config fields + consts.** In `src/config.rs`, add to `GatewayConfig` (after `dedup_txn_id_prefix`):

```rust
    /// Address other replicas reach THIS gateway at (host:port of `listen_addr`,
    /// externally routable). Published to membership; used to forward.
    pub advertised_addr: String,
    /// Internal compacted topic carrying replica membership / owner routing.
    pub membership_topic: String,
```

And add the membership replication const to the `impl GatewayConfig` block:

```rust
    /// Replication factor requested for the membership topic at create time.
    pub const MEMBERSHIP_TOPIC_REPLICATION: i16 = 3;
```

- [ ] **Step 2: CLI args.** In `src/bin/gateway.rs`, add to `struct Args` (after `dedup_txn_id_prefix`):

```rust
    /// Address peers reach this gateway at (e.g. `gw-0.gw:9500`). Required for
    /// active-active forwarding; must be routable from other replicas.
    #[arg(long, env = "CRABKA_GATEWAY_ADVERTISED_ADDR")]
    advertised_addr: String,

    /// Internal membership / owner-routing topic.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_MEMBERSHIP_TOPIC",
        default_value = "__crabka_grpc_gateway_membership"
    )]
    membership_topic: String,
```

- [ ] **Step 3: Build the config** — add the two new fields where `GatewayConfig { ... }` is constructed in `main`:

```rust
        advertised_addr: args.advertised_addr.clone(),
        membership_topic: args.membership_topic.clone(),
```

- [ ] **Step 4: Wire membership + forwarding into `main`.** Add the imports at the top of `bin/gateway.rs`:

```rust
use crabka_grpc_gateway::dedup::membership::{MembershipPublisher, MembershipStore};
use crabka_grpc_gateway::dedup::topic::ensure_membership_topic;
use crabka_grpc_gateway::forward::{self, Forwarder};
```

After the existing `ensure_dedup_topic(...).await?;` call, add membership-topic creation:

```rust
    ensure_membership_topic(
        &config.bootstrap,
        &config.membership_topic,
        GatewayConfig::MEMBERSHIP_TOPIC_REPLICATION,
    )
    .await?;

    let node_id = uuid::Uuid::new_v4().to_string();
```

The current code builds `store`, `readiness`, `shutdown`, then spawns `run_ownership`. **Before** the `run_ownership` spawn, insert membership setup so the first assignment publishes:

```rust
    // Membership: tail the routing table, and install the publisher BEFORE the
    // ownership consumer starts so its first assignment is published.
    let membership = Arc::new(MembershipStore::new());
    {
        let membership = membership.clone();
        let bootstrap = config.bootstrap.clone();
        let client_id = format!("{}-membership", config.client_id);
        let topic = config.membership_topic.clone();
        let group = format!("__crabka_grpc_gateway_membership_reader-{node_id}");
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = membership
                .run_membership(bootstrap, client_id, topic, group, shutdown)
                .await
            {
                tracing::error!(error = %e, "membership reader exited with error");
            }
        });
    }
    let publisher = Arc::new(
        MembershipPublisher::new(
            &config.bootstrap,
            &format!("{}-membership-pub", config.client_id),
            node_id.clone(),
            config.advertised_addr.clone(),
            config.membership_topic.clone(),
        )
        .await?,
    );
    store.set_membership(publisher);
```

(The existing `run_ownership` spawn stays exactly as-is — it now publishes because `set_membership` ran first.)

Where `produce` is built (`ProduceCore::new(...).with_dedup(engine)`), append forwarding:

```rust
    let forwarder = Arc::new(Forwarder::new());
    let produce = ProduceCore::new(&config.bootstrap, &config.client_id, Arc::new(RawCodec))
        .await?
        .with_dedup(engine)
        .with_forwarding(membership.clone(), forwarder, config.advertised_addr.clone());
```

And merge the forward router into the served app:

```rust
    let app = crabka_grpc_gateway::router(state.clone())
        .merge(health::router(readiness))
        .merge(forward::forward_router(state.clone()));
```

(`state` is `Arc<AppState>` — already constructed above this line. Ensure `state` is built before the `app` line; it already is.)

- [ ] **Step 5: Fix the `GatewayConfig` literal in `tests/wire.rs`.** That test constructs a full `GatewayConfig { ... }`. Add the two new fields:

```rust
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_grpc_gateway_membership".into(),
```

- [ ] **Step 6: Gates + commit.** `cargo build -p crabka-grpc-gateway` (bin compiles with the new required arg), full gates. Commit `feat(gateway): wire membership + forwarding into the binary`.

---

## Task 6: Multi-replica forwarding integration test

**Files:** Create `crates/grpc-gateway/tests/forwarding.rs`.

- [ ] **Step 1: Create the test.** Two in-process gateways share one broker; a keyed record for a B-owned partition, submitted through A, is forwarded to B and produced exactly once. Timing-sensitive (group join/rebalance + membership propagation) — generous waits; do not weaken assertions.

```rust
//! Active-active forwarding: two gateway replicas split the dedup partitions and
//! each tails membership. A keyed record whose partition is owned by B, when
//! submitted to A, is forwarded to B over HTTP and produced exactly once;
//! re-submitting the same key dedups. A record with no known owner is Unavailable.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::config::GatewayConfig;
use crabka_grpc_gateway::dedup::membership::{MembershipPublisher, MembershipStore};
use crabka_grpc_gateway::dedup::store::DedupStore;
use crabka_grpc_gateway::dedup::topic::{ensure_dedup_topic, ensure_membership_topic};
use crabka_grpc_gateway::dedup::{partition_for, DedupEngine};
use crabka_grpc_gateway::error::GatewayError;
use crabka_grpc_gateway::forward::{self, Forwarder};
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::state::AppState;
use crabka_grpc_gateway::types::GatewayRecord;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const N: u32 = 4;
const DEDUP: &str = "__crabka_grpc_dedup";
const MEMBERSHIP: &str = "__crabka_grpc_gateway_membership";
const OWNERS_GROUP: &str = "__crabka_grpc_gateway_dedup_owners";
const USER_TOPIC: &str = "fwd-user";

struct Gw {
    addr: String,
    state: Arc<AppState>,
    store: Arc<DedupStore>,
    membership: Arc<MembershipStore>,
    token: CancellationToken,
}

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Bind a listener first (to learn the advertised addr), install the membership
/// publisher, start ownership + membership, then serve Connect + forward routes.
async fn spawn_gateway(bootstrap: &str, client: &str) -> Gw {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let token = CancellationToken::new();

    let store = Arc::new(DedupStore::new(N));
    let node_id = format!("{client}-{addr}");
    let publisher = Arc::new(
        MembershipPublisher::new(
            bootstrap,
            &format!("{client}-pub"),
            node_id.clone(),
            addr.clone(),
            MEMBERSHIP.into(),
        )
        .await
        .unwrap(),
    );
    store.set_membership(publisher);

    // Ownership consumer (shared owners group).
    {
        let store = store.clone();
        let bootstrap = bootstrap.to_string();
        let token = token.clone();
        tokio::spawn(store.run_ownership(
            bootstrap,
            format!("{client}-owner"),
            DEDUP.into(),
            OWNERS_GROUP.into(),
            token,
        ));
    }

    // Membership reader (unique group per replica).
    let membership = Arc::new(MembershipStore::new());
    {
        let membership = membership.clone();
        let bootstrap = bootstrap.to_string();
        let token = token.clone();
        tokio::spawn(membership.run_membership(
            bootstrap,
            format!("{client}-memb"),
            MEMBERSHIP.into(),
            format!("__crabka_grpc_gateway_membership_reader-{node_id}"),
            token,
        ));
    }

    let engine = Arc::new(DedupEngine::new(
        bootstrap,
        client,
        &format!("crabka-grpc-dedup-{client}"),
        DEDUP.into(),
        N,
        store.clone(),
    ));
    let forwarder = Arc::new(Forwarder::new());
    let produce = ProduceCore::new(bootstrap, client, Arc::new(RawCodec))
        .await
        .unwrap()
        .with_dedup(engine)
        .with_forwarding(membership.clone(), forwarder, addr.clone());
    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.to_string(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            client_id: client.into(),
            dedup_topic: DEDUP.into(),
            dedup_partitions: N,
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: format!("crabka-grpc-dedup-{client}"),
            advertised_addr: addr.clone(),
            membership_topic: MEMBERSHIP.into(),
        }),
    });

    // Serve Connect + forward routes (health omitted — not needed here).
    {
        let app = crabka_grpc_gateway::router(state.clone())
            .merge(forward::forward_router(state.clone()));
        let token = token.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await;
        });
    }

    Gw { addr, state, store, membership, token }
}

async fn count_in_user_topic(bootstrap: &str, key_filter: &str) -> usize {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("fwd-verify".into())
        .group_id("fwd-verify-grp".into())
        .subscribe(vec![USER_TOPIC.to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let mut n = 0;
    for _ in 0..10 {
        let batch = consumer.poll(Duration::from_millis(500)).await.unwrap();
        for r in batch {
            if r.value.as_deref() == Some(key_filter.as_bytes()) {
                n += 1;
            }
        }
    }
    let _ = consumer.close().await;
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn keyed_record_forwards_to_owner_and_dedups() {
    let (broker, bootstrap, _dir) = boot().await;
    ensure_dedup_topic(&bootstrap, DEDUP, N, 3_600_000, 1).await.unwrap();
    ensure_membership_topic(&bootstrap, MEMBERSHIP, 1).await.unwrap();
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap)).await.unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec { name: USER_TOPIC.into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }],
            10_000,
        )
        .await
        .unwrap();

    let gw_a = spawn_gateway(&bootstrap, "gwa").await;
    let gw_b = spawn_gateway(&bootstrap, "gwb").await;

    // Wait for a disjoint, covering split where both replicas are warm AND both
    // membership tables route every partition (forwarding can resolve any key).
    let mut ready = false;
    for _ in 0..160 {
        let split_ok = (0..N).all(|p| gw_a.store.owns(p) ^ gw_b.store.owns(p))
            && gw_a.store.has_warmed_once()
            && gw_b.store.has_warmed_once();
        let routes_ok = (0..N).all(|p| gw_a.membership.owner_of(p).is_some())
            && (0..N).all(|p| gw_b.membership.owner_of(p).is_some());
        if split_ok && routes_ok {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(ready, "replicas did not reach a stable split + converged routing");

    // Pick a key owned by B (so submitting through A must forward to B).
    let key = (0..1000)
        .map(|i| format!("k{i}"))
        .find(|k| gw_b.store.owns(partition_for(k, N)))
        .expect("a key owned by B");
    let p = partition_for(&key, N);
    assert!(gw_b.store.owns(p) && !gw_a.store.owns(p));
    assert_eq!(gw_a.membership.owner_of(p).as_deref(), Some(gw_b.addr.as_str()));

    let mk = || GatewayRecord {
        topic: USER_TOPIC.into(),
        key: None,
        value: Bytes::from(key.clone().into_bytes()),
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some(key.clone()),
    };

    // Submit through A → forwarded to B → produced (not deduplicated).
    let first = gw_a.state.produce.produce(mk()).await.unwrap();
    assert!(!first.deduplicated, "first forward should produce");

    // Same key through A again → forwarded to B → B's map hit → deduplicated.
    let second = gw_a.state.produce.produce(mk()).await.unwrap();
    assert!(second.deduplicated, "second forward should dedup");
    assert_eq!(first.offset, second.offset);

    // Exactly one record with that value landed in the user topic.
    assert_eq!(count_in_user_topic(&bootstrap, &key).await, 1);

    gw_a.token.cancel();
    gw_b.token.cancel();
    broker.shutdown().await;
}

#[tokio::test]
async fn no_known_owner_is_unavailable() {
    // A produce core with dedup but an EMPTY membership table: a keyed record
    // for an unowned partition has no route ⇒ Unavailable (origin retries).
    let store = Arc::new(DedupStore::new(N));
    let engine = Arc::new(DedupEngine::new(
        "127.0.0.1:0", "gw", "crabka-grpc-dedup", DEDUP.into(), N, store,
    ));
    let membership = Arc::new(MembershipStore::new());
    let forwarder = Arc::new(Forwarder::new());
    let produce = ProduceCore::new("127.0.0.1:0", "gw", Arc::new(RawCodec))
        .await
        .unwrap()
        .with_dedup(engine)
        .with_forwarding(membership, forwarder, "127.0.0.1:9999".into());
    let rec = GatewayRecord {
        topic: USER_TOPIC.into(),
        key: None,
        value: Bytes::from_static(b"v"),
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some("k".into()),
    };
    let err = produce.produce(rec).await.unwrap_err();
    assert!(matches!(err, GatewayError::Unavailable));
}
```

- [ ] **Step 2: Run + stabilize.** `cargo test -p crabka-grpc-gateway --test forwarding` → PASS. **Re-run 3×** for flakiness. If the split/route wait times out, raise the `0..160` bound (do NOT weaken the split/route/dedup assertions). Then full gates.

- [ ] **Step 3: Commit** `test(gateway): multi-replica forwarding to owner + dedup + no-route Unavailable`.

---

## Final review + finish

After Task 6: dispatch a final adversarial reviewer over the whole P3b diff (`git diff origin/main...claude/gateway-p3b -- crates/grpc-gateway`), focusing on: routing correctness under stale/dead-node records (offset tiebreak), forward-loop impossibility (`forward_handler` → `produce_local` never re-forwards), `produce`/`produce_local` borrow-discipline, no broker changes, no caller-identity/TLS scope creep (those are P5/P4), single-replica behavior unchanged. Address nits, then finish the branch (push + PR vs `main`).

## Self-review notes (author)

- **Spec coverage:** membership topic + `{node_id, advertised_addr, owned, epoch}` publish (T2/T3) ✓; all-replica tail → routing table (T2) ✓; key→owner routing + gateway→gateway forward (T4) ✓; `UNAVAILABLE` on no-route / warm-up (T4, dedup gate) ✓; `transactional.id`-per-partition fencing already in place (P3a) ✓. Identity-forwarding deferred to P5 (spec §4) — intentionally out.
- **Correctness:** single membership partition ⇒ total order ⇒ offset tiebreak resolves stale-owner shadowing; `forward_handler` uses `produce_local` (no re-forward) ⇒ no loops; self-addr route ⇒ local; transport error ⇒ `Unavailable` (retry/re-resolve). Single owner ⇒ no forwarding ⇒ P3a behavior.
- **Greenfield:** no compat shims; `OnceLock` membership slot is a config seam (not a compat toggle) keeping P3a tests green without churn.
