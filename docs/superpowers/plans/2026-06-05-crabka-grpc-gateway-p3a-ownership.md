# Crabka gRPC Gateway — P3a: Dedup Ownership Sharding (correctness half) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the single-owner dedup engine **ownership-aware** so N gateway replicas run safely (no double-produce): each replica owns a subset of `__crabka_grpc_dedup` partitions via a consumer group, and a keyed `Send` for a partition this replica doesn't own / hasn't warmed returns a **retriable `UNAVAILABLE`** instead of producing.

**Architecture:** One **ownership consumer** per gateway in group `__crabka_grpc_gateway_dedup_owners`, `subscribe([__crabka_grpc_dedup])`, `read_committed`, `Earliest`, `CooperativeSticky`, **never committing** — so its `assignment()` *is* the owned-partition set and its poll loop reads each owned partition from earliest to (re)build the claim map. A single replica owns all partitions ⇒ identical behavior to P0–P2. **No gateway→gateway forwarding** (that's P3b); non-owners just answer `UNAVAILABLE` and clients retry.

**Tech Stack:** `crabka-client-consumer` (`assignment()`, `Assignor::CooperativeSticky`, `AutoOffsetReset::Earliest`), `dashmap`, `std::sync::RwLock`, `tokio_util::sync::CancellationToken`. Tests use the in-process broker (`crabka-broker` `test-helpers`).

---

## Scope

**In:** ownership-aware `DedupStore` (owned-set + warm state driven by a background ownership consumer), `dedup_produce` ownership gate → `UNAVAILABLE`, retriable error mapping in the Send wires, bin wiring, single- + multi-replica tests.

**Out (later slices):** P3b gateway→gateway forwarding + `__crabka_grpc_gateway_membership` routing topic; P4–P9. Do NOT add forwarding. Broker NEVER modified.

**Branch:** `claude/gateway-p3` (off `origin/main`, which has P0–P2 + streaming merged). Spec: `docs/superpowers/specs/2026-06-04-crabka-grpc-gateway-design.md` §2.

## File structure

```
crates/grpc-gateway/src/
  error.rs        # T1: + GatewayError::Unavailable
  handlers.rs     # T1: map Unavailable → retriable per-record error (helper)
  streaming.rs    # T1: send_stream_inner uses the same helper
  dedup/store.rs  # T2: ownership rework — owned-set + warm + run_ownership; REMOVE warm_up
  dedup/mod.rs    # T3: dedup_produce ownership gate
  bin/gateway.rs  # T4: spawn ownership task; /readyz on has_warmed_once
crates/grpc-gateway/tests/
  integration_dedup.rs  # T2/T3: migrate the 3 tests off warm_up → run_ownership
  ownership.rs          # T4: multi-replica ownership split test
```

Tasks are **sequential** (shared dedup files).

---

## Task 1: `Unavailable` error + retriable mapping in the Send wires

**Files:** `src/error.rs`, `src/handlers.rs`, `src/streaming.rs`

- [ ] **Step 1: Add the variant.** In `src/error.rs`, add to `GatewayError`:

```rust
    #[error("dedup partition not owned by this replica or still warming up")]
    Unavailable,
```

- [ ] **Step 2: Add a shared error→RecordResult mapper in `handlers.rs`.** Add a `pub(crate)` helper and use it in `send`:

```rust
/// Map a produce error to a per-record `RecordResult`. `Unavailable` is
/// retriable (the caller should re-route to another replica); everything else
/// is reported non-retriable.
pub(crate) fn error_result(e: &crate::error::GatewayError) -> crate::pb::RecordResult {
    let retriable = matches!(e, crate::error::GatewayError::Unavailable);
    let code = if retriable { 14 } else { 1 }; // 14 = gRPC UNAVAILABLE
    crate::pb::RecordResult {
        partition: -1,
        offset: -1,
        deduplicated: false,
        error: Some(crate::pb::ErrorInfo { code, message: e.to_string(), retriable }),
    }
}
```

In the existing `send` handler, replace the inline `Err(e) => pb::RecordResult { ... }` arm with `Err(e) => crate::handlers::error_result(&e)`.

- [ ] **Step 3: Use it in streaming.** In `src/streaming.rs` `send_stream_inner`, replace the inline `Err(e) => pb::RecordResult { ... }` arm with `crate::handlers::error_result(&e)`.

- [ ] **Step 4: Build + gates.** `cargo build -p crabka-grpc-gateway`; `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`; `cargo fmt --check -p crabka-grpc-gateway`. (No new test yet — exercised by later tasks.)

- [ ] **Step 5: Commit** `feat(gateway): GatewayError::Unavailable + retriable error mapping` (stage `src/error.rs src/handlers.rs src/streaming.rs`).

---

## Task 2: `DedupStore` ownership rework (the crux)

**Files:** `src/dedup/store.rs`, `tests/integration_dedup.rs` (migrate the warm-up test)

Replace the global `ready`/`warm_up` with an ownership consumer driving owned-set + warm state.

- [ ] **Step 1: Rewrite the `DedupStore` struct + accessors** in `src/dedup/store.rs`. Replace the struct + `new`/`is_ready` with:

```rust
use std::collections::HashSet;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio_util::sync::CancellationToken;

use crabka_client_consumer::{Assignor, AutoOffsetReset, Consumer, IsolationLevel};

pub struct DedupStore {
    map: DashMap<String, ClaimValue>,
    partitions: u32,
    /// Dedup-partition ids this replica currently owns (its consumer-group
    /// assignment). Written by `run_ownership`, read on the hot path.
    owned: RwLock<HashSet<u32>>,
    /// Caught up reading owned partitions since the last assignment change.
    warm: AtomicBool,
    /// Has been warm at least once (drives `/readyz`).
    warmed_once: AtomicBool,
}

impl DedupStore {
    #[must_use]
    pub fn new(partitions: u32) -> Self {
        Self {
            map: DashMap::new(),
            partitions,
            owned: RwLock::new(HashSet::new()),
            warm: AtomicBool::new(false),
            warmed_once: AtomicBool::new(false),
        }
    }

    /// True if dedup-partition `p` is currently owned by this replica.
    #[must_use]
    pub fn owns(&self, p: u32) -> bool {
        self.owned.read().expect("owned lock").contains(&p)
    }

    /// True once caught up on owned partitions since the last assignment change.
    #[must_use]
    pub fn is_warm(&self) -> bool {
        self.warm.load(Ordering::SeqCst)
    }

    /// Has warmed at least once (readiness probe).
    #[must_use]
    pub fn has_warmed_once(&self) -> bool {
        self.warmed_once.load(Ordering::SeqCst)
    }
```

Keep the existing `get` and `apply` methods unchanged. **Remove** the old `is_ready` and `warm_up` methods. Keep `ClaimValue` and `write_claim`.

- [ ] **Step 2: Add `run_ownership`** (the background loop) to the `impl DedupStore`:

```rust
    /// Run the ownership consumer until `shutdown` fires. Joins the owners
    /// group on the dedup topic; its assignment is the owned-partition set.
    /// Reads owned partitions from earliest (never commits) to (re)build the
    /// claim map, re-arming the warm gate on every assignment change. Closes
    /// the consumer on shutdown so the coordinator task + group member don't
    /// leak (see the gateway consumer-lifecycle rule).
    pub async fn run_ownership(
        self: Arc<Self>,
        bootstrap: String,
        client_id: String,
        dedup_topic: String,
        group: String,
        shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        let mut consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id(client_id)
            .group_id(group)
            .subscribe(vec![dedup_topic.clone()])
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .assignor(Assignor::CooperativeSticky)
            .build()
            .await?;

        let mut current: HashSet<u32> = HashSet::new();
        let mut empty_polls = 0u32;
        let mut poll_err: Option<GatewayError> = None;

        loop {
            let batch = tokio::select! {
                () = shutdown.cancelled() => break,
                b = consumer.poll(Duration::from_millis(500)) => match b {
                    Ok(batch) => batch,
                    Err(e) => { poll_err = Some(e.into()); break; }
                },
            };

            // Reconcile ownership against the live assignment.
            let assigned: HashSet<u32> = consumer
                .assignment()
                .await
                .into_iter()
                .filter(|(t, _)| *t == dedup_topic)
                .filter_map(|(_, p)| u32::try_from(p).ok())
                .collect();
            if assigned != current {
                let revoked: HashSet<u32> = current.difference(&assigned).copied().collect();
                if !revoked.is_empty() {
                    self.map
                        .retain(|k, _| !revoked.contains(&crate::dedup::partition_for(k, self.partitions)));
                }
                *self.owned.write().expect("owned lock") = assigned.clone();
                current = assigned;
                self.warm.store(false, Ordering::SeqCst);
                empty_polls = 0;
            }

            if batch.is_empty() {
                empty_polls += 1;
                if empty_polls >= 2 {
                    self.warm.store(true, Ordering::SeqCst);
                    self.warmed_once.store(true, Ordering::SeqCst);
                }
                continue;
            }
            empty_polls = 0;
            for r in batch {
                let Some(key_bytes) = r.key else { continue };
                let key = String::from_utf8_lossy(&key_bytes).into_owned();
                match r.value {
                    None => {
                        self.map.remove(&key);
                    }
                    // A malformed claim must not kill the ownership loop; skip it.
                    Some(v) => {
                        if let Ok(claim) = serde_json::from_slice::<ClaimValue>(&v) {
                            self.map.insert(key, claim);
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
```

> VERIFY: `Consumer::builder()….assignor(Assignor::CooperativeSticky)` — confirm the builder method is `.assignor(...)` and the variant name `Assignor::CooperativeSticky` against `crates/client-consumer/src/consumer.rs` (builder) + `src/assignor.rs`. `consumer.assignment().await -> Vec<(String, i32)>`. `Consumer::close(self)` is async + consumes self. `ConsumerError` → `GatewayError` via the existing `#[from]` (so `e.into()` works); if not, wrap with `GatewayError::Consumer(e)`.

- [ ] **Step 3: Migrate the warm-up test.** In `tests/integration_dedup.rs`, the `warmup_reads_existing_claims` test calls the removed `store.warm_up(...)`. Replace that call with spawning `run_ownership` and waiting for `has_warmed_once()`. Replace the body's warm step:

```rust
    // (after write_claim + a fresh store2)
    use tokio_util::sync::CancellationToken;
    let token = CancellationToken::new();
    let handle = tokio::spawn(store2.clone().run_ownership(
        bootstrap.clone(),
        "gw-dedup-warm".into(),
        topic.to_string(),
        "__crabka_grpc_gateway_dedup_owners".into(),
        token.clone(),
    ));
    // Wait until warmed (sole member owns all partitions).
    let mut warm = false;
    for _ in 0..60 {
        if store2.has_warmed_once() { warm = true; break; }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    check!(warm);
    check!(store2.get("key-A").map(|c| c.offset) == Some(7));
    check!(store2.get("absent").is_none());
    token.cancel();
    let _ = handle.await;
```

Add `tokio-util` to `[dev-dependencies]` in `crates/grpc-gateway/Cargo.toml` if not present (it's a workspace dep: `tokio-util = { workspace = true }`). The `crabka-grpc-gateway` lib already depends on `tokio-util` (used by health), so it's available; ensure tests can use `CancellationToken` (dev-dep it if the test target can't see it).

- [ ] **Step 4: Build + run the migrated test + gates.**

Run: `cargo test -p crabka-grpc-gateway --test integration_dedup warmup_reads_existing_claims` → PASS.
Run: `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`; `cargo fmt --check -p crabka-grpc-gateway`.

(The other two `integration_dedup` tests + the bin still reference the removed `warm_up`/`is_ready` and will not compile yet — they are fixed in Task 3 / Task 4. If you need a green build between tasks, you may migrate all three dedup tests here; otherwise proceed to Task 3 which fixes the engine + remaining tests. Pick one and keep the build green by the end of each commit — migrating all three `integration_dedup` tests in THIS task is the cleaner cut.)

- [ ] **Step 5: Commit** `feat(gateway): ownership-aware DedupStore (consumer-group ownership + warm gate)`.

---

## Task 3: `DedupEngine` ownership gate

**Files:** `src/dedup/mod.rs`, `tests/integration_dedup.rs` (the two dedup tests, if not already migrated in T2)

- [ ] **Step 1: Gate `dedup_produce` on ownership.** In `src/dedup/mod.rs`, replace the opening readiness check:

```rust
        if !self.store.is_ready() {
            return Err(GatewayError::NotReady);
        }
        let key = rec.idempotency_key.as_deref().ok_or_else(|| {
            GatewayError::Other("dedup_produce called without idempotency_key".into())
        })?;
```

with:

```rust
        let key = rec.idempotency_key.as_deref().ok_or_else(|| {
            GatewayError::Other("dedup_produce called without idempotency_key".into())
        })?;
        let p = partition_for(key, self.partitions);
        // Mutual exclusion: only the owner of `p` may produce its keys, and only
        // once warmed (claim map rebuilt) — otherwise refuse so the caller
        // retries against the owning replica.
        if !self.store.owns(p) || !self.store.is_warm() {
            return Err(GatewayError::Unavailable);
        }
```

Then below, the existing `let p = partition_for(key, self.partitions);` (before the lock) is now a duplicate — remove the second computation and reuse `p`. (The fast-path `store.get(key)`, the `self.slots[p]` lock, the re-check, and `txn_write` all stay unchanged; `txn_write` keeps its local `store.apply` after commit.)

- [ ] **Step 2: Migrate the dedup tests** (if not done in T2). The `duplicate_idempotency_key_produces_once` and `concurrent_duplicates_produce_once` tests build `DedupStore::new(4)` + `store.warm_up(...)` then the engine. Replace the `warm_up` call in each with the spawn-`run_ownership` + wait-`has_warmed_once` pattern from Task 2 Step 3 (a sole group member owns all 4 partitions, so `owns(p)` is true for every `p` once warm). Cancel the token + await the handle at the end of each test (before `broker.shutdown()`), so the ownership consumer is closed.

Example shape for each:
```rust
    let store = Arc::new(DedupStore::new(4));
    let token = CancellationToken::new();
    let own = tokio::spawn(store.clone().run_ownership(
        bootstrap.clone(), "gw-warm".into(), dedup_topic.to_string(),
        "__crabka_grpc_gateway_dedup_owners".into(), token.clone()));
    for _ in 0..60 { if store.has_warmed_once() { break; } tokio::time::sleep(std::time::Duration::from_millis(250)).await; }
    assert!(store.has_warmed_once());
    let engine = Arc::new(DedupEngine::new(&bootstrap, "gw-dedup", "crabka-grpc-dedup", dedup_topic.to_string(), 4, store.clone()));
    // ... existing produce + assertions ...
    token.cancel(); let _ = own.await;
    broker.shutdown().await;
```

- [ ] **Step 3: Build + run all dedup tests + gates.**

Run: `cargo test -p crabka-grpc-gateway --test integration_dedup` → all 3 pass (warm-up rebuild + sequential dup + concurrent dup) — proving single-replica behavior is unchanged.
Run: `cargo test -p crabka-grpc-gateway --test unit_basics dedup_produce_before_warmup_is_not_ready` — NOTE this P0–P2 unit test asserts `NotReady` before warm-up; with the gate change it now returns `Unavailable` (not owned + not warm). UPDATE that test's expectation from `GatewayError::NotReady` to `GatewayError::Unavailable` and rename it to `dedup_produce_before_ownership_is_unavailable`.
Run: `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`; `cargo fmt --check -p crabka-grpc-gateway`.

- [ ] **Step 4: Commit** `feat(gateway): dedup_produce ownership gate (Unavailable for non-owned)`.

---

## Task 4: Binary wiring + multi-replica ownership test

**Files:** `src/bin/gateway.rs`, `tests/ownership.rs`

- [ ] **Step 1: Wire the ownership task into the binary.** In `src/bin/gateway.rs`, the dedup section currently builds the store and spawns `store.warm_up(...)`, flipping readiness on success. Replace that with spawning `run_ownership` under a `CancellationToken`, and gate `/readyz` on `has_warmed_once`. Replace the warm-up spawn block with:

```rust
    use tokio_util::sync::CancellationToken;
    let store = Arc::new(DedupStore::new(config.dedup_partitions));
    let readiness = Readiness::new();
    let shutdown = CancellationToken::new();
    {
        let store = store.clone();
        let bootstrap = config.bootstrap.clone();
        let client_id = format!("{}-dedup-owner", config.client_id);
        let dedup_topic = config.dedup_topic.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = store
                .run_ownership(
                    bootstrap,
                    client_id,
                    dedup_topic,
                    "__crabka_grpc_gateway_dedup_owners".to_string(),
                    shutdown,
                )
                .await
            {
                tracing::error!(error = %e, "dedup ownership task exited with error");
            }
        });
    }
    // Flip /readyz once the store has warmed at least once.
    {
        let store = store.clone();
        let readiness = readiness.clone();
        tokio::spawn(async move {
            loop {
                if store.has_warmed_once() { readiness.set_ready(); break; }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        });
    }
```

Keep the rest of `main` (engine build with `store`, `with_dedup`, router/serve). On `ctrl_c` graceful shutdown, also `shutdown.cancel();` so the ownership consumer closes cleanly — add `shutdown.cancel();` inside the existing `with_graceful_shutdown` closure (after `ctrl_c().await`).

- [ ] **Step 2: Build the binary.** `cargo build -p crabka-grpc-gateway` → clean. (No unit test for `main`; the bin is excluded from coverage.)

- [ ] **Step 3: Write the multi-replica ownership test** `tests/ownership.rs`:

```rust
//! Two ownership consumers in one group split the dedup-topic partitions, so a
//! keyed produce is served only by the OWNING replica; the non-owner returns a
//! retriable Unavailable. Timing-sensitive (group join/rebalance) — generous
//! waits; the repo has prior consumer-group test-flake history.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::dedup::store::DedupStore;
use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
use crabka_grpc_gateway::dedup::{partition_for, DedupEngine};
use crabka_grpc_gateway::error::GatewayError;
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::types::GatewayRecord;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const N: u32 = 4;
const DEDUP: &str = "__crabka_grpc_dedup";
const GROUP: &str = "__crabka_grpc_gateway_dedup_owners";

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ownership_split_non_owner_is_unavailable() {
    let (broker, bootstrap, _dir) = boot().await;
    ensure_dedup_topic(&bootstrap, DEDUP, N, 3_600_000, 1).await.unwrap();
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap)).await.unwrap();
    admin.create_topics(&[CreateTopicSpec { name: "own-user".into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }], 10_000).await.unwrap();

    let store_a = Arc::new(DedupStore::new(N));
    let store_b = Arc::new(DedupStore::new(N));
    let token = CancellationToken::new();
    let ha = tokio::spawn(store_a.clone().run_ownership(bootstrap.clone(), "gw-a".into(), DEDUP.into(), GROUP.into(), token.clone()));
    let hb = tokio::spawn(store_b.clone().run_ownership(bootstrap.clone(), "gw-b".into(), DEDUP.into(), GROUP.into(), token.clone()));

    // Wait for a stable, disjoint split covering all N partitions, both warm.
    let mut split_ok = false;
    for _ in 0..120 {
        let a: Vec<u32> = (0..N).filter(|p| store_a.owns(*p)).collect();
        let b: Vec<u32> = (0..N).filter(|p| store_b.owns(*p)).collect();
        let disjoint = a.iter().all(|p| !b.contains(p));
        let covers = (a.len() + b.len()) as u32 == N;
        if !a.is_empty() && !b.is_empty() && disjoint && covers
            && store_a.has_warmed_once() && store_b.has_warmed_once()
        {
            split_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(split_ok, "ownership did not split across the two replicas in time");

    // Find a key owned by A.
    let key = (0..1000)
        .map(|i| format!("k{i}"))
        .find(|k| store_a.owns(partition_for(k, N)))
        .expect("a key owned by A");
    let p = partition_for(&key, N);
    assert!(store_a.owns(p) && !store_b.owns(p));

    let engine_a = Arc::new(DedupEngine::new(&bootstrap, "gw-a", "crabka-grpc-dedup-a", DEDUP.into(), N, store_a.clone()));
    let engine_b = Arc::new(DedupEngine::new(&bootstrap, "gw-b", "crabka-grpc-dedup-b", DEDUP.into(), N, store_b.clone()));
    let core_a = ProduceCore::new(&bootstrap, "gw-a", Arc::new(RawCodec)).await.unwrap().with_dedup(engine_a);
    let core_b = ProduceCore::new(&bootstrap, "gw-b", Arc::new(RawCodec)).await.unwrap().with_dedup(engine_b);

    let mk = || GatewayRecord {
        topic: "own-user".into(), key: None, value: Bytes::from_static(b"v"),
        headers: vec![], partition: None, timestamp_ms: None, idempotency_key: Some(key.clone()),
    };

    // Non-owner B refuses with a retriable Unavailable.
    let err = core_b.produce(mk()).await.unwrap_err();
    assert!(matches!(err, GatewayError::Unavailable), "non-owner should be Unavailable, got {err:?}");

    // Owner A produces it (deduplicated=false the first time).
    let ok = core_a.produce(mk()).await.unwrap();
    assert!(!ok.deduplicated);

    token.cancel();
    let _ = ha.await;
    let _ = hb.await;
    broker.shutdown().await;
}
```

- [ ] **Step 4: Run the new test + the full suite + gates.**

Run: `cargo test -p crabka-grpc-gateway --test ownership` → PASS (re-run a few times to check for flakiness; if it flakes on the split wait, increase the loop bound — do NOT weaken the assertions).
Run: `cargo test -p crabka-grpc-gateway` → all pass.
Run: `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`; `cargo fmt --check -p crabka-grpc-gateway`.

- [ ] **Step 5: Commit** `feat(gateway): wire dedup ownership task into binary + multi-replica ownership test`.

---

## Final verification

- [ ] `cargo test -p crabka-grpc-gateway` — unit + integration_send/consume/dedup + streaming + wire + ownership all pass.
- [ ] `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings` — clean.
- [ ] `cargo fmt --check -p crabka-grpc-gateway` — no diff.
- [ ] Diff touches only `crates/grpc-gateway/**` (+ `Cargo.lock` if a dev-dep was added) — broker untouched.

## Self-review (completed during planning)

- **Spec coverage (§2 ownership half):** ownership via consumer group ✓ (T2 `run_ownership`); partition assignment = ownership ✓ (`owns`); per-(re)assignment warm-up ✓ (warm gate re-armed on assignment change); `UNAVAILABLE` for non-owned/non-warm ✓ (T3 gate + T1 retriable mapping); txn.id-per-partition fencing — already in P0–P2, preserved (`txn_write` unchanged). Membership topic + forwarding correctly DEFERRED (P3b).
- **Type consistency:** `DedupStore::{owns,is_warm,has_warmed_once,run_ownership,get,apply}` used consistently across store/engine/bin/tests; `GatewayError::Unavailable` defined T1, produced T3, mapped T1; `partition_for`/`ClaimValue`/`DedupEngine::new` unchanged from P0–P2; `run_ownership` signature identical in store, bin, and all tests.
- **Placeholders:** none — complete code each step; the `> VERIFY` callout (builder `.assignor`, `assignment()`, `close`, `ConsumerError→GatewayError`) names exact files to confirm.
- **Behavior preservation:** a sole group member owns all partitions ⇒ `owns(p)` true ∀p once warm ⇒ identical to P0–P2; the migrated dedup tests prove it. The P0–P2 `NotReady`-before-warm unit test is updated to expect `Unavailable` (semantics changed deliberately).

## Risks / things the implementer must verify

1. **The ownership loop (T2) is the crux.** Confirm `.assignor(Assignor::CooperativeSticky)`, `assignment().await`, `Consumer::close(self)`, and `ConsumerError → GatewayError` against `crates/client-consumer`. The select!/break/close-after-loop shape avoids moving `consumer` inside a select arm (the borrow trap from the streaming Subscribe handler) and guarantees the consumer is closed on both shutdown and poll-error — do NOT close inside a select arm.
2. **Never commit** on the ownership consumer — committing would make re-assignment resume from a committed offset and miss claims. Do not call `commit_*`.
3. **Multi-replica test timing (T4).** Group join + rebalance takes seconds; the split-wait loop is generous (120 × 500ms). If it flakes, lengthen the wait — never weaken the disjoint/covers/warm assertions. Per repo history (`eager_rebalance`, `share_consume` flakes), re-run isolated before calling a failure a regression.
4. **Warm granularity is coarse** (whole-assignment "caught up", not per-partition). Acceptable for P3a — a newly-assigned partition makes the whole replica briefly `UNAVAILABLE` for its keys until caught up. Per-partition warm is a possible later refinement.
5. **Consumer lifecycle:** `run_ownership` closes its consumer on every exit (shutdown or poll-error) — mirrors the P3-streaming leak fix; the bin cancels the token on graceful shutdown.

## What lands after this slice

- **P3b — forwarding:** `__crabka_grpc_gateway_membership` routing topic (replicas publish addr + owned partitions → routing table) + gateway→gateway forwarding (non-owner forwards the record to the owner instead of `UNAVAILABLE`). Gating unknown to spike first: the gateway→gateway client (tonic vs reqwest+Connect framing).
- Then P4 (TLS/mTLS), P5 (identity→ACL / `crabka-authz`), P6/P7 (webhooks), P8 (telemetry), P9 (operator).
