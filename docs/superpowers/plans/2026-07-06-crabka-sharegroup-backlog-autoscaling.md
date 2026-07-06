# Serverless share-group backlog autoscaling (MSG-4) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose KIP-932 share-group backlog as a fleet-complete Prometheus gauge on the broker's existing `/metrics`, emitted by the coordinator broker (single-emitter), so KEDA's stock `prometheus` scaler autoscales serverless share-group consumers on queue depth — including scale-to-zero — with `sum(crabka_broker_share_group_backlog{group_id="G"})`.

**Architecture:** The coordinator broker (the `__consumer_offsets-0` leader — the one broker with the complete `initialized` set) runs a periodic poll loop that, per initialized `(group, topic, partition)`, reads SPSO (`SharePersister::read_state`) + HWM (local `Partition::high_watermark`, or a peer `ListOffsets(LATEST)` for data partitions it doesn't co-lead), computes `effective_backlog = (hwm − (spso≥0 ? spso : log_start)).max(0)` (never `-1`), and sets one gauge series. A per-tick coordinator-leadership self-gate + stale-series hygiene keep `sum()` exact.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `prometheus-client 0.25`, `tokio`, the in-process `Broker::start` harness, the `InterBrokerClient`/`Connection` peer-RPC surface, `ListOffsets`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-sharegroup-backlog-autoscaling-design.md`](../specs/2026-07-06-crabka-sharegroup-backlog-autoscaling-design.md).

**PREREQUISITES:** none unlanded — reuses the landed KIP-932 stack, metrics registry + `/metrics`, `ListOffsets`, and the `InterBrokerClient` peer-RPC surface. Independent of the diskless chapter and of MSG-1/2/3.

---

## Invariants

1. **Never emit `-1`** — uninitialized SPSO → `hwm − log_start` (full backlog); non-local partition → remote HWM; every value `≥ 0`.
2. **Exactly one series per `(group, topic, partition)`** — the coordinator broker is the sole emitter; `sum()` never double-counts and never gaps.
3. **Scale-to-zero safe** — `sum() == 0` iff genuinely drained; an unknown/absent state is never a false `0`.
4. **No wire/KIP byte change** — `DescribeShareGroupOffsets` bytes untouched; `ListOffsets` reused internally; no new HTTP surface/port.
5. **Consumer-group lag untouched** — free via the stock KEDA `kafka` scaler (verify-only).
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the gauge; the `effective_backlog` kernel; the coordinator-hosted poll loop (local + remote HWM); the self-gate + stale-series hygiene; the spawn wiring + config; the KEDA manifest/docs.
- **Deferred:** the external-scaler gRPC service; the data-leader-driven scale-out model (partition→groups index); broker autoscaling; a coordinator-side SPSO cache; MSG-2/3/5.

---

## File Structure

- **`crates/broker/src/metrics.rs`** — `ShareGroupLabel` + `share_group_backlog` family (Task 1).
- **`crates/broker/src/share_partition/backlog_poller.rs`** (new) — `effective_backlog` + `spawn_backlog_poller` + the peer-HWM helper (Tasks 2, 3, 4, 5).
- **`crates/broker/src/coordinator/unified/mod.rs`** — enumeration seam visibility + topic-id→name resolver (Task 3).
- **`crates/broker/src/broker.rs:2464-2473`** — spawn after `spawn_lock_sweeper()` (Task 3).
- **`crates/broker/src/config.rs`** — optional poll interval (Task 6).
- **`docs/examples/keda-sharegroup-scaledobject.yaml`** (new) — the KEDA manifest + notes (Task 7).

**Batching:** Tasks 1 (`metrics.rs`) and 2 (new `backlog_poller.rs`) touch disjoint files → run concurrently. Task 3 depends on both and wires `broker.rs`. Tasks 4 and 5 both extend `backlog_poller.rs` → run sequentially (5 then 4, or vice-versa). Tasks 6 (`config.rs`) and 7 (`docs/`) are parallel-safe.

---

## Task 1: The `share_group_backlog` gauge

**Files:**
- Modify: `crates/broker/src/metrics.rs` (`:53-57` label template, `:144` field template, `:358` init template, `:554-559` register template)

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `metrics.rs` (encode-and-assert, mirroring existing metric tests):

```rust
    #[test]
    fn share_group_backlog_encodes_as_gauge() {
        let m = BrokerMetrics::new_for_test(); // or BrokerMetrics::new(&mut Registry::with_prefix("crabka_broker"))
        m.share_group_backlog
            .get_or_create(&ShareGroupLabel {
                group_id: "g".into(),
                topic: "t".into(),
                partition: 0,
            })
            .set(42);
        let text = m.encode_to_string(); // the crate's existing encode helper; else encode(&registry)
        assert!(text.contains(
            "crabka_broker_share_group_backlog{group_id=\"g\",topic=\"t\",partition=\"0\"} 42"
        ));
        // Gauge => NO _total suffix.
        assert!(!text.contains("share_group_backlog_total"));
    }
```

(Match the existing test's construction/encode helper — read the bottom of `metrics.rs` for the exact `BrokerMetrics` test constructor + encode idiom and mirror it.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker --lib metrics::tests::share_group_backlog_encodes_as_gauge`
Expected: FAIL — `ShareGroupLabel` / `share_group_backlog` undefined.

- [ ] **Step 3: Implement (mirror `partition_disk_bytes`)**

Label struct near `PartitionLabel` (`metrics.rs:53-57`):

```rust
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ShareGroupLabel {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
}
```

Field on `BrokerMetrics` (near `:144`): `pub share_group_backlog: Family<ShareGroupLabel, Gauge>,`
Init in `new()` (near `:358`): `let share_group_backlog: Family<ShareGroupLabel, Gauge> = Family::default();`
Register in `new()` (near `:554`):

```rust
registry.register(
    "share_group_backlog",
    "Share-group partition backlog (HWM - effective SPSO) in records, per (group,topic,partition), \
     emitted by the coordinator broker. Fleet total = sum(crabka_broker_share_group_backlog{group_id=\"G\"}).",
    share_group_backlog.clone(),
);
```

Add `share_group_backlog` to the struct literal returned by `new()`.

- [ ] **Step 4: Run to verify it passes; commit**

Run: `cargo test -p crabka-broker --lib metrics::tests::share_group_backlog_encodes_as_gauge` → PASS.

```bash
git add crates/broker/src/metrics.rs
git commit -m "feat(broker): share_group_backlog gauge family"
```

---

## Task 2: The `effective_backlog` kernel

**Files:**
- Create: `crates/broker/src/share_partition/backlog_poller.rs` (+ `mod backlog_poller;` in `share_partition/mod.rs`)

- [ ] **Step 1: Write the failing tests**

```rust
//! Share-group backlog poll loop: the coordinator broker emits a fleet-complete
//! `share_group_backlog` gauge. Pure math is `effective_backlog`.

/// Backlog in records for one partition. `spso < 0` means the share group has
/// never initialized this partition (persister returned None) — its full
/// contents are queued, so the base is the log-start, never `-1`.
pub(crate) fn effective_backlog(hwm: i64, spso: i64, log_start: i64) -> i64 {
    let base = if spso >= 0 { spso } else { log_start };
    (hwm - base).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn initialized_uses_spso() {
        assert!(effective_backlog(100, 40, 0) == 60);
    }

    #[test]
    fn uninitialized_uses_log_start_full_backlog() {
        // spso = -1 (uninitialized) => full available backlog, NOT -1, NOT 0.
        assert!(effective_backlog(100, -1, 10) == 90);
    }

    #[test]
    fn drained_is_zero() {
        assert!(effective_backlog(100, 100, 0) == 0);
    }

    #[test]
    fn negative_clamps_to_zero() {
        // Non-atomic reads can momentarily sample spso > hwm.
        assert!(effective_backlog(100, 120, 0) == 0);
    }
}
```

- [ ] **Step 2: Run to verify it fails, then passes**

Run: `cargo test -p crabka-broker --lib backlog_poller::tests`
Expected: FAIL to compile (module not declared) → add `mod backlog_poller;` → PASS. No implementation beyond the function above is needed for this task.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/src/share_partition/backlog_poller.rs crates/broker/src/share_partition/mod.rs
git commit -m "feat(broker): effective_backlog kernel (never -1) for share-group backlog"
```

---

## Task 3: Coordinator poll loop (local HWM) + spawn

**Files:**
- Modify: `crates/broker/src/share_partition/backlog_poller.rs` (`spawn_backlog_poller`)
- Modify: `crates/broker/src/coordinator/unified/mod.rs` (enumeration seam visibility + topic-id→name resolver)
- Modify: `crates/broker/src/broker.rs:2464-2473` (spawn)
- Test: `crates/broker/tests/sharegroup_backlog.rs` (new, single-broker)

- [ ] **Step 1: Write the failing integration test (single-broker = coordinator co-leads all)**

New `crates/broker/tests/sharegroup_backlog.rs`, mirroring the `Broker::start(BrokerConfig::for_tests(..))` + admin/produce boot pattern from `crates/broker/tests/share_consume.rs`:

```rust
// Boot one broker (it is the __consumer_offsets-0 coordinator AND leads all data
// partitions). Create a share-subscribed topic, produce N records, do NOT consume.
// Tick the backlog poller (spawned by Broker::start), scrape /metrics, and assert
// the series equals N (full backlog, uninitialized SPSO -> hwm - log_start), NOT -1, NOT 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_gauge_reports_full_backlog_for_uninitialized_group() {
    // ... boot, create topic "bk-itest", produce 5 records ...
    // Force/await one poll tick (use a short backlog_poll_interval in for_tests, e.g. 200ms),
    // then GET the broker's /metrics (metrics_server default :9404) and assert:
    //   crabka_broker_share_group_backlog{group_id="bk-g",topic="bk-itest",partition="0"} 5
    // The group is referenced (a ShareFetch session created) so it appears in `initialized`,
    // but with no acks its SPSO stays uninitialized -> full backlog.
}
```

(Read `share_consume.rs` for the exact share-group setup — how a group becomes `initialized` — and `metrics_server.rs:49-64` for the scrape URL. Use a short poll interval in `BrokerConfig::for_tests` so a tick lands within the test.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker --test sharegroup_backlog`
Expected: FAIL — no series emitted (poller not yet spawned/implemented).

- [ ] **Step 3: Implement**

**(a)** In `unified/mod.rs`, ensure `share_group_ids() -> Vec<String>` (`:394`) and `share_state_partition_metadata(&group)` (`:915`, returning `initialized: Vec<(Uuid, Vec<i32>)>`) are reachable from the poller (`pub(crate)` as needed), and add/confirm a topic-id→name resolver on `GroupCoordinator` (mirror `manager.rs:111 topic_name_for`, resolving via the metadata image).

**(b)** In `backlog_poller.rs`, add the poller (mirror `SharePartitionLeaderManager::spawn_lock_sweeper`, `manager.rs:276-300` — `tokio::spawn` + `interval` + snapshot-before-await). This task implements the **local-HWM path only**; the remote branch is a `TODO(Task 4)` returning `None`/skip for non-co-led partitions:

```rust
use std::{collections::HashSet, sync::Arc, time::Duration};
use crabka_ids::PartitionIndex;
use crate::{coordinator::unified::GroupCoordinator, metrics::{BrokerMetrics, ShareGroupLabel},
            partition_registry::PartitionRegistry, share_coordinator::persister_client::SharePersister};

pub(crate) fn spawn_backlog_poller(
    coordinator: Arc<GroupCoordinator>,
    partitions: Arc<PartitionRegistry>,
    persister: Arc<SharePersister>,
    metrics: BrokerMetrics,
    period: Duration,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        let mut last: HashSet<ShareGroupLabel> = HashSet::new();
        loop {
            tick.tick().await;
            // (Task 5 adds the coordinator self-gate + stale-series removal here.)
            let mut seen = HashSet::new();
            for group in coordinator.share_group_ids() {
                let Some(meta) = coordinator.share_state_partition_metadata(&group) else { continue };
                for (topic_id, parts) in meta.initialized {
                    let Some(topic) = coordinator.topic_name_for(topic_id) else { continue };
                    for p in parts {
                        let spso = persister.read_state(&group, topic_id, p).await
                            .ok().flatten().map_or(-1, |s| s.start_offset.0);
                        // LOCAL HWM path (Task 4 adds the remote branch):
                        let Some(part) = partitions.get(&topic, PartitionIndex(p)) else { continue };
                        let hwm = part.high_watermark().await.0;
                        let log_start = part.log_start_offset().0;
                        let backlog = effective_backlog(hwm, spso, log_start);
                        let lbl = ShareGroupLabel { group_id: group.clone(), topic: topic.clone(), partition: p };
                        metrics.share_group_backlog.get_or_create(&lbl).set(backlog);
                        seen.insert(lbl);
                    }
                }
            }
            last = seen; // (Task 5 turns this into label-diff removal)
        }
    });
}
```

**(c)** In `broker.rs`, right after `share_partition_leaders.spawn_lock_sweeper();` (`:2473`):

```rust
        crate::share_partition::backlog_poller::spawn_backlog_poller(
            broker.group_coordinator.clone(),
            partitions.clone(),
            share_persister.clone(),
            metrics.clone(),
            backlog_poll_period, // Task 6 config; hardcode Duration::from_secs(15) until then
        );
```

- [ ] **Step 4: Run to verify it passes; commit**

Run: `cargo test -p crabka-broker --test sharegroup_backlog` → PASS (series == N, full backlog).

```bash
git add crates/broker/src/share_partition/backlog_poller.rs crates/broker/src/coordinator/unified/mod.rs crates/broker/src/broker.rs crates/broker/tests/sharegroup_backlog.rs
git commit -m "feat(broker): coordinator-hosted share-group backlog poll loop (local HWM)"
```

---

## Task 4: Remote-HWM read for non-co-led data partitions

**Files:**
- Modify: `crates/broker/src/share_partition/backlog_poller.rs` (peer-HWM helper + wire the remote branch)
- Test: `crates/broker/tests/sharegroup_backlog.rs` (multi-broker case)

- [ ] **Step 1: Write the failing multi-broker test**

Add a test that stands up a **multi-broker** cluster (mirror the pattern in `crates/broker/tests/durability.rs` / `leader_election.rs`) where the share group's data partition is led by a broker **other** than the `__consumer_offsets-0` coordinator; produce records; tick; assert the coordinator's `/metrics` **still** emits the correct backlog series for that partition (the co-location regression guard — this is exactly the case the local-only Task 3 skips via `continue`).

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker --test sharegroup_backlog remote`
Expected: FAIL — the non-co-led partition is skipped (`continue`), so no series.

- [ ] **Step 3: Implement the peer-HWM read (reuse the `send_to_leader` template)**

Replace the local-only branch's `else { continue }` with a remote read. Add a helper modeled on `SharePersister::send_to_leader` (`persister_client.rs:415-444`): resolve the data-partition leader from `coordinator.current_image().partition(&topic, p).leader` (the fleet-wide leader map, `manager.rs:103`), `InterBrokerClient::connect_as_connection(leader_addr, ...)` → `Connection`, then `conn.send(ListOffsetsRequest{..LATEST for (topic, p)..}).await` (`Connection::send<R: ProtocolRequest>`, `connection.rs:257`). Read the returned partition offset as the HWM.

```rust
        let (hwm, log_start) = match partitions.get(&topic, PartitionIndex(p)) {
            Some(part) => (part.high_watermark().await.0, part.log_start_offset().0),
            None => match fetch_remote_hwm_logstart(&coordinator, &inter_broker, &topic, p).await {
                Some(pair) => pair,
                None => continue, // leader unknown/unreachable this tick; the next tick retries
            },
        };
```

- **HWM semantics:** the local path uses `Partition::high_watermark()`; the remote read must match it. `ListOffsets(LATEST)` returns the partition high-watermark per Kafka semantics — **verify** the in-tree `handlers/list_offsets.rs` returns HWM (not raw LEO) for `LATEST`; at RF=1 they coincide, but align explicitly so remote and local backlogs are consistent. If `ListOffsets` yields LEO only, obtain `log_start` from `ListOffsets(EARLIEST)` and the HWM from the same response's LATEST entry, or note the RF=1 equality the tests run under.
- The `InterBrokerClient` handle: thread it into `spawn_backlog_poller` (`Broker::start` already constructs one for `persister_client`/replication — pass a clone, mirroring how `SharePersister` receives `Arc<InterBrokerClient>`, `persister_client.rs:66,84`).

- [ ] **Step 4: Run to verify it passes; commit**

Run: `cargo test -p crabka-broker --test sharegroup_backlog` → PASS (local + remote cases).

```bash
git add crates/broker/src/share_partition/backlog_poller.rs crates/broker/tests/sharegroup_backlog.rs
git commit -m "feat(broker): remote-HWM read (peer ListOffsets) for fleet-complete backlog"
```

---

## Task 5: Coordinator self-gate + stale-series hygiene

**Files:**
- Modify: `crates/broker/src/share_partition/backlog_poller.rs`
- Test: `crates/broker/tests/sharegroup_backlog.rs`

- [ ] **Step 1: Write the failing tests**

- **Self-gate:** on a multi-broker cluster, a broker that is **not** the `__consumer_offsets-0` leader emits **no** `share_group_backlog` series (assert its `/metrics` has none) — prevents double-count across a coordinator handoff.
- **Stale removal:** produce → tick (series present) → delete the share group's offsets / drain-and-tombstone the partition so it leaves `initialized` → tick → assert the series is **gone** (not stuck at its last value).

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker --test sharegroup_backlog gate` (and `stale`)
Expected: FAIL — every broker emits (no gate); departed series persist (no removal).

- [ ] **Step 3: Implement**

**(a) Self-gate** at the top of the loop body (after `tick.tick().await`): if this broker does not currently lead the `__consumer_offsets-0` partition (reuse the leadership check the coordinator/persister already uses — `is_leader`-style on the offsets partition, cf. `persister_client.rs:234` for the share-state analogue), clear all `share_group_backlog` series and `continue`.

**(b) Stale-series hygiene** via last-tick label diff: after emitting, remove labels in `last` but not in `seen`. Resolve the `prometheus-client 0.25` API: if `Family::remove(&self, &labels) -> bool` exists, use it; else if only `clear()` exists, snapshot-compute all series into a local `Vec` first (all `await`s), then in one synchronous section `clear()` + set-all (bounded empty-scrape window of microseconds). Pick the available API and note it in a comment.

```rust
            // (a) self-gate
            if !coordinator.leads_offsets_partition() { // reuse existing is_leader(__consumer_offsets-0)
                for lbl in last.drain() { metrics.share_group_backlog.remove(&lbl); }
                continue;
            }
            // ... emit into `seen` ...
            // (b) remove departed series
            for lbl in last.difference(&seen) { metrics.share_group_backlog.remove(lbl); }
            last = seen;
```

- [ ] **Step 4: Run to verify it passes; commit**

Run: `cargo test -p crabka-broker --test sharegroup_backlog` → PASS.

```bash
git add crates/broker/src/share_partition/backlog_poller.rs crates/broker/tests/sharegroup_backlog.rs
git commit -m "feat(broker): coordinator self-gate + stale-series hygiene for backlog gauge"
```

---

## Task 6: Poll-interval config (optional)

**Files:**
- Modify: `crates/broker/src/config.rs`, `crates/broker/src/broker.rs`

- [ ] **Step 1:** Add `backlog_poll_interval_secs` to the `ShareGroupConfig` (mirror `partition_disk_scan_interval_secs`), default `15`; short in `for_tests`. Thread it into the `spawn_backlog_poller` call, replacing the hardcoded `Duration::from_secs(15)`.
- [ ] **Step 2:** Run `cargo test -p crabka-broker --test sharegroup_backlog` (still green with the config-driven interval). Commit.

```bash
git add crates/broker/src/config.rs crates/broker/src/broker.rs
git commit -m "feat(broker): configurable share-group backlog poll interval"
```

---

## Task 7: KEDA ScaledObject example + operator docs

**Files:**
- Create: `docs/examples/keda-sharegroup-scaledobject.yaml`

- [ ] **Step 1:** Write the `ScaledObject` from the spec (stock `prometheus` scaler, `query: sum(crabka_broker_share_group_backlog{group_id="my-group"})`, `threshold`, `activationThreshold: 1`, `minReplicaCount: 0`) with comments explaining: fleet aggregation via `sum()`, why scale-to-zero is safe (complete emission, never `-1`), that consumer-group workloads use the stock `kafka` scaler instead, and the KEDA-version `metricName` caveat. No Crabka code.
- [ ] **Step 2:** Commit.

```bash
git add docs/examples/keda-sharegroup-scaledobject.yaml
git commit -m "docs: KEDA ScaledObject example for share-group backlog autoscaling"
```

---

## Task 8: Final gate

- [ ] **Step 1:** `cargo +nightly fmt --check` — no diff.
- [ ] **Step 2:** `cargo clippy -p crabka-broker --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-broker` — PASS, incl. `effective_backlog` unit tests, the encode test, and the single-broker + multi-broker + self-gate + stale-series integration tests.
- [ ] **Step 4:** Verify-only (no code): confirm `ListOffsets`/`OffsetFetch`/`FindCoordinator`/`DescribeGroups` are advertised (`api_catalog.rs`) so the stock KEDA `kafka` scaler covers consumer-group lag; note it in the example docs. Commit any formatting.

---

## Self-Review

**1. Spec coverage:** the gauge (Task 1); the `effective_backlog` `-1`-avoidance kernel (Task 2); the coordinator-single-emitter poll loop, local (Task 3) + remote-HWM (Task 4); the self-gate + stale-series hygiene (Task 5); config (Task 6); KEDA manifest + the consumer-group-lag-is-free note (Task 7); the wire-compat verify (Task 8). Deferred set (external-scaler gRPC, data-leader-driven model, broker autoscaling, SPSO cache) untouched — Scope boundary. ✅

**2. Placeholder scan:** the tractable core (gauge, kernel, poll-loop skeleton, spawn site, peer-RPC template) is concrete code against named seams (`metrics.rs:53-57/144/358/554`, `manager.rs:276-300`, `unified/mod.rs:394/915`, `broker.rs:2473`, `persister_client.rs:415-444`, `connection.rs:257`). Two implementation-time confirmations are explicitly flagged, not hidden: the `prometheus-client 0.25` `remove`/`clear` API (Task 5) and `ListOffsets(LATEST)` HWM-vs-LEO semantics (Task 4). Integration tests are harness assembly from named in-tree patterns (`share_consume.rs`, `durability.rs`).

**3. Type consistency:** `effective_backlog(hwm, spso, log_start) -> i64` (Task 2) is the sole backlog math, called identically in the local (Task 3) and remote (Task 4) branches; `ShareGroupLabel{group_id, topic, partition}` + `share_group_backlog: Family<ShareGroupLabel, Gauge>` (Task 1) are `get_or_create(&lbl).set(i64)`/`remove(&lbl)` throughout; the enumeration returns `initialized: Vec<(Uuid, Vec<i32>)>` (Task 3) consumed by both branches; the remote read reuses `Connection::send<R: ProtocolRequest>` (Task 4).

**4. Invariant check:** never `-1` (Task 2 kernel + Task 4 remote read); one series per partition (coordinator-single-emitter + Task 5 self-gate); scale-to-zero safe (complete emission + drained→0 test); no wire/KIP change (internal gauge; `ListOffsets`/`DescribeShareGroupOffsets` bytes untouched); consumer-group lag free (Task 8 verify). Each task green before commit.

**5. Prerequisites:** none unlanded. The one genuinely new cross-broker read (remote `ListOffsets`) reuses the landed `InterBrokerClient`/`Connection` surface (`network/client.rs`) that the replicator and `persister_client` already use — flagged with its fan-out cost and the documented data-leader-driven scale-out path (Risks).
