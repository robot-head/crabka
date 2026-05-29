# Tiered Storage 48q — Per-broker metadata-partition assignment — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make each broker consume only the `__remote_log_metadata` partitions that carry metadata for the user-topic-partitions it leads or follows, adjusting that set dynamically as leadership changes and gating remote reads behind per-metadata-partition readiness.

**Architecture:** `broker.rs` derives the needed metadata-partition set from `controller.current_image()` for this broker's `node_id`, publishes it on a `tokio::sync::watch`, and re-emits whenever the metadata image changes (via the existing `MetadataSource::watch_image`). A reconciler task in `manager.rs` diffs the desired set against the 48o `AssignmentHandle::assigned()` and calls `add`/`remove`, seeding each new partition's resume offset from 48p's snapshot committed offset. The RLMM read surface gains a `NotReady` error so a freshly-assigned-but-not-caught-up metadata partition is distinguishable from a genuine miss; `RemoteReader` propagates it and the `Fetch`/`ListOffsets` handlers treat it as retryable/conservative.

**Tech Stack:** Rust, tokio, crabka workspace crates (remote-storage-topic, remote-storage, broker)

---

## Locked upstream types (48o / 48p — reuse verbatim, do NOT redefine)

These land in `crates/remote-storage-topic/src/log.rs` from slice 48o and are
the dependency contract for this slice:

```rust
pub struct PartitionStart { pub partition: i32, pub start_offset: i64 }
pub trait AssignmentHandle: Send + Sync {
    fn add(&self, start: PartitionStart);
    fn remove(&self, partition: i32);
    fn assigned(&self) -> Vec<i32>;
}
```

- `manager.rs` (after 48o) holds `assignment: Arc<dyn AssignmentHandle>` and the
  pump tracks `applied: Arc<std::sync::Mutex<Vec<i64>>>` (already present today,
  see `pump_loop`), plus the `applied_tx: watch::Sender<u64>` change signal.
- 48p exposes, per metadata partition, the snapshot **committed** offset used to
  seed resume. This plan assumes the accessor
  `fn snapshot_committed_offset(&self, partition: i32) -> Option<i64>` on the
  manager (returns the highest committed offset captured by the latest snapshot
  for that metadata partition, or `None` when the snapshot has nothing). A newly
  added partition seeds `PartitionStart.start_offset = snapshot_committed + 1`,
  falling back to `0` when `None`.

Do not re-declare any of the above. If a name differs in the merged 48o/48p
branch, adapt call sites to the merged name — never fork a second copy.

---

## File structure

- `crates/remote-storage/src/error.rs` — add `RemoteStorageError::NotReady`.
- `crates/remote-storage/src/inmemory.rs` — reference impl never returns
  `NotReady` (always caught up); unit-test the three read outcomes.
- `crates/remote-storage-topic/src/partitioning.rs` — add
  `metadata_partitions_for` (the deduped set helper).
- `crates/remote-storage-topic/src/manager.rs` — per-metadata-partition
  readiness tracking, `remote_log_segment_metadata` returns `NotReady` when
  assigned-but-not-ready, and the assignment reconciler.
- `crates/broker/src/broker.rs` — derive desired set from the image, publish on
  a watch that re-emits on reconcile, run the reconciler task, seed bootstrap
  with the leadership-derived set.
- `crates/broker/src/remote_reader.rs` — propagate `NotReady` from
  `fetch_batch` / `earliest_offset` / `offset_for_timestamp`.
- `crates/broker/src/handlers/fetch.rs` — `try_remote_read` treats `NotReady`
  as retryable (leave `OFFSET_OUT_OF_RANGE`).
- `crates/broker/src/handlers/list_offsets.rs` — `NotReady` joins the existing
  remote-error branch (warn + conservative answer).

---

### Task 1: Add `NotReady` error variant and pin the three read outcomes

**Files:**
- `crates/remote-storage/src/error.rs`
- `crates/remote-storage/src/inmemory.rs`

The RLMM read method `remote_log_segment_metadata` must be able to say three
distinct things: `Err(NotReady)` (metadata partition assigned but not yet
caught up), `Ok(None)` (caught up, no covering segment — a genuine miss; also
the case when the partition isn't assigned at all), `Ok(Some(_))` (found). The
`InmemoryRemoteLogMetadataManager` is always "caught up" (it has no consumer
lag), so it never returns `NotReady` — but we pin that contract with a test so
the variant's semantics are unambiguous before the topic-backed manager uses it.

- [ ] Add a failing unit test to the `tests` module in
  `crates/remote-storage/src/inmemory.rs` asserting the in-memory manager never
  returns `NotReady` (it is always caught up), so its three outcomes are
  exactly `Ok(Some)`, `Ok(None)`, and an underlying-store error:

```rust
    #[test]
    fn inmemory_read_outcomes_are_some_none_never_not_ready() {
        let m = InmemoryRemoteLogMetadataManager::new();
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        m.update_remote_log_segment_metadata(finish(10)).unwrap();

        // Found.
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), 0, 42),
            Ok(Some(_))
        ));
        // Caught up, no covering segment → genuine miss.
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), 0, 10_000),
            Ok(None)
        ));
        // Unknown partition → genuine miss, NOT NotReady.
        let other = TopicIdPartition::new(Uuid::from_u128(999), "nope", 0);
        let got = m.remote_log_segment_metadata(&other, 0, 0);
        assert!(matches!(got, Ok(None)));
        assert!(
            !matches!(got, Err(RemoteStorageError::NotReady { .. })),
            "in-memory manager has no consumer lag; never NotReady"
        );
    }
```

- [ ] Run `cargo test -p crabka-remote-storage inmemory_read_outcomes_are_some_none_never_not_ready` — expect FAIL to compile: `RemoteStorageError::NotReady` does not exist yet.
- [ ] Add the variant to `crates/remote-storage/src/error.rs`, after the `Backend` variant:

```rust
    /// The metadata partition that would answer this query is assigned to
    /// this broker but its consumer has not yet caught up to the high-water
    /// mark observed when the partition was assigned. The answer is unknown,
    /// not "no segment" — callers should retry rather than treat it as a
    /// definitive miss. `Ok(None)` is reserved for "caught up, no covering
    /// segment" and for partitions this broker does not consume at all.
    #[error("remote log metadata partition {partition} not ready (assigned but not caught up)")]
    NotReady {
        /// The `__remote_log_metadata` partition that is still catching up.
        partition: i32,
    },
```

- [ ] Run `cargo test -p crabka-remote-storage inmemory_read_outcomes_are_some_none_never_not_ready` — expect PASS (the in-memory manager never constructs `NotReady`, so the test holds).
- [ ] Run `cargo fmt --all` and commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(remote-storage): NotReady error variant for assigned-but-uncaught metadata partitions"`.

---

### Task 2: `metadata_partitions_for` — the deduped needed-set helper

**Files:**
- `crates/remote-storage-topic/src/partitioning.rs`

A helper that maps a list of `(topic_id, partition)` the broker leads/follows to
the deduped set of `__remote_log_metadata` partitions, reusing the existing
`metadata_partition_for`. Returns a sorted `Vec<i32>` for deterministic diffing.

- [ ] Add a failing unit test to the `tests` module in
  `crates/remote-storage-topic/src/partitioning.rs`:

```rust
    #[test]
    fn metadata_partitions_for_dedupes_and_sorts() {
        // Two user-partitions that hash to the same metadata partition must
        // collapse to one entry; the result is sorted ascending.
        let a = tp("orders", 0);
        let b = tp("orders", 1);
        let pa = metadata_partition_for(&a, 50);
        let pb = metadata_partition_for(&b, 50);
        let got = metadata_partitions_for([a.clone(), b.clone(), a.clone()].iter(), 50);
        let mut expected: Vec<i32> = vec![pa, pb];
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(got, expected);
        assert!(got.windows(2).all(|w| w[0] < w[1]), "sorted, deduped");
    }

    #[test]
    fn metadata_partitions_for_empty_is_empty() {
        let none: [TopicIdPartition; 0] = [];
        assert!(metadata_partitions_for(none.iter(), 50).is_empty());
    }
```

- [ ] Run `cargo test -p crabka-remote-storage-topic metadata_partitions_for` — expect FAIL to compile: function does not exist.
- [ ] Add the helper to `crates/remote-storage-topic/src/partitioning.rs`, below `metadata_partition_for`:

```rust
/// Deduped, sorted set of `__remote_log_metadata` partitions that carry
/// metadata for the given user-topic-partitions, given the metadata topic's
/// `partition_count`. This is the set a broker must consume to serve remote
/// reads for the partitions it leads or follows.
///
/// # Panics
///
/// Panics when `partition_count <= 0` (via [`metadata_partition_for`]).
#[must_use]
pub fn metadata_partitions_for<'a, I>(tps: I, partition_count: i32) -> Vec<i32>
where
    I: IntoIterator<Item = &'a TopicIdPartition>,
{
    let mut set: Vec<i32> = tps
        .into_iter()
        .map(|tp| metadata_partition_for(tp, partition_count))
        .collect();
    set.sort_unstable();
    set.dedup();
    set
}
```

- [ ] Export it: in `crates/remote-storage-topic/src/lib.rs`, change the
  `pub use partitioning::metadata_partition_for;` line to
  `pub use partitioning::{metadata_partition_for, metadata_partitions_for};`.
- [ ] Run `cargo test -p crabka-remote-storage-topic metadata_partitions_for` — expect PASS.
- [ ] Run `cargo fmt --all` and commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(remote-storage-topic): metadata_partitions_for needed-set helper"`.

---

### Task 3: Per-partition readiness + assignment reconciler in the manager

**Files:**
- `crates/remote-storage-topic/src/manager.rs`

The manager tracks, per metadata partition, the **target HWM observed at
assignment time**. A partition is *ready* once `applied[partition] >=
target_hwm - 1` (mirroring `wait_for_targets`' `applied >= hwm - 1` catch-up
test, since `applied` holds the highest applied offset and HWM is one-past). For
a query, the manager hashes `tp` to its metadata partition; if that partition is
assigned-but-not-ready, `remote_log_segment_metadata` returns
`Err(NotReady { partition })`; otherwise it delegates to `inner`.

`reconcile_assignment(desired: &[i32])` diffs `desired` against
`assignment.assigned()`: each added partition calls
`assignment.add(PartitionStart { partition, start_offset })` where
`start_offset = self.snapshot_committed_offset(partition).map_or(0, |c| c + 1)`,
and records its assignment-time target HWM (read from `self.log.high_water_marks()`);
each removed partition calls `assignment.remove(partition)` and clears its
readiness entry.

> **48o/48p integration note:** this task assumes the merged manager already has
> `assignment: Arc<dyn AssignmentHandle>` (48o) and
> `snapshot_committed_offset` (48p). Add only the readiness map, the `NotReady`
> branch in the read method, and `reconcile_assignment`. Do not re-add fields
> 48o/48p already introduced.

- [ ] Add a failing test to the `tests` module of `manager.rs`. It uses the
  `InProcessMetadataEventLog` fixture and drives the reconciler directly. The
  helper `tp()` already exists in the module (`topic_id = Uuid::from_u128(1)`,
  `"orders"`, partition 0); compute its metadata partition the same way the
  manager does:

```rust
    #[tokio::test(flavor = "multi_thread")]
    async fn add_then_remove_drives_assignment_and_readiness() {
        use crate::partitioning::metadata_partition_for;

        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        // Pre-seed a finished segment for `tp()` so a ready read returns Some.
        {
            let writer = start_manager(log.clone()).await;
            let w2 = writer.clone();
            on_blocking(move || {
                w2.add_remote_log_segment_metadata(started(10, 0, 99))
                    .unwrap();
            })
            .await;
            let w2 = writer.clone();
            on_blocking(move || w2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;
            writer.shutdown();
        }

        let mp = metadata_partition_for(&tp(), log.partition_count());
        let m = start_manager(log).await;

        // Before assignment: the partition is not consumed → genuine miss.
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), 0, 42),
            Ok(None)
        ));

        // Assign it. add() must enqueue a PartitionStart for `mp`, and the
        // pump catches up; once applied >= HWM-1 the read returns Some.
        m.reconcile_assignment(&[mp]);
        assert_eq!(m.assigned_metadata_partitions(), vec![mp]);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match m.remote_log_segment_metadata(&tp(), 0, 42) {
                Ok(Some(md)) => {
                    assert_eq!(md.remote_log_segment_id().id, Uuid::from_u128(10));
                    break;
                }
                Err(RemoteStorageError::NotReady { partition }) => {
                    assert_eq!(partition, mp, "NotReady names the catching-up partition");
                    assert!(
                        std::time::Instant::now() < deadline,
                        "metadata partition never became ready"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                other => panic!("unexpected read outcome: {other:?}"),
            }
        }

        // Remove it: assignment drops, and subsequent reads are a genuine
        // miss (Ok(None)) — the partition is no longer consumed.
        m.reconcile_assignment(&[]);
        assert!(m.assigned_metadata_partitions().is_empty());
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), 0, 42),
            Ok(None)
        ));
        m.shutdown();
    }
```

- [ ] Run `cargo test -p crabka-remote-storage-topic add_then_remove_drives_assignment_and_readiness` — expect FAIL to compile: `reconcile_assignment` / `assigned_metadata_partitions` / readiness branch do not exist.
- [ ] Add the readiness map to the struct in `manager.rs`. Add a field
  `ready_targets: Arc<std::sync::Mutex<std::collections::HashMap<i32, i64>>>`
  (metadata partition → target HWM observed at assignment time). Initialize it
  to an empty map in `start` and clone it into the struct literal. (Keep the
  48o `assignment` field and 48p accessors untouched.)

```rust
    // add near the other Arc fields:
    ready_targets: Arc<std::sync::Mutex<std::collections::HashMap<i32, i64>>>,
```

```rust
    // in start(), before constructing the Arc<Self>:
    let ready_targets = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    // ... pass `ready_targets,` in the struct literal.
```

- [ ] Add the readiness predicate and reconciler as inherent methods on
  `TopicBasedRemoteLogMetadataManager`:

```rust
    /// `true` when metadata partition `mp` is either unassigned (not our
    /// concern) or assigned and caught up to its assignment-time HWM. A
    /// partition is caught up once `applied[mp] >= target - 1` (HWM is one
    /// past the highest offset; `applied` holds the highest applied offset).
    fn metadata_partition_ready(&self, mp: i32) -> bool {
        let target = {
            let guard = self.ready_targets.lock().expect("ready_targets poisoned");
            match guard.get(&mp) {
                Some(&t) => t,
                None => return true, // not assigned → not gated here
            }
        };
        if target == 0 {
            return true; // empty partition: nothing to catch up to
        }
        let Ok(idx) = usize::try_from(mp) else {
            return true;
        };
        let applied = self.applied.lock().expect("applied mutex poisoned");
        idx < applied.len() && applied[idx] >= target - 1
    }

    /// The metadata partitions this manager is currently assigned (tracked
    /// for readiness). Sorted ascending.
    #[must_use]
    pub fn assigned_metadata_partitions(&self) -> Vec<i32> {
        let mut v: Vec<i32> = self
            .ready_targets
            .lock()
            .expect("ready_targets poisoned")
            .keys()
            .copied()
            .collect();
        v.sort_unstable();
        v
    }

    /// Diff `desired` against the current assignment and drive the 48o
    /// [`AssignmentHandle`]: add newly-needed partitions (seeded from the
    /// 48p snapshot committed offset, falling back to 0) and remove ones no
    /// longer needed. Records each added partition's assignment-time HWM so
    /// reads gate on `NotReady` until the pump catches up.
    pub fn reconcile_assignment(&self, desired: &[i32]) {
        use std::collections::HashSet;
        let want: HashSet<i32> = desired.iter().copied().collect();
        let have: HashSet<i32> = self
            .ready_targets
            .lock()
            .expect("ready_targets poisoned")
            .keys()
            .copied()
            .collect();

        // Snapshot HWMs once for any additions (block_on is safe: callers
        // invoke the reconciler off the manager's runtime).
        let needs_add = want.difference(&have).copied().collect::<Vec<_>>();
        let hwms = if needs_add.is_empty() {
            Vec::new()
        } else {
            self.runtime
                .block_on(self.log.high_water_marks())
                .unwrap_or_default()
        };

        for mp in needs_add {
            let start_offset = self.snapshot_committed_offset(mp).map_or(0, |c| c + 1);
            self.assignment.add(PartitionStart {
                partition: mp,
                start_offset,
            });
            let target = usize::try_from(mp)
                .ok()
                .and_then(|i| hwms.get(i).copied())
                .unwrap_or(0);
            self.ready_targets
                .lock()
                .expect("ready_targets poisoned")
                .insert(mp, target);
        }
        for mp in have.difference(&want).copied() {
            self.assignment.remove(mp);
            self.ready_targets
                .lock()
                .expect("ready_targets poisoned")
                .remove(&mp);
        }
    }
```

- [ ] Import the locked 48o types at the top of `manager.rs` (they live in
  `crate::log`): add `PartitionStart` (and `AssignmentHandle` if not already
  imported by 48o) to the existing
  `use crate::log::{MetadataEventLog, MetadataEventStream};` line, e.g.
  `use crate::log::{AssignmentHandle, MetadataEventLog, MetadataEventStream, PartitionStart};`.
- [ ] Gate the read method on readiness. Replace the body of
  `remote_log_segment_metadata` in the `impl RemoteLogMetadataManager`:

```rust
    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let mp = metadata_partition_for(topic_id_partition, self.log.partition_count());
        if !self.metadata_partition_ready(mp) {
            return Err(RemoteStorageError::NotReady { partition: mp });
        }
        self.inner
            .remote_log_segment_metadata(topic_id_partition, leader_epoch, offset)
    }
```

- [ ] Run `cargo test -p crabka-remote-storage-topic add_then_remove_drives_assignment_and_readiness` — expect PASS.
- [ ] Run `cargo test -p crabka-remote-storage-topic` — expect the existing
  manager tests still PASS. (Existing tests never call `reconcile_assignment`,
  so `ready_targets` stays empty and `metadata_partition_ready` returns `true`
  for every partition — no behavior change for un-gated reads.)
- [ ] Run `cargo fmt --all` and commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(remote-storage-topic): per-partition readiness gate + assignment reconciler"`.

---

### Task 4: Derive the desired set from the image and run the reconciler in the broker

**Files:**
- `crates/broker/src/broker.rs`

Compute the needed metadata-partition set from `controller.current_image()` for
this broker's `node_id`: for every partition where this node is the leader or a
replica (follower), build its `TopicIdPartition` and feed the collection through
`metadata_partitions_for`. Publish the set on a `tokio::sync::watch`; a task
subscribes to `MetadataSource::watch_image()` and re-publishes the recomputed
set whenever the image changes. A second task subscribes to the set-watch and
calls `manager.reconcile_assignment(&set)`. The bootstrap path seeds the initial
assignment with the leadership-derived set (not all partitions).

> The metadata consumer must stay on its own connection (broker is serial
> per-connection); the reconciler only calls `add`/`remove` on the
> `AssignmentHandle`, which drives that dedicated consumer — no new socket work
> here.

- [ ] Add a failing unit test in the `#[cfg(test)] mod tests` of `broker.rs`
  (or create one if absent) for the pure derivation helper. The helper takes a
  `&MetadataImage`, a `node_id`, and the metadata `partition_count`, and returns
  the sorted needed set:

```rust
    #[test]
    fn needed_metadata_partitions_covers_led_and_followed() {
        use crabka_metadata::{MetadataImage, MetadataRecord};
        use crabka_metadata::records::{PartitionRecord, TopicRecord};
        use crabka_remote_storage::TopicIdPartition;
        use crabka_remote_storage_topic::metadata_partition_for;
        use uuid::Uuid;

        let topic_id = Uuid::from_u128(0xABCD);
        let mut image = MetadataImage::new(Uuid::from_u128(1));
        image.apply(&MetadataRecord::topic(TopicRecord {
            name: "orders".into(),
            topic_id,
        }));
        // node 7 leads p0, follows p1 (replica), is absent from p2.
        for (partition, leader, replicas) in [
            (0_i32, 7_u64, vec![7_u64, 8]),
            (1, 8, vec![8, 7]),
            (2, 8, vec![8, 9]),
        ] {
            image.apply(&MetadataRecord::partition(PartitionRecord {
                name: "orders".into(),
                partition,
                leader,
                replicas,
                leader_epoch: 0,
                ..Default::default()
            }));
        }

        let got = needed_metadata_partitions(&image, 7, 50);

        let mut expected = vec![
            metadata_partition_for(&TopicIdPartition::new(topic_id, "orders", 0), 50),
            metadata_partition_for(&TopicIdPartition::new(topic_id, "orders", 1), 50),
        ];
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(got, expected, "p2 (node 7 not a replica) must be excluded");
    }
```

> Adapt `MetadataRecord::topic` / `MetadataRecord::partition` /
> `PartitionRecord { .. }` / `image.apply(..)` to the real constructors in
> `crates/metadata/src/records.rs` and `image.rs` (the test compiler will tell
> you the exact shapes; `PartitionRecord` has `name`, `partition`, `leader`,
> `replicas`, `leader_epoch`, `adding_replicas`, `removing_replicas`). The
> assertion logic is what matters.

- [ ] Run `cargo test -p crabka-broker needed_metadata_partitions_covers_led_and_followed` — expect FAIL to compile: `needed_metadata_partitions` does not exist.
- [ ] Add the derivation helper as a free function in `broker.rs`:

```rust
/// The sorted, deduped set of `__remote_log_metadata` partitions this broker
/// (`node_id`) must consume: one entry per metadata partition covering any
/// user-topic-partition this node leads or follows, given the metadata topic's
/// `partition_count`.
fn needed_metadata_partitions(
    image: &crabka_metadata::MetadataImage,
    node_id: crabka_metadata::NodeId,
    partition_count: i32,
) -> Vec<i32> {
    let mut tps: Vec<crabka_remote_storage::TopicIdPartition> = Vec::new();
    for topic in image.topics() {
        for p in image.partitions_of(&topic.name) {
            if p.leader == node_id || p.replicas.contains(&node_id) {
                tps.push(crabka_remote_storage::TopicIdPartition::new(
                    topic.topic_id,
                    topic.name.clone(),
                    p.partition,
                ));
            }
        }
    }
    crabka_remote_storage_topic::metadata_partitions_for(tps.iter(), partition_count)
}
```

- [ ] Run `cargo test -p crabka-broker needed_metadata_partitions_covers_led_and_followed` — expect PASS.
- [ ] Wire the watch + reconciler tasks into `bootstrap_topic_rlmm`. After
  `swap.swap(manager.clone())` (capture the concrete `Arc<TopicBasedRemoteLogMetadataManager>`
  before swapping so we can call `reconcile_assignment`), spawn the two tasks.
  Pass the broker's `node_id`, the metadata `partition_count` (`cfg.cfg.num_partitions`),
  a `watch_image()` receiver, and the `shutdown` token into the bootstrap. The
  bootstrap signature gains the image-watch receiver and `node_id`; thread these
  from the call site at `broker.rs:2101` (the broker has `config.node_id` and
  `controller.watch_image()`):

```rust
    // inside bootstrap_topic_rlmm, after the manager starts:
    swap.swap(manager.clone());
    metrics.tiered_storage_rlmm_topic_backed.set(1);
    tracing::info!("topic-backed RemoteLogMetadataManager activated");

    // Publish the leadership-derived needed-set on a watch; re-emit whenever
    // the metadata image changes. The initial value is the current image's
    // set, so the bootstrap assignment is leadership-derived (not all
    // partitions).
    let partition_count = cfg.cfg.num_partitions;
    let node_id = crabka_metadata::NodeId::from(u64::try_from(cfg.broker_id).unwrap_or(0));
    let initial = needed_metadata_partitions(&image_rx.borrow(), node_id, partition_count);
    let (set_tx, set_rx) = tokio::sync::watch::channel(initial);

    // Image-watcher: recompute on every image change.
    {
        let mut image_rx = image_rx;
        let set_tx = set_tx.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    changed = image_rx.changed() => {
                        if changed.is_err() {
                            return; // image sender dropped
                        }
                        let set = needed_metadata_partitions(
                            &image_rx.borrow_and_update(),
                            node_id,
                            partition_count,
                        );
                        // send_if_modified avoids a reconcile when the set is
                        // unchanged across an image bump that didn't touch us.
                        set_tx.send_if_modified(|cur| {
                            if *cur == set {
                                false
                            } else {
                                *cur = set;
                                true
                            }
                        });
                    }
                }
            }
        });
    }

    // Reconciler: apply the latest set to the manager's AssignmentHandle.
    {
        let manager = manager.clone();
        let mut set_rx = set_rx;
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            // Apply the initial set immediately.
            manager.reconcile_assignment(&set_rx.borrow_and_update());
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    changed = set_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        manager.reconcile_assignment(&set_rx.borrow_and_update());
                    }
                }
            }
        });
    }
```

- [ ] Update the `bootstrap_topic_rlmm` signature and its single call site at
  `broker.rs:~2101` to pass the image-watch receiver and shutdown token:

```rust
async fn bootstrap_topic_rlmm(
    swap: Arc<crabka_remote_storage_topic::SwappableRlmm>,
    cfg: KafkaSwapKickoff,
    runtime: tokio::runtime::Handle,
    metrics: crate::metrics::BrokerMetrics,
    image_rx: tokio::sync::watch::Receiver<Arc<crabka_metadata::MetadataImage>>,
    shutdown: tokio_util::sync::CancellationToken,
) {
```

  At the call site, add `broker.controller.watch_image()` and
  `shutdown_token.clone()` as the new trailing args (the existing
  `bootstrap_topic_rlmm(swap, kafka_cfg, runtime, metrics_for_bootstrap)` call
  becomes `bootstrap_topic_rlmm(swap, kafka_cfg, runtime, metrics_for_bootstrap, broker.controller.watch_image(), shutdown_token.clone())`).
  `KafkaSwapKickoff` already carries `broker_id`; `cfg.cfg.num_partitions` is the
  metadata partition count.
- [ ] Run `cargo build -p crabka-broker` — expect it to compile.
- [ ] Run `cargo test -p crabka-broker needed_metadata_partitions_covers_led_and_followed` — expect PASS (and existing broker tests unaffected).
- [ ] Run `cargo fmt --all` and commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(broker): derive metadata-partition set from image and run assignment reconciler"`.

---

### Task 5: Wire `NotReady` as retryable through the read handlers

**Files:**
- `crates/broker/src/remote_reader.rs`
- `crates/broker/src/handlers/fetch.rs`
- `crates/broker/src/handlers/list_offsets.rs`

`RemoteReader::fetch_batch` / `earliest_offset` / `offset_for_timestamp` already
propagate `RemoteStorageError` via `?`, so `NotReady` flows out unchanged — the
work here is the handler-side treatment and tests pinning it.

In `fetch.rs::try_remote_read`, the `Err(e)` arm already logs and returns `None`
(leaving `OFFSET_OUT_OF_RANGE`, which is retryable for the consumer). `NotReady`
must take that same path but log at `debug` (it's expected churn during
catch-up, not an error). In `list_offsets.rs`, the existing `Err(e)` branches
already warn + answer conservatively (`earliest` stays local; timestamp → `-1`);
`NotReady` is one of those errors and needs no special-casing beyond confirming
it lands there — add a regression test.

- [ ] Add a failing unit test to the `tests` module of `remote_reader.rs` that
  pins `NotReady` propagation. Use a tiny stub RLMM that returns `NotReady` from
  `remote_log_segment_metadata` and empty/`None` elsewhere:

```rust
    struct NotReadyRlmm;
    impl RemoteLogMetadataManager for NotReadyRlmm {
        fn add_remote_log_segment_metadata(
            &self,
            _m: RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
        fn update_remote_log_segment_metadata(
            &self,
            _u: crabka_remote_storage::RemoteLogSegmentMetadataUpdate,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
        fn remote_log_segment_metadata(
            &self,
            _tp: &TopicIdPartition,
            _epoch: i32,
            _offset: i64,
        ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::NotReady { partition: 3 })
        }
        fn highest_offset_for_epoch(
            &self,
            _tp: &TopicIdPartition,
            _epoch: i32,
        ) -> Result<Option<i64>, RemoteStorageError> {
            Ok(None)
        }
        fn list_remote_log_segments(
            &self,
            _tp: &TopicIdPartition,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::NotReady { partition: 3 })
        }
        fn list_remote_log_segments_by_epoch(
            &self,
            _tp: &TopicIdPartition,
            _epoch: i32,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Ok(Vec::new())
        }
        fn put_remote_partition_delete_metadata(
            &self,
            _m: crabka_remote_storage::RemotePartitionDeleteMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn fetch_batch_propagates_not_ready() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(NotReadyRlmm);
        let reader = RemoteReader::new(rsm, rlmm);
        let err = reader.fetch_batch(&tp(), 0, 0, 4096).await.unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }

    #[tokio::test]
    async fn earliest_offset_propagates_not_ready() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(NotReadyRlmm);
        let reader = RemoteReader::new(rsm, rlmm);
        let err = reader.earliest_offset(&tp()).unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { .. }));
    }
```

- [ ] Run `cargo test -p crabka-broker fetch_batch_propagates_not_ready earliest_offset_propagates_not_ready` — expect PASS immediately (the `?` operator already propagates). If they fail, the propagation regressed — fix `remote_reader.rs` to not swallow `NotReady`. (These guard the contract; no production change is expected here.)
- [ ] In `fetch.rs::try_remote_read`, special-case `NotReady` in the match on
  `fetch_batch`'s result so it logs at debug, not warn (still returns `None`,
  leaving `OFFSET_OUT_OF_RANGE`). Replace the `Err(e)` arm:

```rust
        Ok(None) => None,
        Err(RemoteStorageError::NotReady { partition }) => {
            tracing::debug!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                metadata_partition = partition,
                "remote-reader: metadata partition not yet caught up; \
                 leaving OFFSET_OUT_OF_RANGE for client retry"
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                error = %e,
                "remote-reader: fetch_batch failed; leaving OFFSET_OUT_OF_RANGE"
            );
            None
        }
```

  Add `use crabka_remote_storage::RemoteStorageError;` to `fetch.rs` if it is
  not already imported.
- [ ] Confirm `list_offsets.rs` needs no change: the existing `Err(e)` arms in
  both the `EARLIEST` (`earliest_offset`) and timestamp (`offset_for_timestamp`)
  matches already warn + answer conservatively, and `NotReady` is an `Err(_)`,
  so it falls into them. Add a clarifying comment above each `Err(e)` arm:
  `// Includes RemoteStorageError::NotReady (metadata partition catching up):` to
  document the intent.
- [ ] Run `cargo test -p crabka-broker` — expect PASS.
- [ ] Run `cargo fmt --all` and commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(broker): treat RLMM NotReady as retryable in fetch/list_offsets"`.

---

### Task 6: Multi-broker loopback test — split partitions, split metadata consumption

**Files:**
- `crates/remote-storage-topic/src/manager.rs` (test module)

Two managers share one `InProcessMetadataEventLog` (the documented multi-broker
fixture). Broker A is assigned the metadata partitions for the user-partitions
it leads; Broker B the rest. Assert (1) each manager's
`assigned_metadata_partitions()` equals its derived share and the shares are
disjoint and jointly cover the writes, and (2) each serves remote reads for its
own user-partitions (returns `Ok(Some)` once caught up) and returns `Ok(None)`
(genuine miss) for a partition it does not consume.

> This exercises the slice end-to-end at the manager layer without standing up
> real brokers, mirroring `two_managers_sharing_a_log_converge`. Choose two
> user-partitions whose `metadata_partition_for` buckets differ so the split is
> observable; assert the buckets differ in the test so a hashing change that
> collapses them fails loudly rather than silently weakening the test.

- [ ] Add the failing multi-broker test to the `tests` module of `manager.rs`:

```rust
    #[tokio::test(flavor = "multi_thread")]
    async fn two_brokers_split_metadata_partitions() {
        use crate::partitioning::metadata_partition_for;

        // Use a wide metadata topic so two user-partitions land in distinct
        // buckets.
        let n = 16;
        let topic_id = Uuid::from_u128(0xFEED);
        let tp_a = TopicIdPartition::new(topic_id, "orders", 0);
        let tp_b = TopicIdPartition::new(topic_id, "orders", 1);
        let mp_a = metadata_partition_for(&tp_a, n);
        let mp_b = metadata_partition_for(&tp_b, n);
        assert_ne!(mp_a, mp_b, "test needs the two partitions in distinct buckets");

        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(n);

        // Seed one finished segment for each user-partition via a transient
        // writer (consumes all partitions, no assignment gating).
        for (tp, id) in [(tp_a.clone(), 100u128), (tp_b.clone(), 200)] {
            let w = start_manager(log.clone()).await;
            let started = RemoteLogSegmentMetadata::new(
                RemoteLogSegmentId::new(tp.clone(), Uuid::from_u128(id)),
                0,
                99,
                100,
                1,
                100,
                2048,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(0, 0)]),
            )
            .unwrap();
            let w2 = w.clone();
            on_blocking(move || w2.add_remote_log_segment_metadata(started).unwrap()).await;
            let upd = RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: RemoteLogSegmentId::new(tp, Uuid::from_u128(id)),
                event_timestamp_ms: 200,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            };
            let w2 = w.clone();
            on_blocking(move || w2.update_remote_log_segment_metadata(upd).unwrap()).await;
            w.shutdown();
        }

        // Broker A consumes mp_a only; Broker B consumes mp_b only.
        let a = start_manager(log.clone()).await;
        let b = start_manager(log).await;
        a.reconcile_assignment(&[mp_a]);
        b.reconcile_assignment(&[mp_b]);

        assert_eq!(a.assigned_metadata_partitions(), vec![mp_a]);
        assert_eq!(b.assigned_metadata_partitions(), vec![mp_b]);
        // Disjoint shares.
        assert!(
            a.assigned_metadata_partitions()
                .iter()
                .all(|p| !b.assigned_metadata_partitions().contains(p)),
            "shares must be disjoint"
        );

        // Poll until each is caught up and serves its own partition.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let a_own = a.remote_log_segment_metadata(&tp_a, 0, 42);
            let b_own = b.remote_log_segment_metadata(&tp_b, 0, 42);
            if matches!(a_own, Ok(Some(_))) && matches!(b_own, Ok(Some(_))) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "managers did not catch up: a={a_own:?} b={b_own:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // Cross reads (partition the broker does NOT consume) are a genuine
        // miss, not NotReady.
        assert!(
            matches!(a.remote_log_segment_metadata(&tp_b, 0, 42), Ok(None)),
            "A does not consume mp_b → genuine miss"
        );
        assert!(
            matches!(b.remote_log_segment_metadata(&tp_a, 0, 42), Ok(None)),
            "B does not consume mp_a → genuine miss"
        );

        a.shutdown();
        b.shutdown();
    }
```

- [ ] Run `cargo test -p crabka-remote-storage-topic two_brokers_split_metadata_partitions` — expect PASS (all production code already landed in Tasks 1-3; this is a behavior-locking integration test).
- [ ] If it fails because both managers see each other's writes regardless of
  assignment (the `InProcessMetadataEventLog` `subscribe()` replays all
  partitions), that confirms the readiness gate is the correctness boundary, not
  the consumed-byte set — the test asserts on `assigned_metadata_partitions()`
  and on read outcomes, both of which are governed by `ready_targets`, so it
  holds regardless of the fixture's broadcast breadth. (The real Kafka-backed
  `AssignmentHandle` from 48o restricts the consumed bytes; the fixture models
  the assignment/readiness contract.)
- [ ] Run `cargo fmt --all` and commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "test(remote-storage-topic): two-broker metadata-partition split + remote reads"`.

---

### Task 7: Final verification

**Files:** (none — verification only)

- [ ] Run `cargo fmt --all --check` — expect clean (no diff).
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings` — expect no warnings.
- [ ] Run `cargo test --workspace` — expect all tests PASS (no regressions).
- [ ] If any step fails, fix and re-run all three before declaring done. Do not
  commit a separate "fix clippy" commit unless the fix is substantive — fold
  formatting/lint fixes into the task that introduced them where practical.

---

## Self-review notes (resolved inline)

- **Spec coverage:** Design items 1-6 map to Tasks 1-6; (1) NotReady variant +
  three outcomes → Task 1; (2) needed-set helper → Task 2; (3) readiness +
  reconciler with 48p resume seeding → Task 3; (4) broker derivation + watch +
  bootstrap → Task 4; (5) handler wiring → Task 5; (6) multi-broker loopback →
  Task 6. Acceptance gates → Task 7.
- **Type consistency vs locked 48o/48p:** `PartitionStart { partition,
  start_offset }` and `AssignmentHandle::{add, remove, assigned}` are used
  verbatim; `add` takes a `PartitionStart` by value (matches the locked
  signature). `applied: Arc<Mutex<Vec<i64>>>` and `applied_tx` are the existing
  `manager.rs` fields. 48p's resume seed is consumed via
  `snapshot_committed_offset(mp).map_or(0, |c| c + 1)` exactly as the design
  states ("snapshot committed+1, fallback 0").
- **Readiness threshold:** `applied[mp] >= target - 1` matches the existing
  `wait_for_targets` invariant (HWM is one-past-highest; `applied` holds the
  highest applied offset), and `target == 0` (empty partition) short-circuits to
  ready — consistent with `wait_for_targets`' `targets[i] == 0` skip.
- **Ok(None) vs NotReady:** unassigned partition → `ready_targets` has no entry →
  `metadata_partition_ready` returns `true` → delegates to `inner` → `Ok(None)`
  (genuine miss), exactly the design's "not assigned at all is `Ok(None)`".
- **Placeholder scan:** every code block is complete Rust; the only
  adapt-to-real-API notes are the `MetadataRecord`/`PartitionRecord`
  constructors in the Task 4 test (whose field set — `name`, `partition`,
  `leader`, `replicas`, `leader_epoch`, `adding_replicas`, `removing_replicas` —
  is confirmed from `crates/metadata/src/records.rs`) and the merged 48o/48p
  field/accessor names, both explicitly flagged.
- **Serial-per-connection:** the reconciler only calls `AssignmentHandle`
  methods (no socket I/O on the broker's request path); the metadata consumer
  keeps its own connection from 48o. Noted in Task 4.
- **Greenfield:** no compat shims, no `#[serde(default)]`, no V2 variants — the
  `NotReady` variant is added outright and `remote_log_segment_metadata`'s body
  is replaced, not branched behind a flag.
