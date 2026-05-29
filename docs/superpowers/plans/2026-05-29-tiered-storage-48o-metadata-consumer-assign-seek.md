# Tiered Storage 48o — Metadata-log consumer assign + seek — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the `__remote_log_metadata` consumer the ability to consume a runtime-mutable subset of partitions, each starting at a chosen offset, by replacing the all-partitions `MetadataEventLog::subscribe` with an assignment-driven API and reworking `KafkaMetadataEventLog` off the group `Consumer` onto `crabka-client-core` manual per-partition `Fetch` loops.

**Architecture:** `MetadataEventLog::subscribe(Vec<PartitionStart>)` returns the event stream plus an `Arc<dyn AssignmentHandle>` whose `add`/`remove`/`assigned` mutate the live assignment. `InProcessMetadataEventLog` filters its in-memory backlog and live broadcast by the shared assignment set. `KafkaMetadataEventLog` drives one cancellable manual-`Fetch` task per assigned partition over a thin `client-core` helper, emitting `MetadataEventRecord`s into a shared mpsc; the consumer uses a dedicated `client-core` connection (no consumer group, no broker offset commits) because the read position is owned by the RLMM. `manager.rs::start` calls the new `subscribe` with the full assignment from offset 0 (behavior-preserving) and holds the handle for 48p/48q.

**Tech Stack:** Rust, tokio, crabka workspace crates (remote-storage-topic, client-core)

---

## File structure

- `crates/remote-storage-topic/src/log.rs` — `PartitionStart`, `AssignmentHandle` trait, new `MetadataEventLog::subscribe` signature, `InProcessMetadataEventLog` implementation + unit tests.
- `crates/client-core/src/fetch.rs` (new) — minimal single-partition `fetch_partition` helper + unit test; re-exported from `crates/client-core/src/lib.rs`.
- `crates/remote-storage-topic/src/kafka_log.rs` — client-core manual-fetch consumer, `KafkaAssignmentHandle`, partition-task spawn/cancel; drop the group `Consumer` + `group_id`.
- `crates/remote-storage-topic/src/manager.rs` — call the new `subscribe` with the full assignment from offset 0; store the `Arc<dyn AssignmentHandle>` on the struct.
- `crates/broker/tests/tiered_storage_metadata_assign.rs` (new) — loopback integration test for `KafkaMetadataEventLog` subset + non-zero start-offset.

Tasks 1, 2 (log.rs) and Task 3 (client-core fetch.rs + lib.rs) touch disjoint file sets and **can run in the same batch**. Task 4 (kafka_log.rs) depends on Task 1 (the trait) and Task 3 (the helper). Task 5 (manager.rs) depends on Task 1. Task 6 (integration test) depends on Task 4. Task 7 is final verification.

Suggested batching:

- **Batch A (parallel):** Task 1+2 (log.rs), Task 3 (client-core).
- **Batch B (parallel):** Task 4 (kafka_log.rs), Task 5 (manager.rs) — disjoint files, both depend only on Batch A.
- **Batch C (sequential):** Task 6 (integration test, needs Task 4), then Task 7 (verification).

---

### Task 1: `PartitionStart`, `AssignmentHandle`, and the new `subscribe` signature in `log.rs`

Introduce the new public types and change the trait. The `InProcessMetadataEventLog` body is updated in Task 2; here we only get the trait + types compiling with a temporary `todo!()` so the type names are locked first (48p/48q depend on these exact names). This task and Task 2 share `log.rs`, so a single implementer owns both — do Task 1 then Task 2 in sequence within the same working copy.

**Files:**
- `crates/remote-storage-topic/src/log.rs`
- `crates/remote-storage-topic/src/lib.rs` (re-exports)

Steps:

- [ ] Add the `PartitionStart` struct and `AssignmentHandle` trait above the `MetadataEventLog` trait in `log.rs`. Insert directly after the `MetadataEventStream` type alias (around line 35):

  ```rust
  /// One partition to consume and the offset to begin at (inclusive).
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct PartitionStart {
      /// Metadata-topic partition to consume.
      pub partition: i32,
      /// First offset to deliver (inclusive). `0` replays from the start.
      pub start_offset: i64,
  }

  /// Runtime control over a live [`MetadataEventLog`] subscription's
  /// assigned partition set. Returned alongside the stream by
  /// [`MetadataEventLog::subscribe`].
  pub trait AssignmentHandle: Send + Sync {
      /// Begin consuming `start.partition` from `start.start_offset`;
      /// no-op if already assigned. A newly-added partition emits its
      /// backlog (from `start_offset`) into the existing stream, then
      /// live records.
      fn add(&self, start: PartitionStart);
      /// Stop consuming `partition` and stop emitting its events. No-op
      /// if not currently assigned.
      fn remove(&self, partition: i32);
      /// Current assigned partition set (unordered).
      fn assigned(&self) -> Vec<i32>;
  }
  ```

- [ ] Change the trait method on `MetadataEventLog` (replace the existing `fn subscribe(&self) -> MetadataEventStream;` at ~line 67 and its doc comment):

  ```rust
      /// Start consuming the given partitions, each from its start
      /// offset (inclusive). Returns the event stream plus a handle to
      /// mutate the live assignment.
      ///
      /// The stream replays each assigned partition's backlog from its
      /// `start_offset`, then forwards live appends for the currently
      /// assigned partitions. Records are delivered in publish order on
      /// a per-partition basis.
      fn subscribe(
          &self,
          assignment: Vec<PartitionStart>,
      ) -> (MetadataEventStream, std::sync::Arc<dyn AssignmentHandle>);
  ```

- [ ] Temporarily make `InProcessMetadataEventLog::subscribe` compile by replacing its body with `let _ = assignment; todo!("filled in Task 2")` and updating its signature to match. (This keeps the crate type-checking between Task 1 and Task 2; Task 2 replaces the body and the unit tests.)

- [ ] Update `crates/remote-storage-topic/src/lib.rs` re-export of `log::` to add `AssignmentHandle, PartitionStart`:

  ```rust
  pub use log::{
      AssignmentHandle, InProcessMetadataEventLog, MetadataEventLog, MetadataEventRecord,
      MetadataEventStream, PartitionStart,
  };
  ```

- [ ] Run `cargo build -p crabka-remote-storage-topic` — EXPECT FAIL only at the `KafkaMetadataEventLog` and `manager.rs` call sites (they still call the old `subscribe`); the `log.rs` types themselves must compile. (Those callers are fixed in Tasks 4 and 5; this build is a sanity check that the trait + InProcess signature are well-formed. If errors come from `log.rs` itself, fix them before proceeding.)

- [ ] Commit: `cargo fmt && git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "48o: PartitionStart + AssignmentHandle trait + new subscribe signature"`

### Task 2: `InProcessMetadataEventLog` assignment-driven `subscribe` + `AssignmentHandle`

Implement backlog-by-assignment filtering, live forwarding only for assigned partitions, and add/remove/assigned. Same file as Task 1 — same implementer.

**Files:**
- `crates/remote-storage-topic/src/log.rs`

Steps:

- [ ] Add a failing unit test `subscribe_delivers_only_assigned_partitions_from_start_offset` to the `tests` module in `log.rs`:

  ```rust
  #[tokio::test]
  async fn subscribe_delivers_only_assigned_partitions_from_start_offset() {
      let log = InProcessMetadataEventLog::new(3);
      // partition 0: a,b,c ; partition 1: x,y ; partition 2: z
      for p0 in [b"a".as_slice(), b"b", b"c"] {
          log.publish(0, Bytes::copy_from_slice(p0)).await.unwrap();
      }
      for p1 in [b"x".as_slice(), b"y"] {
          log.publish(1, Bytes::copy_from_slice(p1)).await.unwrap();
      }
      log.publish(2, Bytes::from_static(b"z")).await.unwrap();

      // Assign partition 0 from offset 1 and partition 1 from offset 0;
      // partition 2 is NOT assigned.
      let (mut stream, _h) = log.subscribe(vec![
          PartitionStart { partition: 0, start_offset: 1 },
          PartitionStart { partition: 1, start_offset: 0 },
      ]);

      let mut got: Vec<(i32, i64, Vec<u8>)> = Vec::new();
      for _ in 0..3 {
          let r = stream.next().await.unwrap();
          got.push((r.partition, r.offset, r.payload.to_vec()));
      }
      got.sort();
      assert_eq!(
          got,
          vec![
              (0, 1, b"b".to_vec()),
              (0, 2, b"c".to_vec()),
              (1, 0, b"x".to_vec()),
          ]
      );
      // partition 1 offset 1 ("y") is the only remaining assigned record.
      let r = stream.next().await.unwrap();
      assert_eq!((r.partition, r.offset, r.payload.as_ref()), (1, 1, b"y".as_ref()));
  }
  ```

- [ ] Add a failing unit test `live_appends_only_for_assigned_partitions`:

  ```rust
  #[tokio::test]
  async fn live_appends_only_for_assigned_partitions() {
      let log = InProcessMetadataEventLog::new(2);
      let (mut stream, _h) = log.subscribe(vec![
          PartitionStart { partition: 0, start_offset: 0 },
      ]);
      // Unassigned partition write must not appear.
      log.publish(1, Bytes::from_static(b"skip")).await.unwrap();
      log.publish(0, Bytes::from_static(b"keep")).await.unwrap();
      let r = stream.next().await.unwrap();
      assert_eq!((r.partition, r.payload.as_ref()), (0, b"keep".as_ref()));
  }
  ```

- [ ] Add a failing unit test `add_mid_stream_delivers_backlog_then_live`:

  ```rust
  #[tokio::test]
  async fn add_mid_stream_delivers_backlog_then_live() {
      let log = InProcessMetadataEventLog::new(2);
      log.publish(1, Bytes::from_static(b"old0")).await.unwrap();
      log.publish(1, Bytes::from_static(b"old1")).await.unwrap();
      let (mut stream, handle) = log.subscribe(vec![
          PartitionStart { partition: 0, start_offset: 0 },
      ]);
      // Add partition 1 from offset 1: should deliver backlog "old1"
      // then a later live append.
      handle.add(PartitionStart { partition: 1, start_offset: 1 });
      let r = stream.next().await.unwrap();
      assert_eq!((r.partition, r.offset, r.payload.as_ref()), (1, 1, b"old1".as_ref()));
      log.publish(1, Bytes::from_static(b"new")).await.unwrap();
      let r = stream.next().await.unwrap();
      assert_eq!((r.partition, r.offset, r.payload.as_ref()), (1, 2, b"new".as_ref()));
      assert!(handle.assigned().contains(&1));
  }
  ```

- [ ] Add a failing unit test `remove_stops_delivery`:

  ```rust
  #[tokio::test]
  async fn remove_stops_delivery() {
      let log = InProcessMetadataEventLog::new(2);
      let (mut stream, handle) = log.subscribe(vec![
          PartitionStart { partition: 0, start_offset: 0 },
          PartitionStart { partition: 1, start_offset: 0 },
      ]);
      handle.remove(1);
      assert_eq!(handle.assigned(), vec![0]);
      log.publish(1, Bytes::from_static(b"gone")).await.unwrap();
      log.publish(0, Bytes::from_static(b"here")).await.unwrap();
      let r = stream.next().await.unwrap();
      assert_eq!((r.partition, r.payload.as_ref()), (0, b"here".as_ref()));
  }
  ```

- [ ] Run `cargo test -p crabka-remote-storage-topic --lib log::tests` — EXPECT FAIL (the four new tests fail/panic at `todo!()`).

- [ ] Implement the assignment state and handle in `log.rs`. Add an assignment map to `InProcessInner` and an `InProcessAssignmentHandle` type. Insert near `InProcessInner` (the new field is `assignments`, a registry of live subscriptions so `add` can replay backlog into the right stream). Use this design:

  ```rust
  use std::collections::HashMap;

  // Per-subscription live assignment + a sender to inject backlog when a
  // partition is added mid-stream. Keyed by a monotonically-increasing
  // subscription id so multiple subscribers stay independent.
  struct SubscriptionState {
      /// partition -> next offset that has NOT yet been delivered by the
      /// backlog/live path. Presence in the map == assigned.
      assigned: Mutex<HashMap<i32, i64>>,
      /// Inject backlog records for a freshly-added partition.
      inject: mpsc::UnboundedSender<MetadataEventRecord>,
  }
  ```

  Replace `InProcessInner` with:

  ```rust
  struct InProcessInner {
      /// `log[partition][offset] = encoded event payload`.
      log: Mutex<Vec<Vec<Bytes>>>,
      /// Notify subscribers of new writes.
      tx: broadcast::Sender<MetadataEventRecord>,
      /// Constant for the life of the log.
      partition_count: i32,
      /// Live subscriptions, keyed by id, for assignment filtering and
      /// mid-stream backlog injection.
      subscriptions: Mutex<HashMap<u64, Arc<SubscriptionState>>>,
      /// Allocates subscription ids.
      next_sub_id: std::sync::atomic::AtomicU64,
  }
  ```

  Update `InProcessMetadataEventLog::new` to initialize the two new fields:

  ```rust
      Arc::new(Self {
          inner: Arc::new(InProcessInner {
              log: Mutex::new(vec![Vec::new(); cap]),
              tx,
              partition_count,
              subscriptions: Mutex::new(HashMap::new()),
              next_sub_id: std::sync::atomic::AtomicU64::new(0),
          }),
      })
  ```

- [ ] Implement the `InProcessAssignmentHandle` struct and its `AssignmentHandle` impl in `log.rs`:

  ```rust
  struct InProcessAssignmentHandle {
      inner: Arc<InProcessInner>,
      sub_id: u64,
  }

  impl AssignmentHandle for InProcessAssignmentHandle {
      fn add(&self, start: PartitionStart) {
          let subs = self
              .inner
              .subscriptions
              .lock()
              .expect("metadata-log subscriptions mutex poisoned");
          let Some(state) = subs.get(&self.sub_id).cloned() else {
              return;
          };
          drop(subs);
          // Hold the log lock so the backlog snapshot + the assigned
          // insert bracket every concurrent publish exactly once: a
          // publish either lands in the snapshot we inject here, or it is
          // forwarded live (because `assigned` already contains it).
          let log = self
              .inner
              .log
              .lock()
              .expect("metadata-log mutex poisoned");
          let mut assigned = state.assigned.lock().expect("assigned mutex poisoned");
          if assigned.contains_key(&start.partition) {
              return; // already assigned: no-op
          }
          let idx = match usize::try_from(start.partition) {
              Ok(i) if i < log.len() => i,
              _ => return, // out of range: ignore
          };
          let records = &log[idx];
          let begin = usize::try_from(start.start_offset.max(0)).unwrap_or(usize::MAX);
          for (offset, payload) in records.iter().enumerate().skip(begin) {
              let _ = state.inject.send(MetadataEventRecord {
                  partition: start.partition,
                  offset: i64::try_from(offset).expect("offset fits in i64"),
                  payload: payload.clone(),
              });
          }
          // Live records at or after the current end are forwarded by the
          // broadcast path once `assigned` contains the partition.
          let next_live = i64::try_from(records.len()).expect("len fits in i64");
          assigned.insert(start.partition, next_live);
      }

      fn remove(&self, partition: i32) {
          let subs = self
              .inner
              .subscriptions
              .lock()
              .expect("metadata-log subscriptions mutex poisoned");
          if let Some(state) = subs.get(&self.sub_id) {
              state
                  .assigned
                  .lock()
                  .expect("assigned mutex poisoned")
                  .remove(&partition);
          }
      }

      fn assigned(&self) -> Vec<i32> {
          let subs = self
              .inner
              .subscriptions
              .lock()
              .expect("metadata-log subscriptions mutex poisoned");
          let Some(state) = subs.get(&self.sub_id) else {
              return Vec::new();
          };
          let mut v: Vec<i32> = state
              .assigned
              .lock()
              .expect("assigned mutex poisoned")
              .keys()
              .copied()
              .collect();
          v.sort_unstable();
          v
      }
  }
  ```

- [ ] Implement the new `subscribe` body on `InProcessMetadataEventLog` (replace the `todo!()` from Task 1). It builds the per-partition `start_offset` filter, snapshots backlog, registers the subscription, and merges the backlog/injection stream with the assignment-filtered broadcast:

  ```rust
  fn subscribe(
      &self,
      assignment: Vec<PartitionStart>,
  ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
      use std::sync::atomic::Ordering;

      // Bracket snapshot + broadcast subscribe under the log lock so each
      // published record is seen exactly once (snapshot xor live).
      let guard = self.inner.log.lock().expect("metadata-log mutex poisoned");
      let rx = self.inner.tx.subscribe();

      // Initial assigned set: partition -> next live offset (= current
      // len), so the broadcast path forwards only records published after
      // subscribe; everything earlier comes from the snapshot below.
      let mut assigned: HashMap<i32, i64> = HashMap::new();
      let mut snapshot: Vec<MetadataEventRecord> = Vec::new();
      for ps in &assignment {
          let Ok(idx) = usize::try_from(ps.partition) else {
              continue;
          };
          if idx >= guard.len() {
              continue;
          }
          let records = &guard[idx];
          let begin = usize::try_from(ps.start_offset.max(0)).unwrap_or(usize::MAX);
          for (offset, payload) in records.iter().enumerate().skip(begin) {
              snapshot.push(MetadataEventRecord {
                  partition: ps.partition,
                  offset: i64::try_from(offset).expect("offset fits in i64"),
                  payload: payload.clone(),
              });
          }
          assigned.insert(ps.partition, i64::try_from(records.len()).expect("len fits in i64"));
      }

      let (inject_tx, inject_rx) = mpsc::unbounded_channel::<MetadataEventRecord>();
      let state = Arc::new(SubscriptionState {
          assigned: Mutex::new(assigned),
          inject: inject_tx,
      });
      let sub_id = self.inner.next_sub_id.fetch_add(1, Ordering::Relaxed);
      self.inner
          .subscriptions
          .lock()
          .expect("metadata-log subscriptions mutex poisoned")
          .insert(sub_id, state.clone());
      drop(guard);

      let snapshot_stream = stream::iter(snapshot);
      let inject_stream = futures_util::stream::unfold(inject_rx, |mut rx| async move {
          rx.recv().await.map(|r| (r, rx))
      });
      let live = filtered_broadcast(rx, state.clone());
      // Snapshot first (subscribe-time backlog), then a merge of injected
      // backlog (from `add`) and assignment-filtered live records.
      let merged = futures_util::stream::select(inject_stream, live);
      let stream = snapshot_stream.chain(merged).boxed();

      let handle: Arc<dyn AssignmentHandle> = Arc::new(InProcessAssignmentHandle {
          inner: self.inner.clone(),
          sub_id,
      });
      (stream, handle)
  }
  ```

- [ ] Add the `filtered_broadcast` helper to `log.rs` (replaces the role of the old `tokio_stream_from_broadcast`; keep or remove the old one if now unused). It forwards a broadcast record only when its partition is currently assigned and its offset is at/after the recorded live cursor:

  ```rust
  fn filtered_broadcast(
      rx: broadcast::Receiver<MetadataEventRecord>,
      state: Arc<SubscriptionState>,
  ) -> MetadataEventStream {
      unfold((rx, state), |(mut rx, state)| async move {
          loop {
              match rx.recv().await {
                  Ok(record) => {
                      let pass = {
                          let assigned = state.assigned.lock().expect("assigned mutex poisoned");
                          matches!(assigned.get(&record.partition), Some(&from) if record.offset >= from)
                      };
                      if pass {
                          return Some((record, (rx, state)));
                      }
                  }
                  Err(broadcast::error::RecvError::Lagged(_)) => {}
                  Err(broadcast::error::RecvError::Closed) => return None,
              }
          }
      })
      .boxed()
  }
  ```

  Note: remove the now-unused `tokio_stream_from_broadcast` if nothing references it (clippy `-D warnings` will flag dead code).

- [ ] Update the existing `log.rs` unit tests that call the old `subscribe()`: `subscribe_replays_history_then_forwards_new_writes`, `subscribe_attached_after_history_still_sees_history`, and `two_subscribers_see_the_same_history` must now pass a full assignment. Replace each `log.subscribe()` with the assignment-driven form, e.g.:

  ```rust
  let (mut stream, _h) = log.subscribe(vec![PartitionStart { partition: 0, start_offset: 0 }]);
  ```

  For `two_subscribers_see_the_same_history`, both subscriptions assign partition 0 from offset 0.

- [ ] Run `cargo test -p crabka-remote-storage-topic --lib log::tests` — EXPECT PASS (all new and updated tests green).

- [ ] Commit: `cargo fmt && git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "48o: InProcessMetadataEventLog assignment-driven subscribe + handle"`

### Task 3: minimal single-partition `Fetch` helper in `client-core`

`client-core` currently exposes only `Client::send` (bootstrap connection) and `BrokerHandle::send`. `kafka_log.rs` needs to fetch one partition from a given offset and decode v2 batches into payloads. Add a small reusable helper rather than open-coding `Fetch` decode in `kafka_log.rs`.

**Files:**
- `crates/client-core/src/fetch.rs` (new)
- `crates/client-core/src/lib.rs`

Steps:

- [ ] Add the module declaration and re-export to `crates/client-core/src/lib.rs`. After `mod connection;` add `mod fetch;`, and in the `pub use` block add:

  ```rust
  pub use fetch::{FetchedRecord, fetch_partition};
  ```

- [ ] Create `crates/client-core/src/fetch.rs`. Factor the decode loop into a private, socket-free `decode_fetch_response` and unit-test **that** (the `MockBroker` only takes a raw `(api_key, version, corr_id, body) -> Option<Vec<u8>>` handler closure with no canned-response helper, and round-tripping an encoded `FetchResponse` through it is fiddly — a pure decode test is lower-risk and exercises the only non-trivial logic). `fetch_partition` stays a thin async wrapper: `conn.send(FetchRequest{..}).await` then `decode_fetch_response`. Note: `RecordsPayload` is built via `RecordsPayload::from(vec![batch])` (`impl From<Vec<RecordBatch>>`); `RecordBatch`/`Record` are re-exported at `crabka_protocol::records::{RecordBatch, Record}`. Write the failing test first:

  ```rust
  //! Minimal single-partition `Fetch` helper over a raw [`Connection`].
  //!
  //! `crabka-client-consumer`'s group `Consumer` owns subscription-style
  //! consumption; this helper is the manual building block for callers
  //! (e.g. the tiered-storage metadata-log consumer) that drive their own
  //! per-partition fetch loops with externally-owned offsets.

  use bytes::Bytes;

  use crate::connection::Connection;
  use crate::error::ClientError;
  use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
  use crabka_protocol::primitives::uuid::Uuid as WireUuid;

  /// One record decoded from a single-partition fetch.
  #[derive(Debug, Clone)]
  pub struct FetchedRecord {
      /// Absolute offset within the partition.
      pub offset: i64,
      /// Record key, if any.
      pub key: Option<Bytes>,
      /// Record value, if any.
      pub value: Option<Bytes>,
  }

  /// Fetch up to `max_bytes` from `(topic, partition)` starting at
  /// `fetch_offset`, decoding every v2 `RecordBatch` into [`FetchedRecord`]s.
  ///
  /// Records are returned in offset order. An empty result means the
  /// partition had nothing at/after `fetch_offset` within `max_wait_ms`.
  /// Legacy (non-v2) message sets are skipped.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError`] on transport / version-negotiation failure.
  pub async fn fetch_partition(
      conn: &Connection,
      topic: &str,
      topic_id: WireUuid,
      partition: i32,
      fetch_offset: i64,
      max_wait_ms: i32,
      partition_max_bytes: i32,
  ) -> Result<Vec<FetchedRecord>, ClientError> {
      let resp = conn
          .send(FetchRequest {
              max_wait_ms,
              min_bytes: 1,
              max_bytes: 50 * 1024 * 1024,
              topics: vec![FetchTopic {
                  topic: topic.to_string(),
                  topic_id,
                  partitions: vec![FetchPartition {
                      partition,
                      fetch_offset,
                      partition_max_bytes,
                      ..Default::default()
                  }],
                  ..Default::default()
              }],
              ..Default::default()
          })
          .await?;
      Ok(decode_fetch_response(&resp, partition))
  }

  /// Decode every v2 `RecordBatch` for `partition` in `resp` into
  /// offset-ordered [`FetchedRecord`]s. Control batches and legacy
  /// (non-v2) payloads are skipped. Socket-free so it is unit-testable
  /// against a hand-built response.
  fn decode_fetch_response(
      resp: &crabka_protocol::owned::fetch_response::FetchResponse,
      partition: i32,
  ) -> Vec<FetchedRecord> {
      let mut out = Vec::new();
      for t in &resp.responses {
          for p in &t.partitions {
              if p.partition_index != partition {
                  continue;
              }
              let Some(payload) = &p.records else { continue };
              let Some(batches) = payload.as_v2() else { continue };
              for batch in batches {
                  if batch.attributes.is_control_batch() {
                      continue;
                  }
                  for r in &batch.records {
                      out.push(FetchedRecord {
                          offset: batch.base_offset + i64::from(r.offset_delta),
                          key: r.key.clone(),
                          value: r.value.clone(),
                      });
                  }
              }
          }
      }
      out.sort_by_key(|r| r.offset);
      out
  }

  #[cfg(test)]
  mod tests {
      use super::*;
      use crabka_protocol::owned::fetch_response::{
          FetchResponse, FetchableTopicResponse, PartitionData,
      };
      use crabka_protocol::records::{Record, RecordBatch};
      use crabka_protocol::records::payload::RecordsPayload;

      fn batch_with(base_offset: i64, values: &[&[u8]]) -> RecordBatch {
          let records = values
              .iter()
              .enumerate()
              .map(|(i, v)| Record {
                  offset_delta: i32::try_from(i).unwrap(),
                  value: Some(Bytes::copy_from_slice(v)),
                  ..Default::default()
              })
              .collect();
          RecordBatch {
              base_offset,
              last_offset_delta: i32::try_from(values.len().saturating_sub(1)).unwrap(),
              records,
              ..Default::default()
          }
      }

      #[test]
      fn decode_yields_absolute_offsets_for_the_requested_partition() {
          // One batch starting at offset 5 on partition 0; a record on
          // partition 1 that must be ignored when decoding partition 0.
          let resp = FetchResponse {
              responses: vec![FetchableTopicResponse {
                  topic: "t".into(),
                  partitions: vec![
                      PartitionData {
                          partition_index: 0,
                          high_watermark: 7,
                          records: Some(RecordsPayload::from(vec![batch_with(5, &[b"a", b"b"])])),
                          ..Default::default()
                      },
                      PartitionData {
                          partition_index: 1,
                          high_watermark: 1,
                          records: Some(RecordsPayload::from(vec![batch_with(0, &[b"z"])])),
                          ..Default::default()
                      },
                  ],
                  ..Default::default()
              }],
              ..Default::default()
          };
          let got = decode_fetch_response(&resp, 0);
          assert_eq!(got.len(), 2);
          assert_eq!(got[0].offset, 5);
          assert_eq!(got[0].value.as_deref(), Some(b"a".as_ref()));
          assert_eq!(got[1].offset, 6);
          assert_eq!(got[1].value.as_deref(), Some(b"b".as_ref()));
      }
  }
  ```

  Before implementing, confirm by reading `crates/protocol/src/records/payload.rs` that `RecordsPayload` is built via `impl From<Vec<RecordBatch>>` (so `RecordsPayload::from(vec![..])`) and that `RecordBatch`/`Record` are re-exported at `crabka_protocol::records` (the payload module's own tests use `crate::records::{Record, RecordBatch}`). If a re-export path differs, adjust the `use` lines accordingly.

- [ ] Run `cargo test -p crabka-client-core fetch` — EXPECT FAIL (module not yet wired / `decode_fetch_response` not present).

- [ ] Implement `fetch.rs` so it compiles and the decode test passes.

- [ ] Run `cargo test -p crabka-client-core fetch` — EXPECT PASS.

- [ ] Commit: `cargo fmt && git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "48o: client-core single-partition fetch_partition helper"`

### Task 4: rework `KafkaMetadataEventLog` onto client-core manual-fetch loops

Replace the group-`Consumer` consumer with one cancellable manual-`Fetch` task per assigned partition over a dedicated `client-core` `Connection`, emitting `MetadataEventRecord`s into a shared mpsc. Implement `KafkaAssignmentHandle` (add/remove spawn/cancel tasks; assigned reports the live set). Drop `group_id` and the `crabka-client-consumer` dependency from this path.

**Files:**
- `crates/remote-storage-topic/src/kafka_log.rs`

Steps:

- [ ] Resolve the topic id for the metadata topic. The manual `Fetch` path needs the topic `Uuid` (Fetch v≥13 carries `topic_id`, not the name). In `KafkaMetadataEventLog::start`, after `ensure_topic`, issue one `MetadataRequest` over `client` to learn the metadata topic's `topic_id`, and store it on the struct. Add a field `topic_id: crabka_protocol::primitives::uuid::Uuid` to `KafkaMetadataEventLog` and populate it. (If the existing `ensure_topic` already surfaces the id via the admin metadata call, thread it out of `ensure_topic` instead of a second round-trip — read the admin `metadata` return type to decide.)

- [ ] Replace the `subscriptions: tokio::sync::Mutex<Vec<CancellationToken>>` field and the group-consumer machinery with assignment state. Define the shared consumer state in `kafka_log.rs`:

  ```rust
  use std::collections::HashMap;
  use std::sync::Mutex as StdMutex;

  /// Per-subscription live consumer: one cancellable fetch task per
  /// assigned partition, all emitting into the shared `tx`.
  struct ConsumerState {
      bootstrap: String,
      client_id: String,
      topic: String,
      topic_id: crabka_protocol::primitives::uuid::Uuid,
      tx: mpsc::Sender<MetadataEventRecord>,
      /// partition -> cancel token for its fetch task.
      tasks: StdMutex<HashMap<i32, CancellationToken>>,
  }
  ```

- [ ] Implement the per-partition fetch task as a free async fn in `kafka_log.rs`. It owns its own `Connection` (a dedicated connection per partition keeps the metadata consumer off any parkable/shared stream — the broker is serial per-connection, so a long `max_wait_ms` fetch must not head-of-line-block other RPCs):

  ```rust
  async fn partition_fetch_loop(
      state: Arc<ConsumerState>,
      partition: i32,
      start_offset: i64,
      cancel: CancellationToken,
  ) {
      use crabka_client_core::{Connection, ConnectionOptions, fetch_partition};
      use std::net::ToSocketAddrs;

      // Dedicated connection for this partition's fetch loop. Resolve the
      // bootstrap address; on failure, warn and exit (the manager's
      // wait_for_targets will then time out, matching prior behavior).
      let addr = match state.bootstrap.to_socket_addrs().ok().and_then(|mut a| a.next()) {
          Some(a) => a,
          None => {
              warn!(bootstrap = %state.bootstrap, "metadata consumer: bad bootstrap addr");
              return;
          }
      };
      let opts = ConnectionOptions {
          client_id: state.client_id.clone(),
          ..Default::default()
      };
      let conn = match Connection::connect(addr, opts).await {
          Ok(c) => c,
          Err(e) => {
              warn!(error = %e, partition, "metadata consumer: connect failed");
              return;
          }
      };

      let mut next_offset = start_offset.max(0);
      loop {
          tokio::select! {
              biased;
              () = cancel.cancelled() => {
                  conn.close();
                  return;
              }
              res = fetch_partition(
                  &conn,
                  &state.topic,
                  state.topic_id,
                  partition,
                  next_offset,
                  500,
                  1 << 20,
              ) => {
                  match res {
                      Ok(records) => {
                          for r in records {
                              if r.offset < next_offset {
                                  continue; // defensive: never go backwards
                              }
                              let payload = r.value.unwrap_or_default();
                              let record = MetadataEventRecord {
                                  partition,
                                  offset: r.offset,
                                  payload,
                              };
                              next_offset = r.offset + 1;
                              if state.tx.send(record).await.is_err() {
                                  conn.close();
                                  return; // stream dropped
                              }
                          }
                      }
                      Err(e) => {
                          warn!(error = %e, partition, "metadata consumer: fetch failed; retrying");
                          tokio::time::sleep(Duration::from_millis(200)).await;
                      }
                  }
              }
          }
      }
  }
  ```

  **Note on the empty-fetch hot loop:** `fetch_partition` with `max_wait_ms=500` and `min_bytes=1` blocks broker-side up to 500ms when the partition is empty, so an idle partition does not spin. Keep `min_bytes=1`.

- [ ] Implement `spawn_partition` / `cancel_partition` helpers on `ConsumerState`, and the `KafkaAssignmentHandle`:

  ```rust
  impl ConsumerState {
      fn spawn_partition(self: &Arc<Self>, start: PartitionStart) {
          let mut tasks = self.tasks.lock().expect("metadata tasks mutex poisoned");
          if tasks.contains_key(&start.partition) {
              return; // already assigned
          }
          let cancel = CancellationToken::new();
          tasks.insert(start.partition, cancel.clone());
          tokio::spawn(partition_fetch_loop(
              self.clone(),
              start.partition,
              start.start_offset,
              cancel,
          ));
      }

      fn cancel_partition(&self, partition: i32) {
          if let Some(tok) = self
              .tasks
              .lock()
              .expect("metadata tasks mutex poisoned")
              .remove(&partition)
          {
              tok.cancel();
          }
      }

      fn cancel_all(&self) {
          let mut tasks = self.tasks.lock().expect("metadata tasks mutex poisoned");
          for (_, tok) in tasks.drain() {
              tok.cancel();
          }
      }
  }

  struct KafkaAssignmentHandle {
      state: Arc<ConsumerState>,
  }

  impl AssignmentHandle for KafkaAssignmentHandle {
      fn add(&self, start: PartitionStart) {
          self.state.spawn_partition(start);
      }
      fn remove(&self, partition: i32) {
          self.state.cancel_partition(partition);
      }
      fn assigned(&self) -> Vec<i32> {
          let mut v: Vec<i32> = self
              .state
              .tasks
              .lock()
              .expect("metadata tasks mutex poisoned")
              .keys()
              .copied()
              .collect();
          v.sort_unstable();
          v
      }
  }
  ```

- [ ] Replace the `MetadataEventLog::subscribe` impl on `KafkaMetadataEventLog` with the assignment-driven version. Track live `ConsumerState`s on the struct so `shutdown`/`Drop` cancel them. Change the struct field `subscriptions` to `subscriptions: tokio::sync::Mutex<Vec<Arc<ConsumerState>>>`:

  ```rust
  fn subscribe(
      &self,
      assignment: Vec<PartitionStart>,
  ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
      let (tx, rx) = mpsc::channel::<MetadataEventRecord>(1024);
      let state = Arc::new(ConsumerState {
          bootstrap: self.bootstrap.clone(),
          client_id: format!("{}-consumer", self.client_id),
          topic: self.topic.clone(),
          topic_id: self.topic_id,
          tx,
          tasks: StdMutex::new(HashMap::new()),
      });
      for ps in assignment {
          state.spawn_partition(ps);
      }
      if let Ok(mut subs) = self.subscriptions.try_lock() {
          subs.push(state.clone());
      } else {
          warn!("KafkaMetadataEventLog: could not track subscription state");
      }
      let stream = unfold(rx, |mut rx| async move { rx.recv().await.map(|r| (r, rx)) }).boxed();
      let handle: Arc<dyn AssignmentHandle> = Arc::new(KafkaAssignmentHandle { state });
      (stream, handle)
  }
  ```

- [ ] Update `shutdown` and `Drop` to call `cancel_all` on each tracked `ConsumerState` instead of cancelling raw tokens:

  ```rust
  pub async fn shutdown(&self) {
      let mut subs = self.subscriptions.lock().await;
      for state in subs.drain(..) {
          state.cancel_all();
      }
  }
  ```

  ```rust
  impl Drop for KafkaMetadataEventLog {
      fn drop(&mut self) {
          if let Ok(mut subs) = self.subscriptions.try_lock() {
              for state in subs.drain(..) {
                  state.cancel_all();
              }
          }
      }
  }
  ```

- [ ] Remove now-dead code: the `consumer_pump` free fn, the `use crabka_client_consumer::{AutoOffsetReset, Consumer};` import, and the `uuid::Uuid::new_v4()` group-id line. Add the needed imports: `use crate::log::{AssignmentHandle, PartitionStart};` and `use crabka_protocol::owned::metadata_request::MetadataRequest;` (for the topic-id lookup). Remove `crabka-client-consumer` from `crates/remote-storage-topic/Cargo.toml` **only if** no other code in the crate uses it — grep first (`grep -rn crabka_client_consumer crates/remote-storage-topic/src`); the producer path uses `crabka-client-producer`, which stays.

- [ ] Run `cargo build -p crabka-remote-storage-topic` — EXPECT FAIL initially at `manager.rs` (still calls old `subscribe`); `kafka_log.rs` itself must compile. If Task 5 is being done in the same batch, build after both. Otherwise temporarily confirm with `cargo build -p crabka-remote-storage-topic 2>&1 | grep kafka_log` showing no errors from `kafka_log.rs`.

- [ ] Run `cargo test -p crabka-remote-storage-topic --lib` — EXPECT PASS for the `kafka_log::tests::config_defaults_match_kafka` unit test (the only in-crate unit test for this file; integration coverage lands in Task 6).

- [ ] Commit: `cargo fmt && git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "48o: KafkaMetadataEventLog manual per-partition fetch consumer + assignment handle"`

### Task 5: `manager.rs::start` calls new `subscribe`, holds the `AssignmentHandle`

Behavior-preserving: assign all partitions from offset 0. Store the handle on the manager struct for 48p/48q. `pump_loop` is unchanged.

**Files:**
- `crates/remote-storage-topic/src/manager.rs`

Steps:

- [ ] Add a field to `TopicBasedRemoteLogMetadataManager` to hold the handle:

  ```rust
      /// Live assignment handle for the metadata-log subscription. Held so
      /// 48p (resume from snapshot offsets) and 48q (per-broker partition
      /// assignment) can mutate the consumed set at runtime. Unused in
      /// 48o beyond construction (assign-all-from-0).
      #[allow(dead_code)]
      assignment: Arc<dyn AssignmentHandle>,
  ```

  and import it: add `AssignmentHandle, PartitionStart` to the `use crate::log::{...}` line.

- [ ] In `start`, replace `let stream = log.subscribe();` with the full assignment from offset 0:

  ```rust
  let assignment: Vec<PartitionStart> = (0..log.partition_count())
      .map(|partition| PartitionStart {
          partition,
          start_offset: 0,
      })
      .collect();
  let (stream, assignment_handle) = log.subscribe(assignment);
  ```

- [ ] Store `assignment_handle` when constructing the `Arc<Self>`: add `assignment: assignment_handle,` to the struct literal.

- [ ] Run `cargo test -p crabka-remote-storage-topic --lib manager::tests` — EXPECT PASS (all existing round-trip tests: `add_finish_query_round_trip`, `two_managers_sharing_a_log_converge`, `restart_rehydrates_from_log`, `partition_delete_lifecycle_round_trip`, `unknown_partition_query_is_none`, `add_with_wrong_state_is_rejected_eagerly`). These exercise the new InProcess `subscribe` end-to-end with assign-all-from-0.

- [ ] Commit: `cargo fmt && git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "48o: manager subscribes with full assignment from 0 and holds AssignmentHandle"`

### Task 6: loopback integration test for `KafkaMetadataEventLog` subset + non-zero start offset

Prove the reworked Kafka consumer against a real broker: publish across partitions, subscribe to a subset from a non-zero offset, assert exactly the expected records. Model on `crates/broker/tests/tiered_storage_topic_rlmm.rs` (same `support` harness, same loopback-broker boot).

**Files:**
- `crates/broker/tests/tiered_storage_metadata_assign.rs` (new)

Steps:

- [ ] Read `crates/broker/tests/support/mod.rs`. It exposes `init_tracing()`, `bind_and_drop_ports(n)` (pins ports so a bootstrap addr is knowable before bind), and `broker_config(...)`, but **no bare-broker one-liner** — `tiered_storage_topic_rlmm.rs::start_broker_with_topic_rlmm` builds the broker inline via `bind_and_drop_ports(1)` + a hand-rolled `BrokerConfig`. Copy that pinned-port boot here, dropping `remote_storage_backend` and `remote_log_metadata_kafka` (this test constructs the `KafkaMetadataEventLog` directly, not through `Broker::start`'s bootstrap). The `KafkaMetadataEventLog` is a library type in `crabka-remote-storage-topic`; build it pointed at the broker's loopback listener with a small `num_partitions` (e.g. 3) and `replication = 1`.

- [ ] Write the integration test (adjust harness calls to the real `support` API). It is gated `#[cfg(not(target_os = "windows"))]` like the sibling test:

  ```rust
  //! Slice-48o integration: KafkaMetadataEventLog manual per-partition
  //! fetch consumer honors a partition subset and a non-zero start offset.
  #![cfg(not(target_os = "windows"))]
  #![allow(clippy::pedantic, clippy::manual_assert)]

  mod support;

  use std::sync::Arc;
  use std::time::{Duration, Instant};

  use bytes::Bytes;
  use crabka_remote_storage_topic::kafka_log::{KafkaMetadataEventLog, KafkaMetadataLogConfig};
  use crabka_remote_storage_topic::log::{MetadataEventLog, PartitionStart};
  use futures_util::StreamExt;

  // Boot a bare loopback broker. Reuse the sibling test's pinned-port
  // helper so KafkaMetadataLogConfig::bootstrap is knowable before bind.
  // (Copy the minimal boot from start_broker_with_topic_rlmm, dropping
  //  the tiered-storage backend — only the topic transport is needed.)

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn subscribe_subset_from_nonzero_offset_yields_exact_records() {
      // Boot a bare loopback broker via the pinned-port pattern copied
      // from tiered_storage_topic_rlmm.rs (bind_and_drop_ports + inline
      // BrokerConfig, no tiered backend). `broker` is a BrokerHandle and
      // `_dirs` keeps the log TempDir alive.
      let (broker, _dirs) = start_bare_broker().await; // local helper in this file
      let bootstrap = format!("127.0.0.1:{}", broker.listen_addr().port());

      let mut cfg = KafkaMetadataLogConfig::new(bootstrap);
      cfg.num_partitions = 3;
      cfg.replication = 1;
      let log = KafkaMetadataEventLog::start(cfg).await.expect("log start");
      let pc = log.partition_count();
      assert!(pc >= 3);

      // Publish: partition 0 -> a,b,c ; partition 1 -> x,y ; partition 2 -> z
      for v in [b"a".as_slice(), b"b", b"c"] {
          log.publish(0, Bytes::copy_from_slice(v)).await.unwrap();
      }
      for v in [b"x".as_slice(), b"y"] {
          log.publish(1, Bytes::copy_from_slice(v)).await.unwrap();
      }
      log.publish(2, Bytes::from_static(b"z")).await.unwrap();

      // Subscribe to partitions {0 from offset 1, 1 from offset 0}; not 2.
      let (mut stream, _handle) = log.subscribe(vec![
          PartitionStart { partition: 0, start_offset: 1 },
          PartitionStart { partition: 1, start_offset: 0 },
      ]);

      // Expect exactly: (0,1,b), (0,2,c), (1,0,x), (1,1,y). Collect with a
      // deadline; assert no partition-2 record ever arrives.
      let mut got: Vec<(i32, i64, Vec<u8>)> = Vec::new();
      let deadline = Instant::now() + Duration::from_secs(15);
      while got.len() < 4 {
          let next = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
          match next {
              Ok(Some(r)) => {
                  assert_ne!(r.partition, 2, "partition 2 was not assigned");
                  got.push((r.partition, r.offset, r.payload.to_vec()));
              }
              Ok(None) => break,
              Err(_) => {} // timeout tick; keep waiting until deadline
          }
          assert!(Instant::now() <= deadline, "did not receive 4 records in 15s: {got:?}");
      }
      got.sort();
      assert_eq!(
          got,
          vec![
              (0, 1, b"b".to_vec()),
              (0, 2, b"c".to_vec()),
              (1, 0, b"x".to_vec()),
              (1, 1, b"y".to_vec()),
          ]
      );

      // Brief grace: ensure no stray partition-2 record sneaks in.
      if let Ok(Some(extra)) =
          tokio::time::timeout(Duration::from_millis(500), stream.next()).await
      {
          assert_ne!(extra.partition, 2, "partition 2 leaked: {extra:?}");
      }

      log.shutdown().await;
      broker.shutdown().await;
  }
  ```

  **Adapt to the real harness.** `support::start_bare_broker` is a placeholder — use whatever bare-broker boot the `support` module offers (or copy the pinned-port boot from `start_broker_with_topic_rlmm`, omitting `remote_storage_backend` and `remote_log_metadata_kafka` since this test constructs the `KafkaMetadataEventLog` directly rather than through `Broker::start`'s bootstrap). Ensure `crabka-remote-storage-topic` and `futures-util` are dev-dependencies of `crates/broker` (the sibling test crate already depends on `crabka-broker`; check `crates/broker/Cargo.toml` `[dev-dependencies]` and add `crabka-remote-storage-topic` + `futures-util` if absent).

- [ ] Run `cargo test -p crabka-broker --test tiered_storage_metadata_assign` — EXPECT FAIL first if any harness adaptation is wrong; iterate until it compiles and FAILS only on the assertion if behavior is wrong, then PASSES once correct. (If the rework is correct, it should pass on first green compile.)

- [ ] Run `cargo test -p crabka-broker --test tiered_storage_topic_rlmm` — EXPECT PASS (regression: the existing loopback round-trip still works through the reworked consumer, since the manager assigns all partitions from 0).

- [ ] Commit: `cargo fmt && git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "48o: loopback integration test for metadata-log subset + non-zero start offset"`

### Task 7: full-workspace verification

**Files:** none (verification only)

Steps:

- [ ] Run `cargo fmt --all --check` — EXPECT clean (no diff).
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings` — EXPECT clean. Fix any dead-code / unused-import warnings (notably any leftover from removing `consumer_pump` / `tokio_stream_from_broadcast` / the `crabka-client-consumer` dep).
- [ ] Run `cargo test -p crabka-remote-storage-topic -p crabka-broker` — EXPECT PASS (the acceptance-gate subset).
- [ ] Run `cargo test --workspace` — EXPECT PASS (no regressions).
- [ ] If all four are green, the slice is complete. If `cargo fmt --all --check` shows a diff, run `cargo fmt` and amend the last commit.
