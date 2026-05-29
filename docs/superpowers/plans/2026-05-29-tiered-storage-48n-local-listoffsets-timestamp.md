# Tiered Storage 48n — Local ListOffsets by-timestamp — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve `ListOffsets` by-timestamp against the local log (and wire the `MAX_TIMESTAMP` / `EARLIEST_LOCAL` sentinels) so non-tiered topics return a real offset instead of the `-1` stub.

**Architecture:** Add `Segment::offset_for_timestamp` and `Segment::offset_of_max_timestamp` (time-index floor lookup + forward `.log` scan, keeping `time_index` private), then `Log::offset_for_timestamp` / `Log::offset_of_max_timestamp` that walk sealed segments oldest-first then the active segment and delegate to the segment helpers. The `list_offsets` handler resolves the new `MAX_TIMESTAMP` (`-3`) and `EARLIEST_LOCAL` (`-4`) sentinels and falls back to the local log for positive timestamps when the remote tier has no answer, echoing the matched record timestamp in the response.

**Tech Stack:** Rust, tokio, crabka workspace crates

---

## File structure

- `crates/log/src/segment.rs` — **modify.** Add `Segment::offset_for_timestamp(target_ts: i64) -> Option<(i64, i64)>` and `Segment::offset_of_max_timestamp() -> Option<(i64, i64)>`. Both use the private `time_index` for a floor position then scan the segment's `.log` batches forward; `time_index` stays private. New unit tests in the existing `#[cfg(test)] mod tests`.
- `crates/log/src/log.rs` — **modify.** Add `Log::offset_for_timestamp(target_ts: i64) -> Option<(i64, i64)>` and `Log::offset_of_max_timestamp() -> i64`, iterating `self.segments` (sealed, oldest-first) then `self.active` and delegating to the segment helpers. New unit tests in the existing `#[cfg(test)] mod tests`.
- `crates/broker/src/handlers/list_offsets.rs` — **modify.** Add `MAX_TIMESTAMP = -3` and `EARLIEST_LOCAL = -4` consts; resolve them and the positive-timestamp local fallback; set the response `timestamp` field to the matched record timestamp where available.
- `crates/broker/tests/integration.rs` — **modify.** Add broker integration tests: non-tiered topic by-timestamp returns a real offset (was `-1`), `EARLIEST_LOCAL`, and `MAX_TIMESTAMP`.

Reference facts grounded in the current code:
- `Segment` fields: `base_offset: i64`, `log_file: File`, `log_size: u64`, `time_index: TimeIndex` (private), `max_timestamp: i64` (`i64::MIN` when empty), `last_offset: i64`. Helpers: `base_offset()`, `last_offset()`, `max_timestamp()`, `read_log_range(start_pos, &mut buf, max_bytes)` (private), `read(offset, max_bytes) -> Vec<RecordBatch>`.
- `TimeIndex::lookup(target_timestamp: i64) -> u32` returns the **relative offset** of the largest entry with `timestamp <= target` (0 when none). Entries are appended as `(self.max_timestamp, rel_offset_of_batch_base)`.
- `RecordBatch` (owned) fields: `base_offset: i64`, `base_timestamp: i64`, `max_timestamp: i64`, `last_offset_delta: i32`, `records: Vec<Record>`. `Record` fields: `timestamp_delta: i64`, `offset_delta: i32`. Per-record timestamp = `batch.base_timestamp + record.timestamp_delta`; per-record absolute offset = `batch.base_offset + record.offset_delta`.
- `Log` fields: `segments: Vec<Arc<Segment>>` (sealed, ascending), `active: Option<Segment>`. Helpers: `log_start_offset()`, `log_end_offset()`, `local_log_start_offset()`.
- `ListOffsetsPartitionResponse` fields: `partition_index: i32`, `error_code: i16`, `timestamp: i64`, `offset: i64`, `leader_epoch: i32`.

---

### Task 1: `Segment::offset_for_timestamp`

**Files:** `crates/log/src/segment.rs`

Returns `(absolute_offset, record_timestamp)` of the first record in this segment whose timestamp `>= target_ts`, or `None` when no such record exists. Uses the private `time_index` floor lookup, then scans `.log` batches forward via `read`.

- [ ] Write a failing unit test. Add to `mod tests` in `crates/log/src/segment.rs`:
  ```rust
  #[test]
  fn offset_for_timestamp_finds_first_ge() {
      let dir = tempdir().unwrap();
      let mut seg = Segment::create(dir.path(), 0).unwrap();
      // Two batches: offsets 0..=2 ts 100..=102, offsets 3..=4 ts 200..=201.
      seg.append(&sample_batch(0, 3, 100), 0).unwrap();
      seg.append(&sample_batch(3, 2, 200), 0).unwrap();
      // sample_batch sets per-record timestamp_delta = i, base_timestamp = ts_base.
      // Batch 1 records: (off0,ts100),(off1,ts101),(off2,ts102).
      // Batch 2 records: (off3,ts200),(off4,ts201).
      assert_eq!(seg.offset_for_timestamp(100), Some((0, 100)));
      assert_eq!(seg.offset_for_timestamp(101), Some((1, 101)));
      assert_eq!(seg.offset_for_timestamp(150), Some((3, 200)));
      assert_eq!(seg.offset_for_timestamp(201), Some((4, 201)));
      assert_eq!(seg.offset_for_timestamp(202), None);
      drop(dir);
  }
  ```
- [ ] Run it; expect FAIL (method does not exist — compile error):
  `cargo test -p crabka-log offset_for_timestamp_finds_first_ge`
- [ ] Implement `Segment::offset_for_timestamp`. Add this method inside `impl Segment` (after `max_timestamp`):
  ```rust
  /// Absolute offset and record timestamp of the first record in this
  /// segment whose timestamp is `>= target_ts`. Uses the sparse time
  /// index for a floor position, then scans `.log` batches forward
  /// (the index is sparse, so an exact answer needs the post-index
  /// scan — matching Kafka's `LogSegment.findOffsetByTimestamp`).
  /// Returns `None` when no record in this segment qualifies.
  #[must_use]
  pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<(i64, i64)> {
      let floor_rel = self.time_index.lookup(target_ts);
      let scan_from = self.base_offset + i64::from(floor_rel);
      let batches = self.read(scan_from, usize::MAX).ok()?;
      for batch in &batches {
          for rec in &batch.records {
              let ts = batch.base_timestamp + rec.timestamp_delta;
              if ts >= target_ts {
                  return Some((batch.base_offset + i64::from(rec.offset_delta), ts));
              }
          }
      }
      None
  }
  ```
- [ ] Run the test; expect PASS:
  `cargo test -p crabka-log offset_for_timestamp_finds_first_ge`
- [ ] Format, then commit:
  ```
  cargo fmt
  git add crates/log/src/segment.rs
  git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(log): Segment::offset_for_timestamp (time-index floor + forward scan)"
  ```

### Task 2: `Segment::offset_of_max_timestamp`

**Files:** `crates/log/src/segment.rs`

Returns `(absolute_offset, timestamp)` of the record carrying this segment's `max_timestamp`, ties resolving to the earliest offset. `None` for an empty segment.

- [ ] Write a failing unit test. Add to `mod tests`:
  ```rust
  #[test]
  fn offset_of_max_timestamp_earliest_on_tie() {
      let dir = tempdir().unwrap();
      let mut seg = Segment::create(dir.path(), 0).unwrap();
      // Batch records ts: 100,101,102 (max in batch = 102 at offset 2).
      seg.append(&sample_batch(0, 3, 100), 0).unwrap();
      // Second batch: offsets 3,4 ts 200,201 — segment max becomes 201 @4.
      seg.append(&sample_batch(3, 2, 200), 0).unwrap();
      assert_eq!(seg.offset_of_max_timestamp(), Some((4, 201)));

      // Empty segment → None.
      let dir2 = tempdir().unwrap();
      let empty = Segment::create(dir2.path(), 0).unwrap();
      assert_eq!(empty.offset_of_max_timestamp(), None);
      drop(dir);
      drop(dir2);
  }

  #[test]
  fn offset_of_max_timestamp_tie_picks_earliest() {
      let dir = tempdir().unwrap();
      let mut seg = Segment::create(dir.path(), 0).unwrap();
      // All three records share timestamp 500; earliest offset is 0.
      let mut b = RecordBatch {
          base_offset: 0,
          base_timestamp: 500,
          max_timestamp: 500,
          last_offset_delta: 2,
          ..RecordBatch::default()
      };
      for i in 0..3 {
          b.records.push(Record {
              offset_delta: i,
              timestamp_delta: 0,
              value: Some(Bytes::from("v")),
              ..Default::default()
          });
      }
      seg.append(&b, 0).unwrap();
      assert_eq!(seg.offset_of_max_timestamp(), Some((0, 500)));
      drop(dir);
  }
  ```
- [ ] Run; expect FAIL (method does not exist):
  `cargo test -p crabka-log offset_of_max_timestamp`
- [ ] Implement `Segment::offset_of_max_timestamp`. Add inside `impl Segment` after `offset_for_timestamp`:
  ```rust
  /// Absolute offset and timestamp of the record carrying this
  /// segment's `max_timestamp`. Ties resolve to the earliest offset
  /// (Kafka). Returns `None` for an empty segment. Uses the time
  /// index's floor for the max to start the scan, then scans forward
  /// for the first record whose timestamp equals the segment max.
  #[must_use]
  pub fn offset_of_max_timestamp(&self) -> Option<(i64, i64)> {
      if self.max_timestamp == i64::MIN {
          return None;
      }
      let floor_rel = self.time_index.lookup(self.max_timestamp);
      let scan_from = self.base_offset + i64::from(floor_rel);
      let batches = self.read(scan_from, usize::MAX).ok()?;
      for batch in &batches {
          for rec in &batch.records {
              let ts = batch.base_timestamp + rec.timestamp_delta;
              if ts == self.max_timestamp {
                  return Some((batch.base_offset + i64::from(rec.offset_delta), ts));
              }
          }
      }
      None
  }
  ```
- [ ] Run; expect PASS:
  `cargo test -p crabka-log offset_of_max_timestamp`
- [ ] Format, then commit:
  ```
  cargo fmt
  git add crates/log/src/segment.rs
  git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(log): Segment::offset_of_max_timestamp (earliest on tie)"
  ```

### Task 3: `Log::offset_for_timestamp`

**Files:** `crates/log/src/log.rs`

Walks sealed segments oldest-first then the active segment; returns the first qualifying `(offset, timestamp)` from the first segment whose `max_timestamp >= target_ts`. `None` when no local record qualifies (including empty log).

- [ ] Write a failing unit test. Add to `mod tests` in `crates/log/src/log.rs`. Use a helper that appends a single-record batch at a chosen timestamp and forces small segments so multiple sealed segments exist:
  ```rust
  fn ts_batch(ts: i64) -> RecordBatch {
      let mut b = RecordBatch {
          base_offset: 0, // overwritten by Log::append
          base_timestamp: ts,
          max_timestamp: ts,
          last_offset_delta: 0,
          ..RecordBatch::default()
      };
      b.records.push(Record {
          offset_delta: 0,
          timestamp_delta: 0,
          value: Some(Bytes::from("v")),
          ..Default::default()
      });
      b
  }

  #[test]
  fn log_offset_for_timestamp_across_segments() {
      let dir = tempdir().unwrap();
      let config = LogConfig {
          segment_bytes: 1, // roll after every batch → each record its own segment
          ..LogConfig::default()
      };
      let mut log = Log::open(dir.path(), config).unwrap();
      // offsets 0..=4 with timestamps 100,200,300,400,500.
      for (i, ts) in [100, 200, 300, 400, 500].into_iter().enumerate() {
          let mut b = ts_batch(ts);
          assert_eq!(log.append(&mut b).unwrap(), i as i64);
      }
      // before-first → offset 0.
      assert_eq!(log.offset_for_timestamp(50), Some((0, 100)));
      // exact match on a sealed segment.
      assert_eq!(log.offset_for_timestamp(300), Some((2, 300)));
      // between records → next record up.
      assert_eq!(log.offset_for_timestamp(350), Some((3, 400)));
      // landing on the active segment's record.
      assert_eq!(log.offset_for_timestamp(500), Some((4, 500)));
      // after-last → None.
      assert_eq!(log.offset_for_timestamp(600), None);
      log.close();
      drop(dir);
  }

  #[test]
  fn log_offset_for_timestamp_empty_log_is_none() {
      let dir = tempdir().unwrap();
      let log = Log::open(dir.path(), LogConfig::default()).unwrap();
      assert_eq!(log.offset_for_timestamp(0), None);
      log.close();
      drop(dir);
  }
  ```
- [ ] Run; expect FAIL (method does not exist):
  `cargo test -p crabka-log log_offset_for_timestamp`
- [ ] Implement `Log::offset_for_timestamp`. Add inside `impl Log` (e.g. after `local_log_start_offset`):
  ```rust
  /// Earliest local `(offset, record_timestamp)` whose record
  /// timestamp is `>= target_ts`, searching sealed segments
  /// oldest-first then the active segment. The first segment whose
  /// `max_timestamp >= target_ts` holds the answer; the per-segment
  /// helper does the index lookup + forward scan. `None` when no
  /// local record qualifies (including an empty log).
  #[must_use]
  pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<(i64, i64)> {
      for seg in &self.segments {
          if seg.max_timestamp() >= target_ts
              && let Some(hit) = seg.offset_for_timestamp(target_ts)
          {
              return Some(hit);
          }
      }
      if let Some(active) = &self.active
          && active.max_timestamp() >= target_ts
      {
          return active.offset_for_timestamp(target_ts);
      }
      None
  }
  ```
- [ ] Run; expect PASS:
  `cargo test -p crabka-log log_offset_for_timestamp`
- [ ] Format, then commit:
  ```
  cargo fmt
  git add crates/log/src/log.rs
  git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(log): Log::offset_for_timestamp (oldest-first segment walk)"
  ```

### Task 4: `Log::offset_of_max_timestamp`

**Files:** `crates/log/src/log.rs`

Returns the offset of the record carrying the partition's largest timestamp across all segments + active, or `log_start_offset()` when the log is empty. Ties across segments resolve to the earliest offset.

- [ ] Write a failing unit test. Add to `mod tests` (reuses `ts_batch` from Task 3):
  ```rust
  #[test]
  fn log_offset_of_max_timestamp_in_active() {
      let dir = tempdir().unwrap();
      let config = LogConfig {
          segment_bytes: 1, // each record its own segment
          ..LogConfig::default()
      };
      let mut log = Log::open(dir.path(), config).unwrap();
      // timestamps 100,300,200 at offsets 0,1,2. Max is 300 @ offset 1.
      for ts in [100, 300, 200] {
          let mut b = ts_batch(ts);
          log.append(&mut b).unwrap();
      }
      assert_eq!(log.offset_of_max_timestamp(), 1);
      log.close();
      drop(dir);
  }

  #[test]
  fn log_offset_of_max_timestamp_empty_is_log_start() {
      let dir = tempdir().unwrap();
      let log = Log::open(dir.path(), LogConfig::default()).unwrap();
      assert_eq!(log.offset_of_max_timestamp(), log.log_start_offset());
      assert_eq!(log.max_timestamp_offset_and_ts(), None);
      log.close();
      drop(dir);
  }

  #[test]
  fn log_max_timestamp_offset_and_ts_returns_pair() {
      let dir = tempdir().unwrap();
      let config = LogConfig {
          segment_bytes: 1,
          ..LogConfig::default()
      };
      let mut log = Log::open(dir.path(), config).unwrap();
      for ts in [100, 300, 200] {
          let mut b = ts_batch(ts);
          log.append(&mut b).unwrap();
      }
      // Max timestamp 300 lives at offset 1.
      assert_eq!(log.max_timestamp_offset_and_ts(), Some((1, 300)));
      log.close();
      drop(dir);
  }
  ```
- [ ] Run; expect FAIL (methods do not exist):
  `cargo test -p crabka-log log_offset_of_max_timestamp log_max_timestamp_offset_and_ts`
- [ ] Implement both helpers. Add inside `impl Log` after `offset_for_timestamp`. `max_timestamp_offset_and_ts` does the scan; `offset_of_max_timestamp` delegates to it:
  ```rust
  /// Offset and timestamp of the record carrying the partition's
  /// largest timestamp, scanning sealed segments then the active
  /// segment. Ties resolve to the earliest offset (the first segment,
  /// and the first record within it, wins). Returns `None` when the
  /// log holds no records.
  #[must_use]
  pub fn max_timestamp_offset_and_ts(&self) -> Option<(i64, i64)> {
      let mut best: Option<(i64, i64)> = None; // (timestamp, offset)
      let candidates = self
          .segments
          .iter()
          .map(AsRef::as_ref)
          .chain(self.active.as_ref());
      for seg in candidates {
          if let Some((offset, ts)) = seg.offset_of_max_timestamp()
              && best.is_none_or(|(best_ts, _)| ts > best_ts)
          {
              best = Some((ts, offset));
          }
      }
      best.map(|(ts, offset)| (offset, ts))
  }

  /// Offset of the record carrying the partition's largest timestamp,
  /// or `log_start_offset()` when the log holds no records (KIP-734
  /// MAX_TIMESTAMP).
  #[must_use]
  pub fn offset_of_max_timestamp(&self) -> i64 {
      self.max_timestamp_offset_and_ts()
          .map_or_else(|| self.log_start_offset(), |(offset, _)| offset)
  }
  ```
- [ ] Run; expect PASS:
  `cargo test -p crabka-log log_offset_of_max_timestamp log_max_timestamp_offset_and_ts`
- [ ] Run the whole log crate to catch regressions; expect PASS:
  `cargo test -p crabka-log`
- [ ] Format, then commit:
  ```
  cargo fmt
  git add crates/log/src/log.rs
  git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(log): Log::offset_of_max_timestamp + max_timestamp_offset_and_ts (KIP-734)"
  ```

### Task 5: Handler wiring + sentinels in `list_offsets.rs`

**Files:** `crates/broker/src/handlers/list_offsets.rs`, `crates/broker/tests/integration.rs`

Add `MAX_TIMESTAMP = -3` and `EARLIEST_LOCAL = -4`; resolve `EARLIEST_LOCAL → local_log_start_offset()`, `MAX_TIMESTAMP → log.offset_of_max_timestamp()`; for `ts > 0` fall back to the local log when the remote tier has no answer; set the response `timestamp` field to the matched record timestamp for `ts > 0` and `MAX_TIMESTAMP` where available (else `-1`).

- [ ] Write a failing broker integration test. Add to `crates/broker/tests/integration.rs` a test that creates a non-tiered topic, produces records with explicit per-record timestamps, and queries by positive timestamp, `EARLIEST_LOCAL` (`-4`), and `MAX_TIMESTAMP` (`-3`). Add this helper near `record_batch_with_values` (it sets explicit per-record timestamps so the by-timestamp lookup has something to match):
  ```rust
  /// One record per `(value, timestamp)` pair. `base_timestamp` is the
  /// first timestamp; each record's `timestamp_delta` reconstructs the
  /// requested absolute timestamp. `max_timestamp` is the largest.
  fn timestamped_batch(entries: &[(&str, i64)]) -> RecordBatch {
      let base_ts = entries.first().map_or(0, |(_, ts)| *ts);
      let max_ts = entries.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
      let len_i32 = i32::try_from(entries.len()).expect("small");
      let mut batch = RecordBatch {
          base_timestamp: base_ts,
          max_timestamp: max_ts,
          last_offset_delta: (len_i32 - 1).max(0),
          ..RecordBatch::default()
      };
      for (i, (v, ts)) in entries.iter().enumerate() {
          batch.records.push(Record {
              offset_delta: i32::try_from(i).expect("small"),
              timestamp_delta: ts - base_ts,
              value: Some(Bytes::from((*v).to_string())),
              ..Default::default()
          });
      }
      batch
  }
  ```
  Then the test:
  ```rust
  #[tokio::test]
  async fn list_offsets_by_timestamp_local() {
      let p = support::start().await;

      p.client
          .send(CreateTopicsRequest {
              topics: vec![CreatableTopic {
                  name: "by_ts".into(),
                  num_partitions: 1,
                  replication_factor: 1,
                  ..Default::default()
              }],
              timeout_ms: 5_000,
              ..Default::default()
          })
          .await
          .unwrap();
      let topic_id = topic_id_for(&p.client, "by_ts").await;

      // Offsets 0..=2 with timestamps 100, 200, 300.
      p.client
          .send(ProduceRequest {
              acks: 1,
              timeout_ms: 5_000,
              topic_data: vec![TopicProduceData {
                  name: "by_ts".into(),
                  topic_id,
                  partition_data: vec![PartitionProduceData {
                      index: 0,
                      records: Some(
                          timestamped_batch(&[("a", 100), ("b", 200), ("c", 300)]).into(),
                      ),
                      ..Default::default()
                  }],
                  ..Default::default()
              }],
              ..Default::default()
          })
          .await
          .unwrap();

      let query = |ts: i64| {
          let client = p.client.clone();
          async move {
              client
                  .send(ListOffsetsRequest {
                      replica_id: -1,
                      topics: vec![ListOffsetsTopic {
                          name: "by_ts".into(),
                          partitions: vec![ListOffsetsPartition {
                              partition_index: 0,
                              timestamp: ts,
                              ..Default::default()
                          }],
                          ..Default::default()
                      }],
                      ..Default::default()
                  })
                  .await
                  .unwrap()
          }
      };

      // Positive timestamp: first record with ts >= 150 is offset 1 (ts 200).
      let r = query(150).await;
      assert_eq!(r.topics[0].partitions[0].error_code, 0);
      assert_eq!(r.topics[0].partitions[0].offset, 1);
      assert_eq!(r.topics[0].partitions[0].timestamp, 200);

      // EARLIEST_LOCAL (-4) → local log start = 0.
      let r = query(-4).await;
      assert_eq!(r.topics[0].partitions[0].offset, 0);

      // MAX_TIMESTAMP (-3) → offset 2 (ts 300), echoes timestamp 300.
      let r = query(-3).await;
      assert_eq!(r.topics[0].partitions[0].offset, 2);
      assert_eq!(r.topics[0].partitions[0].timestamp, 300);
  }
  ```
- [ ] Run it; expect FAIL (positive-ts returns `-1`, `-4`/`-3` return `-1`):
  `cargo test -p crabka-broker --test integration list_offsets_by_timestamp_local`
- [ ] Add the new sentinel consts. In `crates/broker/src/handlers/list_offsets.rs`, replace the const block:
  ```rust
  const EARLIEST: i64 = -2;
  const LATEST: i64 = -1;
  ```
  with:
  ```rust
  const EARLIEST: i64 = -2;
  const LATEST: i64 = -1;
  const MAX_TIMESTAMP: i64 = -3; // KIP-734
  const EARLIEST_LOCAL: i64 = -4; // KIP-405
  ```
- [ ] Capture `local_log_start_offset` in the per-partition log snapshot. Replace this block:
  ```rust
  let (local_start, local_end, remote_storage_enable) = {
      let log = p.log.lock().expect("log mutex poisoned");
      (
          log.log_start_offset(),
          log.log_end_offset(),
          log.config_snapshot().remote_storage_enable,
      )
  };
  ```
  with:
  ```rust
  let (local_start, local_end, local_log_start, remote_storage_enable) = {
      let log = p.log.lock().expect("log mutex poisoned");
      (
          log.log_start_offset(),
          log.log_end_offset(),
          log.local_log_start_offset(),
          log.config_snapshot().remote_storage_enable,
      )
  };
  ```
- [ ] Replace the resolution `match` so it yields both an offset and a response timestamp, resolves the new sentinels, and falls back to the local log for `ts > 0`. Replace the whole `let offset = match part.timestamp { ... };` block (through its closing `};`) **and** the two lines immediately after it:
  ```rust
  out.error_code = codes::NONE;
  out.offset = offset;
  ```
  with this single block (each arm yields an `(offset, response_timestamp)` tuple; `max_timestamp_offset_and_ts` was added on `Log` in Task 4):
  ```rust
  let (offset, resp_timestamp) = match part.timestamp {
      EARLIEST => {
          let mut earliest = local_start;
          if let (Some(reader), Some(tid)) = (remote_reader.as_ref(), topic_id) {
              let tp = crabka_remote_storage::TopicIdPartition::new(
                  tid,
                  topic.name.clone(),
                  idx,
              );
              match reader.earliest_offset(&tp) {
                  Ok(Some(remote_start)) => earliest = earliest.min(remote_start),
                  Ok(None) => {}
                  Err(e) => tracing::warn!(
                      topic = %topic.name, partition = idx, error = %e,
                      "list_offsets: remote earliest_offset failed"
                  ),
              }
          }
          (earliest, -1)
      }
      LATEST => (local_end, -1),
      EARLIEST_LOCAL => (local_log_start, -1),
      MAX_TIMESTAMP => {
          let log = p.log.lock().expect("log mutex poisoned");
          match log.max_timestamp_offset_and_ts() {
              Some((offset, ts)) => (offset, ts),
              None => (log.offset_of_max_timestamp(), -1),
          }
      }
      ts if ts > 0 => {
          let remote_result =
              if let (Some(reader), Some(tid)) = (remote_reader.as_ref(), topic_id) {
                  let tp = crabka_remote_storage::TopicIdPartition::new(
                      tid,
                      topic.name.clone(),
                      idx,
                  );
                  match reader.offset_for_timestamp(&tp, ts).await {
                      Ok(Some(o)) => Some(o),
                      Ok(None) => None,
                      Err(e) => {
                          tracing::warn!(
                              topic = %topic.name, partition = idx, error = %e,
                              "list_offsets: remote offset_for_timestamp failed"
                          );
                          None
                      }
                  }
              } else {
                  None
              };
          if let Some(o) = remote_result {
              // Remote hit covers the oldest records; the remote reader
              // does not surface the matched record timestamp, so echo -1.
              (o, -1)
          } else {
              let local = {
                  let log = p.log.lock().expect("log mutex poisoned");
                  log.offset_for_timestamp(ts)
              };
              local.map_or((-1, -1), |(o, matched_ts)| (o, matched_ts))
          }
      }
      _ => (-1, -1),
  };

  out.error_code = codes::NONE;
  out.offset = offset;
  out.timestamp = resp_timestamp;
  ```
- [ ] Run the broker integration test; expect PASS:
  `cargo test -p crabka-broker --test integration list_offsets_by_timestamp_local`
- [ ] Run the existing end-to-end test to confirm no regression (LATEST still works):
  `cargo test -p crabka-broker --test integration end_to_end_create_produce_fetch_delete`
- [ ] Update the module doc comment at the top of `list_offsets.rs` to drop the now-stale "out of scope and returns -1" note. Replace:
  ```rust
  //! Local-segment timestamp-index lookup (positive timestamps on non-tiered
  //! topics) is still out of scope and returns -1 as before.
  ```
  with:
  ```rust
  //! Positive-timestamp lookups resolve against the remote tier first
  //! (it holds the oldest records) and fall back to the local log's
  //! time index (KIP-405/734). The MAX_TIMESTAMP (-3) and EARLIEST_LOCAL
  //! (-4) sentinels are resolved against the local log.
  ```
- [ ] Format, run clippy on the broker crate, then commit:
  ```
  cargo fmt
  cargo clippy -p crabka-broker --all-targets -- -D warnings
  git add crates/broker/src/handlers/list_offsets.rs crates/broker/tests/integration.rs
  git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): local ListOffsets by-timestamp + MAX_TIMESTAMP/EARLIEST_LOCAL sentinels"
  ```

### Task 6: Final verification

**Files:** none (verification only)

- [ ] Confirm formatting is clean; expect no output / exit 0:
  `cargo fmt --all --check`
- [ ] Run clippy across the workspace; expect no warnings:
  `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] Run the full test suite; expect all PASS:
  `cargo test --workspace`
- [ ] If everything is green, the slice is complete. If any step fails, fix it in place (re-running the relevant `cargo test`/`clippy` command) before committing the fix:
  ```
  cargo fmt
  git add -A
  git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "fix(48n): address verification findings"
  ```
