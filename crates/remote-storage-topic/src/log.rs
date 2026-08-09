//! [`MetadataEventLog`]: the publish/subscribe seam between the
//! [`TopicBasedRemoteLogMetadataManager`](crate::TopicBasedRemoteLogMetadataManager)
//! and the underlying durable event store.
//!
//! The in-process implementation, [`InProcessMetadataEventLog`], is an
//! in-memory broadcast-channel fixture that unit tests use. It is also a
//! single-process model for the multi-broker case, because multiple manager
//! instances that share the same fixture observe each other's writes. The
//! production Kafka-backed adapter implements the same trait.
//!
//! [`MetadataEventLog::subscribe`] does not consume every partition from
//! offset 0. It takes an explicit [`PartitionStart`] assignment, which is a
//! subset of partitions, each with its own start offset. It returns an
//! [`AssignmentHandle`] that can mutate the live assignment at runtime.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex, atomic::AtomicU64},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{
    StreamExt, stream,
    stream::{Stream, unfold},
};
use tokio::sync::{broadcast, mpsc};

use crate::error::MetadataLogError;

/// One event read from the metadata log.
#[derive(Debug, Clone)]
pub struct MetadataEventRecord {
    /// Metadata-topic partition the event came from.
    pub partition: i32,
    /// Offset within that partition.
    pub offset: i64,
    /// Encoded event payload. See [`crate::serde`].
    pub payload: Bytes,
}

/// Boxed event stream the [`MetadataEventLog`] hands to subscribers.
pub type MetadataEventStream = Pin<Box<dyn Stream<Item = MetadataEventRecord> + Send + 'static>>;

/// One partition to consume and the offset to begin at (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionStart {
    /// Metadata-topic partition to consume.
    pub partition: i32,
    /// First offset to deliver (inclusive). `0` replays from the start.
    pub start_offset: i64,
}

/// Runtime control over a live [`MetadataEventLog`] subscription's assigned
/// partition set. [`MetadataEventLog::subscribe`] returns it together with
/// the stream.
pub trait AssignmentHandle: Send + Sync {
    /// Begin to consume `start.partition` from `start.start_offset`. This
    /// method does nothing if the partition is already assigned. A
    /// newly-added partition emits its backlog from `start_offset` into the
    /// existing stream, and then live records.
    fn add(&self, start: PartitionStart);
    /// Stop the consumption of `partition` and stop the emission of its
    /// events. This method does nothing if the partition is not currently
    /// assigned.
    fn remove(&self, partition: i32);
    /// Current assigned partition set (unordered).
    fn assigned(&self) -> Vec<i32>;
}

/// Publish/subscribe transport that backs the `__remote_log_metadata`
/// topic.
///
/// Implementations must guarantee:
///
/// - `publish(p, _)` resolves to a monotonically-increasing offset
///   within partition `p`, and the assigned offset is also what every
///   subscriber observes for that record.
/// - The stream returned by `subscribe` replays each assigned
///   partition's backlog from its `start_offset` and then forwards new
///   records as they are published for currently-assigned partitions.
///   Subscribers attached after some records were already published
///   still see the history at/after their start offset.
/// - Records are delivered in publish order on a per-partition basis.
#[async_trait]
pub trait MetadataEventLog: Send + Sync {
    /// Number of partitions the log holds. It is stable for the lifetime of
    /// the log. The manager hashes user partitions into
    /// `[0, partition_count())`.
    fn partition_count(&self) -> i32;

    /// Append `event` to `partition`. Resolves to the assigned offset.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError`] if the transport refused the write. That
    /// happens when the partition is out of range, or when the log has been
    /// closed.
    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError>;

    /// Start to consume the given partitions, each from its start offset,
    /// which is inclusive. Returns the event stream and a handle to mutate
    /// the live assignment.
    ///
    /// The stream replays each assigned partition's backlog from its
    /// `start_offset`, then forwards live appends for the currently
    /// assigned partitions. Records are delivered in publish order on
    /// a per-partition basis.
    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>);

    /// One past the highest written offset for each partition,
    /// indexed by partition.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError`] only on an underlying store failure. An
    /// empty partition is `0`, not an error.
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError>;
}

/// Single-process [`MetadataEventLog`] for unit tests and for the
/// multi-broker fixture. Multiple manager instances that clone the same `Arc`
/// observe each other's writes.
pub struct InProcessMetadataEventLog {
    inner: Arc<InProcessInner>,
}

/// Live-assignment cursor for one partition within a subscription.
#[derive(Debug, Clone, Copy)]
struct PartitionCursor {
    /// Next offset that the backlog or live path has NOT yet delivered. The
    /// log filters out records below this offset.
    next: i64,
    /// When set, the log forwards live records for this partition through
    /// the `inject` FIFO rather than emits them directly on the broadcast
    /// path. A partition added mid-stream sets this, so its live appends
    /// queue *behind* its already-injected backlog. Without it,
    /// `stream::select` could interleave a live record ahead of undrained
    /// backlog and violate per-partition publish order. Initially-assigned
    /// partitions leave this `false`. Their backlog goes through the chained
    /// snapshot stream, which fully drains before any live record.
    via_inject: bool,
}

/// Per-subscription live assignment, plus a sender to inject backlog when a
/// partition is added mid-stream. A monotonically-increasing subscription id
/// keys it, so multiple subscribers stay independent.
struct SubscriptionState {
    /// partition -> cursor. Presence in the map means the partition is
    /// assigned.
    assigned: Mutex<HashMap<i32, PartitionCursor>>,
    /// Inject backlog records in FIFO publish order. For `add`-ed partitions
    /// this also injects live records.
    inject: mpsc::UnboundedSender<MetadataEventRecord>,
}

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
    next_sub_id: AtomicU64,
}

impl InProcessMetadataEventLog {
    /// Construct an empty log with `partition_count` partitions.
    ///
    /// # Panics
    ///
    /// Panics when `partition_count <= 0`.
    #[must_use]
    pub fn new(partition_count: i32) -> Arc<Self> {
        assert!(partition_count > 0, "partition_count must be positive");
        let cap = usize::try_from(partition_count).expect("partition_count fits in usize");
        let (tx, _rx) = broadcast::channel(1024);
        Arc::new(Self {
            inner: Arc::new(InProcessInner {
                log: Mutex::new(vec![Vec::new(); cap]),
                tx,
                partition_count,
                subscriptions: Mutex::new(HashMap::new()),
                next_sub_id: AtomicU64::new(0),
            }),
        })
    }
}

#[async_trait]
impl MetadataEventLog for InProcessMetadataEventLog {
    fn partition_count(&self) -> i32 {
        self.inner.partition_count
    }

    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
        if partition < 0 || partition >= self.inner.partition_count {
            return Err(MetadataLogError::PartitionOutOfRange {
                partition,
                count: self.inner.partition_count,
            });
        }
        // Hold the partition lock across the broadcast.send so that any
        // concurrent subscribe() observes either the appended record in
        // its snapshot or as a forwarded broadcast — never both.
        let mut guard = self.inner.log.lock().expect("metadata-log mutex poisoned");
        let idx = usize::try_from(partition).expect("partition non-negative");
        let log_for_p = &mut guard[idx];
        let offset = i64::try_from(log_for_p.len()).expect("offset fits in i64");
        log_for_p.push(event.clone());
        let record = MetadataEventRecord {
            partition,
            offset,
            payload: event,
        };
        // `send` only errors when there are no active receivers; that
        // is fine — the record is still durable in the in-memory log
        // and any later subscriber's snapshot will see it.
        let _ = self.inner.tx.send(record);
        Ok(offset)
    }

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
        let mut assigned: HashMap<i32, PartitionCursor> = HashMap::new();
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
            assigned.insert(
                ps.partition,
                PartitionCursor {
                    next: i64::try_from(records.len()).expect("len fits in i64"),
                    // Initially-assigned: backlog rides the chained
                    // snapshot stream, so live records can go direct.
                    via_inject: false,
                },
            );
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
            .insert(sub_id, Arc::clone(&state));
        drop(guard);

        let snapshot_stream = stream::iter(snapshot);
        let inject_stream = unfold(inject_rx, |mut rx| async move {
            rx.recv().await.map(|r| (r, rx))
        });
        let live = filtered_broadcast(rx, state);
        // Snapshot first (subscribe-time backlog), then a merge of injected
        // backlog (from `add`) and assignment-filtered live records.
        let merged = stream::select(inject_stream, live);
        let stream = snapshot_stream.chain(merged).boxed();

        let handle: Arc<dyn AssignmentHandle> = Arc::new(InProcessAssignmentHandle {
            inner: Arc::clone(&self.inner),
            sub_id,
        });
        (stream, handle)
    }

    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        let guard = self.inner.log.lock().expect("metadata-log mutex poisoned");
        Ok(guard
            .iter()
            .map(|v| i64::try_from(v.len()).expect("hwm fits in i64"))
            .collect())
    }
}

struct InProcessAssignmentHandle {
    inner: Arc<InProcessInner>,
    sub_id: u64,
}

impl Drop for InProcessAssignmentHandle {
    fn drop(&mut self) {
        // Evict this subscription's state so the map does not grow
        // without bound as subscriptions come and go. The stream's live
        // filter holds its own `Arc<SubscriptionState>`, so dropping the
        // map entry never affects an in-flight stream — only `add`/
        // `remove`/`assigned` (which go through the handle) stop working,
        // and the handle is gone.
        if let Ok(mut subs) = self.inner.subscriptions.lock() {
            subs.remove(&self.sub_id);
        }
    }
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
        let log = self.inner.log.lock().expect("metadata-log mutex poisoned");
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
        // broadcast path once `assigned` contains the partition. They are
        // routed through `inject` (via_inject) so they queue *behind* the
        // backlog we just pushed above, preserving per-partition publish
        // order: stream::select must not interleave a live record ahead of
        // undrained backlog.
        let next_live = i64::try_from(records.len()).expect("len fits in i64");
        assigned.insert(
            start.partition,
            PartitionCursor {
                next: next_live,
                via_inject: true,
            },
        );
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

/// What [`filtered_broadcast`] does with a received live record.
enum Forward {
    /// Emit directly on the broadcast stream.
    Emit,
    /// Re-route through the `inject` FIFO for a partition added mid-stream.
    Inject,
    /// Not assigned, or below the cursor. Discard the record.
    Drop,
}

/// Forward a broadcast record only when its partition is currently
/// assigned and its offset is at/after the recorded live cursor for
/// that partition.
///
/// For a partition added mid-stream, which is the `via_inject` case, this
/// function re-routes a passing record into the `inject` FIFO instead of
/// emitting it here. The record then sits behind that partition's
/// already-injected backlog rather than races it through `stream::select`.
fn filtered_broadcast(
    rx: broadcast::Receiver<MetadataEventRecord>,
    state: Arc<SubscriptionState>,
) -> MetadataEventStream {
    unfold((rx, state), |(mut rx, state)| async move {
        loop {
            match rx.recv().await {
                Ok(record) => {
                    let action = {
                        let assigned = state.assigned.lock().expect("assigned mutex poisoned");
                        match assigned.get(&record.partition) {
                            Some(cur) if record.offset >= cur.next => {
                                if cur.via_inject {
                                    Forward::Inject
                                } else {
                                    Forward::Emit
                                }
                            }
                            _ => Forward::Drop,
                        }
                    };
                    match action {
                        Forward::Emit => return Some((record, (rx, state))),
                        Forward::Inject => {
                            // Queue behind backlog; if the receiver is gone
                            // the stream is being dropped anyway.
                            let _ = state.inject.send(record);
                        }
                        Forward::Drop => {}
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // The in-memory snapshot already supplied earlier
                    // records; a lag only happens when the consumer
                    // pump fell behind a single-process write burst
                    // that overflowed the broadcast capacity (1024).
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use futures_util::StreamExt;

    use super::*;

    #[tokio::test]
    async fn publish_assigns_monotonic_offsets() {
        let log = InProcessMetadataEventLog::new(2);
        check!(log.publish(0, Bytes::from_static(b"a")).await.unwrap() == 0);
        check!(log.publish(0, Bytes::from_static(b"b")).await.unwrap() == 1);
        check!(log.publish(1, Bytes::from_static(b"c")).await.unwrap() == 0);
        let hwms = log.high_water_marks().await.unwrap();
        assert!(hwms == vec![2, 1]);
    }

    #[tokio::test]
    async fn subscribe_replays_history_then_forwards_new_writes() {
        let log = InProcessMetadataEventLog::new(1);
        log.publish(0, Bytes::from_static(b"a")).await.unwrap();
        log.publish(0, Bytes::from_static(b"b")).await.unwrap();
        let (mut stream, _h) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        let a = stream.next().await.unwrap();
        let b = stream.next().await.unwrap();
        assert!(a.payload.as_ref() == b"a");
        assert!(b.payload.as_ref() == b"b");
        log.publish(0, Bytes::from_static(b"c")).await.unwrap();
        let c = stream.next().await.unwrap();
        check!(c.payload.as_ref() == b"c");
        check!(c.partition == 0);
        check!(c.offset == 2);
    }

    #[tokio::test]
    async fn subscribe_attached_after_history_still_sees_history() {
        let log = InProcessMetadataEventLog::new(1);
        for i in 0..5 {
            log.publish(0, Bytes::copy_from_slice(&[i])).await.unwrap();
        }
        let (mut stream, _h) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        for i in 0..5 {
            let r = stream.next().await.unwrap();
            assert!(r.payload.as_ref() == &[i]);
            assert!(r.offset == i64::from(i));
        }
    }

    #[tokio::test]
    async fn publish_out_of_range_is_rejected() {
        let log = InProcessMetadataEventLog::new(2);
        let err = log.publish(5, Bytes::from_static(b"x")).await.unwrap_err();
        assert!(matches!(err, MetadataLogError::PartitionOutOfRange { .. }));
    }

    #[tokio::test]
    async fn two_subscribers_see_the_same_history() {
        let log = InProcessMetadataEventLog::new(1);
        log.publish(0, Bytes::from_static(b"a")).await.unwrap();
        let (mut s1, _h1) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        let (mut s2, _h2) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        log.publish(0, Bytes::from_static(b"b")).await.unwrap();
        for s in [&mut s1, &mut s2] {
            assert!(s.next().await.unwrap().payload.as_ref() == b"a");
            assert!(s.next().await.unwrap().payload.as_ref() == b"b");
        }
    }

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
            PartitionStart {
                partition: 0,
                start_offset: 1,
            },
            PartitionStart {
                partition: 1,
                start_offset: 0,
            },
        ]);

        let mut got: Vec<(i32, i64, Vec<u8>)> = Vec::new();
        for _ in 0..3 {
            let r = stream.next().await.unwrap();
            got.push((r.partition, r.offset, r.payload.to_vec()));
        }
        got.sort();
        assert!(
            got == vec![
                (0, 1, b"b".to_vec()),
                (0, 2, b"c".to_vec()),
                (1, 0, b"x".to_vec()),
            ]
        );
        // partition 1 offset 1 ("y") is the only remaining assigned record.
        let r = stream.next().await.unwrap();
        check!(r.partition == 1);
        check!(r.offset == 1);
        check!(r.payload.as_ref() == b"y");
    }

    #[tokio::test]
    async fn live_appends_only_for_assigned_partitions() {
        let log = InProcessMetadataEventLog::new(2);
        let (mut stream, _h) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        // Unassigned partition write must not appear.
        log.publish(1, Bytes::from_static(b"skip")).await.unwrap();
        log.publish(0, Bytes::from_static(b"keep")).await.unwrap();
        let r = stream.next().await.unwrap();
        check!(r.partition == 0);
        check!(r.payload.as_ref() == b"keep");
    }

    #[tokio::test]
    async fn add_mid_stream_delivers_backlog_then_live() {
        let log = InProcessMetadataEventLog::new(2);
        // Three backlog records on partition 1 (offsets 0,1,2).
        for v in [b"old0".as_slice(), b"old1", b"old2"] {
            log.publish(1, Bytes::copy_from_slice(v)).await.unwrap();
        }
        let (mut stream, handle) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        // Add partition 1 from offset 0, then publish a live append
        // IMMEDIATELY — without first draining the injected backlog.
        //
        // This is the ordering trap. The merged stream is
        // `stream::select(inject_stream, live)`, which round-robins
        // between its two inputs when both have a ready item. Under the
        // OLD behavior the live "new" (offset 3) is emitted by the `live`
        // input directly, so select interleaves it between backlog items:
        // old0(inject), new(live), old1(inject), old2(inject) — "new"
        // jumps ahead of old1/old2, violating per-partition publish
        // order. The fix routes a just-added partition's live records
        // through the SAME inject FIFO, so they queue strictly behind the
        // backlog: old0, old1, old2, new.
        handle.add(PartitionStart {
            partition: 1,
            start_offset: 0,
        });
        log.publish(1, Bytes::from_static(b"new")).await.unwrap();

        let mut got = Vec::new();
        for _ in 0..4 {
            let r = stream.next().await.unwrap();
            got.push((r.partition, r.offset, r.payload.to_vec()));
        }
        assert!(
            got == vec![
                (1, 0, b"old0".to_vec()),
                (1, 1, b"old1".to_vec()),
                (1, 2, b"old2".to_vec()),
                (1, 3, b"new".to_vec()),
            ],
            "backlog must drain fully (in offset order) before the live append"
        );
        assert!(handle.assigned().contains(&1));
    }

    #[tokio::test]
    async fn dropping_handle_evicts_subscription_state() {
        let log = InProcessMetadataEventLog::new(1);
        let (_stream, handle) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        assert!(log.inner.subscriptions.lock().unwrap().len() == 1);
        drop(handle);
        assert!(
            log.inner.subscriptions.lock().unwrap().len() == 0,
            "subscription state must be evicted when the handle drops"
        );
    }

    #[tokio::test]
    async fn remove_stops_delivery() {
        let log = InProcessMetadataEventLog::new(2);
        let (mut stream, handle) = log.subscribe(vec![
            PartitionStart {
                partition: 0,
                start_offset: 0,
            },
            PartitionStart {
                partition: 1,
                start_offset: 0,
            },
        ]);
        handle.remove(1);
        assert!(handle.assigned() == vec![0]);
        log.publish(1, Bytes::from_static(b"gone")).await.unwrap();
        log.publish(0, Bytes::from_static(b"here")).await.unwrap();
        let r = stream.next().await.unwrap();
        check!(r.partition == 0);
        check!(r.payload.as_ref() == b"here");
    }
}
