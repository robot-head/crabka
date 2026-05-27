//! [`MetadataEventLog`]: the publish/subscribe seam between the
//! [`TopicBasedRemoteLogMetadataManager`](crate::TopicBasedRemoteLogMetadataManager)
//! and the underlying durable event store.
//!
//! 48f ships one implementation: [`InProcessMetadataEventLog`], an
//! in-memory broadcast-channel fixture used by unit tests (and a
//! single-process model for the multi-broker case — multiple manager
//! instances that share the same fixture observe each other's writes).
//! The production Kafka-backed adapter lands in the broker-integration
//! follow-up.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{Stream, unfold};
use futures_util::{StreamExt, stream};
use tokio::sync::broadcast;

use crate::error::MetadataLogError;

/// One event read from the metadata log.
#[derive(Debug, Clone)]
pub struct MetadataEventRecord {
    /// Metadata-topic partition the event came from.
    pub partition: i32,
    /// Offset within that partition.
    pub offset: i64,
    /// Encoded event payload (see [`crate::serde`]).
    pub payload: Bytes,
}

/// Boxed event stream the [`MetadataEventLog`] hands to subscribers.
pub type MetadataEventStream = Pin<Box<dyn Stream<Item = MetadataEventRecord> + Send + 'static>>;

/// Publish/subscribe transport that backs the `__remote_log_metadata`
/// topic.
///
/// Implementations must guarantee:
///
/// - `publish(p, _)` resolves to a monotonically-increasing offset
///   within partition `p`, and the assigned offset is also what every
///   subscriber observes for that record.
/// - The stream returned by `subscribe` replays the partition from
///   offset 0 and then forwards new records as they are published.
///   Subscribers attached after some records were already published
///   still see the full history.
/// - Records are delivered in publish order on a per-partition basis.
#[async_trait]
pub trait MetadataEventLog: Send + Sync {
    /// Number of partitions the log holds. Stable for the lifetime of
    /// the log; the manager hashes user partitions into
    /// `[0, partition_count())`.
    fn partition_count(&self) -> i32;

    /// Append `event` to `partition`. Resolves to the assigned offset.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError`] if the transport refused the
    /// write — e.g. the partition is out of range, or the log has
    /// been closed.
    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError>;

    /// Subscribe to every partition's events from offset 0 onward.
    fn subscribe(&self) -> MetadataEventStream;

    /// One past the highest written offset for each partition,
    /// indexed by partition.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError`] only on an underlying store
    /// failure; an empty partition is `0`, not an error.
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError>;
}

/// Single-process [`MetadataEventLog`] used by unit tests and as the
/// multi-broker fixture (multiple manager instances cloning the same
/// `Arc` observe each other's writes).
pub struct InProcessMetadataEventLog {
    inner: Arc<InProcessInner>,
}

struct InProcessInner {
    /// `log[partition][offset] = encoded event payload`.
    log: Mutex<Vec<Vec<Bytes>>>,
    /// Notify subscribers of new writes.
    tx: broadcast::Sender<MetadataEventRecord>,
    /// Constant for the life of the log.
    partition_count: i32,
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

    fn subscribe(&self) -> MetadataEventStream {
        // Acquire receiver while holding the partition lock so the
        // snapshot + receiver pair brackets every published record
        // exactly once.
        let guard = self.inner.log.lock().expect("metadata-log mutex poisoned");
        let rx = self.inner.tx.subscribe();
        let snapshot: Vec<MetadataEventRecord> = guard
            .iter()
            .enumerate()
            .flat_map(|(partition, records)| {
                let p = i32::try_from(partition).expect("partition fits in i32");
                records
                    .iter()
                    .enumerate()
                    .map(move |(offset, payload)| MetadataEventRecord {
                        partition: p,
                        offset: i64::try_from(offset).expect("offset fits in i64"),
                        payload: payload.clone(),
                    })
            })
            .collect();
        drop(guard);
        let snapshot_stream = stream::iter(snapshot);
        let forwarded = tokio_stream_from_broadcast(rx);
        snapshot_stream.chain(forwarded).boxed()
    }

    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        let guard = self.inner.log.lock().expect("metadata-log mutex poisoned");
        Ok(guard
            .iter()
            .map(|v| i64::try_from(v.len()).expect("hwm fits in i64"))
            .collect())
    }
}

fn tokio_stream_from_broadcast(
    rx: broadcast::Receiver<MetadataEventRecord>,
) -> MetadataEventStream {
    unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(record) => return Some((record, rx)),
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
    use super::*;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn publish_assigns_monotonic_offsets() {
        let log = InProcessMetadataEventLog::new(2);
        assert_eq!(log.publish(0, Bytes::from_static(b"a")).await.unwrap(), 0);
        assert_eq!(log.publish(0, Bytes::from_static(b"b")).await.unwrap(), 1);
        assert_eq!(log.publish(1, Bytes::from_static(b"c")).await.unwrap(), 0);
        let hwms = log.high_water_marks().await.unwrap();
        assert_eq!(hwms, vec![2, 1]);
    }

    #[tokio::test]
    async fn subscribe_replays_history_then_forwards_new_writes() {
        let log = InProcessMetadataEventLog::new(1);
        log.publish(0, Bytes::from_static(b"a")).await.unwrap();
        log.publish(0, Bytes::from_static(b"b")).await.unwrap();
        let mut stream = log.subscribe();
        let a = stream.next().await.unwrap();
        let b = stream.next().await.unwrap();
        assert_eq!(a.payload.as_ref(), b"a");
        assert_eq!(b.payload.as_ref(), b"b");
        log.publish(0, Bytes::from_static(b"c")).await.unwrap();
        let c = stream.next().await.unwrap();
        assert_eq!(c.payload.as_ref(), b"c");
        assert_eq!((c.partition, c.offset), (0, 2));
    }

    #[tokio::test]
    async fn subscribe_attached_after_history_still_sees_history() {
        let log = InProcessMetadataEventLog::new(1);
        for i in 0..5 {
            log.publish(0, Bytes::copy_from_slice(&[i])).await.unwrap();
        }
        let mut stream = log.subscribe();
        for i in 0..5 {
            let r = stream.next().await.unwrap();
            assert_eq!(r.payload.as_ref(), &[i]);
            assert_eq!(r.offset, i64::from(i));
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
        let mut s1 = log.subscribe();
        let mut s2 = log.subscribe();
        log.publish(0, Bytes::from_static(b"b")).await.unwrap();
        for s in [&mut s1, &mut s2] {
            assert_eq!(s.next().await.unwrap().payload.as_ref(), b"a");
            assert_eq!(s.next().await.unwrap().payload.as_ref(), b"b");
        }
    }
}
