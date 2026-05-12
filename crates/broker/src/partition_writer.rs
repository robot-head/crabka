//! Spawned actor task that owns the only `&mut Log` reference (via the
//! shared `Arc<Mutex<Log>>`) and serializes appends for a single partition.
//!
//! Reads bypass the actor — they take the same mutex briefly. The actor's
//! contribution is: ordered acks back to producers + waking long-poll
//! Fetch consumers via a shared `Notify` after every successful append.

use std::sync::{Arc, Mutex};

use crabka_log::Log;
use tokio::sync::{Notify, mpsc};

use crate::partition::{ProduceJob, WriterMessage};

/// Loop on the receive side of the partition's `WriterMessage` channel.
/// Exits when the channel closes (every sender dropped).
pub async fn run(
    log: Arc<Mutex<Log>>,
    mut rx: mpsc::Receiver<WriterMessage>,
    append_notify: Arc<Notify>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            WriterMessage::Produce(ProduceJob { mut batch, ack }) => {
                // Hold the lock only for the duration of `append`. Readers
                // take this same mutex very briefly.
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.append(&mut batch)
                        .map_err(crate::error::BrokerError::from)
                };
                let ok = result.is_ok();
                // If the receiver dropped, the handler timed out — that's
                // fine, we don't care if the ack is ignored.
                let _ = ack.send(result);
                if ok {
                    append_notify.notify_waiters();
                }
            }
            WriterMessage::Replicate { mut batch, ack } => {
                // Replication appends preserve the leader-assigned offset
                // (`batch.base_offset`) — `Log::append_at` rejects with
                // `OffsetMismatch` if it doesn't line up with the local
                // log's end.
                let offset = batch.base_offset;
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.append_at(&mut batch, offset)
                        .map_err(crate::error::BrokerError::from)
                };
                let ok = result.is_ok();
                let _ = ack.send(result);
                if ok {
                    append_notify.notify_waiters();
                }
            }
            WriterMessage::Truncate { offset, ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.truncate_to(offset)
                        .map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
                // No `append_notify` — truncate doesn't deliver new data.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_log::LogConfig;
    use crabka_protocol::records::{Record, RecordBatch};
    use tempfile::tempdir;
    use tokio::sync::oneshot;

    fn sample_batch(n: i32) -> RecordBatch {
        let mut b = RecordBatch {
            last_offset_delta: n - 1,
            ..RecordBatch::default()
        };
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                ..Default::default()
            });
        }
        b
    }

    #[tokio::test]
    async fn writer_appends_and_acks() {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(log.clone(), rx, notify.clone()));

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            batch: sample_batch(3),
            ack,
        }))
        .await
        .expect("send job");

        let assigned = ack_rx.await.expect("ack recv").expect("append ok");
        assert_eq!(assigned, 0);

        // Second append assigns offset 3.
        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            batch: sample_batch(2),
            ack,
        }))
        .await
        .expect("send job 2");
        assert_eq!(ack_rx.await.expect("ack recv 2").expect("append 2 ok"), 3);

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_fires_notify_after_append() {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(log.clone(), rx, notify.clone()));

        // Subscribe BEFORE sending so we don't miss the notification.
        let waiter = notify.notified();
        tokio::pin!(waiter);

        let (ack, _ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            batch: sample_batch(1),
            ack,
        }))
        .await
        .expect("send job");

        // Should wake within a short timeout.
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("notify did not fire");

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_handles_replicate_with_caller_offset() {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(log.clone(), rx, notify.clone()));

        // First replicate batch must start at offset 0 to match the
        // empty local log's `log_end_offset()`.
        let mut batch = sample_batch(3);
        batch.base_offset = 0;
        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Replicate { batch, ack })
            .await
            .expect("send replicate");
        ack_rx.await.expect("ack recv").expect("replicate ok");
        assert_eq!(log.lock().unwrap().log_end_offset(), 3);

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_replicate_offset_mismatch_surfaces_error() {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(log.clone(), rx, notify.clone()));

        // Wrong offset — log_end_offset is 0 but we claim 7.
        let mut batch = sample_batch(1);
        batch.base_offset = 7;
        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Replicate { batch, ack })
            .await
            .expect("send replicate");
        let err = ack_rx
            .await
            .expect("ack recv")
            .expect_err("expected offset mismatch");
        assert!(matches!(err, crate::error::BrokerError::Log(_)));
        // Local log must not have advanced.
        assert_eq!(log.lock().unwrap().log_end_offset(), 0);

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_truncate_drops_records() {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(log.clone(), rx, notify.clone()));

        // Produce two batches so the log has some data.
        for _ in 0..2 {
            let (ack, ack_rx) = oneshot::channel();
            tx.send(WriterMessage::Produce(ProduceJob {
                batch: sample_batch(2),
                ack,
            }))
            .await
            .expect("send produce");
            ack_rx.await.expect("ack").expect("ok");
        }
        assert_eq!(log.lock().unwrap().log_end_offset(), 4);

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Truncate { offset: 0, ack })
            .await
            .expect("send truncate");
        ack_rx.await.expect("ack").expect("truncate ok");
        assert_eq!(log.lock().unwrap().log_end_offset(), 0);

        drop(tx);
        writer.await.expect("writer join");
    }
}
