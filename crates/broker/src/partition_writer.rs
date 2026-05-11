//! Spawned actor task that owns the only `&mut Log` reference (via the
//! shared `Arc<Mutex<Log>>`) and serializes appends for a single partition.
//!
//! Reads bypass the actor — they take the same mutex briefly. The actor's
//! contribution is: ordered acks back to producers + waking long-poll
//! Fetch consumers via a shared `Notify` after every successful append.

// `run` is spawned from `Broker::start` in a later batch.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use crabka_log::Log;
use tokio::sync::{Notify, mpsc};

use crate::partition::ProduceJob;

/// Loop on the receive side of the partition's `ProduceJob` channel.
/// Exits when the channel closes (every sender dropped).
pub async fn run(
    log: Arc<Mutex<Log>>,
    mut rx: mpsc::Receiver<ProduceJob>,
    append_notify: Arc<Notify>,
) {
    while let Some(mut job) = rx.recv().await {
        // Hold the lock only for the duration of `append`. Readers take
        // this same mutex very briefly.
        let result = {
            let mut log = log.lock().expect("log mutex poisoned");
            log.append(&mut job.batch)
                .map_err(crate::error::BrokerError::from)
        };
        let ok = result.is_ok();
        // If the receiver dropped, the handler timed out — that's fine,
        // we don't care if the ack is ignored.
        let _ = job.ack.send(result);
        if ok {
            append_notify.notify_waiters();
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
        tx.send(ProduceJob {
            batch: sample_batch(3),
            ack,
        })
        .await
        .expect("send job");

        let assigned = ack_rx.await.expect("ack recv").expect("append ok");
        assert_eq!(assigned, 0);

        // Second append assigns offset 3.
        let (ack, ack_rx) = oneshot::channel();
        tx.send(ProduceJob {
            batch: sample_batch(2),
            ack,
        })
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
        tx.send(ProduceJob {
            batch: sample_batch(1),
            ack,
        })
        .await
        .expect("send job");

        // Should wake within a short timeout.
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("notify did not fire");

        drop(tx);
        writer.await.expect("writer join");
    }
}
