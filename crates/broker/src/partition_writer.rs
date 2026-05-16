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
use crate::replica_state::ReplicaState;

/// Loop on the receive side of the partition's `WriterMessage` channel.
/// Exits when the channel closes (every sender dropped).
pub async fn run(
    log: Arc<Mutex<Log>>,
    mut rx: mpsc::Receiver<WriterMessage>,
    append_notify: Arc<Notify>,
    replica_state: Arc<tokio::sync::Mutex<ReplicaState>>,
    hw_advance_notify: Arc<Notify>,
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
                    // Re-lock log briefly to read LEO, then update HW.
                    // The log mutex is std::sync (sync callers), held only
                    // during the LEO read. The replica_state mutex is
                    // tokio::sync so we .await it cooperatively.
                    let leader_leo = log.lock().expect("log mutex poisoned").log_end_offset();
                    let advanced = {
                        let mut st = replica_state.lock().await;
                        let prev = st.hw;
                        let new = st.recompute_hw_for_leader_append(leader_leo);
                        new > prev
                    };
                    if advanced {
                        hw_advance_notify.notify_waiters();
                    }
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
            WriterMessage::ResetTo { new_base, ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.reset_to(new_base)
                        .map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
                // No `append_notify` — reset_to drops data rather than
                // delivering it.
            }
            WriterMessage::TrimToOffset { new_start, ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.trim_to_offset(new_start)
                        .map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
                // No `append_notify` — trim drops data rather than producing it.
            }
            WriterMessage::SetLogConfig { config, ack } => {
                log.lock().expect("log mutex poisoned").set_config(config);
                let _ = ack.send(());
            }
            WriterMessage::Compact { ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.compact().map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
                // No `append_notify` — compaction doesn't produce new
                // records, only consolidates existing ones at the same
                // absolute offsets.
            }
            #[cfg(any(test, feature = "test-helpers"))]
            WriterMessage::TestSetLogStart { new_start, ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.set_log_start_offset(new_start)
                        .map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
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
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
        ));

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
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
        ));

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
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
        ));

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
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
        ));

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
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
        ));

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

    #[tokio::test]
    async fn writer_fires_hw_notify_after_produce_when_rf_one() {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        {
            let mut st = replica_state.lock().await;
            st.install_isr(&[1], 1);
        }
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            append_notify.clone(),
            replica_state.clone(),
            hw_advance_notify.clone(),
        ));

        let waiter = hw_advance_notify.notified();
        tokio::pin!(waiter);

        let (ack, _ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            batch: sample_batch(2),
            ack,
        }))
        .await
        .expect("send job");

        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("hw_advance_notify did not fire");

        assert_eq!(replica_state.lock().await.hw, 2);

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_set_log_config_swaps_config() {
        use crabka_log::LogConfig;
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            append_notify,
            replica_state,
            hw_advance_notify,
        ));

        let new_cfg = LogConfig {
            retention_ms: Some(std::time::Duration::from_mins(2)),
            ..LogConfig::default()
        };
        let (ack, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(WriterMessage::SetLogConfig {
            config: new_cfg.clone(),
            ack,
        })
        .await
        .expect("send");
        ack_rx.await.expect("ack");

        let observed = log.lock().expect("lock").config_snapshot();
        assert_eq!(observed.retention_ms, new_cfg.retention_ms);

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_trim_to_offset_advances_log_start() {
        use crabka_log::LogConfig;
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        // Pre-populate with two batches → LEO = 4.
        for _ in 0..2 {
            log.lock()
                .expect("lock")
                .append(&mut sample_batch(2))
                .expect("append");
        }

        let (tx, rx) = mpsc::channel(1);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            append_notify,
            replica_state,
            hw_advance_notify,
        ));

        let (ack, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(WriterMessage::TrimToOffset { new_start: 3, ack })
            .await
            .expect("send");
        let new_start = ack_rx.await.expect("ack").expect("trim ok");
        assert!(new_start >= 3);
        assert_eq!(log.lock().expect("lock").log_start_offset(), new_start);

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_does_not_advance_hw_when_followers_lagging() {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        {
            let mut st = replica_state.lock().await;
            st.install_isr(&[1, 2, 3], 1);
        }
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            append_notify.clone(),
            replica_state.clone(),
            hw_advance_notify.clone(),
        ));

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            batch: sample_batch(3),
            ack,
        }))
        .await
        .expect("send job");
        ack_rx.await.expect("ack").expect("append ok");

        assert_eq!(replica_state.lock().await.hw, 0);

        drop(tx);
        writer.await.expect("writer join");
    }
}
