//! Spawned actor task that owns the only `&mut Log` reference (via the
//! shared `Arc<Mutex<Log>>`) and serializes appends for a single partition.
//!
//! Reads bypass the actor — they take the same mutex briefly. The actor's
//! contribution is: ordered acks back to producers + waking long-poll
//! Fetch consumers via a shared `Notify` after every successful append.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use crabka_ids::PartitionIndex;
use crabka_log::{Log, Offset};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::{Notify, mpsc},
};

use crate::{
    log_dir_status::LogDirRegistry,
    partition::{ProduceData, ProduceJob, SwapOutcome, WriterMessage},
    producer_state::ProducerState,
    replica_state::ReplicaState,
};

/// Inactivity window after which an idempotent / transactional producer's
/// in-memory entry is considered expired and excluded from the
/// `RETAIN_EMPTY` active-producer snapshot fed to compaction. Mirrors
/// Kafka's `producer.id.expiration.ms` default (24h). Hard-coded for now;
/// can be wired to a broker config (`producer.id.expiration.ms`) later.
const PRODUCER_ID_EXPIRATION_MS: i64 = 86_400_000;

/// Upper bound on how many queued `Produce` jobs the writer folds into a single
/// group commit (one lock acquisition + one `spawn_blocking`). Caps worst-case
/// memory and per-group latency if a producer floods faster than the writer
/// drains; in practice the group is bounded by the channel backlog and is 1
/// under light load.
const MAX_PRODUCE_GROUP: usize = 1024;

/// Inspect a `BrokerError` returned by a partition-writer mutation
/// (`append`, `append_at`, `truncate_to`, `reset_to`, `compact`,
/// `trim_to_offset`) and, if it looks like an underlying storage
/// failure (a `LogError::Io(_)`), mark the partition's owning log dir
/// offline on the broker-wide registry.
///
/// We err on the side of pessimism: any `io::Error` propagated by the
/// log layer is a credible "the disk just went sideways" signal. A
/// false positive (e.g. a transient `ENOENT` from a misconfigured
/// scratch path) costs one partition's availability — KIP-113 fail-over
/// elsewhere on the cluster keeps the topic live. A false negative
/// silently corrupts produce acks, which is the failure mode this
/// whole slice exists to prevent.
fn flag_storage_failure(
    err: &crate::error::BrokerError,
    log_dir: &ArcSwap<PathBuf>,
    log_dir_status: &LogDirRegistry,
) -> bool {
    if let crate::error::BrokerError::Log(crabka_log::LogError::Io(io_err)) = err {
        let dir = log_dir.load();
        return log_dir_status
            .mark_offline(&dir, &format!("partition write/fsync failed: {io_err}"));
    }
    false
}

/// Lock the partition log, recovering the guard if the mutex was
/// poisoned by a panic in some other critical section.
///
/// In this greenfield single-writer model the log data is consistent
/// enough to keep serving after a poison — the alternative (`expect`)
/// silently kills the writer task (its `JoinHandle` is discarded),
/// taking the whole partition offline. Recovering via `into_inner`
/// keeps the partition live.
fn lock_log(log: &Mutex<Log>) -> std::sync::MutexGuard<'_, Log> {
    log.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Build a `BrokerError` standing in for a panic inside a
/// `spawn_blocking` storage closure. A panic during `write_all` /
/// `fsync` is a credible "the disk just went sideways" signal, so we
/// model it as a `LogError::Io` — which `flag_storage_failure` then
/// recognizes and uses to mark the owning log dir offline.
fn storage_failure_error(
    context: &str,
    detail: impl std::fmt::Display,
) -> crate::error::BrokerError {
    let io = std::io::Error::other(format!("{context}: {detail}"));
    crate::error::BrokerError::Log(crabka_log::LogError::Io(io))
}

/// Append a whole group of produce jobs under a single lock acquisition,
/// returning the per-job results (base offset / error, in input order) plus the
/// post-append log-end offset for the group's HW recompute. Verbatim jobs go
/// straight to `append_verbatim`; owned jobs are recompressed to the topic's
/// configured codec (read once under the same lock). Sequential appends stamp
/// sequential base offsets, so ordering across the group is preserved.
fn append_produce_batch(
    log: &Mutex<Log>,
    datas: Vec<ProduceData>,
) -> (Vec<Result<Offset, crate::error::BrokerError>>, Offset) {
    let mut guard = lock_log(log);
    let target = guard.config_snapshot().compression_type;
    let mut results = Vec::with_capacity(datas.len());
    for data in datas {
        let r = match data {
            ProduceData::Verbatim(batch) => guard
                .append_verbatim(&batch)
                .map_err(crate::error::BrokerError::from),
            ProduceData::Owned(mut batch) => {
                if let Some(target) = target
                    && batch.attributes.compression() != target
                {
                    batch.attributes = batch.attributes.with_compression(target);
                }
                guard
                    .append(&mut batch)
                    .map_err(crate::error::BrokerError::from)
            }
        };
        results.push(r);
    }
    // Read the post-append LEO once under the same lock so the HW recompute
    // reflects the whole group.
    let leo = guard.log_end_offset();
    (results, leo)
}

/// Run [`append_produce_batch`] away from normal async polling. On the broker's
/// multi-thread runtime, `block_in_place` avoids the per-batch `spawn_blocking`
/// scheduling hop while letting Tokio hand the worker's other tasks to a
/// replacement thread; current-thread test runtimes keep the `spawn_blocking`
/// fallback because `block_in_place` is illegal there. The writer loop is still
/// the single serializer for this partition, so append ordering is unchanged.
async fn run_produce_append_batch(
    log: Arc<Mutex<Log>>,
    datas: Vec<ProduceData>,
) -> Result<(Vec<Result<Offset, crate::error::BrokerError>>, Offset), crate::error::BrokerError> {
    match Handle::current().runtime_flavor() {
        RuntimeFlavor::MultiThread => catch_unwind(AssertUnwindSafe(|| {
            tokio::task::block_in_place(move || append_produce_batch(&log, datas))
        }))
        .map_err(|_| storage_failure_error("append task panicked", "block_in_place panic")),
        _ => tokio::task::spawn_blocking(move || append_produce_batch(&log, datas))
            .await
            .map_err(|join_err| storage_failure_error("append task panicked", &join_err)),
    }
}

/// Loop on the receive side of the partition's `WriterMessage` channel.
/// Exits when the channel closes (every sender dropped).
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub async fn run(
    topic: String,
    partition: PartitionIndex,
    log: Arc<Mutex<Log>>,
    log_dir: Arc<ArcSwap<PathBuf>>,
    mut rx: mpsc::Receiver<WriterMessage>,
    append_notify: Arc<Notify>,
    replica_state: Arc<tokio::sync::Mutex<ReplicaState>>,
    hw_advance_notify: Arc<Notify>,
    log_dir_status: LogDirRegistry,
    producer_state: Arc<ProducerState>,
) {
    // `pending` holds a non-Produce message that was pulled off the channel
    // while group-draining Produce jobs (see the Produce arm). It is handled on
    // the next iteration so control messages are never reordered ahead of the
    // produces that preceded them in the channel.
    let mut pending: Option<WriterMessage> = None;
    loop {
        let msg = match pending.take() {
            Some(m) => m,
            None => match rx.recv().await {
                Some(m) => m,
                None => break, // channel closed: every sender dropped
            },
        };
        match msg {
            WriterMessage::Produce(first) => {
                // Group commit. The writer is the single serial appender for
                // this partition, so instead of one append (lock + ack + HW
                // recompute) per produce, drain every Produce job already queued
                // and append them all under one lock acquisition, then fan out
                // their acks. Under small-record load the per-message task
                // handoff dominates CPU (futex wakeups); collapsing it is the
                // win. A non-Produce message met while draining is stashed in
                // `pending` so control messages keep their channel order relative
                // to these appends.
                //
                // `try_recv` only pulls already-queued messages, so a lone
                // produce still appends immediately (no added latency); the
                // batch only grows when the broker is actually backed up.
                let mut jobs = vec![first];
                while jobs.len() < MAX_PRODUCE_GROUP {
                    match rx.try_recv() {
                        Ok(WriterMessage::Produce(j)) => jobs.push(j),
                        Ok(other) => {
                            pending = Some(other);
                            break;
                        }
                        Err(_) => break, // Empty or Disconnected
                    }
                }

                // Split acks from data: the (Send) data moves into the blocking
                // append; the acks stay here, paired by index, to be resolved
                // after it returns.
                let mut acks = Vec::with_capacity(jobs.len());
                let mut datas = Vec::with_capacity(jobs.len());
                for ProduceJob { data, ack } in jobs {
                    acks.push(ack);
                    datas.push(data);
                }

                // Append the whole group under one lock, off the async poller
                // (`block_in_place` on the multi-thread runtime, `spawn_blocking`
                // on current-thread test runtimes — see `run_produce_append_batch`).
                let (results, leo) = match run_produce_append_batch(log.clone(), datas).await {
                    Ok(v) => v,
                    Err(err) => {
                        // The whole append section panicked — treat it as a
                        // storage failure and fail every ack in the group.
                        // `BrokerError` is not `Clone`, so build one per ack.
                        flag_storage_failure(&err, &log_dir, &log_dir_status);
                        for ack in acks {
                            let _ = ack.send(Err(storage_failure_error(
                                "append task panicked",
                                "group append panic",
                            )));
                        }
                        continue;
                    }
                };

                let mut any_ok = false;
                for (ack, result) in acks.into_iter().zip(results) {
                    match &result {
                        Ok(_) => any_ok = true,
                        Err(e) => {
                            flag_storage_failure(e, &log_dir, &log_dir_status);
                        }
                    }
                    // If the receiver dropped, the handler timed out — fine.
                    let _ = ack.send(result);
                }

                if any_ok {
                    // Wake long-poll fetchers once for the whole group.
                    append_notify.notify_waiters();
                    // Advance HW from the final LEO (covers every append in the
                    // group); recompute is idempotent so one notify suffices.
                    let advanced = {
                        let mut st = replica_state.lock().await;
                        let prev = st.hw;
                        let new = st.recompute_hw_for_leader_append(leo);
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
                // Run the blocking `write_all` + `fsync` off the reactor;
                // serial loop + `.await` preserves ordering.
                let log_for_blocking = log.clone();
                let join = tokio::task::spawn_blocking(move || {
                    lock_log(&log_for_blocking)
                        .append_at(&mut batch, Offset(offset))
                        .map_err(crate::error::BrokerError::from)
                });
                let result = match join.await {
                    Ok(v) => v,
                    Err(join_err) => {
                        let err = storage_failure_error("replicate task panicked", &join_err);
                        flag_storage_failure(&err, &log_dir, &log_dir_status);
                        let _ = ack.send(Err(err));
                        continue;
                    }
                };
                let ok = result.is_ok();
                if let Err(ref e) = result {
                    flag_storage_failure(e, &log_dir, &log_dir_status);
                }
                let _ = ack.send(result);
                if ok {
                    append_notify.notify_waiters();
                }
            }
            WriterMessage::Truncate { offset, ack } => {
                // Truncate rewrites segment files + fsyncs — run it off
                // the reactor like the append paths.
                let log_for_blocking = log.clone();
                let join = tokio::task::spawn_blocking(move || {
                    lock_log(&log_for_blocking)
                        .truncate_to(offset)
                        .map_err(crate::error::BrokerError::from)
                });
                let result = match join.await {
                    Ok(v) => v,
                    Err(join_err) => {
                        let err = storage_failure_error("truncate task panicked", &join_err);
                        flag_storage_failure(&err, &log_dir, &log_dir_status);
                        let _ = ack.send(Err(err));
                        continue;
                    }
                };
                let ok = result.is_ok();
                if let Err(ref e) = result {
                    flag_storage_failure(e, &log_dir, &log_dir_status);
                }
                let _ = ack.send(result);
                // No `append_notify` — truncate doesn't deliver new data. But a
                // truncation that drops records below the high-watermark must
                // lower the HW to preserve HW <= LEO: otherwise an acks=all gate
                // can be satisfied by a stale HW pointing past the truncated end
                // of the log (and a consumer told to read vanished offsets).
                if ok {
                    let new_leo = lock_log(&log).log_end_offset();
                    replica_state
                        .lock()
                        .await
                        .recompute_hw_for_leader_append(new_leo);
                }
            }
            WriterMessage::ResetTo { new_base, ack } => {
                let log_for_blocking = log.clone();
                let join = tokio::task::spawn_blocking(move || {
                    lock_log(&log_for_blocking)
                        .reset_to(new_base)
                        .map_err(crate::error::BrokerError::from)
                });
                let result = match join.await {
                    Ok(v) => v,
                    Err(join_err) => {
                        let err = storage_failure_error("reset_to task panicked", &join_err);
                        flag_storage_failure(&err, &log_dir, &log_dir_status);
                        let _ = ack.send(Err(err));
                        continue;
                    }
                };
                let ok = result.is_ok();
                if let Err(ref e) = result {
                    flag_storage_failure(e, &log_dir, &log_dir_status);
                }
                let _ = ack.send(result);
                // No `append_notify` — reset_to drops data rather than
                // delivering it. Clamp the HW to the new (empty-at-new_base) LEO
                // so a stale HW can't satisfy an acks=all gate for a vanished
                // offset (see the Truncate handler).
                if ok {
                    let new_leo = lock_log(&log).log_end_offset();
                    replica_state
                        .lock()
                        .await
                        .recompute_hw_for_leader_append(new_leo);
                }
            }
            WriterMessage::TrimToOffset { new_start, ack } => {
                let log_for_blocking = log.clone();
                let join = tokio::task::spawn_blocking(move || {
                    lock_log(&log_for_blocking)
                        .trim_to_offset(new_start)
                        .map_err(crate::error::BrokerError::from)
                });
                let result = match join.await {
                    Ok(v) => v,
                    Err(join_err) => {
                        let err = storage_failure_error("trim_to_offset task panicked", &join_err);
                        flag_storage_failure(&err, &log_dir, &log_dir_status);
                        let _ = ack.send(Err(err));
                        continue;
                    }
                };
                if let Err(ref e) = result {
                    flag_storage_failure(e, &log_dir, &log_dir_status);
                }
                let _ = ack.send(result);
                // No `append_notify` — trim drops data rather than producing it.
            }
            WriterMessage::SetLogConfig { config, ack } => {
                lock_log(&log).set_config(config);
                let _ = ack.send(());
            }
            WriterMessage::Compact { ack } => {
                // Compaction rewrites segments + fsyncs — off the reactor.
                //
                // The `CompactionContext` carries the wall clock (for KIP-534
                // delete-horizon stamping/expiry) and the set of active
                // producers (so an active producer's last batch is preserved
                // via `RETAIN_EMPTY` even when fully compacted away — its
                // on-disk sequence/epoch state then survives the clean).
                //
                // We stamp `now` once and derive `now_ms` from the same
                // instant so the active-producer inactivity window and the
                // delete-horizon expiry agree on a single wall clock.
                let now = std::time::SystemTime::now();
                let now_ms = now
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
                let active_producers = producer_state
                    .active_snapshot(&topic, partition, now_ms, PRODUCER_ID_EXPIRATION_MS)
                    .await
                    // Wrap the broker-side `i64` pids/last-offsets into the log
                    // layer's `ProducerId`/`Offset` at the compaction seam.
                    .into_iter()
                    .map(|(pid, off)| (crabka_log::ProducerId(pid), Offset(off)))
                    .collect();
                let ctx = crabka_log::CompactionContext {
                    now,
                    active_producers,
                };
                let log_for_blocking = log.clone();
                let join = tokio::task::spawn_blocking(move || {
                    lock_log(&log_for_blocking)
                        .compact(&ctx)
                        .map_err(crate::error::BrokerError::from)
                });
                let result = match join.await {
                    Ok(v) => v,
                    Err(join_err) => {
                        let err = storage_failure_error("compact task panicked", &join_err);
                        flag_storage_failure(&err, &log_dir, &log_dir_status);
                        let _ = ack.send(Err(err));
                        continue;
                    }
                };
                if let Err(ref e) = result {
                    flag_storage_failure(e, &log_dir, &log_dir_status);
                }
                let _ = ack.send(result);
                // No `append_notify` — compaction doesn't produce new
                // records, only consolidates existing ones at the same
                // absolute offsets.
            }
            #[cfg(any(test, feature = "test-helpers"))]
            WriterMessage::TestSetLogStart { new_start, ack } => {
                let result = lock_log(&log)
                    .set_log_start_offset(new_start)
                    .map_err(crate::error::BrokerError::from);
                let _ = ack.send(result);
            }
            WriterMessage::SwapFutureLog {
                target_log_dir,
                future_log,
                future_path,
                target_partition_path,
                ack,
            } => {
                let result = swap_future_log(
                    &log,
                    &log_dir,
                    target_log_dir,
                    &future_log,
                    &future_path,
                    &target_partition_path,
                );
                let _ = ack.send(result);
                // No `append_notify` — swap doesn't deliver new data,
                // and consumers re-read from the swapped `log` against
                // identical offsets.
            }
        }
    }
}

/// KIP-113 intra-broker log-dir swap. Called from the writer task —
/// holds the partition's `log` mutex for the duration of the rename so
/// no other appender sees a half-swapped state.
///
/// The future log MUST be caught up: its LEO == the current log's LEO.
/// If a producer slipped a batch in between the caller's catch-up
/// check and this writer cycle, we report `NotCaughtUp` so the
/// replicator loop drains the lag and retries.
fn swap_future_log(
    log: &Arc<Mutex<Log>>,
    log_dir: &Arc<ArcSwap<PathBuf>>,
    target_log_dir: PathBuf,
    future_log: &Arc<Mutex<Log>>,
    future_path: &std::path::Path,
    target_partition_path: &std::path::Path,
) -> Result<SwapOutcome, crate::error::BrokerError> {
    // Acquire both logs under the writer's serialization and re-check
    // the caught-up invariant. If the future log fell behind between
    // the caller's check and this cycle, refuse the swap and let the
    // replicator catch up.
    let mut log_guard = lock_log(log);
    let config = log_guard.config_snapshot();
    let current_leo = log_guard.log_end_offset();
    let mut future_guard = lock_log(future_log);
    if future_guard.log_end_offset() < current_leo {
        return Ok(SwapOutcome::NotCaughtUp);
    }

    let source_partition_path = log_guard.dir().to_path_buf();

    // Release segment file descriptors on both Logs before mutating
    // the filesystem. `Log::close` consumes the value, so we move
    // both out via `mem::replace` against throwaway Logs anchored to
    // a sacrificial `*.tomb` directory we delete at the end.
    let tomb_dir = future_path.with_extension("crabka-swap-tomb");
    std::fs::create_dir_all(&tomb_dir)?;
    let old_current = std::mem::replace(&mut *log_guard, Log::open(&tomb_dir, config.clone())?);
    old_current.close();
    let old_future = std::mem::replace(&mut *future_guard, Log::open(&tomb_dir, config.clone())?);
    old_future.close();
    drop(future_guard);

    // Atomically promote the future dir into the canonical
    // `<topic>-<partition>` slot under the target log.dir, then
    // remove the source dir. If the rename fails, reopen the source
    // so the partition keeps serving and bubble the error.
    if let Err(e) = std::fs::rename(future_path, target_partition_path) {
        // Best-effort recovery: reopen the original log in the
        // source dir so the partition keeps serving against the
        // pre-swap location.
        match Log::open(&source_partition_path, config) {
            Ok(reopened) => *log_guard = reopened,
            Err(reopen_err) => {
                tracing::error!(
                    error = %reopen_err,
                    "swap_future_log: rename failed AND source reopen failed; \
                     partition is offline until restart"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&tomb_dir);
        return Err(crate::error::BrokerError::from(e));
    }

    if let Err(e) = std::fs::remove_dir_all(&source_partition_path) {
        tracing::warn!(
            source = %source_partition_path.display(),
            error = %e,
            "swap_future_log: failed to remove source partition dir; \
             partition is live at target, source will be cleaned on next restart"
        );
    }
    let _ = std::fs::remove_dir_all(&tomb_dir);

    *log_guard = Log::open(target_partition_path, config)?;
    log_dir.store(Arc::new(target_log_dir));
    Ok(SwapOutcome::Swapped)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_compression::CompressionType;
    use crabka_log::LogConfig;
    use crabka_protocol::records::{Record, RecordBatch};
    use tempfile::tempdir;
    use tokio::sync::oneshot;

    use super::*;

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

    fn open_log_with_records(path: &std::path::Path, records: i32) -> Log {
        let mut log = Log::open(path, LogConfig::default()).expect("open log");
        if records > 0 {
            log.append(&mut sample_batch(records)).expect("append");
        }
        log
    }

    #[test]
    fn flag_storage_failure_marks_io_errors_offline() {
        let dir = tempdir().expect("tempdir");
        let status = crate::log_dir_status::LogDirRegistry::probe(&[dir.path().to_path_buf()]);
        let log_dir = ArcSwap::from_pointee(dir.path().to_path_buf());
        let err = storage_failure_error("append failed", "synthetic EIO");

        assert!(flag_storage_failure(&err, &log_dir, &status));

        assert!(status.is_offline(dir.path()));
        let expected_offline = vec![(
            dir.path().to_path_buf(),
            "partition write/fsync failed: append failed: synthetic EIO".to_string(),
        )];
        assert!(status.offline() == expected_offline);
    }

    #[test]
    fn flag_storage_failure_ignores_non_storage_errors() {
        let dir = tempdir().expect("tempdir");
        let status = crate::log_dir_status::LogDirRegistry::probe(&[dir.path().to_path_buf()]);
        let log_dir = ArcSwap::from_pointee(dir.path().to_path_buf());
        let err = crate::error::BrokerError::UnsupportedApi {
            api_key: 123,
            version: 0,
        };

        check!(!flag_storage_failure(&err, &log_dir, &status));
        check!(!status.is_offline(dir.path()));
        check!(status.offline().is_empty());
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
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(3)),
            ack,
        }))
        .await
        .expect("send job");

        let assigned = ack_rx.await.expect("ack recv").expect("append ok");
        assert!(assigned == Offset(0));

        // Second append assigns offset 3.
        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(2)),
            ack,
        }))
        .await
        .expect("send job 2");
        assert!(ack_rx.await.expect("ack recv 2").expect("append 2 ok") == Offset(3));

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_groups_queued_produces_up_to_configured_cap() {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(MAX_PRODUCE_GROUP);
        let notify = Arc::new(Notify::new());

        let mut acks = Vec::with_capacity(MAX_PRODUCE_GROUP);
        for _ in 0..MAX_PRODUCE_GROUP {
            let (ack, ack_rx) = oneshot::channel();
            tx.send(WriterMessage::Produce(ProduceJob {
                data: ProduceData::Owned(sample_batch(1)),
                ack,
            }))
            .await
            .expect("queue produce");
            acks.push(ack_rx);
        }

        let writer = tokio::spawn(run(
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            notify,
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        let mut acks = acks.into_iter();
        let first = acks.next().expect("first ack");
        assert!(first.await.expect("ack 0").expect("append 0 ok") == Offset(0));
        for (idx, mut ack) in acks.enumerate() {
            let assigned = ack
                .try_recv()
                .expect("same group ack is ready")
                .expect("append ok");
            assert!(assigned == i64::try_from(idx + 1).unwrap());
        }
        assert!(
            log.lock().unwrap().log_end_offset()
                == Offset(i64::try_from(MAX_PRODUCE_GROUP).unwrap())
        );

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writer_appends_and_acks_on_multi_thread_runtime() {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(3)),
            ack,
        }))
        .await
        .expect("send job");

        let assigned = ack_rx.await.expect("ack recv").expect("append ok");
        assert!(assigned == Offset(0));

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_appends_verbatim_byte_exact() {
        use crabka_log::VerbatimBatch;
        use crabka_protocol::records::RecordBatch as ProtoBatch;

        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        // "Producer" batch with a bogus base_offset + epoch the log overwrites.
        let mut producer = sample_batch(1);
        producer.base_offset = 555;
        producer.partition_leader_epoch = -1;
        producer.max_timestamp = 1_234;
        let mut wire = bytes::BytesMut::new();
        producer.encode(&mut wire).unwrap();
        let wire = wire.freeze();

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Verbatim(VerbatimBatch {
                bytes: wire.clone(),
                last_offset_delta: 0,
                max_timestamp: 1_234,
                leader_epoch: crabka_log::LeaderEpoch(5),
                producer_id: crabka_log::ProducerId(-1),
                is_transactional: false,
            }),
            ack,
        }))
        .await
        .expect("send verbatim job");
        let assigned = ack_rx.await.expect("ack").expect("append ok");
        assert!(assigned == Offset(0));

        // Read back: bytes 21.. must equal the producer's, only offset+epoch changed.
        let r = log
            .lock()
            .unwrap()
            .read_raw(Offset(0), Offset(1), 10 * 1024 * 1024)
            .unwrap();
        assert!(&r.bytes[21..] == &wire[21..], "CRC-covered region verbatim");
        assert!(&r.bytes[17..21] == &wire[17..21], "CRC unchanged");
        // Decodes with the assigned offset + stamped epoch.
        let mut cur: &[u8] = &r.bytes;
        let decoded = ProtoBatch::decode(&mut cur).unwrap();
        assert!(decoded.base_offset == 0);
        assert!(decoded.partition_leader_epoch == 5);

        drop(tx);
        writer.await.expect("writer join");
    }

    #[test]
    fn append_owned_batch_recompresses_to_configured_log_codec() {
        let dir = tempdir().expect("tempdir");
        let log = Mutex::new(
            Log::open(
                dir.path(),
                LogConfig {
                    compression_type: Some(CompressionType::Lz4),
                    ..LogConfig::default()
                },
            )
            .expect("open log"),
        );

        let original = sample_batch(2);
        assert!(original.attributes.compression() == CompressionType::None);

        let (results, leo) = append_produce_batch(&log, vec![ProduceData::Owned(original)]);
        assert!(results.len() == 1);
        let assigned = results.into_iter().next().unwrap().expect("append ok");
        assert!(assigned == Offset(0));
        assert!(leo == Offset(2));

        let read = log
            .lock()
            .unwrap()
            .read(Offset(0), 10 * 1024 * 1024)
            .unwrap();
        assert!(read.batches.len() == 1);
        check!(read.batches[0].attributes.compression() == CompressionType::Lz4);
        check!(read.batches[0].records.len() == 2);
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
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        // Subscribe BEFORE sending so we don't miss the notification.
        let waiter = notify.notified();
        tokio::pin!(waiter);

        let (ack, _ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(1)),
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
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
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
        assert!(log.lock().unwrap().log_end_offset() == 3);

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
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
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
        assert!(log.lock().unwrap().log_end_offset() == 0);

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
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            notify.clone(),
            Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            Arc::new(Notify::new()),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        // Produce two batches so the log has some data.
        for _ in 0..2 {
            let (ack, ack_rx) = oneshot::channel();
            tx.send(WriterMessage::Produce(ProduceJob {
                data: ProduceData::Owned(sample_batch(2)),
                ack,
            }))
            .await
            .expect("send produce");
            ack_rx.await.expect("ack").expect("ok");
        }
        assert!(log.lock().unwrap().log_end_offset() == 4);

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Truncate {
            offset: Offset(0),
            ack,
        })
        .await
        .expect("send truncate");
        ack_rx.await.expect("ack").expect("truncate ok");
        assert!(log.lock().unwrap().log_end_offset() == 0);

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
            st.install_isr(
                &[crabka_audit::NodeId(1)],
                &[crabka_audit::NodeId(1)],
                crabka_audit::NodeId(1),
                std::time::Instant::now(),
            );
        }
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            append_notify.clone(),
            replica_state.clone(),
            hw_advance_notify.clone(),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        let waiter = hw_advance_notify.notified();
        tokio::pin!(waiter);

        let (ack, _ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(2)),
            ack,
        }))
        .await
        .expect("send job");

        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("hw_advance_notify did not fire");

        assert!(replica_state.lock().await.hw == Offset(2));

        drop(tx);
        writer.await.expect("writer join");
    }

    #[tokio::test]
    async fn writer_does_not_notify_hw_when_append_leaves_hw_unchanged() {
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
            st.install_isr(
                &[crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
                &[crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
                crabka_audit::NodeId(1),
                std::time::Instant::now(),
            );
        }
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            append_notify,
            replica_state.clone(),
            hw_advance_notify.clone(),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        let waiter = hw_advance_notify.notified();
        tokio::pin!(waiter);

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(1)),
            ack,
        }))
        .await
        .expect("send job");
        ack_rx.await.expect("ack").expect("append ok");

        assert!(replica_state.lock().await.hw == Offset(0));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), waiter)
                .await
                .is_err()
        );

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
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            append_notify,
            replica_state,
            hw_advance_notify,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
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
        assert!(observed.retention_ms == new_cfg.retention_ms);

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
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            append_notify,
            replica_state,
            hw_advance_notify,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        let (ack, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(WriterMessage::TrimToOffset {
            new_start: Offset(3),
            ack,
        })
        .await
        .expect("send");
        let new_start = ack_rx.await.expect("ack").expect("trim ok");
        assert!(new_start >= Offset(3));
        assert!(log.lock().expect("lock").log_start_offset() == new_start);

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
            st.install_isr(
                &[
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                &[
                    crabka_audit::NodeId(1),
                    crabka_audit::NodeId(2),
                    crabka_audit::NodeId(3),
                ],
                crabka_audit::NodeId(1),
                std::time::Instant::now(),
            );
        }
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            "t".to_string(),
            PartitionIndex(0),
            log.clone(),
            Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            rx,
            append_notify.clone(),
            replica_state.clone(),
            hw_advance_notify.clone(),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
        ));

        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(sample_batch(3)),
            ack,
        }))
        .await
        .expect("send job");
        ack_rx.await.expect("ack").expect("append ok");

        assert!(replica_state.lock().await.hw == Offset(0));

        drop(tx);
        writer.await.expect("writer join");
    }

    #[test]
    fn swap_future_log_accepts_future_at_same_leo() {
        let dir = tempdir().expect("tempdir");
        let source_dir = dir.path().join("source");
        let target_dir = dir.path().join("target");
        let source_partition = source_dir.join("t-0");
        let future_path = target_dir.join("t-0.future");
        let target_partition_path = target_dir.join("t-0");

        let log = Arc::new(Mutex::new(open_log_with_records(&source_partition, 2)));
        let future_log = Arc::new(Mutex::new(open_log_with_records(&future_path, 2)));
        let log_dir = Arc::new(ArcSwap::from_pointee(source_dir.clone()));

        let result = swap_future_log(
            &log,
            &log_dir,
            target_dir.clone(),
            &future_log,
            &future_path,
            &target_partition_path,
        )
        .expect("swap");

        // Pull both log observations under one lock acquisition — two
        // `lock()` temporaries in a single assert statement would deadlock.
        let (leo, log_dir_now) = {
            let guard = log.lock().unwrap();
            (guard.log_end_offset(), guard.dir().to_path_buf())
        };
        check!(result == SwapOutcome::Swapped);
        check!(leo == Offset(2));
        check!(log_dir_now == target_partition_path.clone());
        check!(log_dir.load().as_ref().clone() == target_dir);
        check!(!source_partition.exists());
        check!(target_partition_path.exists());
    }

    #[test]
    fn swap_future_log_rejects_future_behind_current_leo() {
        let dir = tempdir().expect("tempdir");
        let source_dir = dir.path().join("source");
        let target_dir = dir.path().join("target");
        let source_partition = source_dir.join("t-0");
        let future_path = target_dir.join("t-0.future");
        let target_partition_path = target_dir.join("t-0");

        let log = Arc::new(Mutex::new(open_log_with_records(&source_partition, 2)));
        let future_log = Arc::new(Mutex::new(open_log_with_records(&future_path, 1)));
        let log_dir = Arc::new(ArcSwap::from_pointee(source_dir.clone()));

        let result = swap_future_log(
            &log,
            &log_dir,
            target_dir,
            &future_log,
            &future_path,
            &target_partition_path,
        )
        .expect("not caught up response");

        // Pull both log observations under one lock acquisition — two
        // `lock()` temporaries in a single assert statement would deadlock.
        let (leo, log_dir_now) = {
            let guard = log.lock().unwrap();
            (guard.log_end_offset(), guard.dir().to_path_buf())
        };
        check!(result == SwapOutcome::NotCaughtUp);
        check!(leo == Offset(2));
        check!(log_dir_now == source_partition.clone());
        check!(log_dir.load().as_ref().clone() == source_dir);
        check!(source_partition.exists());
        check!(future_path.exists());
        check!(!target_partition_path.exists());
    }
}
