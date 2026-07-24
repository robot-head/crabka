//! KIP-113 (`AlterReplicaLogDirs`): intra-broker log-dir reassignment.
//!
//! When the `AlterReplicaLogDirs` handler accepts a move
//! `(topic, partition) → target log.dir`, it asks this module to:
//!
//! 1. Open a fresh `crabka_log::Log` at
//!    `<target_log_dir>/<topic>-<partition>-future/`.
//! 2. Spawn a per-move replicator task that reads batches from the
//!    partition's current `Log` and appends them to the future log via
//!    `Log::append_at`, preserving leader-assigned offsets.
//! 3. Once `future_log.LEO == current_log.LEO`, ask the partition
//!    writer to swap atomically via `WriterMessage::SwapFutureLog`.
//!
//! The on-disk `*-future` directory is the only persisted state. A
//! crash mid-move leaves it behind; broker startup re-discovers it via
//! `log_dir::scan_future` and re-spawns the replicator.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use crabka_ids::PartitionIndex;
use crabka_log::{Log, LogConfig, Offset};
use dashmap::DashMap;
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    error::BrokerError,
    log_dir,
    partition::{Partition, SwapOutcome, WriterMessage},
    partition_registry::PartitionRegistry,
};

/// One in-progress intra-broker log-dir move. Inserted into
/// `Broker.future_logs` keyed by `(topic, partition)`.
///
/// Fields are held to keep ownership of the future log alive and to
/// allow `DescribeLogDirs` + future cancellation paths to consult the
/// move's state through the registry; the writer task consumes them
/// indirectly via the `SwapFutureLog` message, which Rust's dead-code
/// pass can't see through.
#[allow(dead_code)]
#[derive(Debug)]
pub struct FutureLogState {
    /// Parent `log.dir` the move targets — one of the broker's
    /// configured `log.dirs`. Used by the handler to make a duplicate
    /// `AlterReplicaLogDirs` for the same `(topic, partition)`
    /// idempotent (or reject a conflicting target).
    pub target_log_dir: PathBuf,
    /// The future log's `<target>/<topic>-<partition>-future` path.
    pub future_path: PathBuf,
    /// The future log itself. Shared with the replicator task and the
    /// `SwapFutureLog` writer message so all three hold the same
    /// `Arc<Mutex<Log>>`.
    pub future_log: Arc<Mutex<Log>>,
    /// Cancelled by the swap to unwind the replicator task. Also
    /// cancelled if a follow-up `AlterReplicaLogDirs` cancels an
    /// in-progress move (not implemented; future work).
    pub cancel: CancellationToken,
    /// Kept alive so the replicator task is reaped when the entry is
    /// removed from the registry.
    pub task: std::sync::Mutex<Option<JoinHandle<()>>>,
}

/// Why a [`start_move`] / [`resume_move`] call could not be honoured.
/// Translated to the wire error codes
/// [`crate::codes::LOG_DIR_NOT_FOUND`],
/// [`crate::codes::REPLICA_NOT_AVAILABLE`],
/// [`crate::codes::KAFKA_STORAGE_ERROR`] by the handler.
#[derive(Debug)]
pub enum MoveError {
    /// Target path is not one of this broker's configured `log.dirs`.
    LogDirNotFound,
    /// The named partition is not hosted on this broker.
    ReplicaNotAvailable,
    /// A different move is already in flight for this partition with
    /// a different target. Matches Kafka — a second alter only takes
    /// effect after the first move completes (or is cancelled).
    AlreadyMoving,
    /// `crabka_log::Log::open` or `mkdir` failed while staging the
    /// future log. The inner error is held for tracing / future use;
    /// the handler maps every storage failure to `KAFKA_STORAGE_ERROR`
    /// on the wire.
    Storage(#[allow(dead_code)] BrokerError),
}

impl From<BrokerError> for MoveError {
    fn from(e: BrokerError) -> Self {
        MoveError::Storage(e)
    }
}

impl From<crabka_log::LogError> for MoveError {
    fn from(e: crabka_log::LogError) -> Self {
        MoveError::Storage(BrokerError::from(e))
    }
}

impl From<std::io::Error> for MoveError {
    fn from(e: std::io::Error) -> Self {
        MoveError::Storage(BrokerError::from(e))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MovePolicy {
    pub retry_backoff: Duration,
    pub read_chunk_bytes: usize,
}

/// Start (or no-op-confirm) a move of `(topic, partition)` to
/// `target_log_dir`. Returns immediately after spawning the replicator
/// task; the `AlterReplicaLogDirs` handler can then ack success.
///
/// Idempotency: if a move with the same target is already in flight,
/// returns `Ok(())` without spawning a second task. A move with a
/// *different* target returns `Err(MoveError::AlreadyMoving)`.
pub(crate) fn start_move(
    partitions: &Arc<PartitionRegistry>,
    future_logs: &Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    all_log_dirs: &[PathBuf],
    log_config: &LogConfig,
    topic_partition: (&str, PartitionIndex),
    target_log_dir: &Path,
    policy: MovePolicy,
) -> Result<(), MoveError> {
    let (topic, partition) = topic_partition;
    // (1) Validate the target is a configured log.dir. Path comparison
    //     is canonical-form to side-step trailing-slash / `.` quirks.
    let target_canon = canonicalize_or_self(target_log_dir);
    let target_match = all_log_dirs
        .iter()
        .find(|d| canonicalize_or_self(d) == target_canon)
        .cloned();
    let Some(target_log_dir) = target_match else {
        return Err(MoveError::LogDirNotFound);
    };

    // (2) Partition must be hosted on this broker.
    let key = (topic.to_string(), partition);
    let part = partitions
        .get(topic, partition)
        .ok_or(MoveError::ReplicaNotAvailable)?;

    // (3) Already in the target dir? No-op success.
    let current_log_dir = part.log_dir.load_full();
    if canonicalize_or_self(&current_log_dir) == canonicalize_or_self(&target_log_dir) {
        return Ok(());
    }

    // (4) Already moving? Idempotent for same target, conflict for
    //     different target.
    if let Some(existing) = future_logs.get(&key).map(|e| e.value().clone()) {
        if canonicalize_or_self(&existing.target_log_dir) == canonicalize_or_self(&target_log_dir) {
            return Ok(());
        }
        return Err(MoveError::AlreadyMoving);
    }

    // (5) Open the future log at <target>/<topic>-<partition>-future.
    let future_path = log_dir::future_partition_dir(&target_log_dir, topic, partition.get());
    std::fs::create_dir_all(&future_path)?;
    let future_log = Arc::new(Mutex::new(Log::open(&future_path, log_config.clone())?));

    spawn_move(MoveTask {
        partitions: partitions.clone(),
        future_logs: future_logs.clone(),
        target_log_dir,
        future_path,
        future_log,
        topic: topic.to_string(),
        partition,
        part,
        policy,
    });
    Ok(())
}

/// Recover an interrupted move discovered on disk at broker startup
/// (a `<topic>-<partition>-future` directory in a configured log.dir
/// whose corresponding partition exists). Re-opens the future log
/// and re-spawns the replicator, picking up at whatever offset the
/// future log already holds.
pub(crate) fn resume_move(
    partitions: &Arc<PartitionRegistry>,
    future_logs: &Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    target_log_dir: &Path,
    log_config: &LogConfig,
    topic: &str,
    partition: PartitionIndex,
    policy: MovePolicy,
) -> Result<(), MoveError> {
    let part = partitions
        .get(topic, partition)
        .ok_or(MoveError::ReplicaNotAvailable)?;
    let future_path = log_dir::future_partition_dir(target_log_dir, topic, partition.get());
    let future_log = Arc::new(Mutex::new(Log::open(&future_path, log_config.clone())?));
    spawn_move(MoveTask {
        partitions: partitions.clone(),
        future_logs: future_logs.clone(),
        target_log_dir: target_log_dir.to_path_buf(),
        future_path,
        future_log,
        topic: topic.to_string(),
        partition,
        part,
        policy,
    });
    Ok(())
}

/// Shared between [`start_move`] and [`resume_move`]: build the
/// `FutureLogState`, insert it into the registry, and spawn the
/// per-move replicator task.
struct MoveTask {
    partitions: Arc<PartitionRegistry>,
    future_logs: Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    target_log_dir: PathBuf,
    future_path: PathBuf,
    future_log: Arc<Mutex<Log>>,
    topic: String,
    partition: PartitionIndex,
    part: Arc<Partition>,
    policy: MovePolicy,
}

fn spawn_move(task: MoveTask) {
    let cancel = CancellationToken::new();
    let target_partition_path =
        log_dir::partition_dir(&task.target_log_dir, &task.topic, task.partition.get());
    let replicator = tokio::spawn(replicator_loop(ReplicatorTask {
        part: task.part,
        future_log: task.future_log.clone(),
        future_path: task.future_path.clone(),
        target_partition_path,
        target_log_dir: task.target_log_dir.clone(),
        cancel: cancel.clone(),
        _partitions: task.partitions,
        future_logs: task.future_logs.clone(),
        topic: task.topic.clone(),
        partition: task.partition,
        policy: task.policy,
    }));
    let state = Arc::new(FutureLogState {
        target_log_dir: task.target_log_dir,
        future_path: task.future_path,
        future_log: task.future_log,
        cancel,
        task: std::sync::Mutex::new(Some(replicator)),
    });
    task.future_logs.insert((task.topic, task.partition), state);
}

/// Replicator task body: incrementally copy batches from
/// `part.log` to `future_log`, then ask the partition writer to swap.
struct ReplicatorTask {
    part: Arc<Partition>,
    future_log: Arc<Mutex<Log>>,
    future_path: PathBuf,
    target_partition_path: PathBuf,
    target_log_dir: PathBuf,
    cancel: CancellationToken,
    _partitions: Arc<PartitionRegistry>,
    future_logs: Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    topic: String,
    partition: PartitionIndex,
    policy: MovePolicy,
}

async fn replicator_loop(task: ReplicatorTask) {
    let ReplicatorTask {
        part,
        future_log,
        future_path,
        target_partition_path,
        target_log_dir,
        cancel,
        _partitions,
        future_logs,
        topic,
        partition,
        policy,
    } = task;
    debug!(
        topic = %topic, partition = partition.get(),
        target = %target_log_dir.display(),
        "future-log replicator started"
    );
    loop {
        if cancel.is_cancelled() {
            break;
        }
        // Read whatever is missing from the future log up to the
        // source's current LEO.
        let advance = match catch_up(&part, &future_log, policy.read_chunk_bytes) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    topic = %topic, partition = partition.get(),
                    error = %e,
                    "future-log replicator catch-up failed; retrying"
                );
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(policy.retry_backoff) => continue,
                }
            }
        };

        if !advance.caught_up {
            // Make forward progress, then immediately re-check. We
            // only wait on `append_notify` once we believe we are
            // caught up.
            continue;
        }

        // We believe we're caught up; ask the writer to swap.
        let (ack_tx, ack_rx) = oneshot::channel();
        let send = part
            .writer_tx
            .send(WriterMessage::SwapFutureLog {
                target_log_dir: target_log_dir.clone(),
                future_log: future_log.clone(),
                future_path: future_path.clone(),
                target_partition_path: target_partition_path.clone(),
                ack: ack_tx,
            })
            .await;
        if send.is_err() {
            warn!(
                topic = %topic, partition = partition.get(),
                "future-log replicator: partition writer is dead; aborting move"
            );
            break;
        }
        match ack_rx.await {
            Ok(Ok(SwapOutcome::Swapped)) => {
                debug!(topic = %topic, partition = partition.get(), "future-log swap complete");
                break;
            }
            Ok(Ok(SwapOutcome::NotCaughtUp)) => {
                // Producers wrote in between catch_up and the writer
                // receiving the message — loop and try again.
            }
            Ok(Err(e)) => {
                warn!(
                    topic = %topic, partition = partition.get(),
                    error = %e,
                    "future-log swap failed; aborting move (partition continues on source dir)"
                );
                break;
            }
            Err(_) => {
                warn!(topic = %topic, partition = partition.get(), "future-log swap ack dropped");
                break;
            }
        }

        // Wait for the next append (or cancellation) before retrying.
        tokio::select! {
            () = cancel.cancelled() => break,
            () = part.append_notify.notified() => {}
        }
    }
    // Whatever the outcome, the future-log entry is no longer useful.
    future_logs.remove(&(topic, partition));
}

/// One catch-up iteration: read whatever the future log is missing,
/// up to `read_chunk_bytes`, and append it. Returns whether the
/// future log was caught up at the end of the iteration (i.e. nothing
/// was read AND `future.LEO >= source.LEO`).
struct CatchUpProgress {
    caught_up: bool,
}

fn catch_up(
    part: &Arc<Partition>,
    future_log: &Arc<Mutex<Log>>,
    read_chunk_bytes: usize,
) -> Result<CatchUpProgress, BrokerError> {
    // Snapshot offsets cheaply; the partition log mutex is dropped
    // immediately after each helper.
    let current_leo = part.log_end_offset();
    // Recover the guard if a panic elsewhere poisoned the mutex rather
    // than killing this (discarded-JoinHandle) replicator task.
    let future_leo = future_log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .log_end_offset();
    if future_leo >= current_leo {
        return Ok(CatchUpProgress { caught_up: true });
    }

    // Pull the next chunk of batches from the source.
    let read = {
        let log = part
            .log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        log.read(future_leo, read_chunk_bytes)?
    };
    if read.batches.is_empty() {
        // Source advanced its log_start past `future_leo` (retention
        // or trim). Treat as caught up for this iteration; on the
        // next pass `future_leo` will equal `current_leo` and we'll
        // swap. Realistically the future log would need to be reset
        // — KIP-113 doesn't specify this corner; we treat it as a
        // soft no-op.
        return Ok(CatchUpProgress { caught_up: true });
    }

    let mut future = future_log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for mut batch in read.batches {
        let base = batch.base_offset;
        future
            .append_at(&mut batch, Offset(base))
            .map_err(BrokerError::from)?;
    }
    Ok(CatchUpProgress { caught_up: false })
}

/// Canonicalize a path for equality comparisons; falls back to the
/// lexical path when canonicalisation fails (the directory may not
/// exist yet — fine for log-dir comparisons since we compare via the
/// configured value as well).
fn canonicalize_or_self(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    fn test_policy() -> MovePolicy {
        MovePolicy {
            retry_backoff: Duration::from_millis(5),
            read_chunk_bytes: 1024 * 1024,
        }
    }

    #[test]
    fn move_policy_preserves_nondefault_values() {
        let policy = MovePolicy {
            retry_backoff: Duration::from_millis(7),
            read_chunk_bytes: 4096,
        };

        assert!(policy.retry_backoff == Duration::from_millis(7));
        assert!(policy.read_chunk_bytes == 4096);
    }

    #[test]
    fn move_error_log_dir_not_found_when_target_unknown() {
        // Empty broker — no partitions, no log dirs. `start_move`
        // returns LogDirNotFound before it ever looks at the
        // partition map.
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let log_dirs: Vec<PathBuf> = vec![];
        let bogus = tempdir().unwrap();
        let err = start_move(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            bogus.path(),
            test_policy(),
        )
        .expect_err("expected LogDirNotFound");
        assert!(matches!(err, MoveError::LogDirNotFound));
    }

    #[test]
    fn move_error_replica_not_available_when_partition_missing() {
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let dir = tempdir().unwrap();
        let err = start_move(
            &partitions,
            &future_logs,
            &[dir.path().to_path_buf()],
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            dir.path(),
            test_policy(),
        )
        .expect_err("expected ReplicaNotAvailable");
        assert!(matches!(err, MoveError::ReplicaNotAvailable));
    }

    /// Build a `Partition` rooted at `<log_dir>/<topic>-<partition>`
    /// without going through `Broker::start`. Returns the parent dir
    /// and the `Arc<Partition>`.
    fn fixture_partition(log_dir: &Path, topic: &str, partition: PartitionIndex) -> Arc<Partition> {
        let part_dir = log_dir::partition_dir(log_dir, topic, partition.get());
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = Log::open(&part_dir, LogConfig::default()).unwrap();
        crate::broker::spawn_partition(
            topic.to_string(),
            partition,
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        )
    }

    fn append_records(part: &Arc<Partition>, count: i32) {
        use bytes::Bytes;
        use crabka_protocol::records::{Attributes, Record, RecordBatch};

        let mut batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default(),
            last_offset_delta: count - 1,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: (0..count)
                .map(|i| Record {
                    attributes: 0,
                    offset_delta: i,
                    timestamp_delta: 0,
                    key: None,
                    value: Some(Bytes::from_static(b"v")),
                    headers: vec![],
                })
                .collect(),
        };
        part.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .append(&mut batch)
            .expect("append source records");
    }

    fn append_value_batch(part: &Arc<Partition>, value_size: usize) {
        use bytes::Bytes;
        use crabka_protocol::records::{Attributes, Record, RecordBatch};

        let mut batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default(),
            last_offset_delta: 0,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                attributes: 0,
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from(vec![b'x'; value_size])),
                headers: vec![],
            }],
        };
        part.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .append(&mut batch)
            .expect("append source batch");
    }

    #[tokio::test]
    async fn start_move_to_current_dir_is_noop() {
        // Asking to move a partition to the directory it already
        // lives in returns success without touching `future_logs`.
        let primary = tempdir().unwrap();
        let extra = tempdir().unwrap();
        let log_dirs = vec![primary.path().to_path_buf(), extra.path().to_path_buf()];
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        partitions.insert("t".to_string(), PartitionIndex(0), part);

        start_move(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            primary.path(),
            test_policy(),
        )
        .expect("noop should succeed");
        assert!(
            future_logs.is_empty(),
            "noop must not register a future log"
        );
    }

    #[test]
    fn resume_move_errors_when_partition_missing() {
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let target = tempdir().unwrap();

        let err = resume_move(
            &partitions,
            &future_logs,
            target.path(),
            &LogConfig::default(),
            "missing",
            PartitionIndex(0),
            test_policy(),
        )
        .expect_err("missing partition must reject resume");

        assert!(matches!(err, MoveError::ReplicaNotAvailable));
        assert!(future_logs.is_empty());
    }

    #[tokio::test]
    async fn resume_move_catches_up_and_swaps_future_log() {
        let primary = tempdir().unwrap();
        let target = tempdir().unwrap();
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        append_records(&part, 3);
        partitions.insert("t".to_string(), PartitionIndex(0), part.clone());

        let future_path = log_dir::future_partition_dir(target.path(), "t", 0);
        std::fs::create_dir_all(&future_path).unwrap();

        resume_move(
            &partitions,
            &future_logs,
            target.path(),
            &LogConfig::default(),
            "t",
            PartitionIndex(0),
            test_policy(),
        )
        .expect("resume should spawn a future-log move");

        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                let moved = canonicalize_or_self(&part.log_dir.load_full())
                    == canonicalize_or_self(target.path());
                if moved && future_logs.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("future log should catch up and swap");

        assert!(part.log_end_offset() == 3);
        assert!(
            canonicalize_or_self(&part.log_dir.load_full()) == canonicalize_or_self(target.path())
        );
    }

    #[tokio::test]
    async fn resume_move_continues_after_partial_catch_up() {
        let primary = tempdir().unwrap();
        let target = tempdir().unwrap();
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        for _ in 0..4 {
            append_value_batch(&part, 400 * 1024);
        }
        partitions.insert("t".to_string(), PartitionIndex(0), part.clone());

        let future_path = log_dir::future_partition_dir(target.path(), "t", 0);
        std::fs::create_dir_all(&future_path).unwrap();

        resume_move(
            &partitions,
            &future_logs,
            target.path(),
            &LogConfig::default(),
            "t",
            PartitionIndex(0),
            test_policy(),
        )
        .expect("resume should spawn a future-log move");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let moved = canonicalize_or_self(&part.log_dir.load_full())
                    == canonicalize_or_self(target.path());
                if moved && future_logs.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("future log should keep copying after a partial catch-up pass");

        assert!(part.log_end_offset() == 4);
    }

    #[tokio::test]
    async fn start_move_idempotent_for_same_target() {
        // Two ARLD calls with the same target while the first move is
        // still in flight collapse to one — second call returns Ok(())
        // and the registry still has one entry.
        let primary = tempdir().unwrap();
        let extra = tempdir().unwrap();
        let log_dirs = vec![primary.path().to_path_buf(), extra.path().to_path_buf()];
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        partitions.insert("t".to_string(), PartitionIndex(0), part);

        // Plant a registry entry as if a prior ARLD already kicked off
        // a move — exercises the "already moving, same target" branch
        // without racing the replicator's swap-and-remove.
        let future_path = log_dir::future_partition_dir(extra.path(), "t", 0);
        std::fs::create_dir_all(&future_path).unwrap();
        let future_log = Arc::new(Mutex::new(
            Log::open(&future_path, LogConfig::default()).unwrap(),
        ));
        future_logs.insert(
            ("t".to_string(), PartitionIndex(0)),
            Arc::new(FutureLogState {
                target_log_dir: extra.path().to_path_buf(),
                future_path: future_path.clone(),
                future_log,
                cancel: CancellationToken::new(),
                task: std::sync::Mutex::new(None),
            }),
        );

        start_move(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            extra.path(),
            test_policy(),
        )
        .expect("same-target alter must be idempotent");
        assert!(future_logs.len() == 1);
    }

    #[tokio::test]
    async fn start_move_rejects_conflicting_target() {
        // ARLD for `(t, 0)` to dir A, then again to dir B while the
        // first move is still registered → AlreadyMoving.
        let primary = tempdir().unwrap();
        let extra = tempdir().unwrap();
        let third = tempdir().unwrap();
        let log_dirs = vec![
            primary.path().to_path_buf(),
            extra.path().to_path_buf(),
            third.path().to_path_buf(),
        ];
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        partitions.insert("t".to_string(), PartitionIndex(0), part);

        // Plant a registry entry pointing at `extra`.
        let future_path = log_dir::future_partition_dir(extra.path(), "t", 0);
        std::fs::create_dir_all(&future_path).unwrap();
        let future_log = Arc::new(Mutex::new(
            Log::open(&future_path, LogConfig::default()).unwrap(),
        ));
        future_logs.insert(
            ("t".to_string(), PartitionIndex(0)),
            Arc::new(FutureLogState {
                target_log_dir: extra.path().to_path_buf(),
                future_path,
                future_log,
                cancel: CancellationToken::new(),
                task: std::sync::Mutex::new(None),
            }),
        );

        let err = start_move(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            third.path(),
            test_policy(),
        )
        .expect_err("conflicting-target alter must reject");
        assert!(matches!(err, MoveError::AlreadyMoving));
    }
}
