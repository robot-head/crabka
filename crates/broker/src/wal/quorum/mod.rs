//! Quorum-backed diskless WAL implementation.

pub(crate) mod engine;
pub(crate) mod follower;
pub(crate) mod log_view;
pub(crate) mod placement;
pub(crate) mod registry;
pub(crate) mod wire;

use std::{
    fs,
    io::Write as _,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use crabka_ids::{Offset, PartitionIndex};
use crabka_kraft_core::NodeId;
use crabka_log::{Log, LogConfig};
use crabka_units::{ByteSize, convert::ByteSizeExt as _};
use uuid::Uuid;

use self::engine::{OpenMode, WalShardEngine};
use super::WalStore;
use crate::error::BrokerError;

const QUORUM_STATE_FILE: &str = "quorum-state.json";
const QUORUM_STATE_BACKUP_FILE: &str = "quorum-state.json.bak";

/// A [`WalStore`] backed by a quorum of durable WAL replica logs.
#[derive(Debug, Clone)]
pub(crate) struct QuorumWalStore {
    source: Arc<Mutex<Log>>,
    engine: Arc<WalShardEngine>,
    hot_tail: Option<HotTailTarget>,
}

#[derive(Debug, Clone)]
struct HotTailTarget {
    topic_id: Uuid,
    partition: PartitionIndex,
    cache: Arc<crate::diskless::hot_tail::HotTailCache>,
}

impl QuorumWalStore {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new(source: Arc<Mutex<Log>>, engine: Arc<WalShardEngine>) -> Self {
        Self {
            source,
            engine,
            hot_tail: None,
        }
    }

    pub(crate) fn for_partition(
        topic: &str,
        topic_id: Option<Uuid>,
        partition: PartitionIndex,
        log_dir: &std::path::Path,
        source: Arc<Mutex<Log>>,
        hot_tail: Option<Arc<crate::diskless::hot_tail::HotTailCache>>,
        replica_count: usize,
    ) -> Result<Self, BrokerError> {
        let config = source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .config_snapshot();
        let mut replicas = Vec::with_capacity(replica_count);
        replicas.push(engine::WalReplica::new(NodeId(0), source.clone()));
        let root = shard_dir(log_dir, topic, topic_id, partition);
        for id in 1..replica_count {
            let id = NodeId(u64::try_from(id).map_err(|_| {
                BrokerError::Replication("diskless WAL replica count exceeds u64".into())
            })?);
            let replica_dir = root.join(format!("replica-{}", id.0));
            let log = Log::open(&replica_dir, replica_config(&config))?;
            replicas.push(engine::WalReplica::new(id, Arc::new(Mutex::new(log))));
        }
        let voter_ids: Vec<_> = replicas.iter().map(engine::WalReplica::id).collect();
        let is_new = load_or_prepare_quorum_membership(&root, &voter_ids)?;
        let mode = if is_new {
            OpenMode::BootstrapFrom(NodeId(0))
        } else {
            OpenMode::Recover
        };
        let engine = Arc::new(WalShardEngine::new(replicas, mode)?);
        if is_new {
            persist_quorum_membership(&root, &voter_ids)?;
        }
        let hot_tail = topic_id
            .zip(hot_tail)
            .map(|(topic_id, cache)| HotTailTarget {
                topic_id,
                partition,
                cache,
            });
        Ok(Self {
            source,
            engine,
            hot_tail,
        })
    }

    pub(crate) fn for_distributed_partition(
        topic_id: Uuid,
        partition: PartitionIndex,
        source: Arc<Mutex<Log>>,
        hot_tail: Option<Arc<crate::diskless::hot_tail::HotTailCache>>,
        voter_count: usize,
    ) -> Result<Self, BrokerError> {
        let engine = Arc::new(WalShardEngine::new_distributed(
            source.clone(),
            voter_count,
        )?);
        let hot_tail = hot_tail.map(|cache| HotTailTarget {
            topic_id,
            partition,
            cache,
        });
        Ok(Self {
            source,
            engine,
            hot_tail,
        })
    }

    pub(crate) fn engine(&self) -> Arc<WalShardEngine> {
        self.engine.clone()
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct PersistedQuorumMembership {
    voters: Vec<u64>,
}

fn load_or_prepare_quorum_membership(
    root: &std::path::Path,
    voter_ids: &[NodeId],
) -> Result<bool, BrokerError> {
    fs::create_dir_all(root)?;
    let path = root.join(QUORUM_STATE_FILE);
    let backup = root.join(QUORUM_STATE_BACKUP_FILE);
    let existing = if path.exists() {
        Some(&path)
    } else if backup.exists() {
        Some(&backup)
    } else {
        None
    };
    if let Some(existing) = existing {
        let bytes = fs::read(existing)?;
        let persisted: PersistedQuorumMembership =
            serde_json::from_slice(&bytes).map_err(|err| {
                BrokerError::Replication(format!(
                    "decode WAL quorum membership {}: {err}",
                    existing.display()
                ))
            })?;
        let persisted_ids = persisted.voters.into_iter().map(NodeId).collect::<Vec<_>>();
        if persisted_ids != voter_ids {
            return Err(BrokerError::Replication(format!(
                "WAL quorum voter set changed for {}: persisted {:?}, configured {:?}",
                existing.display(),
                persisted_ids,
                voter_ids
            )));
        }
        return Ok(false);
    }

    Ok(true)
}

fn persist_quorum_membership(
    root: &std::path::Path,
    voter_ids: &[NodeId],
) -> Result<(), BrokerError> {
    let path = root.join(QUORUM_STATE_FILE);
    let persisted = PersistedQuorumMembership {
        voters: voter_ids.iter().map(|id| id.0).collect(),
    };
    let bytes = serde_json::to_vec_pretty(&persisted).map_err(|err| {
        BrokerError::Replication(format!(
            "encode WAL quorum membership {}: {err}",
            path.display()
        ))
    })?;
    let temporary = path.with_extension("json.tmp");
    let mut file = fs::File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);

    let backup = root.join(QUORUM_STATE_BACKUP_FILE);
    if backup.exists() {
        if path.exists() {
            fs::remove_file(&backup)?;
        } else {
            fs::rename(&backup, &path)?;
        }
    }
    if path.exists() {
        fs::rename(&path, &backup)?;
    }
    if let Err(error) = fs::rename(&temporary, &path) {
        restore_membership_backup(&backup, &path);
        return Err(error.into());
    }
    if backup.exists() {
        fs::remove_file(&backup)?;
    }

    // A durable file is not enough on filesystems where the rename itself is
    // only stable after the parent directory is synced. Rust does not expose
    // directory handles that can be flushed on Windows; the file sync above is
    // the strongest portable guarantee there, matching `crabka-log`.
    #[cfg(unix)]
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn restore_membership_backup(backup: &std::path::Path, path: &std::path::Path) {
    if let (Ok(true), Ok(false)) = (backup.try_exists(), path.try_exists()) {
        let _ = fs::rename(backup, path);
    }
}

fn replica_config(config: &LogConfig) -> LogConfig {
    let mut config = config.clone();
    config.validate_on_open = true;
    config
}

fn sanitize_topic(topic: &str) -> String {
    topic
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[must_use]
pub(crate) fn shard_dir(
    log_dir: &std::path::Path,
    topic: &str,
    topic_id: Option<Uuid>,
    partition: PartitionIndex,
) -> std::path::PathBuf {
    let identity = topic_id.map_or_else(
        || sanitize_topic(topic),
        |topic_id| format!("{}-{topic_id}", sanitize_topic(topic)),
    );
    log_dir
        .join("__diskless_wal_quorum")
        .join(format!("{identity}-{}", partition.0))
}

pub(crate) fn remove_shard(
    registry: &registry::WalShardRegistry,
    log_dir: &std::path::Path,
    topic: &str,
    topic_id: Uuid,
    partition: PartitionIndex,
) -> std::io::Result<()> {
    registry.remove(registry::ShardId {
        topic_id,
        partition,
    });
    match fs::remove_dir_all(shard_dir(log_dir, topic, Some(topic_id), partition)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

pub(crate) fn remove_leader_shard(
    registry: &registry::WalShardRegistry,
    log_dir: &std::path::Path,
    topic: &str,
    topic_id: Uuid,
    partition: PartitionIndex,
) -> std::io::Result<()> {
    registry.remove(registry::ShardId {
        topic_id,
        partition,
    });
    let root = shard_dir(log_dir, topic, Some(topic_id), partition);
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_string_lossy().starts_with("voter-") {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        }?;
    }
    Ok(())
}

/// Remove follower-only WAL shard directories that the current metadata image
/// no longer assigns to this broker.
///
/// Reconciliation calls this after obsolete WAL-follower tasks have stopped.
/// A root is eligible only when every child is a `voter-*` follower directory;
/// roots carrying any leader/runtime state are left to the owner-aware
/// partition prune path. This closes the offline-delete/reassignment case that
/// an in-memory task reconciliation cannot observe after a process restart.
pub(crate) fn prune_orphaned_shard_dirs(
    log_dirs: &[std::path::PathBuf],
    keep: &std::collections::HashSet<std::path::PathBuf>,
) -> std::io::Result<()> {
    for log_dir in log_dirs {
        let root = log_dir.join("__diskless_wal_quorum");
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if keep.contains(&path) {
                continue;
            }
            if !path.is_dir() {
                continue;
            }
            let follower_only = fs::read_dir(&path)?.all(|child| {
                child.is_ok_and(|child| {
                    child.path().is_dir()
                        && child.file_name().to_string_lossy().starts_with("voter-")
                })
            });
            if follower_only {
                fs::remove_dir_all(path)?;
            }
        }
    }
    Ok(())
}

#[async_trait]
impl WalStore for QuorumWalStore {
    async fn sync_durable(&self, leo: Offset) -> Result<Offset, BrokerError> {
        let start = self.engine.durable_watermark();
        let durable = self.engine.replicate_and_sync(&self.source, leo).await?;
        if let Some(target) = &self.hot_tail
            && durable > start
        {
            let raw = self
                .source
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                // The hot tail must mirror every batch that just went durable,
                // so the read is uncapped.
                .read_raw(start, durable, ByteSize::from_bytes(u64::MAX))?;
            target
                .cache
                .insert_run(target.topic_id, target.partition, &raw.bytes);
        }
        Ok(durable)
    }

    async fn trim_to_offset(&self, new_start: Offset) -> Result<Offset, BrokerError> {
        self.engine.trim_to_offset(&self.source, new_start).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use assert2::assert;
    use crabka_compression::CompressionType;
    use crabka_kraft_core::NodeId;
    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::{Record, RecordBatch};

    use super::*;

    #[test]
    fn strict_majority_requires_more_than_half_of_every_voter_set() {
        for (voters, required) in [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3)] {
            assert!(engine::strict_majority(voters) == required);
        }
    }

    async fn append_source(
        store: &QuorumWalStore,
        records: i32,
    ) -> (Vec<Result<Offset, BrokerError>>, Offset) {
        crate::partition_writer::run_produce_append_batch(
            store.source.clone(),
            vec![crate::partition::ProduceData::Owned(batch(records))],
        )
        .await
        .unwrap()
    }

    #[test]
    fn split_batches_preserves_compressed_wire_boundaries() {
        let mut wire = bytes::BytesMut::new();
        for base_offset in [0, 20] {
            let mut compressed = batch(20);
            compressed.base_offset = base_offset;
            compressed.attributes = compressed.attributes.with_compression(CompressionType::Lz4);
            for record in &mut compressed.records {
                record.value = Some(bytes::Bytes::from(vec![b'x'; 256]));
            }
            compressed.encode(&mut wire).unwrap();
        }

        let batches = engine::split_batches(&wire.freeze()).unwrap();

        assert!(batches.len() == 2);
        assert!(batches[0].base_offset == Offset(0));
        assert!(batches[0].last_offset == Offset(19));
        assert!(batches[1].base_offset == Offset(20));
        assert!(batches[1].last_offset == Offset(39));
    }

    #[test]
    fn shard_directory_distinguishes_same_name_topic_recreations() {
        let root = std::path::Path::new("data");
        let first_id = Uuid::from_u128(1);
        let second_id = Uuid::from_u128(2);

        let first = shard_dir(root, "orders", Some(first_id), PartitionIndex(3));
        let second = shard_dir(root, "orders", Some(second_id), PartitionIndex(3));

        assert!(first != second);
        assert!(first.ends_with(format!("orders-{first_id}-3")));
        assert!(second.ends_with(format!("orders-{second_id}-3")));
    }

    #[test]
    fn remove_shard_is_idempotent_but_reports_other_io_errors() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry::WalShardRegistry::new(NodeId(0));
        let topic_id = Uuid::from_u128(3);
        let partition = PartitionIndex(4);

        remove_shard(&registry, root.path(), "orders", topic_id, partition).unwrap();

        let path = shard_dir(root.path(), "orders", Some(topic_id), partition);
        fs::create_dir_all(path.join("voter-2")).unwrap();
        fs::write(path.join("voter-2/checkpoint"), b"durable").unwrap();
        remove_shard(&registry, root.path(), "orders", topic_id, partition).unwrap();
        assert!(!path.exists());

        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"not a directory").unwrap();
        assert!(remove_shard(&registry, root.path(), "orders", topic_id, partition).is_err());
    }

    #[test]
    fn remove_leader_shard_preserves_follower_checkpoint() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry::WalShardRegistry::new(NodeId(2));
        let topic_id = Uuid::from_u128(4);
        let partition = PartitionIndex(0);
        let shard = shard_dir(root.path(), "orders", Some(topic_id), partition);
        fs::create_dir_all(shard.join("voter-2")).unwrap();
        fs::write(shard.join("voter-2/checkpoint"), b"durable").unwrap();
        fs::write(shard.join("quorum-state.json"), b"leader").unwrap();

        remove_leader_shard(&registry, root.path(), "orders", topic_id, partition).unwrap();

        assert!(shard.join("voter-2/checkpoint").exists());
        assert!(!shard.join("quorum-state.json").exists());
    }

    #[test]
    fn prune_removes_only_unassigned_wal_shards() {
        let root = tempfile::tempdir().unwrap();
        let keep = shard_dir(
            root.path(),
            "orders",
            Some(Uuid::from_u128(5)),
            PartitionIndex(0),
        );
        let orphan = shard_dir(
            root.path(),
            "deleted",
            Some(Uuid::from_u128(6)),
            PartitionIndex(1),
        );
        fs::create_dir_all(keep.join("voter-2")).unwrap();
        fs::create_dir_all(orphan.join("voter-2")).unwrap();
        fs::write(orphan.join("voter-2/checkpoint"), b"durable").unwrap();
        let active = shard_dir(
            root.path(),
            "active",
            Some(Uuid::from_u128(7)),
            PartitionIndex(2),
        );
        fs::create_dir_all(active.join("voter-2")).unwrap();
        fs::write(active.join(QUORUM_STATE_FILE), b"leader runtime").unwrap();

        prune_orphaned_shard_dirs(
            &[root.path().to_path_buf()],
            &std::collections::HashSet::from([keep.clone()]),
        )
        .unwrap();

        assert!(keep.exists());
        assert!(!orphan.exists());
        assert!(active.exists());
    }

    #[test]
    fn partition_quorum_uses_configured_local_replica_count() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));

        QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source,
            None,
            2,
        )
        .unwrap();

        let root = dir.path().join("__diskless_wal_quorum/topic-0");
        assert!(root.join("replica-1").is_dir());
        assert!(!root.join("replica-2").exists());
    }

    #[test]
    fn partition_quorum_bootstraps_existing_source_into_every_replica() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        source.lock().unwrap().append(&mut batch(3)).unwrap();
        source.lock().unwrap().sync().unwrap();

        let store = QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source,
            None,
            3,
        )
        .unwrap();

        assert!(store.engine.durable_watermark() == Offset(3));
        assert!(store.engine.replica_end_offsets() == vec![Offset(3), Offset(3), Offset(3)]);
        assert!(
            dir.path()
                .join("__diskless_wal_quorum/topic-0/quorum-state.json")
                .is_file()
        );
    }

    #[tokio::test]
    async fn quorum_wal_store_commits_on_f_plus_1_and_survives_one_loss() {
        let source_dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(source_dir.path(), LogConfig::default()).unwrap(),
        ));
        let replica_dirs = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let engine = Arc::new(WalShardEngine::for_logs(
            [NodeId(1), NodeId(2), NodeId(3)]
                .into_iter()
                .zip(replica_dirs.iter().map(|dir| {
                    Arc::new(Mutex::new(
                        Log::open(dir.path(), LogConfig::default()).unwrap(),
                    ))
                }))
                .collect(),
        ));
        let store = QuorumWalStore::new(source.clone(), engine.clone());

        let (results, leo) = append_source(&store, 3).await;
        assert!(results.iter().all(Result::is_ok));
        assert!(leo == Offset(3));
        assert!(store.sync_durable(leo).await.unwrap() == Offset(3));
        assert!(engine.durable_watermark() == Offset(3));

        engine.set_replica_alive(NodeId(3), false);
        let (_results, leo) = append_source(&store, 2).await;
        assert!(store.sync_durable(leo).await.unwrap() == Offset(5));
        assert!(engine.durable_watermark() == Offset(5));

        engine.set_replica_alive(NodeId(2), false);
        let (_results, leo) = append_source(&store, 1).await;
        assert!(store.sync_durable(leo).await.is_err());
        assert!(engine.durable_watermark() == Offset(5));

        engine.set_replica_alive(NodeId(3), true);
        let (_results, leo) = append_source(&store, 1).await;
        assert!(store.sync_durable(leo).await.unwrap() == Offset(7));
        assert!(engine.replica_end_offsets()[2] == Offset(7));
    }

    #[tokio::test]
    async fn five_voter_quorum_requires_three_durable_copies() {
        let source_dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(source_dir.path(), LogConfig::default()).unwrap(),
        ));
        let replica_dirs = (0..5)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let engine = Arc::new(WalShardEngine::for_logs(
            (1_u64..=5)
                .map(NodeId)
                .zip(replica_dirs.iter().map(|dir| {
                    Arc::new(Mutex::new(
                        Log::open(dir.path(), LogConfig::default()).unwrap(),
                    ))
                }))
                .collect(),
        ));
        let store = QuorumWalStore::new(source, engine.clone());
        for voter in [NodeId(3), NodeId(4), NodeId(5)] {
            engine.set_replica_alive(voter, false);
        }
        let (_results, leo) = append_source(&store, 1).await;

        assert!(store.sync_durable(leo).await.is_err());
        assert!(engine.durable_watermark() == Offset(0));

        engine.set_replica_alive(NodeId(3), true);
        assert!(store.sync_durable(leo).await.unwrap() == leo);
    }

    #[tokio::test]
    async fn quorum_wal_store_populates_hot_tail_after_durable_sync() {
        let source_dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(source_dir.path(), LogConfig::default()).unwrap(),
        ));
        let replica_dirs = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let engine = Arc::new(WalShardEngine::for_logs(
            [NodeId(1), NodeId(2), NodeId(3)]
                .into_iter()
                .zip(replica_dirs.iter().map(|dir| {
                    Arc::new(Mutex::new(
                        Log::open(dir.path(), LogConfig::default()).unwrap(),
                    ))
                }))
                .collect(),
        ));
        let cache = Arc::new(crate::diskless::hot_tail::HotTailCache::default());
        let topic_id = Uuid::from_u128(9);
        let partition = PartitionIndex(0);
        let store = QuorumWalStore {
            source: source.clone(),
            engine,
            hot_tail: Some(HotTailTarget {
                topic_id,
                partition,
                cache: cache.clone(),
            }),
        };

        let (_results, leo) = append_source(&store, 2).await;
        assert!(store.sync_durable(leo).await.unwrap() == Offset(2));

        assert!(cache.get(topic_id, partition, 1, usize::MAX).is_some());
    }

    #[tokio::test]
    async fn quorum_wal_store_can_commit_a_source_prefix_without_regressing() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source,
            None,
            3,
        )
        .unwrap();

        let (_results, first) = append_source(&store, 1).await;
        let (_results, second) = append_source(&store, 1).await;
        assert!(store.sync_durable(first).await.unwrap() == Offset(1));
        assert!(store.sync_durable(second).await.unwrap() == Offset(2));
        assert!(store.sync_durable(first).await.unwrap() == Offset(2));
        assert!(store.engine.durable_watermark() == Offset(2));
    }

    #[tokio::test]
    async fn wal_fetch_serves_the_uncommitted_tail_with_separate_frontiers() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source,
            None,
            3,
        )
        .unwrap();

        let (_results, first) = append_source(&store, 1).await;
        let (_results, second) = append_source(&store, 1).await;
        store.sync_durable(first).await.unwrap();

        let fetch = store
            .engine
            .serve_fetch(first, ByteSize::from_bytes(u64::MAX))
            .unwrap();

        assert!(fetch.high_watermark == first);
        assert!(fetch.log_end_offset == second);
        assert!(fetch.log_start_offset == Offset(0));
        assert!(!fetch.offset_out_of_range);
        assert!(!fetch.records.is_empty());
    }

    #[tokio::test]
    async fn wal_fetch_accepts_the_log_end_and_a_zero_byte_limit() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source,
            None,
            3,
        )
        .unwrap();
        let (_results, log_end) = append_source(&store, 1).await;

        let at_end = store
            .engine
            .serve_fetch(log_end, ByteSize::from_bytes(u64::MAX))
            .unwrap();
        assert!(!at_end.offset_out_of_range);
        assert!(at_end.records.is_empty());

        let zero_bytes = store.engine.serve_fetch(Offset(0), ByteSize::ZERO).unwrap();
        assert!(!zero_bytes.offset_out_of_range);
        assert!(zero_bytes.records.is_empty());
    }

    #[tokio::test]
    async fn distributed_wal_waits_for_a_remote_fsync_ack() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = Arc::new(
            QuorumWalStore::for_distributed_partition(
                Uuid::from_u128(99),
                PartitionIndex(0),
                source,
                None,
                3,
            )
            .unwrap(),
        );
        store
            .engine
            .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let (_results, leo) = append_source(&store, 1).await;
        let syncing = Arc::clone(&store);
        let mut sync = tokio::spawn(async move { syncing.sync_durable(leo).await });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut sync)
                .await
                .is_err()
        );
        assert!(!store.engine.record_follower_ack(NodeId(9), leo));
        assert!(!store.engine.record_follower_ack(NodeId(1), leo));
        assert!(
            !store
                .engine
                .record_follower_ack(NodeId(2), Offset(leo.0 + 1))
        );
        assert!(store.engine.record_follower_ack(NodeId(2), leo));

        assert!(sync.await.unwrap().unwrap() == leo);
        assert!(store.engine.durable_watermark() == leo);
        assert!(store.engine.replica_end_offsets() == vec![leo]);
        assert!(store.trim_to_offset(leo).await.unwrap() == leo);
        assert!(store.engine.replica_start_offsets() == vec![leo]);
        assert!(
            !store
                .engine
                .record_follower_ack(NodeId(2), Offset(leo.0 - 1))
        );

        let (_results, next) = append_source(&store, 1).await;
        let syncing = Arc::clone(&store);
        let mut sync = tokio::spawn(async move { syncing.sync_durable(next).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut sync)
                .await
                .is_err()
        );
        store.engine.configure_distributed(NodeId(1), &[]);
        let error = sync.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("placement disappeared"));
    }

    #[tokio::test]
    async fn durable_advance_waits_for_an_offset_strictly_after_the_observation() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = Arc::new(
            QuorumWalStore::for_distributed_partition(
                Uuid::new_v4(),
                PartitionIndex(0),
                source,
                None,
                3,
            )
            .unwrap(),
        );
        store
            .engine
            .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let (_results, first) = append_source(&store, 1).await;
        let syncing = Arc::clone(&store);
        let sync = tokio::spawn(async move { syncing.sync_durable(first).await });
        assert!(store.engine.record_follower_ack(NodeId(2), first));
        assert!(sync.await.unwrap().unwrap() == first);

        let engine = Arc::clone(&store.engine);
        let mut waiting = tokio::spawn(async move { engine.wait_for_durable_advance(first).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );

        let (_results, second) = append_source(&store, 1).await;
        let syncing = Arc::clone(&store);
        let sync = tokio::spawn(async move { syncing.sync_durable(second).await });
        assert!(
            !store
                .engine
                .record_follower_ack(NodeId(2), Offset(first.0 - 1))
        );
        assert!(store.engine.record_follower_ack(NodeId(2), second));

        assert!(sync.await.unwrap().unwrap() == second);
        assert!(waiting.await.unwrap() == second);
    }

    #[tokio::test]
    async fn distributed_wal_rejects_misordered_or_incomplete_voter_sets() {
        for voters in [
            vec![NodeId(2), NodeId(1), NodeId(3)],
            vec![NodeId(1), NodeId(2)],
        ] {
            let dir = tempfile::tempdir().unwrap();
            let source = Arc::new(Mutex::new(
                Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
            ));
            let store = QuorumWalStore::for_distributed_partition(
                Uuid::new_v4(),
                PartitionIndex(0),
                source,
                None,
                3,
            )
            .unwrap();
            let (_results, leo) = append_source(&store, 1).await;

            store.engine.configure_distributed(NodeId(1), &voters);

            assert!(!store.engine.record_follower_ack(NodeId(2), leo));
            assert!(store.engine.durable_watermark() == Offset(0));
        }
    }

    #[tokio::test]
    async fn distributed_wal_reconfiguration_replaces_the_remote_voter_set() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = QuorumWalStore::for_distributed_partition(
            Uuid::new_v4(),
            PartitionIndex(0),
            source,
            None,
            3,
        )
        .unwrap();
        let (_results, leo) = append_source(&store, 1).await;
        store
            .engine
            .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);

        store
            .engine
            .configure_distributed(NodeId(1), &[NodeId(1), NodeId(3), NodeId(4)]);

        assert!(!store.engine.record_follower_ack(NodeId(2), leo));
        assert!(store.engine.record_follower_ack(NodeId(3), leo));
        assert!(store.engine.durable_watermark() == leo);
    }

    #[tokio::test]
    async fn distributed_wal_reopens_without_truncating_the_source() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("source");
        let source = Arc::new(Mutex::new(
            Log::open(&source_dir, LogConfig::default()).unwrap(),
        ));
        let mut batch = batch(2);
        source.lock().unwrap().append(&mut batch).unwrap();
        source.lock().unwrap().sync().unwrap();
        let store = QuorumWalStore::for_distributed_partition(
            Uuid::from_u128(100),
            PartitionIndex(0),
            source.clone(),
            None,
            3,
        )
        .unwrap();
        assert!(store.engine.durable_watermark() == Offset(0));
        assert!(store.engine.replica_end_offsets() == vec![Offset(2)]);
        drop(store);
        drop(source);

        let source = Arc::new(Mutex::new(
            Log::open(&source_dir, LogConfig::default()).unwrap(),
        ));
        let reopened = QuorumWalStore::for_distributed_partition(
            Uuid::from_u128(100),
            PartitionIndex(0),
            source,
            None,
            3,
        )
        .unwrap();

        assert!(reopened.engine.durable_watermark() == Offset(0));
        assert!(reopened.engine.replica_end_offsets() == vec![Offset(2)]);
        reopened
            .engine
            .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        assert!(reopened.engine.record_follower_ack(NodeId(2), Offset(2)));
        assert!(reopened.engine.durable_watermark() == Offset(2));
    }

    #[tokio::test]
    async fn quorum_wal_store_trims_every_replica_before_the_source() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source.clone(),
            None,
            3,
        )
        .unwrap();
        let (_results, leo) = append_source(&store, 3).await;
        store.sync_durable(leo).await.unwrap();

        let start = store.trim_to_offset(Offset(2)).await.unwrap();

        assert!(start >= Offset(2));
        assert!(source.lock().unwrap().log_start_offset() == start);
        assert!(store.engine.replica_start_offsets() == vec![start, start, start]);
    }

    #[tokio::test]
    async fn quorum_wal_store_rejects_a_source_outside_the_voter_set() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let replica = Arc::new(Mutex::new(
            Log::open(dir.path().join("replica"), LogConfig::default()).unwrap(),
        ));
        let engine = Arc::new(WalShardEngine::for_logs(BTreeMap::from([(
            NodeId(1),
            replica,
        )])));
        let store = QuorumWalStore::new(source, engine);

        let error = store.trim_to_offset(Offset(0)).await.unwrap_err();

        assert!(
            matches!(error, BrokerError::Replication(message) if message == "wal quorum source is not its first replica")
        );
    }

    #[tokio::test]
    async fn partition_quorum_recovers_watermark_and_repairs_one_lost_replica() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("source");
        let source = Arc::new(Mutex::new(
            Log::open(&source_dir, LogConfig::default()).unwrap(),
        ));
        let store = QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source.clone(),
            None,
            3,
        )
        .unwrap();

        let (_results, leo) = append_source(&store, 1).await;
        assert!(store.sync_durable(leo).await.unwrap() == Offset(1));
        drop(store);
        drop(source);

        let lost_replica = dir.path().join("__diskless_wal_quorum/topic-0/replica-2");
        std::fs::remove_dir_all(&lost_replica).unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(&source_dir, LogConfig::default()).unwrap(),
        ));
        let reopened = QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source,
            None,
            3,
        )
        .unwrap();

        assert!(reopened.engine.durable_watermark() == Offset(1));
        assert!(reopened.engine.replica_end_offsets() == vec![Offset(1), Offset(1), Offset(1)]);
        let fetch = reopened
            .engine
            .serve_fetch(Offset(0), ByteSize::from_bytes(u64::MAX))
            .unwrap();
        assert!(fetch.high_watermark == Offset(1));
        assert!(!fetch.records.is_empty());
    }

    #[tokio::test]
    async fn partition_quorum_discards_uncommitted_suffix_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = dir.path().join("source");
        let source = Arc::new(Mutex::new(
            Log::open(&source_dir, LogConfig::default()).unwrap(),
        ));
        let store = QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source.clone(),
            None,
            3,
        )
        .unwrap();
        let (_results, leo) = append_source(&store, 2).await;
        assert!(store.sync_durable(leo).await.unwrap() == Offset(2));

        store.engine.set_replica_alive(NodeId(1), false);
        store.engine.set_replica_alive(NodeId(2), false);
        let (_results, leo) = append_source(&store, 1).await;
        assert!(store.sync_durable(leo).await.is_err());
        assert!(store.engine.replica_end_offsets() == vec![Offset(3), Offset(2), Offset(2)]);
        drop(store);
        drop(source);

        let source = Arc::new(Mutex::new(
            Log::open(&source_dir, LogConfig::default()).unwrap(),
        ));
        let reopened = QuorumWalStore::for_partition(
            "topic",
            None,
            PartitionIndex(0),
            dir.path(),
            source,
            None,
            3,
        )
        .unwrap();

        assert!(reopened.engine.durable_watermark() == Offset(2));
        assert!(reopened.engine.replica_end_offsets() == vec![Offset(2), Offset(2), Offset(2)]);
    }

    fn batch(records: i32) -> RecordBatch {
        let mut batch = RecordBatch {
            last_offset_delta: records - 1,
            ..RecordBatch::default()
        };
        for offset_delta in 0..records {
            batch.records.push(Record {
                offset_delta,
                ..Record::default()
            });
        }
        batch
    }

    #[test]
    fn quorum_membership_descriptor_survives_reopen() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        assert!(load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        let is_new = load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap();
        let persisted: PersistedQuorumMembership =
            serde_json::from_slice(&fs::read(root.path().join(QUORUM_STATE_FILE)).unwrap())
                .unwrap();

        assert!(!is_new);
        assert!(persisted.voters == vec![0, 1, 2]);
        assert!(root.path().join(QUORUM_STATE_FILE).exists());
        assert!(
            !root
                .path()
                .join(QUORUM_STATE_FILE)
                .with_extension("json.tmp")
                .exists()
        );
        assert!(!root.path().join(QUORUM_STATE_BACKUP_FILE).exists());
    }

    #[test]
    fn quorum_membership_persist_replaces_a_stale_temporary_file() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        assert!(load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
        let temporary = root
            .path()
            .join(QUORUM_STATE_FILE)
            .with_extension("json.tmp");
        fs::write(&temporary, b"incomplete").unwrap();

        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        assert!(!temporary.exists());
        assert!(!load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
    }

    #[test]
    fn quorum_membership_persist_replaces_an_existing_descriptor() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        fs::create_dir_all(root.path()).unwrap();
        fs::write(
            root.path().join(QUORUM_STATE_FILE),
            br#"{"voters":[0,1,2],"leader_epoch":4,"leader_id":1}"#,
        )
        .unwrap();

        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(root.path().join(QUORUM_STATE_FILE)).unwrap())
                .unwrap();
        assert!(persisted == serde_json::json!({"voters": [0, 1, 2]}));
        assert!(!root.path().join(QUORUM_STATE_BACKUP_FILE).exists());
    }

    #[test]
    fn quorum_membership_loads_backup_left_between_replace_renames() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        persist_quorum_membership(root.path(), &voter_ids).unwrap();
        fs::rename(
            root.path().join(QUORUM_STATE_FILE),
            root.path().join(QUORUM_STATE_BACKUP_FILE),
        )
        .unwrap();

        assert!(!load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
    }

    #[test]
    fn quorum_membership_restore_only_uses_a_backup_when_the_primary_is_missing() {
        let root = tempfile::tempdir().unwrap();
        let primary = root.path().join(QUORUM_STATE_FILE);
        let backup = root.path().join(QUORUM_STATE_BACKUP_FILE);
        fs::write(&backup, b"backup-only").unwrap();

        restore_membership_backup(&backup, &primary);

        assert!(fs::read(&primary).unwrap() == b"backup-only");
        assert!(!backup.exists());

        fs::write(&primary, b"current").unwrap();
        fs::write(&backup, b"stale-backup").unwrap();
        restore_membership_backup(&backup, &primary);
        assert!(fs::read(&primary).unwrap() == b"current");
        assert!(fs::read(&backup).unwrap() == b"stale-backup");
    }

    #[test]
    fn legacy_quorum_state_descriptor_ignores_unused_election_fields() {
        let root = tempfile::tempdir().unwrap();
        let cluster_id = Uuid::from_u128(17);
        fs::write(
            root.path().join(QUORUM_STATE_FILE),
            format!(
                r#"{{"cluster_id":"{cluster_id}","voters":[0,1,2],"kraft_version":1,"leader_epoch":7,"leader_id":9,"voted_key":{{"id":9,"directory_id":"{}"}}}}"#,
                Uuid::from_u128(99)
            ),
        )
        .unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];

        assert!(!load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
    }

    #[test]
    fn quorum_membership_rejects_changed_voter_set() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        let changed = vec![NodeId(0), NodeId(1), NodeId(3)];
        assert!(load_or_prepare_quorum_membership(root.path(), &changed).is_err());
    }
}
