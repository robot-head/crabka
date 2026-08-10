//! Quorum-backed diskless WAL implementation.

pub(crate) mod engine;
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
        let voters = engine::voter_set(voter_ids.iter().copied());
        let opened = load_or_prepare_quorum_state(&root, &voter_ids, voters)?;
        let mode = if opened.is_new {
            OpenMode::BootstrapFrom(NodeId(0))
        } else {
            OpenMode::Recover
        };
        let engine = Arc::new(WalShardEngine::new(
            NodeId(0),
            opened.state.clone(),
            replicas,
            mode,
        )?);
        if opened.is_new {
            persist_quorum_state(&root, &opened.state, &voter_ids)?;
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

    pub(crate) fn engine(&self) -> Arc<WalShardEngine> {
        self.engine.clone()
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct PersistedQuorumState {
    cluster_id: uuid::Uuid,
    voters: Vec<u64>,
    #[serde(default)]
    kraft_version: u16,
    #[serde(default)]
    leader_epoch: u32,
    #[serde(default)]
    leader_id: Option<u64>,
    #[serde(default)]
    voted_key: Option<PersistedReplicaKey>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct PersistedReplicaKey {
    id: u64,
    directory_id: uuid::Uuid,
}

#[derive(Debug)]
struct OpenedQuorumState {
    state: crabka_kraft_core::QuorumState,
    is_new: bool,
}

fn load_or_prepare_quorum_state(
    root: &std::path::Path,
    voter_ids: &[NodeId],
    voters: crabka_voters::VoterSet,
) -> Result<OpenedQuorumState, BrokerError> {
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
        let persisted: PersistedQuorumState = serde_json::from_slice(&bytes).map_err(|err| {
            BrokerError::Replication(format!(
                "decode WAL quorum state {}: {err}",
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
        let leader_id = persisted.leader_id.map(NodeId);
        let voted_key = persisted
            .voted_key
            .map(|key| crabka_kraft_core::ReplicaKey {
                id: NodeId(key.id),
                directory_id: key.directory_id,
            });
        if leader_id.is_some_and(|id| !voters.contains(id))
            || voted_key.is_some_and(|key| !voters.contains(key.id))
        {
            return Err(BrokerError::Replication(format!(
                "WAL quorum state {} names a leader or vote outside its voter set",
                existing.display()
            )));
        }
        return Ok(OpenedQuorumState {
            state: crabka_kraft_core::QuorumState {
                cluster_id: persisted.cluster_id,
                kraft_version: persisted.kraft_version,
                leader_epoch: persisted.leader_epoch,
                leader_id,
                voted_key,
                voters,
            },
            is_new: false,
        });
    }

    let state = crabka_kraft_core::QuorumState::bootstrap(uuid::Uuid::new_v4(), voters);
    Ok(OpenedQuorumState {
        state,
        is_new: true,
    })
}

fn persist_quorum_state(
    root: &std::path::Path,
    state: &crabka_kraft_core::QuorumState,
    voter_ids: &[NodeId],
) -> Result<(), BrokerError> {
    let path = root.join(QUORUM_STATE_FILE);
    let persisted = PersistedQuorumState {
        cluster_id: state.cluster_id,
        voters: voter_ids.iter().map(|id| id.0).collect(),
        kraft_version: state.kraft_version,
        leader_epoch: state.leader_epoch,
        leader_id: state.leader_id.map(|id| id.0),
        voted_key: state.voted_key.map(|key| PersistedReplicaKey {
            id: key.id.0,
            directory_id: key.directory_id,
        }),
    };
    let bytes = serde_json::to_vec_pretty(&persisted).map_err(|err| {
        BrokerError::Replication(format!("encode WAL quorum state {}: {err}", path.display()))
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
        if backup.exists() && !path.exists() {
            let _ = fs::rename(&backup, &path);
        }
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
    use crabka_kraft_core::NodeId;
    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::{Record, RecordBatch};

    use super::*;

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
        let registry = registry::WalShardRegistry::new();
        let topic_id = Uuid::from_u128(3);
        let partition = PartitionIndex(4);

        remove_shard(&registry, root.path(), "orders", topic_id, partition).unwrap();

        let path = shard_dir(root.path(), "orders", Some(topic_id), partition);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"not a directory").unwrap();
        assert!(remove_shard(&registry, root.path(), "orders", topic_id, partition).is_err());
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
    fn quorum_state_descriptor_survives_reopen() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        let voters = engine::voter_set(voter_ids.iter().copied());
        let mut first = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();
        assert!(first.is_new);
        first.state.kraft_version = 1;
        first.state.leader_epoch = 7;
        first.state.leader_id = Some(NodeId(2));
        first.state.voted_key = Some(crabka_kraft_core::ReplicaKey {
            id: NodeId(1),
            directory_id: Uuid::from_u128(99),
        });
        persist_quorum_state(root.path(), &first.state, &voter_ids).unwrap();

        let voters = engine::voter_set(voter_ids.iter().copied());
        let reopened = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();

        assert!(!reopened.is_new);
        assert!(reopened.state == first.state);
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
    fn quorum_state_persist_replaces_a_stale_temporary_file() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        let voters = engine::voter_set(voter_ids.iter().copied());
        let opened = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();
        let temporary = root
            .path()
            .join(QUORUM_STATE_FILE)
            .with_extension("json.tmp");
        fs::write(&temporary, b"incomplete").unwrap();

        persist_quorum_state(root.path(), &opened.state, &voter_ids).unwrap();

        assert!(!temporary.exists());
        let voters = engine::voter_set(voter_ids.iter().copied());
        let reopened = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();
        assert!(reopened.state.cluster_id == opened.state.cluster_id);
    }

    #[test]
    fn quorum_state_persist_replaces_an_existing_descriptor() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        let voters = engine::voter_set(voter_ids.iter().copied());
        let mut opened = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();
        persist_quorum_state(root.path(), &opened.state, &voter_ids).unwrap();
        opened.state.leader_epoch = 4;
        opened.state.leader_id = Some(NodeId(1));

        persist_quorum_state(root.path(), &opened.state, &voter_ids).unwrap();

        let voters = engine::voter_set(voter_ids.iter().copied());
        let reopened = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();
        assert!(reopened.state == opened.state);
        assert!(!root.path().join(QUORUM_STATE_BACKUP_FILE).exists());
    }

    #[test]
    fn quorum_state_loads_backup_left_between_replace_renames() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        let voters = engine::voter_set(voter_ids.iter().copied());
        let opened = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();
        persist_quorum_state(root.path(), &opened.state, &voter_ids).unwrap();
        fs::rename(
            root.path().join(QUORUM_STATE_FILE),
            root.path().join(QUORUM_STATE_BACKUP_FILE),
        )
        .unwrap();

        let voters = engine::voter_set(voter_ids.iter().copied());
        let recovered = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();

        assert!(!recovered.is_new);
        assert!(recovered.state == opened.state);
    }

    #[test]
    fn legacy_quorum_state_descriptor_loads_with_consensus_defaults() {
        let root = tempfile::tempdir().unwrap();
        let cluster_id = Uuid::from_u128(17);
        fs::write(
            root.path().join(QUORUM_STATE_FILE),
            format!(r#"{{"cluster_id":"{cluster_id}","voters":[0,1,2]}}"#),
        )
        .unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        let voters = engine::voter_set(voter_ids.iter().copied());

        let opened = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();

        assert!(opened.state.cluster_id == cluster_id);
        assert!(opened.state.kraft_version == 0);
        assert!(opened.state.leader_epoch == 0);
        assert!(opened.state.leader_id.is_none());
        assert!(opened.state.voted_key.is_none());
    }

    #[test]
    fn quorum_state_rejects_leader_outside_voter_set() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        let voters = engine::voter_set(voter_ids.iter().copied());
        let mut opened = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();
        opened.state.leader_id = Some(NodeId(9));
        persist_quorum_state(root.path(), &opened.state, &voter_ids).unwrap();

        let voters = engine::voter_set(voter_ids.iter().copied());
        assert!(load_or_prepare_quorum_state(root.path(), &voter_ids, voters).is_err());
    }

    #[test]
    fn quorum_state_rejects_changed_voter_set() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        let voters = engine::voter_set(voter_ids.iter().copied());
        let first = load_or_prepare_quorum_state(root.path(), &voter_ids, voters).unwrap();
        persist_quorum_state(root.path(), &first.state, &voter_ids).unwrap();

        let changed = vec![NodeId(0), NodeId(1), NodeId(3)];
        let voters = engine::voter_set(changed.iter().copied());
        assert!(load_or_prepare_quorum_state(root.path(), &changed, voters).is_err());
    }
}
