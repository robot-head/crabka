//! Quorum-backed diskless WAL implementation.

pub(crate) mod engine;
pub(crate) mod log_view;
pub(crate) mod placement;
pub(crate) mod registry;
pub(crate) mod wire;

use std::{
    fs,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use crabka_ids::{Offset, PartitionIndex};
use crabka_kraft_core::NodeId;
use crabka_log::{Log, LogConfig};
use crabka_units::{ByteSize, convert::ByteSizeExt as _};
use uuid::Uuid;

use self::engine::WalShardEngine;
use super::WalStore;
use crate::error::BrokerError;

const QUORUM_STATE_FILE: &str = "quorum-state.json";

/// A [`WalStore`] backed by a quorum of durable WAL replica logs.
#[allow(dead_code)]
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
    #[allow(dead_code)]
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
        let root = log_dir.join("__diskless_wal_quorum").join(format!(
            "{}-{}",
            sanitize_topic(topic),
            partition.0
        ));
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
        let state = load_or_bootstrap_quorum_state(&root, voter_ids, voters)?;
        let engine = Arc::new(WalShardEngine::new(NodeId(0), state, replicas));
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
}

fn load_or_bootstrap_quorum_state(
    root: &std::path::Path,
    voter_ids: Vec<NodeId>,
    voters: crabka_voters::VoterSet,
) -> Result<crabka_kraft_core::QuorumState, BrokerError> {
    fs::create_dir_all(root)?;
    let path = root.join(QUORUM_STATE_FILE);
    if path.exists() {
        let bytes = fs::read(&path)?;
        let persisted: PersistedQuorumState = serde_json::from_slice(&bytes).map_err(|err| {
            BrokerError::Replication(format!("decode WAL quorum state {}: {err}", path.display()))
        })?;
        let persisted_ids = persisted.voters.into_iter().map(NodeId).collect::<Vec<_>>();
        if persisted_ids != voter_ids {
            return Err(BrokerError::Replication(format!(
                "WAL quorum voter set changed for {}: persisted {:?}, configured {:?}",
                path.display(),
                persisted_ids,
                voter_ids
            )));
        }
        return Ok(crabka_kraft_core::QuorumState::bootstrap(
            persisted.cluster_id,
            voters,
        ));
    }

    let state = crabka_kraft_core::QuorumState::bootstrap(uuid::Uuid::new_v4(), voters);
    let persisted = PersistedQuorumState {
        cluster_id: state.cluster_id,
        voters: voter_ids.into_iter().map(|id| id.0).collect(),
    };
    let bytes = serde_json::to_vec_pretty(&persisted).map_err(|err| {
        BrokerError::Replication(format!("encode WAL quorum state {}: {err}", path.display()))
    })?;
    fs::write(path, bytes)?;
    Ok(state)
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
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

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
        let first = load_or_bootstrap_quorum_state(root.path(), voter_ids.clone(), voters).unwrap();

        let voters = engine::voter_set(voter_ids.iter().copied());
        let reopened = load_or_bootstrap_quorum_state(root.path(), voter_ids, voters).unwrap();

        assert!(reopened.cluster_id == first.cluster_id);
        assert!(root.path().join(QUORUM_STATE_FILE).exists());
    }

    #[test]
    fn quorum_state_rejects_changed_voter_set() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        let voters = engine::voter_set(voter_ids.iter().copied());
        load_or_bootstrap_quorum_state(root.path(), voter_ids, voters).unwrap();

        let changed = vec![NodeId(0), NodeId(1), NodeId(3)];
        let voters = engine::voter_set(changed.iter().copied());
        assert!(load_or_bootstrap_quorum_state(root.path(), changed, voters).is_err());
    }
}
