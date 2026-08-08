//! Share-partition leader manager (KIP-932).
//!
//! The manager owns one [`AcquisitionState`] machine per
//! `(group, topic_id, partition)` that this broker leads. It loads each machine
//! lazily from the durable `SharePersister` and persists it again each time it
//! goes dirty. The `ShareFetch` and `ShareAcknowledge` handlers drive the
//! per-cell state under its `tokio::sync::Mutex`. A background sweeper expires
//! the acquisition locks.
//!
//! Locking discipline: the `DashMap` guard is NEVER held across an `.await`.
//! Callers clone the cell `Arc` out of the map first, then lock and await.

use std::{sync::Arc, time::Duration};

use crabka_ids::PartitionIndex;
use crabka_log::Offset;
use crabka_metadata::NodeId;
use dashmap::DashMap;
use tokio::sync::Mutex;
use tracing::warn;

use crate::{
    coordinator::unified::share::config::ShareGroupConfig,
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
    share_coordinator::persister_client::SharePersister,
    share_partition::{session::ShareSessionCache, state::AcquisitionState},
};

/// Live acquisition-state machines keyed by `(group, topic_id, partition)`.
type LeaderKey = (String, uuid::Uuid, i32);

/// Per-broker owner of the share-partition acquisition state machines.
///
/// The manager owns one machine for each `(group, topic, partition)` triple
/// that this broker leads.
pub(crate) struct SharePartitionLeaderManager {
    node_id: NodeId,
    partitions: Arc<PartitionRegistry>,
    controller: Arc<dyn MetadataSource>,
    persister: Arc<SharePersister>,
    config: Arc<ShareGroupConfig>,
    sessions: ShareSessionCache,
    leaders: DashMap<LeaderKey, Arc<Mutex<AcquisitionState>>>,
}

impl std::fmt::Debug for SharePartitionLeaderManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharePartitionLeaderManager")
            .field("node_id", &self.node_id)
            .field("live_partitions", &self.leaders.len())
            .finish_non_exhaustive()
    }
}

impl SharePartitionLeaderManager {
    pub(crate) fn new(
        node_id: NodeId,
        partitions: Arc<PartitionRegistry>,
        controller: Arc<dyn MetadataSource>,
        persister: Arc<SharePersister>,
        config: Arc<ShareGroupConfig>,
        unlimited_session_fallback: usize,
    ) -> Self {
        // The share-session cache is capped at the same per-broker session
        // ceiling as classic fetch sessions; `max_groups` of 0 means
        // "unbounded" in `ShareGroupConfig`, so use the broker fallback.
        let session_max = if config.max_groups == 0 {
            unlimited_session_fallback
        } else {
            config.max_groups.saturating_mul(config.max_size.max(1))
        };
        Self {
            node_id,
            partitions,
            controller,
            persister,
            config,
            sessions: ShareSessionCache::new(session_max),
            leaders: DashMap::new(),
        }
    }

    /// Validates the share session for `(group, member)` and advances it.
    ///
    /// See [`ShareSessionCache::validate`].
    pub(crate) fn validate_session(
        &self,
        group: &str,
        member: &str,
        epoch: i32,
    ) -> Result<(), i16> {
        self.sessions.validate(group, member, epoch)
    }

    /// Resolves the wire `(leader_id, leader_epoch)` for
    /// `(topic_id, partition)`.
    ///
    /// The values come from the metadata image. A not-leader `ShareFetch` or
    /// `ShareAcknowledge` response carries them as the `current_leader`
    /// redirect hint. This method returns `(-1, -1)` when the topic or the
    /// partition is unknown.
    pub(crate) fn current_leader_of(&self, topic_id: uuid::Uuid, partition: i32) -> (i32, i32) {
        let image = self.controller.current_image();
        let Some(topic) = image.topics().find(|t| t.topic_id == topic_id) else {
            return (-1, -1);
        };
        image
            .partition(&topic.name, partition)
            .map_or((-1, -1), |p| {
                (i32::try_from(p.leader.0).unwrap_or(-1), p.leader_epoch.0)
            })
    }

    /// Resolves the data-topic name for `topic_id` from the metadata image.
    ///
    /// Returns `None` when the id is unknown. The share path carries only
    /// `topic_id`. The handlers need the name to look up the local
    /// [`PartitionRegistry`] entry and to key the per-topic `Read` ACL checks.
    pub(crate) fn topic_name_for(&self, topic_id: uuid::Uuid) -> Option<String> {
        self.controller
            .current_image()
            .topics()
            .find(|t| t.topic_id == topic_id)
            .map(|t| t.name.clone())
    }

    /// Returns `true` if this broker leads the partition of the data topic
    /// `topic_id`.
    ///
    /// The method resolves the topic name from the metadata image, because the
    /// share path carries only `topic_id`. It then compares the partition
    /// leader to `node_id`.
    ///
    /// The `ShareFetch` and `ShareAcknowledge` handlers call this method.
    pub(crate) fn topic_leader_is_self(&self, topic_id: uuid::Uuid, partition: i32) -> bool {
        let image = self.controller.current_image();
        let Some(topic) = image.topics().find(|t| t.topic_id == topic_id) else {
            return false;
        };
        image
            .partition(&topic.name, partition)
            .is_some_and(|p| p.leader == self.node_id)
    }

    /// Current `leader_epoch` for `(topic_id, partition)`.
    ///
    /// The value comes from the atomic of the local partition. The method
    /// returns `0` when the partition is not materialized on this broker.
    fn leader_epoch_for(&self, topic_id: uuid::Uuid, partition: i32) -> i32 {
        let image = self.controller.current_image();
        let Some(topic) = image.topics().find(|t| t.topic_id == topic_id) else {
            return 0;
        };
        self.partitions
            .get(&topic.name, PartitionIndex(partition))
            .map_or(0, |p| {
                p.current_leader_epoch
                    .load(std::sync::atomic::Ordering::Acquire)
            })
    }

    /// Gets the acquisition-state cell for `(group, topic_id, partition)`, and
    /// loads it lazily on a miss.
    ///
    /// On a cache miss the method reads the durable state from the persister
    /// and folds it into a fresh [`AcquisitionState`]. If no durable state
    /// exists, it uses an empty [`AcquisitionState`]. The method drops the
    /// `DashMap` guard before the load `.await`. A concurrent loader that loses
    /// the insert race adopts the cell of the winner.
    ///
    /// The `ShareFetch` and `ShareAcknowledge` handlers call this method.
    pub(crate) async fn get_or_load(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Arc<Mutex<AcquisitionState>> {
        let key = (group.to_string(), topic_id, partition);
        if let Some(cell) = self.leaders.get(&key) {
            return cell.value().clone();
        }

        // Miss: load from the persister WITHOUT holding any DashMap guard.
        let leader_epoch = self.leader_epoch_for(topic_id, partition);
        let loaded = match self.persister.read_state(group, topic_id, partition).await {
            Ok(Some(persisted)) => {
                let mut st = AcquisitionState::new(persisted.start_offset);
                st.load_from(
                    persisted.start_offset,
                    persisted.state_epoch,
                    leader_epoch,
                    persisted.delivery_complete_count,
                    &persisted.state_batches,
                );
                st
            }
            Ok(None) => {
                let mut st = AcquisitionState::new(Offset(0));
                st.leader_epoch = leader_epoch;
                st
            }
            Err(e) => {
                warn!(
                    group,
                    %topic_id, partition, error = %e,
                    "share-partition state load failed; starting from empty window"
                );
                let mut st = AcquisitionState::new(Offset(0));
                st.leader_epoch = leader_epoch;
                st
            }
        };

        let cell = Arc::new(Mutex::new(loaded));
        // Adopt the winner if another task loaded the same key concurrently.
        self.leaders.entry(key).or_insert(cell).value().clone()
    }

    /// Test-only: borrows the live acquisition cell without a persister load.
    ///
    /// Returns `None` if this node does not currently lead the partition or has
    /// not loaded the cell.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn peek_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<std::sync::Arc<tokio::sync::Mutex<AcquisitionState>>> {
        self.leaders
            .get(&(group.to_string(), topic_id, partition))
            .map(|c| c.value().clone())
    }

    /// Drops the cached acquisition-state cell for
    /// `(group, topic_id, partition)`.
    ///
    /// The next `get_or_load` then re-reads the durable SPSO. The admin offset
    /// RPCs call this method after `AlterShareGroupOffsets` or
    /// `DeleteShareGroupOffsets` rewrites the persister state. A later
    /// `ShareFetch` on this broker thus sees an in-flight reset. A cell on
    /// another broker refreshes on its own next load, which matches the classic
    /// offset-reset behavior.
    pub(crate) fn invalidate(&self, group: &str, topic_id: uuid::Uuid, partition: i32) {
        self.leaders
            .remove(&(group.to_string(), topic_id, partition));
    }

    /// Persists `st` if it is dirty, then clears the dirty flag.
    ///
    /// The method logs each error and then discards it. Persistence is
    /// best-effort. It never panics, and it never fails the fetch or the ack
    /// that called it.
    pub(crate) async fn persist_if_dirty(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        st: &mut AcquisitionState,
    ) {
        if !st.dirty {
            return;
        }
        let (start, dcc, batches) = st.to_persist_batches();
        match self
            .persister
            .write_state(
                group,
                topic_id,
                partition,
                (st.state_epoch, st.leader_epoch),
                (start, dcc),
                batches,
            )
            .await
        {
            // Clear `dirty` only on a durable write. On failure we leave it set
            // so the background sweeper (and the next fetch/ack) retries.
            Ok(()) => st.dirty = false,
            Err(e) => warn!(
                group,
                %topic_id, partition, error = %e,
                "share-partition state persist failed; will retry on next change"
            ),
        }
    }

    /// Spawns the background acquisition-lock-timeout sweeper.
    ///
    /// The sweeper runs every `record_lock_duration / 2`, with a minimum of
    /// 100ms. On each run it snapshots the live cells. It clones their `Arc`s
    /// out of the `DashMap`, so it holds no guard across an `.await`. It then
    /// expires each timed-out lock and persists again the cells that changed.
    /// The sweeper runs detached for the lifetime of the broker.
    pub(crate) fn spawn_lock_sweeper(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        let period = (mgr.config.record_lock_duration / 2).max(Duration::from_millis(100));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            loop {
                tick.tick().await;
                // Snapshot keys + cells, releasing all DashMap guards first.
                let cells: Vec<(LeaderKey, Arc<Mutex<AcquisitionState>>)> = mgr
                    .leaders
                    .iter()
                    .map(|e| (e.key().clone(), e.value().clone()))
                    .collect();
                let now = std::time::Instant::now();
                for ((group, topic_id, partition), cell) in cells {
                    let mut st = cell.lock().await;
                    st.expire_locks(now);
                    if st.dirty {
                        mgr.persist_if_dirty(&group, topic_id, partition, &mut st)
                            .await;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, net::SocketAddr};

    use assert2::assert;

    use super::*;

    const LOCK: Duration = Duration::from_secs(30);

    use async_trait::async_trait;
    use crabka_metadata::{MetadataImage, MetadataRecord};
    use crabka_raft::{
        AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
        UpdateVoter,
    };
    use crabka_security::ListenerProtocol;
    use tokio::sync::watch;

    use crate::{
        network::client::InterBrokerClient,
        share_coordinator::{config::ShareCoordinatorConfig, coordinator::ShareCoordinator},
    };

    /// Minimal `MetadataSource` over a fixed image that holds no brokers.
    ///
    /// The bootstrap of the share-state topic cannot run against this image,
    /// because it has no brokers. The `read_state` of the persister thus stops
    /// early with an error, before any routing. This exercises the best-effort
    /// empty-window fallback of `get_or_load` without an inter-broker server.
    struct MockSource {
        image: Arc<MetadataImage>,
        leader_rx: watch::Receiver<Option<NodeId>>,
        _leader_tx: watch::Sender<Option<NodeId>>,
    }

    impl MockSource {
        fn new() -> Self {
            Self::with_image(Arc::new(MetadataImage::new(uuid::Uuid::nil())))
        }

        fn with_image(image: Arc<MetadataImage>) -> Self {
            let (tx, rx) = watch::channel(Some(crabka_metadata::NodeId(1)));
            Self {
                image,
                leader_rx: rx,
                _leader_tx: tx,
            }
        }
    }

    #[async_trait]
    impl MetadataSource for MockSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }
        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            unimplemented!()
        }
        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            self.leader_rx.clone()
        }
        fn quorum_state(&self) -> QuorumState {
            unimplemented!()
        }
        async fn submit_change(
            &self,
            _records: Vec<MetadataRecord>,
        ) -> Result<crabka_raft::SubmitChangeResult, RaftError> {
            Ok(crabka_raft::SubmitChangeResult::default())
        }
        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            unimplemented!()
        }
        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            unimplemented!()
        }
        fn controller_bound_addr(&self) -> SocketAddr {
            unimplemented!()
        }
        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            unimplemented!()
        }
        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            unimplemented!()
        }
        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!()
        }
        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!()
        }
        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!()
        }
        async fn cancel(&self) {}
    }

    fn manager() -> Arc<SharePartitionLeaderManager> {
        manager_with_unlimited_fallback(
            crate::config::BrokerConfig::default().share_session_cache_max_when_unlimited,
        )
    }

    /// A manager whose controller serves `image`.
    ///
    /// `current_leader_of` and the related methods thus resolve real topic and
    /// partition leadership.
    fn manager_with_image(image: Arc<MetadataImage>) -> Arc<SharePartitionLeaderManager> {
        let reg = Arc::new(PartitionRegistry::new());
        let controller: Arc<dyn MetadataSource> = Arc::new(MockSource::with_image(image));
        let coord = Arc::new(ShareCoordinator::new(
            crabka_audit::NodeId(1),
            reg.clone(),
            ShareCoordinatorConfig::default(),
        ));
        let client = Arc::new(InterBrokerClient::new(None, None));
        let persister = Arc::new(SharePersister::new(
            crabka_audit::NodeId(1),
            coord,
            controller.clone(),
            client,
            ListenerProtocol::Plaintext,
            "INTERNAL".to_string(),
        ));
        Arc::new(SharePartitionLeaderManager::new(
            crabka_audit::NodeId(1),
            reg,
            controller,
            persister,
            Arc::new(ShareGroupConfig::default()),
            crate::config::BrokerConfig::default().share_session_cache_max_when_unlimited,
        ))
    }

    fn manager_with_unlimited_fallback(fallback: usize) -> Arc<SharePartitionLeaderManager> {
        let reg = Arc::new(PartitionRegistry::new());
        let controller: Arc<dyn MetadataSource> = Arc::new(MockSource::new());
        let coord = Arc::new(ShareCoordinator::new(
            crabka_audit::NodeId(1),
            reg.clone(),
            ShareCoordinatorConfig::default(),
        ));
        let client = Arc::new(InterBrokerClient::new(None, None));
        let persister = Arc::new(SharePersister::new(
            crabka_audit::NodeId(1),
            coord,
            controller.clone(),
            client,
            ListenerProtocol::Plaintext,
            "INTERNAL".to_string(),
        ));
        Arc::new(SharePartitionLeaderManager::new(
            crabka_audit::NodeId(1),
            reg,
            controller,
            persister,
            Arc::new(ShareGroupConfig::default()),
            fallback,
        ))
    }

    #[test]
    fn nondefault_unlimited_fallback_bounds_sessions() {
        let manager = manager_with_unlimited_fallback(2);

        assert!(manager.validate_session("g", "m1", 0) == Ok(()));
        assert!(manager.validate_session("g", "m2", 0) == Ok(()));
        assert!(
            manager.validate_session("g", "m3", 0)
                == Err(crate::codes::SHARE_SESSION_LIMIT_REACHED)
        );
    }

    #[tokio::test]
    async fn get_or_load_fresh_returns_empty_window_and_caches() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([21; 16]);

        let cell = mgr.get_or_load("g1", tid, 0).await;
        let st = cell.lock().await;
        assert!(st.start_offset == 0);
        assert!(!st.dirty);
        drop(st);
        // A second call returns the same cached cell.
        let cell2 = mgr.get_or_load("g1", tid, 0).await;
        assert!(Arc::ptr_eq(&cell, &cell2));
    }

    #[tokio::test]
    async fn persist_if_dirty_is_noop_when_clean() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([22; 16]);

        let cell = mgr.get_or_load("g1", tid, 0).await;
        let mut st = cell.lock().await;
        assert!(!st.dirty);
        // Clean state: no-op, no panic, stays clean.
        mgr.persist_if_dirty("g1", tid, 0, &mut st).await;
        assert!(!st.dirty);
    }

    #[tokio::test]
    async fn persist_if_dirty_keeps_dirty_on_write_failure() {
        // Under MockSource the persister can't bootstrap the share-state topic,
        // so `write_state` errors. A failed durable write must leave `dirty`
        // set so the sweeper/next-ack retries (F4 durability fix).
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([25; 16]);

        let cell = mgr.get_or_load("g1", tid, 0).await;
        let mut st = cell.lock().await;
        // Make the state dirty with persistable content.
        st.materialize(Offset(4), 100);
        let _ = st.acquire("m1", 10, i32::MAX, std::time::Instant::now(), LOCK, 5);
        assert!(st.dirty);

        mgr.persist_if_dirty("g1", tid, 0, &mut st).await;
        // Write failed -> dirty stays set for retry.
        assert!(st.dirty);
    }

    #[tokio::test]
    async fn topic_leader_is_self_false_for_unknown_topic() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([23; 16]);
        assert!(!mgr.topic_leader_is_self(tid, 0));
    }

    #[tokio::test]
    async fn invalidate_removes_cached_cell() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([24; 16]);

        // Populate the cache, then invalidate; a subsequent load yields a
        // fresh, distinct cell.
        let cell = mgr.get_or_load("g1", tid, 0).await;
        mgr.invalidate("g1", tid, 0);
        let cell2 = mgr.get_or_load("g1", tid, 0).await;
        assert!(!Arc::ptr_eq(&cell, &cell2));
    }

    #[tokio::test]
    async fn current_leader_of_reads_image_leader_and_epoch() {
        use crabka_ids::LeaderEpoch;
        use crabka_metadata::{PartitionRecord, TopicRecord};

        let tid = uuid::Uuid::from_bytes([31; 16]);
        // A topic-partition led by node 2 at leader epoch 5. Both components are
        // non-default and differ from every fixed-tuple mutant
        // ((0,0)/(0,1)/(-1,0)/(1,1)).
        let image = Arc::new(MetadataImage::from_records(
            uuid::Uuid::nil(),
            &[
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: tid,
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "t".into(),
                    partition: 0,
                    leader: NodeId(2),
                    replicas: vec![NodeId(2)],
                    isr: vec![NodeId(2)],
                    leader_epoch: LeaderEpoch(5),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                }),
            ],
        ));
        let mgr = manager_with_image(image);

        // Known partition resolves to (leader_id, leader_epoch) from the image.
        assert!(mgr.current_leader_of(tid, 0) == (2, 5));
        // Unknown partition of a known topic -> (-1, -1).
        assert!(mgr.current_leader_of(tid, 9) == (-1, -1));
        // Unknown topic -> (-1, -1).
        assert!(mgr.current_leader_of(uuid::Uuid::from_bytes([99; 16]), 0) == (-1, -1));
    }
}
