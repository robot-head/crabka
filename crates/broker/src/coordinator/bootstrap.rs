//! `__consumer_offsets` topic lifecycle: ensure the topic exists at
//! startup, then synchronously replay every record into the in-memory
//! `GroupManager`.

use std::sync::Arc;

use crabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use crabka_protocol::records::RecordBatch;
use crabka_raft::{ControllerHandle, RaftError};

use crate::broker::spawn_partition;
use crate::config::BrokerConfig;
use crate::coordinator::GroupManager;
use crate::coordinator::group::{Group, Member, OffsetEntry};
use crate::coordinator::persistence::{self, GroupMetadataValue, Key, OffsetCommitValue};
use crate::error::BrokerError;
use crate::log_dir;
use crate::partition::Partition;

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";
pub const OFFSETS_PARTITION: i32 = 0;

/// Ensure the `__consumer_offsets-0` partition exists on disk, open its
/// `Log`, spawn a writer task, and replay every record into the supplied
/// `GroupManager`. Registers the topic via the metadata quorum
/// (`controller.submit_change(...)`) as a 1-partition internal topic;
/// `TopicExists` is treated as success so a restart that finds the topic
/// already in the log is a no-op.
///
/// Called exactly once from `Broker::start`, BEFORE the TCP listener binds
/// and AFTER the controller has elected a leader (see `Broker::start`).
pub async fn bootstrap(
    config: &BrokerConfig,
    controller: &Arc<ControllerHandle>,
    partitions: &Arc<dashmap::DashMap<(String, i32), Arc<Partition>>>,
    group_manager: &GroupManager,
) -> Result<(), BrokerError> {
    let topic_dir =
        log_dir::place_partition_dir(&config.all_log_dirs(), OFFSETS_TOPIC, OFFSETS_PARTITION);
    std::fs::create_dir_all(&topic_dir)?;
    let log = crabka_log::Log::open(&topic_dir, config.log_config.clone())?;
    let owning_dir = topic_dir
        .parent()
        .expect("placed partition dir always has a parent log.dir")
        .to_path_buf();

    // Register the topic via the metadata quorum. Tolerate `TopicExists`
    // because on broker restart the record is already in the replicated log.
    if controller.current_image().topic(OFFSETS_TOPIC).is_none() {
        let records = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: OFFSETS_TOPIC.to_string(),
                topic_id: uuid::Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: OFFSETS_TOPIC.to_string(),
                partition: OFFSETS_PARTITION,
                leader: config.node_id,
                replicas: vec![config.node_id],
                isr: vec![config.node_id],
                leader_epoch: 0,
                adding_replicas: vec![],
                removing_replicas: vec![],
            }),
        ];
        match controller.submit_change(records).await {
            // Another broker (or an earlier boot of ours) already registered
            // it — fine, treat as success.
            Ok(()) | Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => {}
            Err(e) => return Err(BrokerError::Startup(e.to_string())),
        }
    }

    // Replay before spawning the writer so reads see consistent state.
    replay_records(&log, group_manager).await?;

    // Spawn a writer + register the partition handle.
    let partition = spawn_partition(
        OFFSETS_TOPIC.to_string(),
        OFFSETS_PARTITION,
        owning_dir,
        log,
    );
    partitions.insert((OFFSETS_TOPIC.into(), OFFSETS_PARTITION), partition);
    Ok(())
}

/// Walk every `RecordBatch` in the log from offset 0 to `log_end_offset()`
/// and apply each record's key/value to the in-memory `GroupManager`.
async fn replay_records(
    log: &crabka_log::Log,
    group_manager: &GroupManager,
) -> Result<(), BrokerError> {
    let mut next = log.log_start_offset();
    let end = log.log_end_offset();
    while next < end {
        let out = log.read(next, 1024 * 1024)?;
        if out.batches.is_empty() {
            break;
        }
        let mut advanced_to = next;
        for batch in &out.batches {
            for record in &batch.records {
                if let (Some(key_bytes), Some(value_bytes)) = (&record.key, &record.value) {
                    let key = persistence::parse_key(key_bytes)?;
                    apply_record(group_manager, key, value_bytes, batch).await?;
                }
            }
            advanced_to = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
        }
        if advanced_to <= next {
            break;
        }
        next = advanced_to;
    }
    Ok(())
}

async fn apply_record(
    group_manager: &GroupManager,
    key: Key,
    value_bytes: &bytes::Bytes,
    batch: &RecordBatch,
) -> Result<(), BrokerError> {
    match key {
        Key::OffsetCommit {
            group_id,
            topic,
            partition,
        } => {
            // A value with negative length would have decoded to empty; we
            // already filtered on `Some(value)` so we have at least an empty
            // buf. Tombstones (`value = None`) are skipped upstream.
            let v = OffsetCommitValue::decode_value(value_bytes)?;
            let handle = group_manager.get_or_create(&group_id);
            let mut g = handle.state.lock().await;
            g.committed_offsets.insert(
                (topic, partition),
                OffsetEntry {
                    offset: v.offset,
                    leader_epoch: v.leader_epoch,
                    metadata: v.metadata,
                    commit_timestamp_ms: v.commit_timestamp_ms,
                },
            );
        }
        Key::GroupMetadata { group_id } => {
            let v = GroupMetadataValue::decode_value(value_bytes)?;
            let handle = group_manager.get_or_create(&group_id);
            let mut g = handle.state.lock().await;
            apply_group_metadata(&mut g, v, batch.max_timestamp);
        }
    }
    Ok(())
}

fn apply_group_metadata(g: &mut Group, v: GroupMetadataValue, replay_timestamp_ms: i64) {
    g.protocol_type = Some(v.protocol_type);
    g.generation_id = v.generation;
    g.leader_id = v.leader;
    g.protocol_name = v.protocol_name;
    // Repopulate members. `last_heartbeat` defaults to `now` inside
    // `Member::new` so they don't immediately time out; the client will
    // re-join anyway after a coordinator restart.
    g.members.clear();
    g.static_members.clear();
    for m in v.members {
        let session_timeout = std::time::Duration::from_millis(
            u64::try_from(m.session_timeout_ms.max(0)).unwrap_or(30_000),
        );
        let rebalance_timeout = std::time::Duration::from_millis(
            u64::try_from(m.rebalance_timeout_ms.max(0)).unwrap_or(60_000),
        );
        let mut member = Member::new(
            m.member_id.clone(),
            m.client_id,
            m.client_host,
            session_timeout,
            rebalance_timeout,
            Vec::new(),
        )
        .with_instance_id(m.group_instance_id.clone());
        member.protocol_metadata = m.subscription;
        member.assignment = Some(m.assignment);
        if let Some(iid) = m.group_instance_id {
            g.static_members.insert(iid, m.member_id.clone());
        }
        g.members.insert(m.member_id, member);
    }
    g.state = if g.members.is_empty() {
        crate::coordinator::group::GroupState::Empty
    } else {
        crate::coordinator::group::GroupState::Stable
    };
    let _ = replay_timestamp_ms; // currently unused; logged for debug
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BrokerConfig;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    /// Spin up a controller, wait until it reports a leader, return the handle.
    async fn controller_with_leader(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let cfg = crabka_raft::ControllerConfig {
            node_id: 1,
            voters: vec![(1, addr)],
            controller_listen_addr: addr,
            log_dir,
            election_timeout: Duration::from_millis(200),
            heartbeat_interval: Duration::from_millis(50),
            client_id: "test".into(),
            bootstrap_mode: crabka_raft::BootstrapMode::Bootstrap,
            cluster_id: None,
            dialer: None,
            handshake: None,
        };
        let handle = Arc::new(crabka_raft::Controller::start(cfg).await.unwrap());
        let mut rx = handle.watch_leader();
        let deadline = Instant::now() + Duration::from_secs(5);
        while rx.borrow().is_none() {
            assert!(Instant::now() < deadline, "no leader elected in 5s");
            let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
        }
        handle
    }

    #[tokio::test]
    async fn bootstrap_creates_topic_dir() {
        let dir = tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let controller = controller_with_leader(dir.path().join("__cluster_metadata_test")).await;
        let partitions: Arc<dashmap::DashMap<(String, i32), Arc<Partition>>> =
            Arc::new(dashmap::DashMap::new());
        let gm = GroupManager::new();
        bootstrap(&config, &controller, &partitions, &gm)
            .await
            .unwrap();
        let topic_dir = log_dir::partition_dir(&config.log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
        assert!(topic_dir.exists());
        assert!(partitions.contains_key(&(OFFSETS_TOPIC.into(), OFFSETS_PARTITION)));
        assert!(controller.current_image().topic(OFFSETS_TOPIC).is_some());
    }
}
