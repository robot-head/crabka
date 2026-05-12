//! `__consumer_offsets` topic lifecycle: ensure the topic exists at
//! startup, then synchronously replay every record into the in-memory
//! `GroupManager`.

use std::sync::Arc;

use crabka_protocol::records::RecordBatch;

use crate::broker::spawn_partition;
use crate::config::BrokerConfig;
use crate::coordinator::GroupManager;
use crate::coordinator::group::{Group, Member, OffsetEntry};
use crate::coordinator::persistence::{self, GroupMetadataValue, Key, OffsetCommitValue};
use crate::error::BrokerError;
use crate::log_dir;
use crate::metadata::MetadataImage;
use crate::partition::Partition;

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";
pub const OFFSETS_PARTITION: i32 = 0;

/// Ensure the `__consumer_offsets-0` partition exists on disk, open its
/// `Log`, spawn a writer task, and replay every record into the supplied
/// `GroupManager`. Adds the topic to the metadata image as a 1-partition
/// internal topic.
///
/// Called exactly once from `Broker::start`, BEFORE the TCP listener binds.
pub async fn bootstrap(
    config: &BrokerConfig,
    metadata: &Arc<std::sync::RwLock<MetadataImage>>,
    partitions: &Arc<dashmap::DashMap<(String, i32), Arc<Partition>>>,
    group_manager: &GroupManager,
) -> Result<(), BrokerError> {
    let topic_dir = log_dir::partition_dir(&config.log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
    std::fs::create_dir_all(&topic_dir)?;
    let log = crabka_log::Log::open(&topic_dir, config.log_config.clone())?;

    // Register the topic in metadata.
    {
        let mut meta = metadata.write().expect("metadata poisoned");
        if meta.get(OFFSETS_TOPIC).is_none() {
            // 1 partition, leader = this broker.
            meta.insert_topic(OFFSETS_TOPIC, 1, config.broker_id);
        }
    }

    // Replay before spawning the writer so reads see consistent state.
    replay_records(&log, group_manager).await?;

    // Spawn a writer + register the partition handle.
    let partition = spawn_partition(OFFSETS_TOPIC.to_string(), OFFSETS_PARTITION, log);
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
            m.subscription,
        );
        member.assignment = Some(m.assignment);
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
    use tempfile::tempdir;

    #[tokio::test]
    async fn bootstrap_creates_topic_dir() {
        let dir = tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let metadata = Arc::new(std::sync::RwLock::new(MetadataImage::new()));
        let partitions: Arc<dashmap::DashMap<(String, i32), Arc<Partition>>> =
            Arc::new(dashmap::DashMap::new());
        let gm = GroupManager::new();
        bootstrap(&config, &metadata, &partitions, &gm)
            .await
            .unwrap();
        let topic_dir = log_dir::partition_dir(&config.log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
        assert!(topic_dir.exists());
        assert!(partitions.contains_key(&(OFFSETS_TOPIC.into(), OFFSETS_PARTITION)));
        assert!(metadata.read().unwrap().get(OFFSETS_TOPIC).is_some());
    }
}
