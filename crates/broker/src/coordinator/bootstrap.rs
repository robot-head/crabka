//! `__consumer_offsets` topic lifecycle: ensure the topic exists at
//! startup, then synchronously replay every record into the in-memory
//! `GroupCoordinator`.

use std::{collections::HashMap, sync::Arc};

use crabka_ids::PartitionIndex;
use crabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use crabka_protocol::records::RecordBatch;
use crabka_raft::RaftError;

use crate::{
    broker::spawn_partition,
    config::BrokerConfig,
    coordinator::{
        GroupCoordinator,
        persistence::{self, GroupMetadataValue, Key, OffsetCommitValue},
        unified::{
            classic_state::{
                Group as ClassicState, GroupState as ClassicGroupState, Member, OffsetEntry,
            },
            group::{Group, GroupKind},
        },
    },
    error::BrokerError,
    log_dir,
    partition_registry::PartitionRegistry,
};

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";
pub const OFFSETS_PARTITION: i32 = 0;
/// Number of partitions in `__consumer_offsets`. Bootstrap creates a
/// 1-partition topic (`OFFSETS_PARTITION = 0`), so all group-ids map to
/// partition 0. Shared so the transaction handlers (`AddOffsetsToTxn`,
/// `EndTxn`) agree on the partition a group's offset commits land in.
///
/// CAVEAT for a future multi-partition `__consumer_offsets`: Kafka partitions
/// this topic by group with `abs(groupId.hashCode()) % N` (Java
/// `String.hashCode`), NOT murmur2. `AddOffsetsToTxn` currently computes the
/// group's partition with `partition_for_tid` (murmur2) — correct only while
/// `N == 1`. Growing this past 1 requires a dedicated `partition_for_group`
/// using the Java-hashCode rule, applied consistently here and in the offset
/// storage path (which today hardcodes `OFFSETS_PARTITION = 0`).
pub const OFFSETS_NUM_PARTITIONS: i32 = 1;

/// Internal topic carrying tamper-evident OCSF audit records (`FedRAMP` MLA).
pub const AUDIT_TOPIC: &str = "__crabka_audit";

/// Create `__crabka_audit` with one partition per registered broker (RF=1).
///
/// Broker-affinity: partition `i` is led by the i-th broker (ascending node
/// id), so each broker leads exactly one audit partition and writes to it
/// locally. Idempotent: a `TopicExists` error from the controller is treated
/// as success (another broker or a restart already created the topic).
///
/// Only the quorum leader submits the metadata records, mirroring the
/// `__consumer_offsets` bootstrap path to avoid TOCTOU duplicate-id races.
/// Followers skip submission entirely; the leader's records replicate into
/// their image through the normal raft log.
///
/// Returns `Ok(())` immediately when `config.audit_enabled` is `false`.
pub async fn bootstrap_audit_topic(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
) -> Result<(), BrokerError> {
    if !config.audit_enabled {
        return Ok(());
    }

    // Only the quorum leader submits; copy out the leader id before any `.await`
    // so we don't hold the `Ref` across an await point.
    let am_leader = *controller.watch_leader().borrow() == Some(config.node_id);
    if !am_leader {
        return Ok(());
    }

    // Already created (idempotent restart or another leader beat us).
    if controller
        .current_image()
        .topic(&config.audit_topic)
        .is_some()
    {
        return Ok(());
    }

    let image = controller.current_image();
    let mut brokers: Vec<crabka_raft::NodeId> = image.brokers().map(|b| b.node_id).collect();
    drop(image);
    if brokers.is_empty() {
        brokers.push(config.node_id);
    }
    brokers.sort_unstable();

    let num_partitions = i32::try_from(brokers.len()).unwrap_or(1);
    // RF=1: partition i → brokers[i % len] as sole replica/leader.
    // Use the crate-internal round_robin helper; falls back to explicit
    // per-broker assignment when brokers.len() == num_partitions (the common
    // single-broker test case also satisfies this).
    let assignments =
        crate::handlers::create_topics::round_robin_replicas(&brokers, num_partitions, 1);

    let mut records = Vec::with_capacity(1 + usize::try_from(num_partitions).unwrap_or(0));
    records.push(MetadataRecord::V1Topic(TopicRecord {
        name: config.audit_topic.clone(),
        topic_id: uuid::Uuid::new_v4(),
        partitions: num_partitions,
        replication_factor: 1,
    }));
    for (p, replicas) in assignments.iter().enumerate() {
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: config.audit_topic.clone(),
            partition: i32::try_from(p).expect("audit partition index overflows i32"),
            leader: replicas[0],
            replicas: replicas.clone(),
            isr: replicas.clone(),
            leader_epoch: crabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }

    match controller.submit_change(records).await {
        // Idempotent: another broker / a restart already created it.
        Ok(()) | Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => Ok(()),
        Err(e) => Err(BrokerError::Startup(e.to_string())),
    }
}

/// Bootstrap-time accumulator. Committed offsets are protocol-agnostic, so we
/// collect them per group and attach them once the group's kind is known;
/// classic `GroupMetadata` builds a `ClassicState` in place. Next-gen records
/// feed the coordinator's own seed accumulator (`replay_*`), drained by
/// `finalize_bootstrap`.
#[derive(Default)]
struct Replayed {
    classic: HashMap<String, ClassicState>,
    committed: HashMap<String, HashMap<(String, i32), OffsetEntry>>,
}

/// Ensure the `__consumer_offsets-0` partition exists on disk, open its
/// `Log`, spawn a writer task, and replay every record into the supplied
/// `GroupCoordinator`. Registers the topic via the metadata quorum
/// (`controller.submit_change(...)`) as a 1-partition internal topic;
/// `TopicExists` is treated as success so a restart that finds the topic
/// already in the log is a no-op.
///
/// Called exactly once from `Broker::start`, BEFORE the TCP listener binds
/// and AFTER the controller has elected a leader (see `Broker::start`).
pub async fn bootstrap(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    partitions: &Arc<PartitionRegistry>,
    coordinator: &Arc<GroupCoordinator>,
    log_dir_status: &crate::log_dir_status::LogDirRegistry,
    producer_state: &Arc<crate::producer_state::ProducerState>,
) -> Result<(), BrokerError> {
    // KIP-113 offline-dir handling: exclude dirs flagged offline by the
    // startup probe; placing `__consumer_offsets-N` on a known-bad dir
    // would fail immediately at `Log::open` below and leave the broker
    // unable to bootstrap the group coordinator.
    let placement_dirs = log_dir_status.online_subset(&config.all_log_dirs());
    if placement_dirs.is_empty() {
        return Err(BrokerError::Io(std::io::Error::other(
            "every configured log.dir failed the startup writability probe; \
             cannot bootstrap the group-coordinator partition",
        )));
    }
    let topic_dir = log_dir::place_partition_dir(&placement_dirs, OFFSETS_TOPIC, OFFSETS_PARTITION);
    std::fs::create_dir_all(&topic_dir)?;
    let log = crabka_log::Log::open(&topic_dir, config.log_config.clone())?;
    let owning_dir = topic_dir
        .parent()
        .expect("placed partition dir always has a parent log.dir")
        .to_path_buf();

    // Register the topic via the metadata quorum, but only from a SINGLE
    // consistent writer: the controller leader. The previous `is_none()` ->
    // `submit_change` path was a TOCTOU race — when two voters boot
    // concurrently, both observe `__consumer_offsets` absent and both submit a
    // `TopicRecord` (`topic_id` is a random `Uuid::new_v4()` per node) plus a
    // `PartitionRecord` (`leader`/`replicas`/`isr` differ per node). The
    // controller's `TopicExists` dedup is apply-time, so BOTH conflicting
    // records land in the replicated metadata log, and a JVM follower
    // replicating that far fatal-faults with "Found duplicate TopicRecord for
    // __consumer_offsets with a different ID than before."
    //
    // Fix: only the leader registers the topic (one writer => one id, one
    // partition placement). Followers wait for the record to replicate into
    // their image rather than submitting a possibly-conflicting copy.
    if controller.current_image().topic(OFFSETS_TOPIC).is_none() {
        // Copy the leader id out of the watch `Ref` BEFORE any `.await` so we
        // don't hold the borrow across an await point.
        let am_leader = *controller.watch_leader().borrow() == Some(config.node_id);
        if am_leader {
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
                    leader_epoch: crabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                }),
            ];
            match controller.submit_change(records).await {
                // An earlier boot of ours already registered it (single
                // writer, so no conflicting-id race) — treat as success.
                Ok(())
                | Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => {}
                Err(e) => return Err(BrokerError::Startup(e.to_string())),
            }
        } else {
            // Follower: do NOT submit (that's the race). Wait for the leader's
            // record to replicate into our image. Failing loudly on timeout is
            // correct — submitting a duplicate on timeout is what caused the
            // JVM fatal fault.
            let mut images = controller.watch_image();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            while controller.current_image().topic(OFFSETS_TOPIC).is_none() {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(BrokerError::Startup(format!(
                        "timed out waiting for the controller leader to register \
                         {OFFSETS_TOPIC} in the metadata image"
                    )));
                }
                if tokio::time::timeout(remaining, images.changed())
                    .await
                    .is_err()
                {
                    return Err(BrokerError::Startup(format!(
                        "timed out waiting for the controller leader to register \
                         {OFFSETS_TOPIC} in the metadata image"
                    )));
                }
            }
        }
    }

    // Replay before spawning the writer so reads see consistent state.
    let replayed = replay_records(&log, coordinator)?;
    finalize(coordinator, replayed);

    // Spawn a writer + register the partition handle.
    let partition = spawn_partition(
        OFFSETS_TOPIC.to_string(),
        PartitionIndex(OFFSETS_PARTITION),
        owning_dir,
        log,
        log_dir_status.clone(),
        producer_state.clone(),
        false,
    );
    partitions.insert(
        OFFSETS_TOPIC.into(),
        PartitionIndex(OFFSETS_PARTITION),
        partition,
    );
    Ok(())
}

/// Walk every `RecordBatch` in the log from offset 0 to `log_end_offset()`
/// and apply each record's key/value into the accumulator (classic + offsets)
/// or, for next-gen records, the coordinator's seed accumulator.
fn replay_records(
    log: &crabka_log::Log,
    coordinator: &Arc<GroupCoordinator>,
) -> Result<Replayed, BrokerError> {
    let mut acc = Replayed::default();
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
                let Some(key_bytes) = &record.key else {
                    continue;
                };
                let key = persistence::parse_key(key_bytes)?;
                match &record.value {
                    Some(value_bytes) => {
                        apply_record(coordinator, &mut acc, key, value_bytes, batch)?;
                    }
                    None => {
                        apply_tombstone(coordinator, key);
                    }
                }
            }
            // The loop threads the log's `Offset` cursor (`next`/`end` feed
            // `Log::read`); wrap the batch-derived next offset back into `Offset`.
            advanced_to =
                crabka_log::Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
        if advanced_to <= next {
            break;
        }
        next = advanced_to;
    }
    Ok(acc)
}

fn apply_record(
    coordinator: &Arc<GroupCoordinator>,
    acc: &mut Replayed,
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
            let v = OffsetCommitValue::decode_value(value_bytes)?;
            acc.committed.entry(group_id).or_default().insert(
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
            let state = acc
                .classic
                .entry(group_id.clone())
                .or_insert_with(|| ClassicState::new(group_id));
            apply_group_metadata(state, v, batch.max_timestamp);
        }
        Key::NextGen(ng_key) => {
            apply_next_gen_record(coordinator, ng_key, value_bytes)?;
        }
        Key::Share(share_key) => {
            apply_share_record(coordinator, share_key, value_bytes)?;
        }
        Key::Streams(streams_key) => apply_streams_record(coordinator, streams_key, value_bytes)?,
    }
    Ok(())
}

fn apply_next_gen_record(
    coordinator: &Arc<GroupCoordinator>,
    key: crate::coordinator::unified::persistence_next_gen::NextGenKey,
    value_bytes: &bytes::Bytes,
) -> Result<(), BrokerError> {
    use crate::coordinator::unified::persistence_next_gen as ng;
    match key {
        ng::NextGenKey::GroupMetadata { group_id } => {
            coordinator
                .replay_group_metadata(&group_id, ng::GroupMetadataValue::decode(value_bytes)?);
        }
        ng::NextGenKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            coordinator.replay_member_metadata(
                &group_id,
                &member_id,
                ng::MemberMetadataValue::decode(value_bytes)?,
            );
        }
        ng::NextGenKey::TargetAssignmentMetadata { group_id } => {
            coordinator.replay_target_assignment_metadata(
                &group_id,
                ng::TargetAssignmentMetadataValue::decode(value_bytes)?,
            );
        }
        ng::NextGenKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            coordinator.replay_target_assignment_member(
                &group_id,
                &member_id,
                ng::TargetAssignmentMemberValue::decode(value_bytes)?,
            );
        }
        ng::NextGenKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            coordinator.replay_current_member_assignment(
                &group_id,
                &member_id,
                ng::CurrentMemberAssignmentValue::decode(value_bytes)?,
            );
        }
    }
    Ok(())
}

fn apply_share_record(
    coordinator: &Arc<GroupCoordinator>,
    key: crate::coordinator::unified::share::persistence::ShareGroupKey,
    value_bytes: &bytes::Bytes,
) -> Result<(), BrokerError> {
    use crate::coordinator::unified::share::persistence as sp;
    match key {
        sp::ShareGroupKey::GroupMetadata { group_id } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_group_metadata(
                &group_id,
                sp::ShareGroupMetadataValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_member_metadata(
                &group_id,
                &member_id,
                sp::ShareGroupMemberMetadataValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::TargetAssignmentMetadata { group_id } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_target_assignment_metadata(
                &group_id,
                sp::ShareGroupTargetAssignmentMetadataValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_target_assignment_member(
                &group_id,
                &member_id,
                sp::ShareGroupTargetAssignmentMemberValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_current_member_assignment(
                &group_id,
                &member_id,
                sp::ShareGroupCurrentMemberAssignmentValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::StatePartitionMetadata { group_id } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_state_partition_metadata(
                &group_id,
                sp::ShareGroupStatePartitionMetadataValue::decode(value_bytes)?,
            );
        }
    }
    Ok(())
}

fn apply_streams_record(
    coordinator: &Arc<GroupCoordinator>,
    key: crate::coordinator::unified::streams::persistence::StreamsGroupKey,
    value_bytes: &bytes::Bytes,
) -> Result<(), BrokerError> {
    use crate::coordinator::unified::streams::persistence as sp;
    match key {
        sp::StreamsGroupKey::GroupMetadata { group_id } => {
            coordinator.mark_streams(&group_id);
            let v = sp::StreamsGroupMetadataValue::decode(value_bytes)?;
            coordinator.replay_streams_group_metadata(&group_id, v.epoch);
        }
        sp::StreamsGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_member_metadata(
                &group_id,
                &member_id,
                sp::StreamsGroupMemberMetadataValue::decode(value_bytes)?,
            );
        }
        sp::StreamsGroupKey::Topology { group_id } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_topology(
                &group_id,
                sp::StreamsGroupTopologyValue::decode(value_bytes)?,
            );
        }
        sp::StreamsGroupKey::PartitionMetadata { group_id } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_partition_metadata(
                &group_id,
                sp::StreamsGroupPartitionMetadataValue::decode(value_bytes)?,
            );
        }
        sp::StreamsGroupKey::TargetAssignmentMetadata { group_id } => {
            coordinator.mark_streams(&group_id);
            let v = sp::StreamsGroupTargetAssignmentMetadataValue::decode(value_bytes)?;
            coordinator.replay_streams_target_assignment_metadata(&group_id, v.assignment_epoch);
        }
        sp::StreamsGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_target_assignment_member(
                &group_id,
                &member_id,
                sp::StreamsGroupTargetAssignmentMemberValue::decode(value_bytes)?,
            );
        }
        sp::StreamsGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_current_member_assignment(
                &group_id,
                &member_id,
                sp::StreamsGroupCurrentMemberAssignmentValue::decode(value_bytes)?,
            );
        }
    }
    Ok(())
}

/// Apply a tombstone (record with `value = None`). Classic offset-commit /
/// group-metadata tombstones are no-ops during replay (preserved from the
/// classic coordinator, whose in-memory snapshot is rebuilt fresh on restart);
/// next-gen (KIP-848) and share-group (KIP-932) tombstones are honored so
/// leave/eviction semantics survive a restart.
fn apply_tombstone(coordinator: &Arc<GroupCoordinator>, key: Key) {
    match key {
        Key::NextGen(ng_key) => coordinator.replay_next_gen_tombstone(&ng_key),
        Key::Share(share_key) => coordinator.replay_share_tombstone(&share_key),
        Key::Streams(streams_key) => coordinator.replay_streams_tombstone(&streams_key),
        Key::OffsetCommit { .. } | Key::GroupMetadata { .. } => {}
    }
}

/// Decide each group's kind and seed its actor. Next-gen groups (those that
/// accumulated next-gen records) spawn via `finalize_bootstrap`; their
/// committed offsets are attached afterward. Every other group with classic
/// metadata or committed offsets replays as a classic actor.
fn finalize(coordinator: &Arc<GroupCoordinator>, mut replayed: Replayed) {
    // Next-gen group ids are those present in the coordinator's seed map.
    let next_gen_ids: std::collections::HashSet<String> =
        coordinator.seeds.iter().map(|e| e.key().clone()).collect();

    // Spawn + seed next-gen (consumer) actors.
    coordinator.finalize_bootstrap();

    // Attach committed offsets to consumer groups; the rest are classic.
    let committed_groups: Vec<String> = replayed.committed.keys().cloned().collect();
    for gid in committed_groups {
        if next_gen_ids.contains(&gid)
            && let Some(offsets) = replayed.committed.remove(&gid)
            && let Some(handle) = coordinator.find(&gid)
        {
            let entries: Vec<((String, i32), OffsetEntry)> = offsets.into_iter().collect();
            let (tx, _rx) = tokio::sync::oneshot::channel();
            let _ = handle.tx.try_send(
                crate::coordinator::unified::actor::GroupActorMessage::UpdateCommitted {
                    entries,
                    reply: tx,
                },
            );
        }
    }

    // Classic groups: those with classic metadata, plus offset-only groups
    // that are not next-gen.
    let classic_ids: std::collections::HashSet<String> = replayed
        .classic
        .keys()
        .cloned()
        .chain(replayed.committed.keys().cloned())
        .filter(|gid| !next_gen_ids.contains(gid))
        .collect();
    for gid in classic_ids {
        let state = replayed
            .classic
            .remove(&gid)
            .unwrap_or_else(|| ClassicState::new(gid.clone()));
        let committed_offsets = replayed.committed.remove(&gid).unwrap_or_default();
        let group = Box::new(Group {
            group_id: gid.clone(),
            kind: GroupKind::Classic(state),
            committed_offsets,
        });
        coordinator.seed_classic(&gid, group);
    }
}

fn apply_group_metadata(g: &mut ClassicState, v: GroupMetadataValue, replay_timestamp_ms: i64) {
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
        ClassicGroupState::Empty
    } else {
        ClassicGroupState::Stable
    };
    let _ = replay_timestamp_ms; // currently unused; logged for debug
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use assert2::{assert, check};
    use crabka_raft::ControllerHandle;
    use tempfile::tempdir;

    use super::*;
    use crate::config::BrokerConfig;

    /// Spin up a controller, wait until it reports a leader, return the handle.
    async fn controller_with_leader(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
        let cfg = crabka_raft::ControllerConfig {
            election_timeout: Duration::from_millis(200),
            heartbeat_interval: Duration::from_millis(50),
            client_id: "test".into(),
            ..crabka_raft::ControllerConfig::for_tests(crabka_raft::NodeId(1), log_dir)
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

    /// Replaying a share-group's records (group metadata, member metadata,
    /// target + current assignment) must reconstruct the cached seed so a
    /// freshly-spawned actor restores the same membership after a restart.
    #[tokio::test]
    async fn share_group_records_replay_into_seed() {
        use crabka_protocol::primitives::uuid::Uuid;

        use crate::coordinator::unified::{
            GroupCoordinator, offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
            share::persistence as sp,
        };

        #[derive(Debug)]
        struct EmptyMeta;
        impl crate::coordinator::unified::actor::MetadataProvider for EmptyMeta {
            fn snapshot(&self) -> ReconcileInput {
                ReconcileInput::default()
            }
        }

        let coord = Arc::new(GroupCoordinator::new(
            crate::coordinator::unified::config::NextGenConfig::default(),
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            Arc::new(EmptyMeta),
            Arc::new(InMemoryOffsetsLog::default()),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));

        let tid = Uuid([9; 16]);
        // Drive the same path bootstrap takes: parse_key on the encoded key,
        // then apply_record on the value bytes.
        let recs: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
            (
                sp::encode_share_key(&sp::ShareGroupKey::GroupMetadata {
                    group_id: "sg".into(),
                }),
                sp::ShareGroupMetadataValue { epoch: 4 }.encode(),
            ),
            (
                sp::encode_share_key(&sp::ShareGroupKey::MemberMetadata {
                    group_id: "sg".into(),
                    member_id: "m1".into(),
                }),
                sp::ShareGroupMemberMetadataValue {
                    rack_id: None,
                    client_id: "c1".into(),
                    client_host: "/127.0.0.1".into(),
                    subscribed_topic_names: vec!["t".into()],
                }
                .encode(),
            ),
            (
                sp::encode_share_key(&sp::ShareGroupKey::CurrentMemberAssignment {
                    group_id: "sg".into(),
                    member_id: "m1".into(),
                }),
                sp::ShareGroupCurrentMemberAssignmentValue {
                    member_epoch: 4,
                    assigned_partitions: vec![(tid, vec![0, 1])],
                }
                .encode(),
            ),
        ];
        let batch = RecordBatch::default();
        let mut acc = Replayed::default();
        for (k, v) in recs {
            let key = persistence::parse_key(&k).unwrap();
            apply_record(&coord, &mut acc, key, &v, &batch).unwrap();
        }

        // Type locked + seed reconstructed.
        assert!(coord.group_type("sg") == Some(crate::coordinator::unified::GroupType::Share));
        let seed = coord.cached_share_seed("sg").expect("seed cached");
        check!(seed.group_epoch == 4);
        check!(seed.members.contains_key("m1"));
        check!(seed.current_per_member["m1"].member_epoch == 4);

        // A member tombstone scrubs the member from the seed.
        let tomb_key =
            persistence::parse_key(&sp::encode_share_key(&sp::ShareGroupKey::MemberMetadata {
                group_id: "sg".into(),
                member_id: "m1".into(),
            }))
            .unwrap();
        apply_tombstone(&coord, tomb_key);
        let seed = coord.cached_share_seed("sg").expect("seed still present");
        assert!(!seed.members.contains_key("m1"), "tombstone removed member");
    }

    /// Replaying a streams-group's records (group metadata, member metadata,
    /// current assignment) must lock the group type to Streams and reconstruct
    /// the cached seed; a member tombstone scrubs that member from the seed.
    #[tokio::test]
    async fn streams_group_records_replay_into_seed() {
        use std::collections::BTreeMap;

        use crate::coordinator::unified::{
            GroupCoordinator, offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
            streams::persistence as sp,
        };

        #[derive(Debug)]
        struct EmptyMeta;
        impl crate::coordinator::unified::actor::MetadataProvider for EmptyMeta {
            fn snapshot(&self) -> ReconcileInput {
                ReconcileInput::default()
            }
        }

        let coord = Arc::new(GroupCoordinator::new(
            crate::coordinator::unified::config::NextGenConfig::default(),
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            Arc::new(EmptyMeta),
            Arc::new(InMemoryOffsetsLog::default()),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));

        // Drive the same path bootstrap takes: parse_key on the encoded key,
        // then apply_record on the value bytes.
        let recs: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
            (
                sp::encode_streams_key(&sp::StreamsGroupKey::GroupMetadata {
                    group_id: "stg".into(),
                }),
                sp::StreamsGroupMetadataValue { epoch: 7 }.encode(),
            ),
            (
                sp::encode_streams_key(&sp::StreamsGroupKey::MemberMetadata {
                    group_id: "stg".into(),
                    member_id: "m1".into(),
                }),
                sp::StreamsGroupMemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "c1".into(),
                    client_host: "/127.0.0.1".into(),
                    process_id: "p1".into(),
                    user_endpoint: None,
                    client_tags: vec![],
                    rebalance_timeout_ms: 60_000,
                    topology_epoch: 2,
                }
                .encode(),
            ),
            (
                sp::encode_streams_key(&sp::StreamsGroupKey::CurrentMemberAssignment {
                    group_id: "stg".into(),
                    member_id: "m1".into(),
                }),
                sp::StreamsGroupCurrentMemberAssignmentValue {
                    member_epoch: 7,
                    previous_member_epoch: 6,
                    state: 0,
                    active: BTreeMap::from([("0".to_string(), vec![0, 1])]),
                    standby: BTreeMap::new(),
                    warmup: BTreeMap::new(),
                    active_pending_revocation: BTreeMap::new(),
                }
                .encode(),
            ),
        ];
        let batch = RecordBatch::default();
        let mut acc = Replayed::default();
        for (k, v) in recs {
            let key = persistence::parse_key(&k).unwrap();
            apply_record(&coord, &mut acc, key, &v, &batch).unwrap();
        }

        // Type locked to Streams + seed reconstructed.
        assert!(coord.group_type("stg") == Some(crate::coordinator::unified::GroupType::Streams));
        let seed = coord.cached_streams_seed("stg").expect("seed cached");
        check!(seed.group_epoch == 7);
        check!(seed.members.contains_key("m1"));
        check!(seed.current_per_member["m1"].member_epoch == 7);

        // A member tombstone scrubs the member from the seed.
        let tomb_key = persistence::parse_key(&sp::encode_streams_key(
            &sp::StreamsGroupKey::MemberMetadata {
                group_id: "stg".into(),
                member_id: "m1".into(),
            },
        ))
        .unwrap();
        apply_tombstone(&coord, tomb_key);
        let seed = coord
            .cached_streams_seed("stg")
            .expect("seed still present");
        assert!(!seed.members.contains_key("m1"), "tombstone removed member");
    }

    fn test_coordinator(
        controller: &Arc<dyn crate::metadata_source::MetadataSource>,
        partitions: &Arc<PartitionRegistry>,
    ) -> Arc<GroupCoordinator> {
        let offsets_log: Arc<dyn crate::coordinator::unified::offsets_log::OffsetsLog> = Arc::new(
            crate::coordinator::unified::offsets_log::ProductionOffsetsLog::new(partitions.clone()),
        );
        Arc::new(GroupCoordinator::new(
            crate::coordinator::unified::config::NextGenConfig::default(),
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            Arc::new(crate::coordinator::unified::ImageMetadataProvider {
                controller: controller.clone(),
            }),
            offsets_log,
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ))
    }

    #[tokio::test]
    async fn bootstrap_creates_topic_dir() {
        let dir = tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let controller: Arc<dyn crate::metadata_source::MetadataSource> =
            controller_with_leader(dir.path().join("__cluster_metadata_test")).await;
        let partitions: Arc<PartitionRegistry> = Arc::new(PartitionRegistry::new());
        let coordinator = test_coordinator(&controller, &partitions);
        let log_dir_status = crate::log_dir_status::LogDirRegistry::probe(&config.all_log_dirs());
        bootstrap(
            &config,
            &controller,
            &partitions,
            &coordinator,
            &log_dir_status,
            &Arc::new(crate::producer_state::ProducerState::new()),
        )
        .await
        .unwrap();
        let topic_dir = log_dir::partition_dir(&config.log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
        check!(topic_dir.exists());
        check!(partitions.contains(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION)));
        check!(controller.current_image().topic(OFFSETS_TOPIC).is_some());
    }

    /// Regression for the bootstrap TOCTOU: a SECOND bootstrap against a
    /// controller that already has `__consumer_offsets` (the leader registered
    /// it on the first boot) must NOT submit a second, conflicting
    /// `TopicRecord`. It must observe the existing topic, no-op the
    /// registration, succeed, and leave EXACTLY ONE `__consumer_offsets` topic
    /// in the image. This exercises the "already exists => no-op" arm plus the
    /// leader-gate (the test node 1 is the leader, so the first boot is the
    /// single writer; the second boot finds the topic present and skips).
    #[tokio::test]
    async fn second_bootstrap_does_not_duplicate_offsets_topic() {
        let dir = tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let controller: Arc<dyn crate::metadata_source::MetadataSource> =
            controller_with_leader(dir.path().join("__cluster_metadata_test")).await;
        let partitions: Arc<PartitionRegistry> = Arc::new(PartitionRegistry::new());
        let coordinator = test_coordinator(&controller, &partitions);
        let log_dir_status = crate::log_dir_status::LogDirRegistry::probe(&config.all_log_dirs());

        // First boot: this node IS the leader, so it registers the topic.
        bootstrap(
            &config,
            &controller,
            &partitions,
            &coordinator,
            &log_dir_status,
            &Arc::new(crate::producer_state::ProducerState::new()),
        )
        .await
        .unwrap();
        let id_after_first = controller
            .current_image()
            .topic(OFFSETS_TOPIC)
            .expect("offsets topic registered on first boot")
            .topic_id;

        // Second boot (simulating a restart / a second broker reaching
        // bootstrap): topic already present => must be a no-op, no second
        // submit, and must succeed.
        bootstrap(
            &config,
            &controller,
            &partitions,
            &coordinator,
            &log_dir_status,
            &Arc::new(crate::producer_state::ProducerState::new()),
        )
        .await
        .unwrap();

        // Exactly one `__consumer_offsets` topic, and its id is unchanged
        // (no conflicting duplicate landed in the log).
        let image = controller.current_image();
        let count = image.topics().filter(|t| t.name == OFFSETS_TOPIC).count();
        assert!(
            count == 1,
            "expected exactly one __consumer_offsets, got {count}"
        );
        assert!(
            image.topic(OFFSETS_TOPIC).unwrap().topic_id == id_after_first,
            "topic_id changed across boots — a duplicate TopicRecord was submitted"
        );
    }

    #[test]
    fn apply_group_metadata_rebuilds_members_and_state() {
        use bytes::Bytes;

        use crate::coordinator::persistence::MemberMetadata;

        let mut g = ClassicState::new("g");
        let v = GroupMetadataValue {
            protocol_type: "consumer".into(),
            generation: 5,
            protocol_name: Some("range".into()),
            leader: Some("m1".into()),
            current_state_timestamp_ms: 0,
            members: vec![MemberMetadata {
                member_id: "m1".into(),
                group_instance_id: Some("inst".into()),
                client_id: "c".into(),
                client_host: "h".into(),
                rebalance_timeout_ms: 60_000,
                session_timeout_ms: 30_000,
                subscription: Bytes::new(),
                assignment: Bytes::from_static(b"asn"),
            }],
        };
        apply_group_metadata(&mut g, v, 0);
        check!(g.generation_id == 5);
        check!(g.protocol_type.as_deref() == Some("consumer"));
        check!(g.leader_id.as_deref() == Some("m1"));
        check!(g.state == ClassicGroupState::Stable);
        check!(g.members.contains_key("m1"));
        check!(g.members["m1"].assignment.as_deref() == Some(b"asn" as &[u8]));
        check!(g.current_member_id_for_instance("inst") == Some("m1"));

        // No members → Empty state.
        let mut empty = ClassicState::new("g2");
        apply_group_metadata(
            &mut empty,
            GroupMetadataValue {
                protocol_type: "consumer".into(),
                generation: 0,
                protocol_name: None,
                leader: None,
                current_state_timestamp_ms: 0,
                members: vec![],
            },
            0,
        );
        assert!(empty.state == ClassicGroupState::Empty);
    }

    /// Build a bare `GroupCoordinator` with no metadata/persister wiring — the
    /// same shape the share/streams replay tests use. Suitable for driving the
    /// `apply_record` / `apply_tombstone` / `finalize` replay path directly.
    fn bare_coordinator() -> Arc<GroupCoordinator> {
        use crate::coordinator::unified::{
            offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
        };

        #[derive(Debug)]
        struct EmptyMeta;
        impl crate::coordinator::unified::actor::MetadataProvider for EmptyMeta {
            fn snapshot(&self) -> ReconcileInput {
                ReconcileInput::default()
            }
        }

        Arc::new(GroupCoordinator::new(
            crate::coordinator::unified::config::NextGenConfig::default(),
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            Arc::new(EmptyMeta),
            Arc::new(InMemoryOffsetsLog::default()),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ))
    }

    /// Encode a classic k2 `GroupMetadata` (key, value) record pair for group
    /// `g` carrying a single member `m1`.
    fn classic_group_record(group_id: &str, member_id: &str) -> (bytes::Bytes, bytes::Bytes) {
        use crate::coordinator::persistence::MemberMetadata;
        let key = GroupMetadataValue::encode_key(group_id);
        let value = GroupMetadataValue {
            protocol_type: "consumer".into(),
            generation: 3,
            protocol_name: Some("range".into()),
            leader: Some(member_id.into()),
            current_state_timestamp_ms: 0,
            members: vec![MemberMetadata {
                member_id: member_id.into(),
                group_instance_id: None,
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                rebalance_timeout_ms: 60_000,
                session_timeout_ms: 30_000,
                subscription: bytes::Bytes::new(),
                assignment: bytes::Bytes::from_static(b"asn"),
            }],
        }
        .encode_value();
        (key, value)
    }

    /// PROBLEM A (the downgrade trap): a group that was created classic,
    /// UPGRADED to next-gen, then DOWNGRADED back to classic must replay as a
    /// CLASSIC group — not as an empty next-gen group. The downgrade drops the
    /// k3 `GroupMetadata` with a tombstone; replay must remove the next-gen
    /// seed entirely so the later fresh k2 record reconstructs the classic
    /// group. Log order wins.
    #[tokio::test]
    async fn downgraded_group_replays_as_classic() {
        use crate::coordinator::unified::{
            GroupType, persistence_next_gen as ng, persistence_next_gen,
        };

        let coord = bare_coordinator();

        // Helper to encode a next-gen (group/member) record key.
        let ng_group_key = |gid: &str| {
            ng::encode_key(&ng::NextGenKey::GroupMetadata {
                group_id: gid.into(),
            })
        };
        let ng_member_key = |gid: &str, mid: &str| {
            ng::encode_key(&ng::NextGenKey::MemberMetadata {
                group_id: gid.into(),
                member_id: mid.into(),
            })
        };

        // Record stream in log order.
        let (k2_key, k2_val) = classic_group_record("g", "m1");
        let (k2_key2, k2_val2) = classic_group_record("g", "m1");
        let stream: Vec<(bytes::Bytes, Option<bytes::Bytes>)> = vec![
            // 1. initial classic group
            (k2_key, Some(k2_val)),
            // 2. upgrade drops k2 (tombstone)
            (GroupMetadataValue::encode_key("g"), None),
            // 3. upgrade: next-gen group metadata
            (
                ng_group_key("g"),
                Some(persistence_next_gen::GroupMetadataValue { epoch: 1 }.encode()),
            ),
            // 4. upgrade: next-gen member metadata
            (
                ng_member_key("g", "m1"),
                Some(
                    persistence_next_gen::MemberMetadataValue {
                        instance_id: None,
                        rack_id: None,
                        client_id: "c1".into(),
                        client_host: "/127.0.0.1".into(),
                        subscribed_topic_names: vec!["t".into()],
                        subscribed_topic_regex: None,
                        server_assignor: None,
                        rebalance_timeout_ms: 60_000,
                        classic: None,
                    }
                    .encode(),
                ),
            ),
            // 5. downgrade drops k3 (next-gen group tombstone)
            (ng_group_key("g"), None),
            // 6. downgrade drops k5 (next-gen member tombstone)
            (ng_member_key("g", "m1"), None),
            // 7. downgrade writes a fresh k2 classic group
            (k2_key2, Some(k2_val2)),
        ];

        let batch = RecordBatch::default();
        let mut acc = Replayed::default();
        for (k, v) in stream {
            let key = persistence::parse_key(&k).unwrap();
            match v {
                Some(value) => apply_record(&coord, &mut acc, key, &value, &batch).unwrap(),
                None => apply_tombstone(&coord, key),
            }
        }
        finalize(&coord, acc);

        // The group must NOT be next-gen, and the classic describe path must
        // surface it with member "m1".
        assert!(coord.group_type("g") != Some(GroupType::NextGen));
        let snap = coord
            .describe_group("g")
            .await
            .expect("classic group present");
        assert!(snap.members.iter().any(|m| m.member_id == "m1"));
        // And there is no next-gen consumer actor for "g".
        assert!(
            coord.find("g").is_some_and(
                |h| h.kind == crate::coordinator::unified::actor::GroupKindTag::Classic
            )
        );
    }

    /// PROBLEM A under LOG COMPACTION (the resurrection trap): a downgraded
    /// group whose batch tombstoned the k3 `GroupMetadata` but NOT the
    /// group-level k6 `TargetAssignmentMetadata` would, after compaction GCs the
    /// tombstoned k3, leave a surviving k6 write behind. `__consumer_offsets` is
    /// compacted by default, so replay then sees the post-compaction residue:
    /// just the surviving k6 write plus the fresh classic k2 — NO k3, NO k3
    /// tombstone. Because `replay_target_assignment_metadata` does
    /// `seeds.entry(..).or_default()`, that lone k6 re-creates a next-gen seed,
    /// `finalize` classifies the group next-gen, and the classic k2 is dropped —
    /// resurrecting the group as an empty next-gen consumer. The fix tombstones
    /// k6 in the downgrade batch so compaction retains the k6 TOMBSTONE (last
    /// value per key) instead of a stale write; this test pins the corrected
    /// post-compaction shape and asserts the group replays CLASSIC.
    #[tokio::test]
    async fn compacted_downgrade_residue_replays_as_classic() {
        use crate::coordinator::unified::{GroupType, persistence_next_gen as ng};

        let coord = bare_coordinator();

        // Post-compaction record stream. Compaction keeps only the LAST value
        // per key, and the k3 + its tombstone both GC away (both gone), leaving:
        let (k2_key, k2_val) = classic_group_record("g", "m1");
        let stream: Vec<(bytes::Bytes, Option<bytes::Bytes>)> = vec![
            // The k6 TOMBSTONE the fix emits in the downgrade batch survives
            // compaction as the last value for the group-level k6 key. Replaying
            // a tombstone must NOT create a next-gen seed.
            (
                ng::encode_key(&ng::NextGenKey::TargetAssignmentMetadata {
                    group_id: "g".into(),
                }),
                None,
            ),
            // The fresh classic k2 written by the downgrade.
            (k2_key, Some(k2_val)),
        ];

        let batch = RecordBatch::default();
        let mut acc = Replayed::default();
        for (k, v) in stream {
            let key = persistence::parse_key(&k).unwrap();
            match v {
                Some(value) => apply_record(&coord, &mut acc, key, &value, &batch).unwrap(),
                None => apply_tombstone(&coord, key),
            }
        }
        finalize(&coord, acc);

        // The group must replay CLASSIC, not resurrect as next-gen.
        assert!(coord.group_type("g") != Some(GroupType::NextGen));
        let snap = coord
            .describe_group("g")
            .await
            .expect("classic group present");
        assert!(snap.members.iter().any(|m| m.member_id == "m1"));
        assert!(
            coord.find("g").is_some_and(
                |h| h.kind == crate::coordinator::unified::actor::GroupKindTag::Classic
            )
        );
    }

    /// Counterpoint to `compacted_downgrade_residue_replays_as_classic`: WITHOUT
    /// the k6 tombstone, a surviving k6 WRITE re-creates a next-gen seed and the
    /// group wrongly resurrects as next-gen (the bug being fixed). This pins the
    /// hazard so a regression that drops the k6 tombstone is caught.
    #[tokio::test]
    async fn surviving_k6_write_resurrects_as_next_gen_without_fix() {
        use crate::coordinator::unified::persistence_next_gen as ng;

        let coord = bare_coordinator();
        let (k2_key, k2_val) = classic_group_record("g", "m1");
        let stream: Vec<(bytes::Bytes, Option<bytes::Bytes>)> = vec![
            // A surviving k6 WRITE (what compaction would retain if the
            // downgrade had NOT tombstoned k6).
            (
                ng::encode_key(&ng::NextGenKey::TargetAssignmentMetadata {
                    group_id: "g".into(),
                }),
                Some(
                    ng::TargetAssignmentMetadataValue {
                        assignment_epoch: 1,
                    }
                    .encode(),
                ),
            ),
            (k2_key, Some(k2_val)),
        ];

        let batch = RecordBatch::default();
        let mut acc = Replayed::default();
        for (k, v) in stream {
            let key = persistence::parse_key(&k).unwrap();
            match v {
                Some(value) => apply_record(&coord, &mut acc, key, &value, &batch).unwrap(),
                None => apply_tombstone(&coord, key),
            }
        }

        // The lone k6 write re-created a next-gen seed via the
        // `seeds.entry(..).or_default()` in `replay_target_assignment_metadata`
        // — the exact hazard the k6 tombstone prevents. `finalize` derives its
        // next-gen id set from `coordinator.seeds`, so this stray seed is what
        // makes it suppress the classic k2 reconstruction.
        assert!(coord.seeds.contains_key("g"));

        finalize(&coord, acc);

        // Resurrection: `finalize` spawned a CONSUMER (next-gen) actor for "g"
        // off that stray seed instead of the classic actor the k2 should have
        // produced. (Asserting the spawned actor's kind, set synchronously at
        // spawn, avoids the async `group_types` mark the actor records only as
        // it processes its seed.)
        assert!(
            coord.find("g").is_some_and(
                |h| h.kind == crate::coordinator::unified::actor::GroupKindTag::Consumer
            )
        );
    }

    /// Existing upgrade-only replay (k3 live, no later tombstone) must still
    /// yield a CONSUMER (next-gen) group. Guards the PROBLEM A fix against an
    /// over-eager seed removal.
    #[tokio::test]
    async fn upgraded_group_without_tombstone_replays_as_consumer() {
        use crate::coordinator::unified::{
            GroupType, persistence_next_gen as ng, persistence_next_gen,
        };

        let coord = bare_coordinator();
        let stream: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
            (
                ng::encode_key(&ng::NextGenKey::GroupMetadata {
                    group_id: "g".into(),
                }),
                persistence_next_gen::GroupMetadataValue { epoch: 1 }.encode(),
            ),
            (
                ng::encode_key(&ng::NextGenKey::MemberMetadata {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                }),
                persistence_next_gen::MemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "c1".into(),
                    client_host: "/127.0.0.1".into(),
                    subscribed_topic_names: vec!["t".into()],
                    subscribed_topic_regex: None,
                    server_assignor: None,
                    rebalance_timeout_ms: 60_000,
                    classic: None,
                }
                .encode(),
            ),
        ];
        let batch = RecordBatch::default();
        let mut acc = Replayed::default();
        for (k, v) in stream {
            let key = persistence::parse_key(&k).unwrap();
            apply_record(&coord, &mut acc, key, &v, &batch).unwrap();
        }
        finalize(&coord, acc);

        assert!(coord.group_type("g") != Some(GroupType::Classic));
        let handle = coord.find("g").expect("consumer actor present");
        assert!(handle.kind == crate::coordinator::unified::actor::GroupKindTag::Consumer);
    }

    /// PROBLEM B (facade not restored): a k5 `MemberMetadataValue` carrying a
    /// `classic` block must reconstruct the in-memory member's
    /// `ClassicMemberFacade` on replay. The replayed consumer group's member
    /// "m1" must report `is_classic == true` via the next-gen `Describe` view.
    #[tokio::test]
    async fn member_with_classic_block_replays_facade() {
        use tokio::sync::oneshot;

        use crate::coordinator::unified::{
            actor::{GroupActorMessage, GroupKindTag},
            persistence_next_gen as ng, persistence_next_gen,
        };

        let coord = bare_coordinator();
        let stream: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
            (
                ng::encode_key(&ng::NextGenKey::GroupMetadata {
                    group_id: "g".into(),
                }),
                persistence_next_gen::GroupMetadataValue { epoch: 2 }.encode(),
            ),
            (
                ng::encode_key(&ng::NextGenKey::MemberMetadata {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                }),
                persistence_next_gen::MemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "c1".into(),
                    client_host: "/127.0.0.1".into(),
                    subscribed_topic_names: vec!["t".into()],
                    subscribed_topic_regex: None,
                    server_assignor: None,
                    rebalance_timeout_ms: 60_000,
                    classic: Some(persistence_next_gen::ClassicMemberMetadata {
                        session_timeout_ms: 30_000,
                        supported_protocols: vec![(
                            "range".into(),
                            bytes::Bytes::from_static(b"meta"),
                        )],
                        last_synced_assignment: bytes::Bytes::from_static(b"asn"),
                    }),
                }
                .encode(),
            ),
        ];
        let batch = RecordBatch::default();
        let mut acc = Replayed::default();
        for (k, v) in stream {
            let key = persistence::parse_key(&k).unwrap();
            apply_record(&coord, &mut acc, key, &v, &batch).unwrap();
        }
        finalize(&coord, acc);

        let handle = coord.find("g").expect("consumer actor present");
        assert!(handle.kind == GroupKindTag::Consumer);
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        let view = rx.await.unwrap();
        let m1 = view
            .members
            .iter()
            .find(|m| m.member_id == "m1")
            .expect("member m1 present");
        assert!(m1.is_classic, "facade reconstructed from k5 classic block");
    }

    /// `replay_records` must walk EVERY batch in the log, not just the first.
    /// The cursor that advances between batches is
    /// `base_offset + last_offset_delta + 1`; a two-record first batch
    /// (`last_offset_delta == 1`) followed by a second batch is only fully
    /// replayed if that arithmetic is exact. Both offset-commit records from
    /// the first batch AND the commit from the second batch must land in
    /// `acc.committed`.
    #[tokio::test]
    async fn replay_records_walks_all_batches() {
        use crabka_log::Offset;
        use crabka_protocol::records::Record;

        use crate::coordinator::unified::{
            GroupCoordinator, offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
        };

        #[derive(Debug)]
        struct EmptyMeta;
        impl crate::coordinator::unified::actor::MetadataProvider for EmptyMeta {
            fn snapshot(&self) -> ReconcileInput {
                ReconcileInput::default()
            }
        }

        let coord = Arc::new(GroupCoordinator::new(
            crate::coordinator::unified::config::NextGenConfig::default(),
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            Arc::new(EmptyMeta),
            Arc::new(InMemoryOffsetsLog::default()),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));

        // Build an offset-commit record with a distinct committed offset per
        // (topic, partition), so we can tell which batches were replayed.
        let commit_record = |partition: i32, offset: i64| Record {
            offset_delta: 0,
            key: Some(OffsetCommitValue::encode_key("g", "t", partition)),
            value: Some(
                OffsetCommitValue {
                    offset: Offset(offset),
                    leader_epoch: -1,
                    metadata: String::new(),
                    commit_timestamp_ms: 0,
                }
                .encode_value(),
            ),
            ..Default::default()
        };

        let dir = tempdir().unwrap();
        let mut log = crabka_log::Log::open(dir.path(), crabka_log::LogConfig::default()).unwrap();

        // First batch spans TWO offsets (last_offset_delta == 1): partitions 0
        // and 1 commit at offsets 100 and 101.
        let mut batch1 = RecordBatch {
            last_offset_delta: 1,
            ..RecordBatch::default()
        };
        batch1.records.push(commit_record(0, 100));
        batch1.records.push(Record {
            offset_delta: 1,
            ..commit_record(1, 101)
        });
        log.append(&mut batch1).unwrap();

        // Second batch: partition 2 commits at offset 202. Reaching it requires
        // the inter-batch cursor to have advanced past the first batch.
        let mut batch2 = RecordBatch::default();
        batch2.records.push(commit_record(2, 202));
        log.append(&mut batch2).unwrap();

        let replayed = replay_records(&log, &coord).unwrap();
        let committed = replayed
            .committed
            .get("g")
            .expect("group g has committed offsets");

        // All three commits present — the second batch is only reached when the
        // cursor arithmetic `base_offset + last_offset_delta + 1` is exact.
        check!(committed.len() == 3);
        check!(committed[&("t".to_string(), 0)].offset == 100);
        check!(committed[&("t".to_string(), 1)].offset == 101);
        check!(committed[&("t".to_string(), 2)].offset == 202);
    }
}
