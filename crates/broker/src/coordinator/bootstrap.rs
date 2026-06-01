//! `__consumer_offsets` topic lifecycle: ensure the topic exists at
//! startup, then synchronously replay every record into the in-memory
//! `GroupCoordinator`.

use std::collections::HashMap;
use std::sync::Arc;

use crabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use crabka_protocol::records::RecordBatch;
use crabka_raft::RaftError;

use crate::broker::spawn_partition;
use crate::config::BrokerConfig;
use crate::coordinator::GroupCoordinator;
use crate::coordinator::persistence::{self, GroupMetadataValue, Key, OffsetCommitValue};
use crate::coordinator::unified::classic_state::{
    Group as ClassicState, GroupState as ClassicGroupState, Member, OffsetEntry,
};
use crate::coordinator::unified::group::{Group, GroupKind};
use crate::error::BrokerError;
use crate::log_dir;
use crate::partition_registry::PartitionRegistry;

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
    let replayed = replay_records(&log, coordinator)?;
    finalize(coordinator, replayed);

    // Spawn a writer + register the partition handle.
    let partition = spawn_partition(
        OFFSETS_TOPIC.to_string(),
        OFFSETS_PARTITION,
        owning_dir,
        log,
        log_dir_status.clone(),
    );
    partitions.insert(OFFSETS_TOPIC.into(), OFFSETS_PARTITION, partition);
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
            advanced_to = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
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

/// Apply a tombstone (record with `value = None`). Classic offset-commit /
/// group-metadata tombstones are no-ops during replay (preserved from the
/// classic coordinator, whose in-memory snapshot is rebuilt fresh on restart);
/// next-gen (KIP-848) and share-group (KIP-932) tombstones are honored so
/// leave/eviction semantics survive a restart.
fn apply_tombstone(coordinator: &Arc<GroupCoordinator>, key: Key) {
    match key {
        Key::NextGen(ng_key) => coordinator.replay_next_gen_tombstone(&ng_key),
        Key::Share(share_key) => coordinator.replay_share_tombstone(&share_key),
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
    use super::*;
    use crate::config::BrokerConfig;
    use assert2::assert;
    use crabka_raft::ControllerHandle;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    /// Spin up a controller, wait until it reports a leader, return the handle.
    async fn controller_with_leader(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
        let cfg = crabka_raft::ControllerConfig {
            election_timeout: Duration::from_millis(200),
            heartbeat_interval: Duration::from_millis(50),
            client_id: "test".into(),
            ..crabka_raft::ControllerConfig::for_tests(1, log_dir)
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
        use crate::coordinator::unified::GroupCoordinator;
        use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;
        use crate::coordinator::unified::reconciler::ReconcileInput;
        use crate::coordinator::unified::share::persistence as sp;
        use crabka_protocol::primitives::uuid::Uuid;

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
        assert!(seed.group_epoch == 4);
        assert!(seed.members.contains_key("m1"));
        assert!(seed.current_per_member["m1"].member_epoch == 4);

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
        )
        .await
        .unwrap();
        let topic_dir = log_dir::partition_dir(&config.log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
        assert!(topic_dir.exists());
        assert!(partitions.contains(OFFSETS_TOPIC, OFFSETS_PARTITION));
        assert!(controller.current_image().topic(OFFSETS_TOPIC).is_some());
    }

    #[test]
    fn apply_group_metadata_rebuilds_members_and_state() {
        use crate::coordinator::persistence::MemberMetadata;
        use bytes::Bytes;

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
        assert!(g.generation_id == 5);
        assert!(g.protocol_type.as_deref() == Some("consumer"));
        assert!(g.leader_id.as_deref() == Some("m1"));
        assert!(g.state == ClassicGroupState::Stable);
        assert!(g.members.contains_key("m1"));
        assert!(g.members["m1"].assignment.as_deref() == Some(b"asn" as &[u8]));
        assert!(g.current_member_id_for_instance("inst") == Some("m1"));

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
}
