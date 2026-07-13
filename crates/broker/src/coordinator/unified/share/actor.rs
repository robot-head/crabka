//! Per-share-group tokio actor (KIP-932). Owns [`ShareGroupState`] for one
//! share group. Heartbeats arrive as mpsc messages; responses go back via
//! oneshot channels.
//!
//! Mirrors the consumer next-gen [`crate::coordinator::unified::actor`] minus
//! all offset-validation and partition-revocation machinery: share-group
//! assignment is non-exclusive, so a member's epoch advances straight to the
//! group epoch with no acknowledgement round-trip.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use crabka_protocol::{
    owned::{
        common::share_group_heartbeat_response::topic_partitions::TopicPartitions,
        share_group_describe_response::{DescribedGroup, Member as DescribeMember},
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::{
            Assignment as RespAssignment, ShareGroupHeartbeatResponse,
        },
    },
    primitives::uuid::Uuid,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use super::{
    assignor::ShareGroupAssignor,
    config::ShareGroupConfig,
    persistence::{
        ShareGroupCurrentMemberAssignmentValue, ShareGroupKey, ShareGroupMemberMetadataValue,
        ShareGroupMetadataValue, ShareGroupStatePartitionMetadataValue,
        ShareGroupTargetAssignmentMemberValue, ShareGroupTargetAssignmentMetadataValue,
        encode_share_key,
    },
    state::{ShareGroupState, ShareMemberState},
};
use crate::{
    codes,
    coordinator::unified::{
        OffsetRecordBatchBuilder,
        actor::MetadataProvider,
        assignor::{MemberSubscription, TopicMetadata},
        first_join_member_id,
        offsets_log::OffsetsLog,
        reconciler::ReconcileInput,
        validate_member_epoch,
    },
};

#[derive(Debug)]
pub enum ShareGroupActorMessage {
    Heartbeat {
        request: ShareGroupHeartbeatRequest,
        client_host: String,
        reply: oneshot::Sender<ShareGroupHeartbeatResponse>,
    },
    Describe {
        reply: oneshot::Sender<ShareDescribeView>,
    },
    /// KIP-932 lifecycle: a `DeleteShareGroupOffsets` removed every initialized
    /// partition of `topic_id`. Drop the topic from `state.initialized` and
    /// rewrite the `ShareGroupStatePartitionMetadata` (key v14) so the topic no
    /// longer appears after a restart. `topic_id` is the metadata-image
    /// (`uuid::Uuid`) id, matching the persister's delete key.
    DropTopicMetadata {
        topic_id: uuid::Uuid,
        reply: oneshot::Sender<()>,
    },
    Seed(super::super::ShareGroupSeed),
    Shutdown(oneshot::Sender<()>),
}

/// Read-only projection of [`ShareGroupState`], consumed by the
/// `ShareGroupDescribe` handler (a later dispatch).
#[derive(Debug, Clone)]
pub struct ShareDescribeView {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub group_state: String,
    pub assignor_name: String,
    pub members: Vec<ShareDescribeMember>,
}

#[derive(Debug, Clone)]
pub struct ShareDescribeMember {
    pub member_id: String,
    pub member_epoch: i32,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
}

impl ShareDescribeView {
    /// Render this view into a `ShareGroupDescribe` `DescribedGroup` wire row.
    /// `error_code`/`authorized_operations` keep their defaults — the handler
    /// owns the ACL outcome.
    #[must_use]
    pub fn into_described_group(self) -> DescribedGroup {
        use crabka_protocol::owned::common::share_group_describe_response::{
            assignment::Assignment, topic_partitions::TopicPartitions,
        };

        let members = self
            .members
            .into_iter()
            .map(|m| {
                let topic_partitions = m
                    .assigned_partitions
                    .into_iter()
                    .map(|(tid, parts)| TopicPartitions {
                        topic_id: tid,
                        partitions: parts,
                        ..Default::default()
                    })
                    .collect();
                DescribeMember {
                    member_id: m.member_id,
                    rack_id: m.rack_id,
                    member_epoch: m.member_epoch,
                    client_id: m.client_id,
                    client_host: m.client_host,
                    subscribed_topic_names: m.subscribed_topic_names,
                    assignment: Assignment {
                        topic_partitions,
                        ..Default::default()
                    },
                    ..Default::default()
                }
            })
            .collect();
        DescribedGroup {
            group_id: self.group_id,
            group_state: self.group_state,
            group_epoch: self.group_epoch,
            assignment_epoch: self.assignment_epoch,
            assignor_name: self.assignor_name,
            members,
            error_code: codes::NONE,
            ..Default::default()
        }
    }
}

#[derive(Debug)]
pub struct ShareGroupActorHandle {
    pub tx: mpsc::Sender<ShareGroupActorMessage>,
    _task: JoinHandle<()>,
}

impl ShareGroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<ShareGroupConfig>,
        metadata: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
        coordinator: Arc<super::super::GroupCoordinator>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(64);
        let task = tokio::spawn(actor_loop(
            group_id,
            config,
            metadata,
            offsets_log,
            coordinator,
            rx,
        ));
        Self { tx, _task: task }
    }
}

async fn actor_loop(
    group_id: String,
    config: Arc<ShareGroupConfig>,
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
    coordinator: Arc<super::super::GroupCoordinator>,
    mut rx: mpsc::Receiver<ShareGroupActorMessage>,
) {
    let mut state = ShareGroupState::new(group_id);
    let mut tick = tokio::time::interval(config.heartbeat_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    ShareGroupActorMessage::Heartbeat { request, client_host, reply } => {
                        match handle_heartbeat(
                            &mut state,
                            &config,
                            &*metadata,
                            &*offsets_log,
                            &coordinator,
                            &request,
                            &client_host,
                        )
                        .await
                        {
                            Ok(resp) => {
                                let _ = reply.send(resp);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    group_id = %state.group_id,
                                    error = %e,
                                    "share-group actor exiting after log-write failure",
                                );
                                let _ = reply.send(ShareGroupHeartbeatResponse {
                                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                                    ..Default::default()
                                });
                                break;
                            }
                        }
                    }
                    ShareGroupActorMessage::Describe { reply } => {
                        let _ = reply.send(build_describe(&state));
                    }
                    ShareGroupActorMessage::DropTopicMetadata { topic_id, reply } => {
                        state
                            .initialized
                            .retain(|(tid, _)| uuid::Uuid::from_bytes(tid.0) != topic_id);
                        let pending = PendingShareRecords {
                            state_partition_metadata: Some(state_partition_metadata_from(&state)),
                            ..Default::default()
                        };
                        if let Err(e) = flush_pending(
                            &state,
                            pending,
                            &*offsets_log,
                            &coordinator,
                            chrono_now_ms(),
                        )
                        .await
                        {
                            tracing::warn!(
                                group_id = %state.group_id,
                                topic_id = %topic_id,
                                error = %e,
                                "rewriting ShareGroupStatePartitionMetadata after topic delete failed; in-memory set updated",
                            );
                        }
                        let _ = reply.send(());
                    }
                    ShareGroupActorMessage::Seed(seed) => {
                        apply_seed(&mut state, seed);
                    }
                    ShareGroupActorMessage::Shutdown(reply) => {
                        let _ = reply.send(());
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                if handle_session_tick(&mut state, &config, &*metadata, &*offsets_log, &coordinator).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// Called on every heartbeat-interval tick. Evicts expired members and writes
/// the resulting tombstones to `__consumer_offsets`. Returns `Err` if the log
/// write fails (the actor should exit).
async fn handle_session_tick(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::super::GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let evicted = state.evict_expired(Instant::now(), config.session_timeout);
    if evicted.is_empty() {
        return Ok(());
    }
    // `evict_expired` set `dirty`; the reconcile owns the single `bump_epoch`.
    reconcile(state, metadata);
    let mut pending = PendingShareRecords {
        group_metadata: Some(ShareGroupMetadataValue {
            epoch: state.group_epoch,
        }),
        ..Default::default()
    };
    if state.target.epoch > 0 {
        pending.target_metadata = Some(ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    for mid in &evicted {
        pending.member_metadata.push((mid.clone(), None));
        pending.target_per_member.push((mid.clone(), None));
        pending.current_per_member.push((mid.clone(), None));
    }
    let now_ms = chrono_now_ms();
    if let Err(e) = flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
        tracing::warn!(
            group_id = %state.group_id,
            error = %e,
            "share-group actor exiting after tick log-write failure",
        );
        return Err(e);
    }
    // KIP-932 lifecycle: eviction may have dropped a member's partitions (or
    // emptied the group); Delete share-state no longer assigned.
    reconcile_share_state(state, offsets_log, coordinator, now_ms).await;
    Ok(())
}

async fn handle_heartbeat(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::super::GroupCoordinator,
    req: &ShareGroupHeartbeatRequest,
    client_host: &str,
) -> Result<ShareGroupHeartbeatResponse, crate::error::BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();

    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return handle_leave(state, config, offsets_log, coordinator, req, now_ms).await;
    }

    // ─── First-join path ─────────────────────────────────────────
    // KIP-932 mirrors KIP-848: the client mints its own member UUID and
    // sends it with `member_epoch == 0`. Treat epoch 0 from an unknown member
    // as a first-join, adopting the client-supplied id; an empty id is
    // tolerated by minting a server-side UUID.
    if req.member_epoch == 0 && !state.members.contains_key(&req.member_id) {
        let new_member_id = first_join_member_id(&req.member_id);
        let m = build_member(&new_member_id, req, client_host, now);
        state.add_or_update_member(m);
        reconcile(state, metadata);
        state.advance_member_epoch(&new_member_id);
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&new_member_id));
        flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
        reconcile_share_state(state, offsets_log, coordinator, now_ms).await;
        return Ok(build_assignment_resp(state, &new_member_id, config));
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    let cur_epoch = match validate_member_epoch(
        state.members.get(&req.member_id).map(|m| m.member_epoch),
        req.member_epoch,
    ) {
        Ok(epoch) => epoch,
        Err(error_code) => return Ok(error_resp(error_code, config)),
    };

    // ─── Steady-state: update subscription / last_seen ───────────
    let changed = update_member_state(state, metadata, req, now, cur_epoch);
    if changed {
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&req.member_id));
        flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    }
    // KIP-932 lifecycle: every steady-state heartbeat re-checks the assignment
    // and Initializes any not-yet-initialized share-states (best-effort retry).
    reconcile_share_state(state, offsets_log, coordinator, now_ms).await;
    Ok(build_assignment_resp(state, &req.member_id, config))
}

/// KIP-932 lifecycle hook. Runs AFTER `reconcile`, off the
/// sync state machine: gathers the group's full assigned `(topic_id,
/// partition)` set and, for each not already Initialized, drives
/// [`SharePersister::initialize`]. On success it records the partition in
/// `state.initialized` and persists an updated `ShareGroupStatePartitionMetadata`
/// (key v14) via the offsets log.
///
/// Best-effort: a persister error leaves the partition un-recorded so the next
/// heartbeat retries; it never fails the heartbeat. `state_epoch` is the group
/// epoch (monotonic, bumped on every membership change); `start_offset` is 0
/// (a freshly-initialized share partition starts at the beginning).
async fn reconcile_share_state(
    state: &mut ShareGroupState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::super::GroupCoordinator,
    now_ms: i64,
) {
    let Some(persister) = coordinator.share_persister() else {
        // No persister wired (pure-coordinator unit tests): nothing to do.
        return;
    };

    // The union of every member's assigned partitions is the set of
    // (topic_id, partition) the group actively uses.
    let mut assigned: HashSet<(Uuid, i32)> = HashSet::new();
    for m in state.members.values() {
        for (tid, parts) in &m.assigned_partitions {
            for p in parts {
                assigned.insert((*tid, *p));
            }
        }
    }

    let to_init: Vec<(Uuid, i32)> = assigned
        .iter()
        .copied()
        .filter(|tp| !state.initialized.contains(tp))
        .collect();
    // Partitions previously Initialized but no longer assigned (a topic left
    // the subscription, or the group emptied) get their share-state Deleted.
    let to_delete: Vec<(Uuid, i32)> = state
        .initialized
        .iter()
        .copied()
        .filter(|tp| !assigned.contains(tp))
        .collect();
    if to_init.is_empty() && to_delete.is_empty() {
        return;
    }

    let state_epoch = state.group_epoch;
    let mut changed = false;
    for (tid, partition) in to_init {
        let topic_uuid = uuid::Uuid::from_bytes(tid.0);
        match persister
            .initialize(
                &state.group_id,
                topic_uuid,
                partition,
                state_epoch,
                crabka_log::Offset(0),
            )
            .await
        {
            Ok(()) => {
                state.initialized.insert((tid, partition));
                changed = true;
            }
            Err(e) => {
                tracing::warn!(
                    group_id = %state.group_id,
                    topic_id = %topic_uuid,
                    partition,
                    error = %e,
                    "share-state Initialize failed; will retry next heartbeat",
                );
            }
        }
    }
    for (tid, partition) in to_delete {
        let topic_uuid = uuid::Uuid::from_bytes(tid.0);
        match persister
            .delete(&state.group_id, topic_uuid, partition)
            .await
        {
            Ok(()) => {
                state.initialized.remove(&(tid, partition));
                changed = true;
            }
            Err(e) => {
                tracing::warn!(
                    group_id = %state.group_id,
                    topic_id = %topic_uuid,
                    partition,
                    error = %e,
                    "share-state Delete failed; will retry next heartbeat",
                );
            }
        }
    }

    if changed {
        let pending = PendingShareRecords {
            state_partition_metadata: Some(state_partition_metadata_from(state)),
            ..Default::default()
        };
        if let Err(e) = flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
            tracing::warn!(
                group_id = %state.group_id,
                error = %e,
                "persisting ShareGroupStatePartitionMetadata failed; in-memory set retained",
            );
        }
    }
}

/// Apply steady-state member updates and run reconciliation. Returns `true`
/// if anything changed that requires a log write.
fn update_member_state(
    state: &mut ShareGroupState,
    metadata: &dyn MetadataProvider,
    req: &ShareGroupHeartbeatRequest,
    now: Instant,
    cur_epoch: i32,
) -> bool {
    let mut subscription_changed = false;
    if let Some(m) = state.members.get_mut(&req.member_id) {
        m.last_seen = now;
        if let Some(ref names) = req.subscribed_topic_names {
            let set: HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                state.dirty = true;
                subscription_changed = true;
            }
        }
    }
    let was_dirty = state.dirty;
    reconcile(state, metadata);
    let epoch_advanced = state.target.epoch > cur_epoch;
    if epoch_advanced {
        state.advance_member_epoch(&req.member_id);
    }
    subscription_changed || was_dirty || epoch_advanced
}

/// Handle a leave-group heartbeat (`member_epoch == -1`).
async fn handle_leave(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::super::GroupCoordinator,
    req: &ShareGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<ShareGroupHeartbeatResponse, crate::error::BrokerError> {
    let mut pending = PendingShareRecords::default();
    if state.members.contains_key(&req.member_id) {
        pending.member_metadata.push((req.member_id.clone(), None));
        pending
            .target_per_member
            .push((req.member_id.clone(), None));
        pending
            .current_per_member
            .push((req.member_id.clone(), None));
    }
    state.remove_member(&req.member_id);
    state.bump_epoch();
    pending.group_metadata = Some(ShareGroupMetadataValue {
        epoch: state.group_epoch,
    });
    flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    // KIP-932 lifecycle: a leave can empty the group (or drop a member's
    // partitions), so Delete any share-state that is no longer assigned.
    reconcile_share_state(state, offsets_log, coordinator, now_ms).await;
    Ok(base_resp(0, req.member_epoch, config))
}

/// Recompute the target assignment when the group is dirty. Builds the
/// assignor inputs from the latest metadata snapshot, runs the share-group
/// assignor, bumps the epoch, and installs the new target.
fn reconcile(state: &mut ShareGroupState, metadata: &dyn MetadataProvider) {
    if !state.dirty {
        return;
    }
    let input = metadata.snapshot();
    let subscriptions: Vec<MemberSubscription> = state
        .members
        .values()
        .map(|m| MemberSubscription {
            member_id: m.member_id.clone(),
            rack_id: m.rack_id.clone(),
            subscribed_topic_ids: resolve_subscribed_topic_ids(m, &input),
        })
        .collect();
    let topics = TopicMetadata {
        partitions_per_topic: input.partitions_per_topic.clone(),
        partition_racks: input.partition_racks,
    };
    let assignment = ShareGroupAssignor.assign(&subscriptions, &topics);
    state.bump_epoch();
    state.install_target(assignment);
    state.dirty = false;
}

/// Resolve a share member's effective topic-id subscription. Share groups
/// only support exact-name subscriptions (no regex), so this is a simple
/// name → id lookup against the current metadata.
fn resolve_subscribed_topic_ids(member: &ShareMemberState, input: &ReconcileInput) -> Vec<Uuid> {
    member
        .subscribed_topic_names
        .iter()
        .filter_map(|n| input.topic_id_by_name.get(n).copied())
        .collect()
}

fn build_member(
    member_id: &str,
    req: &ShareGroupHeartbeatRequest,
    host: &str,
    now: Instant,
) -> ShareMemberState {
    let subs: HashSet<String> = req
        .subscribed_topic_names
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let mut m = ShareMemberState::joining(member_id, String::new(), host, subs);
    m.rack_id.clone_from(&req.rack_id);
    m.last_seen = now;
    m
}

fn base_resp(
    error_code: i16,
    member_epoch: i32,
    config: &ShareGroupConfig,
) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        error_code,
        member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(5_000),
        ..Default::default()
    }
}

fn error_resp(error_code: i16, config: &ShareGroupConfig) -> ShareGroupHeartbeatResponse {
    base_resp(error_code, 0, config)
}

fn build_assignment_resp(
    state: &ShareGroupState,
    member_id: &str,
    config: &ShareGroupConfig,
) -> ShareGroupHeartbeatResponse {
    let m = state
        .members
        .get(member_id)
        .expect("member exists at build_assignment_resp");
    let assignment = Some(RespAssignment {
        topic_partitions: m
            .assigned_partitions
            .iter()
            .map(|(tid, parts)| TopicPartitions {
                topic_id: *tid,
                partitions: parts.clone(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    });
    ShareGroupHeartbeatResponse {
        error_code: 0,
        member_id: Some(member_id.into()),
        member_epoch: m.member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(5_000),
        assignment,
        ..Default::default()
    }
}

fn build_describe(state: &ShareGroupState) -> ShareDescribeView {
    let group_state = if state.members.is_empty() {
        "Empty"
    } else {
        "Stable"
    };
    ShareDescribeView {
        group_id: state.group_id.clone(),
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        group_state: group_state.into(),
        assignor_name: ShareGroupAssignor.name().into(),
        members: state
            .members
            .values()
            .map(|m| ShareDescribeMember {
                member_id: m.member_id.clone(),
                member_epoch: m.member_epoch,
                rack_id: m.rack_id.clone(),
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
                assigned_partitions: m.assigned_partitions.clone(),
            })
            .collect(),
    }
}

fn apply_seed(state: &mut ShareGroupState, seed: super::super::ShareGroupSeed) {
    state.group_epoch = seed.group_epoch;
    state.target.epoch = seed.target_epoch;
    for (mid, meta) in seed.members {
        let subs: HashSet<String> = meta.subscribed_topic_names.into_iter().collect();
        let mut m = ShareMemberState::joining(mid.clone(), meta.client_id, meta.client_host, subs);
        m.rack_id = meta.rack_id;
        state.members.insert(mid, m);
    }
    for (mid, cur) in seed.current_per_member {
        if let Some(m) = state.members.get_mut(&mid) {
            m.member_epoch = cur.member_epoch;
            for (tid, parts) in cur.assigned_partitions {
                m.assigned_partitions.insert(tid, parts);
            }
        }
    }
    for (mid, tv) in seed.target_per_member {
        let entry: HashMap<Uuid, Vec<i32>> = tv.topic_partitions.into_iter().collect();
        state.target.per_member.insert(mid, entry);
    }
    // KIP-932: rehydrate the already-Initialized share-state set so the
    // lifecycle hook skips partitions whose state survived the restart.
    state.initialized.clear();
    for (topic_id, partitions) in &seed.state_partition_metadata.initialized {
        let tid = Uuid(*topic_id.as_bytes());
        for p in partitions {
            state.initialized.insert((tid, *p));
        }
    }
    state.dirty = false;
}

// ---------------------------------------------------------------------------
// PendingShareRecords — collects mutations for one group-state transition and
// encodes them as a single RecordBatch ready for OffsetsLog::append.
// ---------------------------------------------------------------------------

use crabka_protocol::records::RecordBatch;

#[derive(Debug, Default)]
pub(crate) struct PendingShareRecords {
    pub group_metadata: Option<ShareGroupMetadataValue>,
    /// `Some(value)` writes the record; `None` writes a tombstone (null value).
    pub member_metadata: Vec<(String, Option<ShareGroupMemberMetadataValue>)>,
    pub target_metadata: Option<ShareGroupTargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<ShareGroupTargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<ShareGroupCurrentMemberAssignmentValue>)>,
    /// KIP-932 `ShareGroupStatePartitionMetadata` (key v14). `Some` writes the
    /// updated Initialized/deleting record after a lifecycle Initialize/Delete.
    pub state_partition_metadata: Option<ShareGroupStatePartitionMetadataValue>,
}

impl PendingShareRecords {
    fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
            && self.state_partition_metadata.is_none()
    }

    pub fn into_batch(self, group_id: &str, now_ms: i64) -> RecordBatch {
        let mut batch = OffsetRecordBatchBuilder::default();

        if let Some(v) = self.group_metadata {
            batch.push(
                encode_share_key(&ShareGroupKey::GroupMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.member_metadata {
            batch.push(
                encode_share_key(&ShareGroupKey::MemberMetadata {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.target_metadata {
            batch.push(
                encode_share_key(&ShareGroupKey::TargetAssignmentMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.target_per_member {
            batch.push(
                encode_share_key(&ShareGroupKey::TargetAssignmentMember {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        for (member_id, v) in self.current_per_member {
            batch.push(
                encode_share_key(&ShareGroupKey::CurrentMemberAssignment {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.state_partition_metadata {
            batch.push(
                encode_share_key(&ShareGroupKey::StatePartitionMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }

        batch.finish(now_ms)
    }
}

/// Build a `PendingShareRecords` set reflecting the state changes for the
/// listed `affected_members`. Always includes the current group epoch and
/// (if non-zero) target epoch.
fn snapshot_pending_after_change(
    state: &ShareGroupState,
    affected_members: &[String],
) -> PendingShareRecords {
    let mut pending = PendingShareRecords {
        group_metadata: Some(ShareGroupMetadataValue {
            epoch: state.group_epoch,
        }),
        ..Default::default()
    };
    if state.target.epoch > 0 {
        pending.target_metadata = Some(ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    for mid in affected_members {
        if let Some(m) = state.members.get(mid) {
            pending.member_metadata.push((
                mid.clone(),
                Some(ShareGroupMemberMetadataValue {
                    rack_id: m.rack_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
                }),
            ));
            pending.current_per_member.push((
                mid.clone(),
                Some(ShareGroupCurrentMemberAssignmentValue {
                    member_epoch: m.member_epoch,
                    assigned_partitions: m
                        .assigned_partitions
                        .iter()
                        .map(|(tid, parts)| (*tid, parts.clone()))
                        .collect(),
                }),
            ));
            if let Some(target) = state.target.per_member.get(mid) {
                pending.target_per_member.push((
                    mid.clone(),
                    Some(ShareGroupTargetAssignmentMemberValue {
                        topic_partitions: target
                            .iter()
                            .map(|(tid, parts)| (*tid, parts.clone()))
                            .collect(),
                    }),
                ));
            }
        }
    }
    pending
}

/// Snapshot a `ShareGroupState` into a `ShareGroupSeed` suitable for restoring
/// a freshly-respawned actor. Mirrors what bootstrap replay produces.
fn snapshot_seed(state: &ShareGroupState) -> super::super::ShareGroupSeed {
    let mut members = HashMap::new();
    let mut target_per_member = HashMap::new();
    let mut current_per_member = HashMap::new();
    for (mid, m) in &state.members {
        members.insert(
            mid.clone(),
            ShareGroupMemberMetadataValue {
                rack_id: m.rack_id.clone(),
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
            },
        );
        current_per_member.insert(
            mid.clone(),
            ShareGroupCurrentMemberAssignmentValue {
                member_epoch: m.member_epoch,
                assigned_partitions: m
                    .assigned_partitions
                    .iter()
                    .map(|(tid, parts)| (*tid, parts.clone()))
                    .collect(),
            },
        );
        if let Some(target) = state.target.per_member.get(mid) {
            target_per_member.insert(
                mid.clone(),
                ShareGroupTargetAssignmentMemberValue {
                    topic_partitions: target
                        .iter()
                        .map(|(tid, parts)| (*tid, parts.clone()))
                        .collect(),
                },
            );
        }
    }
    super::super::ShareGroupSeed {
        group_epoch: state.group_epoch,
        target_epoch: state.target.epoch,
        members,
        target_per_member,
        current_per_member,
        // KIP-932 lifecycle: project the live Initialized set back into the
        // persisted record so the cache (and a respawned actor) stay consistent
        // with what the lifecycle hook wrote to the log.
        state_partition_metadata: state_partition_metadata_from(state),
    }
}

/// Build the `ShareGroupStatePartitionMetadata` (key v14) value from the live
/// Initialized set: one `(topic_id, partitions)` row per topic, partitions
/// sorted for a stable encoding.
fn state_partition_metadata_from(state: &ShareGroupState) -> ShareGroupStatePartitionMetadataValue {
    let mut by_topic: HashMap<Uuid, Vec<i32>> = HashMap::new();
    for (tid, p) in &state.initialized {
        by_topic.entry(*tid).or_default().push(*p);
    }
    let mut initialized: Vec<(uuid::Uuid, Vec<i32>)> = by_topic
        .into_iter()
        .map(|(tid, mut parts)| {
            parts.sort_unstable();
            (uuid::Uuid::from_bytes(tid.0), parts)
        })
        .collect();
    initialized.sort_by_key(|(tid, _)| *tid);
    ShareGroupStatePartitionMetadataValue {
        initialized,
        deleting: Vec::new(),
    }
}

async fn flush_pending(
    state: &ShareGroupState,
    pending: PendingShareRecords,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::super::GroupCoordinator,
    now_ms: i64,
) -> Result<(), crate::error::BrokerError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = pending.into_batch(&state.group_id, now_ms);
    offsets_log.append(batch).await?;
    coordinator.update_share_cache(&state.group_id, snapshot_seed(state));
    Ok(())
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use crabka_protocol::primitives::uuid::Uuid;

    use super::*;
    use crate::coordinator::unified::{
        GroupCoordinator, config::NextGenConfig, offsets_log::fake::InMemoryOffsetsLog,
        reconciler::ReconcileInput,
    };

    #[derive(Debug)]
    struct StaticMetadata {
        input: ReconcileInput,
    }
    impl MetadataProvider for StaticMetadata {
        fn snapshot(&self) -> ReconcileInput {
            self.input.clone()
        }
    }

    /// Metadata snapshot with a single topic of `parts` partitions.
    fn metadata_with_topic(name: &str, parts: i32) -> (Arc<dyn MetadataProvider>, Uuid) {
        let id = Uuid([7; 16]);
        let input = ReconcileInput {
            topic_id_by_name: [(name.to_string(), id)].into(),
            partitions_per_topic: [(id, parts)].into(),
            ..Default::default()
        };
        (Arc::new(StaticMetadata { input }), id)
    }

    fn make_coordinator(
        metadata: Arc<dyn MetadataProvider>,
    ) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            ShareGroupConfig::default(),
            metadata,
            log.clone(),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        (coord, log)
    }

    async fn heartbeat(
        handle: &ShareGroupActorHandle,
        req: ShareGroupHeartbeatRequest,
    ) -> ShareGroupHeartbeatResponse {
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(ShareGroupActorMessage::Heartbeat {
                request: req,
                client_host: "/127.0.0.1".into(),
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_member_join_gets_assignment() {
        let (metadata, id) = metadata_with_topic("t", 4);
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let resp = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == 0);
        assert!(resp.member_epoch == 1, "epoch advances to group epoch 1");
        let asg = resp.assignment.expect("assignment present");
        let total: usize = asg
            .topic_partitions
            .iter()
            .map(|tp| tp.partitions.len())
            .sum();
        assert!(total == 4, "one member gets all 4 partitions");
        assert!(asg.topic_partitions[0].topic_id == id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_members_reconcile() {
        let (metadata, _id) = metadata_with_topic("t", 4);
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");

        let r1 = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(r1.error_code == 0);

        let r2 = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m2".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(r2.error_code == 0);
        // Second join recomputes: each member should now own a 2-partition slice.
        let total2: usize = r2
            .assignment
            .expect("m2 assignment")
            .topic_partitions
            .iter()
            .map(|tp| tp.partitions.len())
            .sum();
        assert!(total2 == 2, "with two members each owns 2 of 4 partitions");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leave_removes_member() {
        let (metadata, _id) = metadata_with_topic("t", 4);
        let (coord, log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let joined = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        let mid = joined.member_id.unwrap();
        let pre_leave = log.batches().await.len();

        let resp = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: mid,
                member_epoch: -1,
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == 0);
        let batches = log.batches().await;
        assert!(batches.len() == pre_leave + 1);
        let leave_batch = &batches[batches.len() - 1];
        assert!(
            leave_batch.records.iter().any(|r| r.value.is_none()),
            "leave batch must contain at least one tombstone"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_tick_evicts() {
        use std::time::Duration;
        let (metadata, _id) = metadata_with_topic("t", 4);
        let config = ShareGroupConfig {
            session_timeout: Duration::from_millis(1),
            ..ShareGroupConfig::default()
        };
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            config.clone(),
            metadata.clone(),
            log.clone(),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));

        let mut state = ShareGroupState::new("g");
        let mut m = build_member(
            "m1",
            &ShareGroupHeartbeatRequest {
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
            "/127.0.0.1",
            Instant::now(),
        );
        m.last_seen = Instant::now()
            .checked_sub(Duration::from_millis(50))
            .expect("50ms is always within Instant range");
        state.add_or_update_member(m);
        reconcile(&mut state, &*metadata);
        assert!(!state.dirty, "baseline must be clean before eviction");
        let epoch_before = state.group_epoch;

        handle_session_tick(&mut state, &config, &*metadata, &*log, &coord)
            .await
            .expect("tick should succeed");

        assert!(state.members.is_empty(), "expired member evicted");
        assert!(
            state.group_epoch == epoch_before + 1,
            "single eviction advances epoch by exactly 1"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_epoch_is_fenced() {
        let (metadata, _id) = metadata_with_topic("t", 4);
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let joined = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(joined.member_epoch == 1);
        // Re-send with an epoch ahead of the server → fenced.
        let resp = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 99,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == codes::FENCED_MEMBER_EPOCH);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn known_member_epoch_zero_is_stale_not_first_join() {
        let (metadata, _id) = metadata_with_topic("t", 4);
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let joined = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(joined.member_epoch == 1);

        let resp = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;

        assert!(resp.error_code == codes::STALE_MEMBER_EPOCH);
    }

    #[test]
    fn pending_records_tombstone_omits_value() {
        let p = PendingShareRecords {
            member_metadata: vec![("m1".into(), None)],
            ..Default::default()
        };
        let batch = p.into_batch("g", 0);
        assert!(batch.records.len() == 1);
        assert!(batch.records[0].value.is_none());
    }

    #[test]
    fn snapshot_seed_round_trips_through_apply() {
        // A populated state → snapshot_seed → apply_seed reconstructs members,
        // epochs, and assignments (the bootstrap-replay invariant).
        let id = Uuid([7; 16]);
        let mut state = ShareGroupState::new("g");
        let mut m = ShareMemberState::joining(
            "m1",
            "c1",
            "/127.0.0.1",
            ["t".to_string()].into_iter().collect(),
        );
        m.member_epoch = 3;
        m.assigned_partitions.insert(id, vec![0, 1]);
        state.members.insert("m1".into(), m);
        state.group_epoch = 3;
        state.target.epoch = 3;
        state
            .target
            .per_member
            .insert("m1".into(), [(id, vec![0, 1])].into());

        let seed = snapshot_seed(&state);
        let mut restored = ShareGroupState::new("g");
        apply_seed(&mut restored, seed);

        assert!(restored.group_epoch == 3);
        assert!(restored.target.epoch == 3);
        let rm = restored.members.get("m1").expect("member restored");
        check!(rm.member_epoch == 3);
        check!(rm.assigned_partitions[&id] == vec![0, 1]);
        check!(restored.target.per_member["m1"][&id] == vec![0, 1]);
    }
}
