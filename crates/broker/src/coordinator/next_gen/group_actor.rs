//! Per-group tokio actor. Owns `GroupState` for one next-gen consumer
//! group. Heartbeats are mpsc messages; responses go back via oneshot
//! channels.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::consumer_group_heartbeat_response::{
    Assignment as RespAssignment, ConsumerGroupHeartbeatResponse,
};
use crabka_protocol::primitives::uuid::Uuid;

use crate::codes;

use super::config::NextGenConfig;
use super::group_state::{GroupState, MemberState};
use super::offsets_log::OffsetsLog;
use super::persistence::MemberAssignmentState;
use super::reconciler::{self, ReconcileInput};

#[derive(Debug)]
pub enum GroupActorMessage {
    Heartbeat {
        request: ConsumerGroupHeartbeatRequest,
        client_host: String,
        reply: oneshot::Sender<ConsumerGroupHeartbeatResponse>,
    },
    OffsetValidate {
        member_id: String,
        member_epoch: i32,
        reply: oneshot::Sender<Result<(), i16>>,
    },
    Describe {
        reply: oneshot::Sender<DescribeView>,
    },
    Seed(super::GroupSeed),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug, Clone)]
pub struct DescribeView {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub members: Vec<DescribeMember>,
}

#[derive(Debug, Clone)]
pub struct DescribeMember {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub member_epoch: i32,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
}

#[derive(Debug)]
pub struct GroupActorHandle {
    pub tx: mpsc::Sender<GroupActorMessage>,
    _task: JoinHandle<()>,
}

impl GroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<NextGenConfig>,
        metadata_provider: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
        coordinator: Arc<super::NextGenCoordinator>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(64);
        let task = tokio::spawn(actor_loop(
            group_id,
            config,
            metadata_provider,
            offsets_log,
            coordinator,
            rx,
        ));
        Self { tx, _task: task }
    }
}

pub trait MetadataProvider: Send + Sync + std::fmt::Debug {
    fn snapshot(&self) -> ReconcileInput;
}

async fn actor_loop(
    group_id: String,
    config: Arc<NextGenConfig>,
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
    coordinator: Arc<super::NextGenCoordinator>,
    mut rx: mpsc::Receiver<GroupActorMessage>,
) {
    let mut state = GroupState::new(group_id);
    let mut tick = tokio::time::interval(config.heartbeat_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    GroupActorMessage::Heartbeat { request, client_host, reply } => {
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
                                    "next-gen actor exiting after log-write failure",
                                );
                                let _ = reply.send(ConsumerGroupHeartbeatResponse {
                                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                                    ..Default::default()
                                });
                                break;
                            }
                        }
                    }
                    GroupActorMessage::OffsetValidate { member_id, member_epoch, reply } => {
                        let result = match state.members.get(&member_id) {
                            None => Err(codes::UNKNOWN_MEMBER_ID),
                            Some(m) if member_epoch < m.member_epoch => Err(codes::STALE_MEMBER_EPOCH),
                            Some(m) if member_epoch > m.member_epoch => Err(codes::FENCED_MEMBER_EPOCH),
                            Some(_) => Ok(()),
                        };
                        let _ = reply.send(result);
                    }
                    GroupActorMessage::Describe { reply } => {
                        let _ = reply.send(build_describe(&state));
                    }
                    GroupActorMessage::Seed(seed) => {
                        apply_seed(&mut state, seed);
                    }
                    GroupActorMessage::Shutdown(reply) => {
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

/// Called on every heartbeat-interval tick. Evicts expired members and
/// writes the resulting tombstones to `__consumer_offsets`. Returns `Err`
/// if the log write fails (the actor should exit).
async fn handle_session_tick(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::NextGenCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let evicted = state.evict_expired(Instant::now(), config.session_timeout);
    if evicted.is_empty() {
        return Ok(());
    }
    state.bump_epoch();
    run_reconcile(state, config, metadata);
    let mut pending = PendingRecords {
        group_metadata: Some(GroupMetadataValue {
            epoch: state.group_epoch,
        }),
        ..Default::default()
    };
    if state.target.epoch > 0 {
        pending.target_metadata = Some(TargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    for mid in &evicted {
        pending.member_metadata.push((mid.clone(), None));
        pending.target_per_member.push((mid.clone(), None));
        pending.current_per_member.push((mid.clone(), None));
    }
    let now_ms = chrono_now_ms();
    if let Err(e) = flush_pending(state, &pending, offsets_log, coordinator, now_ms).await {
        tracing::warn!(
            group_id = %state.group_id,
            error = %e,
            "next-gen actor exiting after tick log-write failure",
        );
        return Err(e);
    }
    Ok(())
}

fn apply_seed(state: &mut GroupState, seed: super::GroupSeed) {
    state.group_epoch = seed.group_epoch;
    state.target.epoch = seed.target_epoch;
    for (mid, meta) in seed.members {
        let mut sub = std::collections::HashSet::new();
        for n in meta.subscribed_topic_names {
            sub.insert(n);
        }
        state.add_or_update_member(MemberState {
            member_id: mid.clone(),
            instance_id: meta.instance_id,
            rack_id: meta.rack_id,
            client_id: meta.client_id,
            client_host: meta.client_host,
            subscribed_topic_names: sub,
            subscribed_topic_regex: meta.subscribed_topic_regex,
            server_assignor: meta.server_assignor,
            rebalance_timeout: Duration::from_millis(
                u64::try_from(meta.rebalance_timeout_ms.max(0)).unwrap_or(60_000),
            ),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
        });
    }
    for (mid, cur) in seed.current_per_member {
        if let Some(m) = state.members.get_mut(&mid) {
            m.member_epoch = cur.member_epoch;
            m.previous_member_epoch = cur.previous_member_epoch;
            m.assignment_state = cur.state;
            for tp in cur.assigned_partitions {
                m.assigned_partitions.insert(tp.topic_id, tp.partitions);
            }
            for tp in cur.partitions_pending_revocation {
                m.partitions_pending_revocation
                    .insert(tp.topic_id, tp.partitions);
            }
        }
    }
    state.dirty = false;
}

async fn handle_heartbeat(
    state: &mut super::group_state::GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::NextGenCoordinator,
    req: &ConsumerGroupHeartbeatRequest,
    client_host: &str,
) -> Result<ConsumerGroupHeartbeatResponse, crate::error::BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();

    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return handle_leave(state, config, offsets_log, coordinator, req, now_ms).await;
    }

    // ─── Validate assignor selection ─────────────────────────────
    if req
        .server_assignor
        .as_deref()
        .is_some_and(|name| !config.assignor_enabled(name))
    {
        return Ok(error_resp(codes::UNSUPPORTED_ASSIGNOR, config));
    }

    // ─── First-join path ─────────────────────────────────────────
    if req.member_epoch == 0 && req.member_id.is_empty() {
        let new_member_id = uuid::Uuid::new_v4().to_string();
        if let Some(iid) = req.instance_id.as_deref()
            && state
                .current_member_for_instance(iid)
                .and_then(|existing| state.members.get(existing))
                .is_some_and(|m| m.member_epoch != 0)
        {
            return Ok(error_resp(codes::UNRELEASED_INSTANCE_ID, config));
        }
        let m = build_member(&new_member_id, req, client_host, now);
        state.add_or_update_member(m);
        run_reconcile(state, config, metadata);
        state.advance_member_epoch(&new_member_id);
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&new_member_id));
        flush_pending(state, &pending, offsets_log, coordinator, now_ms).await?;
        return Ok(build_assignment_resp(state, &new_member_id, config));
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    let cur_epoch = state
        .members
        .get(&req.member_id)
        .map_or(-2, |m| m.member_epoch);
    if cur_epoch == -2 {
        return Ok(error_resp(codes::UNKNOWN_MEMBER_ID, config));
    }
    if req.member_epoch < cur_epoch {
        return Ok(error_resp(codes::STALE_MEMBER_EPOCH, config));
    }
    if req.member_epoch > cur_epoch {
        return Ok(error_resp(codes::FENCED_MEMBER_EPOCH, config));
    }

    // ─── Steady-state: update last_seen / subscription / owned ───
    let any_change = update_member_state(state, config, metadata, req, now, cur_epoch);
    if any_change {
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&req.member_id));
        flush_pending(state, &pending, offsets_log, coordinator, now_ms).await?;
    }
    Ok(build_assignment_resp(state, &req.member_id, config))
}

/// Apply steady-state member updates and run reconciliation.
/// Returns `true` if any change occurred that requires a log write.
fn update_member_state(
    state: &mut super::group_state::GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    now: Instant,
    cur_epoch: i32,
) -> bool {
    let mut subscription_changed = false;
    let mut became_dirty = false;
    if let Some(m) = state.members.get_mut(&req.member_id) {
        m.last_seen = now;
        if let Some(ref names) = req.subscribed_topic_names {
            let set: std::collections::HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                became_dirty = true;
                subscription_changed = true;
            }
        }
        // KIP-848 v1+: `subscribed_topic_regex` may change independently
        // of `subscribed_topic_names`. Only mark dirty when it actually
        // changes; the client re-sends the same regex on every
        // heartbeat as long as the subscription is stable.
        if req.subscribed_topic_regex != m.subscribed_topic_regex {
            m.subscribed_topic_regex
                .clone_from(&req.subscribed_topic_regex);
            state.dirty = true;
        }
        if let Some(ref tp) = req.topic_partitions {
            let owned: HashMap<Uuid, Vec<i32>> = tp
                .iter()
                .map(|t| (t.topic_id, t.partitions.clone()))
                .collect();
            m.assigned_partitions = owned;
            if m.partitions_pending_revocation.is_empty() {
                m.assignment_state = MemberAssignmentState::Stable;
            }
        }
    }
    if became_dirty {
        state.dirty = true;
    }
    let was_dirty = state.dirty;
    run_reconcile(state, config, metadata);
    let epoch_advanced = state.target.epoch > cur_epoch;
    if epoch_advanced {
        state.advance_member_epoch(&req.member_id);
    }
    subscription_changed || was_dirty || epoch_advanced
}

/// Handle a leave-group heartbeat (`member_epoch == -1`).
async fn handle_leave(
    state: &mut super::group_state::GroupState,
    config: &NextGenConfig,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::NextGenCoordinator,
    req: &ConsumerGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<ConsumerGroupHeartbeatResponse, crate::error::BrokerError> {
    let mut pending = PendingRecords::default();
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
    pending.group_metadata = Some(GroupMetadataValue {
        epoch: state.group_epoch,
    });
    flush_pending(state, &pending, offsets_log, coordinator, now_ms).await?;
    Ok(base_resp(0, req.member_epoch, config))
}

fn run_reconcile(state: &mut GroupState, config: &NextGenConfig, metadata: &dyn MetadataProvider) {
    let input = metadata.snapshot();
    let assignor = pick_assignor(state, config);
    reconciler::reconcile_if_dirty(state, &input, &*assignor);
}

fn pick_assignor(
    state: &GroupState,
    config: &NextGenConfig,
) -> std::sync::Arc<dyn super::assignor::Assignor> {
    for m in state.members.values() {
        if let Some(name) = m.server_assignor.as_deref()
            && let Some(a) = config.find_assignor(name)
        {
            return a;
        }
    }
    config
        .assignors
        .first()
        .cloned()
        .expect("NextGenConfig must have at least one registered assignor")
}

fn build_member(
    member_id: &str,
    req: &ConsumerGroupHeartbeatRequest,
    host: &str,
    now: Instant,
) -> MemberState {
    let subs: std::collections::HashSet<String> = req
        .subscribed_topic_names
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    MemberState {
        member_id: member_id.into(),
        instance_id: req.instance_id.clone(),
        rack_id: req.rack_id.clone(),
        client_id: String::new(),
        client_host: host.into(),
        subscribed_topic_names: subs,
        subscribed_topic_regex: req.subscribed_topic_regex.clone(),
        server_assignor: req.server_assignor.clone(),
        rebalance_timeout: Duration::from_millis(
            u64::try_from(req.rebalance_timeout_ms.max(0)).unwrap_or(60_000),
        ),
        member_epoch: 0,
        previous_member_epoch: 0,
        assignment_state: MemberAssignmentState::Stable,
        assigned_partitions: HashMap::new(),
        partitions_pending_revocation: HashMap::new(),
        last_seen: now,
    }
}

fn base_resp(
    error_code: i16,
    member_epoch: i32,
    config: &NextGenConfig,
) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code,
        member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(5_000),
        ..Default::default()
    }
}

fn error_resp(error_code: i16, config: &NextGenConfig) -> ConsumerGroupHeartbeatResponse {
    base_resp(error_code, 0, config)
}

fn build_assignment_resp(
    state: &GroupState,
    member_id: &str,
    config: &NextGenConfig,
) -> ConsumerGroupHeartbeatResponse {
    let m = state
        .members
        .get(member_id)
        .expect("member exists at build_assignment_resp");
    // KIP-848: the `assignment` field in the heartbeat response carries the
    // server's TARGET assignment for this member — the full set of partitions
    // the member should eventually own.  The client acknowledges receipt by
    // echoing its current partition set back in subsequent heartbeats
    // (`topic_partitions` on the request side).  Using the target here
    // (rather than the client-acked subset) ensures new members learn their
    // initial assignment on the very first heartbeat.
    let target_partitions = state
        .target
        .per_member
        .get(member_id)
        .cloned()
        .unwrap_or_default();
    let assignment = Some(RespAssignment {
        topic_partitions: target_partitions
            .iter()
            .map(
                |(tid, parts)| crabka_protocol::owned::common::topic_partitions::TopicPartitions {
                    topic_id: *tid,
                    partitions: parts.clone(),
                    ..Default::default()
                },
            )
            .collect(),
        ..Default::default()
    });
    ConsumerGroupHeartbeatResponse {
        error_code: 0,
        member_id: Some(member_id.into()),
        member_epoch: m.member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(5_000),
        assignment,
        ..Default::default()
    }
}

fn build_describe(state: &GroupState) -> DescribeView {
    DescribeView {
        group_id: state.group_id.clone(),
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        members: state
            .members
            .values()
            .map(|m| DescribeMember {
                member_id: m.member_id.clone(),
                instance_id: m.instance_id.clone(),
                member_epoch: m.member_epoch,
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
                assigned_partitions: m.assigned_partitions.clone(),
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// PendingRecords — collects mutations for one group-state transition and
// encodes them as a single RecordBatch ready for OffsetsLog::append.
// ---------------------------------------------------------------------------

use bytes::Bytes;
use crabka_protocol::records::{Record, RecordBatch};

use super::persistence::{
    CurrentMemberAssignmentValue, GroupMetadataValue, MemberMetadataValue, NextGenKey,
    TargetAssignmentMemberValue, TargetAssignmentMetadataValue, encode_key,
};

#[derive(Debug, Default, Clone)]
pub(crate) struct PendingRecords {
    pub group_metadata: Option<GroupMetadataValue>,
    /// `Some(value)` writes the record; `None` writes a tombstone (null value).
    pub member_metadata: Vec<(String, Option<MemberMetadataValue>)>,
    pub target_metadata: Option<TargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<TargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<CurrentMemberAssignmentValue>)>,
}

impl PendingRecords {
    pub fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
    }

    pub fn into_batch(self, group_id: &str, now_ms: i64) -> RecordBatch {
        let mut records: Vec<Record> = Vec::new();
        let mut push = |key: Bytes, value: Option<Bytes>| {
            let delta = i32::try_from(records.len()).expect("batch size fits i32");
            records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(key),
                value,
                ..Default::default()
            });
        };

        if let Some(v) = self.group_metadata {
            push(
                encode_key(&NextGenKey::GroupMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.member_metadata {
            push(
                encode_key(&NextGenKey::MemberMetadata {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.target_metadata {
            push(
                encode_key(&NextGenKey::TargetAssignmentMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.target_per_member {
            push(
                encode_key(&NextGenKey::TargetAssignmentMember {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        for (member_id, v) in self.current_per_member {
            push(
                encode_key(&NextGenKey::CurrentMemberAssignment {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }

        let last_delta = i32::try_from(records.len().saturating_sub(1)).unwrap_or(0);
        RecordBatch {
            max_timestamp: now_ms,
            records,
            last_offset_delta: last_delta,
            ..RecordBatch::default()
        }
    }

    /// Build a batch without consuming self.
    pub fn clone_into_batch(&self, group_id: &str, now_ms: i64) -> RecordBatch {
        self.clone().into_batch(group_id, now_ms)
    }
}

/// Snapshot a `GroupState` into a `GroupSeed` suitable for restoring a
/// freshly-respawned actor. Mirrors what bootstrap replay would produce.
pub(crate) fn snapshot_seed(state: &super::group_state::GroupState) -> super::GroupSeed {
    use crate::coordinator::next_gen::persistence as p;
    let mut members = std::collections::HashMap::new();
    let mut target_per_member = std::collections::HashMap::new();
    let mut current_per_member = std::collections::HashMap::new();
    for (mid, m) in &state.members {
        let mm = p::MemberMetadataValue {
            instance_id: m.instance_id.clone(),
            rack_id: m.rack_id.clone(),
            client_id: m.client_id.clone(),
            client_host: m.client_host.clone(),
            subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
            subscribed_topic_regex: m.subscribed_topic_regex.clone(),
            server_assignor: m.server_assignor.clone(),
            rebalance_timeout_ms: i32::try_from(m.rebalance_timeout.as_millis()).unwrap_or(60_000),
        };
        members.insert(mid.clone(), mm);

        let cur = p::CurrentMemberAssignmentValue {
            member_epoch: m.member_epoch,
            previous_member_epoch: m.previous_member_epoch,
            state: m.assignment_state,
            assigned_partitions: m
                .assigned_partitions
                .iter()
                .map(|(tid, parts)| p::AssignedTopicPartitions {
                    topic_id: *tid,
                    partitions: parts.clone(),
                })
                .collect(),
            partitions_pending_revocation: m
                .partitions_pending_revocation
                .iter()
                .map(|(tid, parts)| p::AssignedTopicPartitions {
                    topic_id: *tid,
                    partitions: parts.clone(),
                })
                .collect(),
        };
        current_per_member.insert(mid.clone(), cur);

        if let Some(target) = state.target.per_member.get(mid) {
            let tv = p::TargetAssignmentMemberValue {
                topic_partitions: target
                    .iter()
                    .map(|(tid, parts)| p::AssignedTopicPartitions {
                        topic_id: *tid,
                        partitions: parts.clone(),
                    })
                    .collect(),
            };
            target_per_member.insert(mid.clone(), tv);
        }
    }
    super::GroupSeed {
        group_epoch: state.group_epoch,
        target_epoch: state.target.epoch,
        members,
        target_per_member,
        current_per_member,
    }
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

/// Build a `PendingRecords` set reflecting the state changes for the
/// listed `affected_members`. Always includes the current group epoch
/// and (if non-zero) target epoch.
fn snapshot_pending_after_change(
    state: &super::group_state::GroupState,
    affected_members: &[String],
) -> PendingRecords {
    use crate::coordinator::next_gen::persistence as p;
    let mut pending = PendingRecords {
        group_metadata: Some(p::GroupMetadataValue {
            epoch: state.group_epoch,
        }),
        ..Default::default()
    };
    if state.target.epoch > 0 {
        pending.target_metadata = Some(p::TargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    for mid in affected_members {
        if let Some(m) = state.members.get(mid) {
            pending.member_metadata.push((
                mid.clone(),
                Some(p::MemberMetadataValue {
                    instance_id: m.instance_id.clone(),
                    rack_id: m.rack_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
                    subscribed_topic_regex: m.subscribed_topic_regex.clone(),
                    server_assignor: m.server_assignor.clone(),
                    rebalance_timeout_ms: i32::try_from(m.rebalance_timeout.as_millis())
                        .unwrap_or(60_000),
                }),
            ));
            pending.current_per_member.push((
                mid.clone(),
                Some(p::CurrentMemberAssignmentValue {
                    member_epoch: m.member_epoch,
                    previous_member_epoch: m.previous_member_epoch,
                    state: m.assignment_state,
                    assigned_partitions: m
                        .assigned_partitions
                        .iter()
                        .map(|(tid, parts)| p::AssignedTopicPartitions {
                            topic_id: *tid,
                            partitions: parts.clone(),
                        })
                        .collect(),
                    partitions_pending_revocation: m
                        .partitions_pending_revocation
                        .iter()
                        .map(|(tid, parts)| p::AssignedTopicPartitions {
                            topic_id: *tid,
                            partitions: parts.clone(),
                        })
                        .collect(),
                }),
            ));
            if let Some(target) = state.target.per_member.get(mid) {
                pending.target_per_member.push((
                    mid.clone(),
                    Some(p::TargetAssignmentMemberValue {
                        topic_partitions: target
                            .iter()
                            .map(|(tid, parts)| p::AssignedTopicPartitions {
                                topic_id: *tid,
                                partitions: parts.clone(),
                            })
                            .collect(),
                    }),
                ));
            }
        }
    }
    pending
}

async fn flush_pending(
    state: &super::group_state::GroupState,
    pending: &PendingRecords,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::NextGenCoordinator,
    now_ms: i64,
) -> Result<(), crate::error::BrokerError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = pending.clone_into_batch(&state.group_id, now_ms);
    offsets_log.append(batch).await?;
    coordinator.update_cache(&state.group_id, snapshot_seed(state));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::coordinator::next_gen::NextGenCoordinator;
    use crate::coordinator::next_gen::config::NextGenConfig;
    use crate::coordinator::next_gen::offsets_log::fake::InMemoryOffsetsLog;
    use crate::coordinator::next_gen::reconciler::ReconcileInput;
    use std::sync::Arc;

    #[derive(Debug)]
    struct StaticMetadata {
        input: ReconcileInput,
    }
    impl MetadataProvider for StaticMetadata {
        fn snapshot(&self) -> ReconcileInput {
            self.input.clone()
        }
    }

    fn empty_metadata() -> Arc<dyn MetadataProvider> {
        Arc::new(StaticMetadata {
            input: ReconcileInput::default(),
        })
    }

    fn make_coordinator() -> (Arc<NextGenCoordinator>, Arc<InMemoryOffsetsLog>) {
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(NextGenCoordinator::new(
            NextGenConfig::default(),
            empty_metadata(),
            log.clone(),
        ));
        (coord, log)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_join_emits_one_batch() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create("g");
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let resp = rx.await.unwrap();
        assert_eq!(resp.error_code, 0);
        let batches = log.batches().await;
        assert_eq!(
            batches.len(),
            1,
            "first join should write exactly one batch"
        );
        // Minimum: k3 (group metadata) + k5 (member metadata) + k8 (current).
        assert!(batches[0].records.len() >= 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unchanged_heartbeat_emits_no_batch() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create("g");
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let resp1 = rx.await.unwrap();
        let mid = resp1.member_id.clone().unwrap();
        let batches_after_join = log.batches().await.len();

        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: mid,
                    member_epoch: resp1.member_epoch,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let _ = rx.await.unwrap();
        let batches_after_steady = log.batches().await.len();
        assert_eq!(
            batches_after_steady, batches_after_join,
            "steady-state heartbeat should not write"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leave_emits_tombstone_batch() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create("g");
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let mid = rx.await.unwrap().member_id.unwrap();
        let pre_leave = log.batches().await.len();

        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: mid,
                    member_epoch: -1,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let _ = rx.await.unwrap();
        let batches = log.batches().await;
        assert_eq!(batches.len(), pre_leave + 1);
        let leave_batch = &batches[batches.len() - 1];
        assert!(
            leave_batch.records.iter().any(|r| r.value.is_none()),
            "leave batch must contain at least one tombstone"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actor_exits_on_append_error() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create("g");
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await;
        let resp = rx.await.unwrap();
        assert_eq!(resp.error_code, codes::COORDINATOR_LOAD_IN_PROGRESS);

        // Wait briefly for the actor to drain.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            handle.tx.is_closed(),
            "actor mpsc should be closed after exit"
        );

        // get_or_create should respawn a fresh actor.
        let fresh = coord.get_or_create("g");
        assert!(!fresh.tx.is_closed());
    }

    #[test]
    fn pending_records_empty_yields_empty_batch() {
        let p = PendingRecords::default();
        let batch = p.into_batch("g", 0);
        assert!(batch.records.is_empty());
    }

    #[test]
    fn pending_records_offset_deltas_are_sequential() {
        let p = PendingRecords {
            group_metadata: Some(GroupMetadataValue { epoch: 1 }),
            member_metadata: vec![(
                "m1".into(),
                Some(MemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "c".into(),
                    client_host: "h".into(),
                    subscribed_topic_names: vec!["t".into()],
                    subscribed_topic_regex: None,
                    server_assignor: None,
                    rebalance_timeout_ms: 60_000,
                }),
            )],
            target_metadata: Some(TargetAssignmentMetadataValue {
                assignment_epoch: 1,
            }),
            ..Default::default()
        };
        let batch = p.into_batch("g", 0);
        assert_eq!(batch.records.len(), 3);
        let deltas: Vec<i32> = batch.records.iter().map(|r| r.offset_delta).collect();
        assert_eq!(deltas, vec![0, 1, 2]);
        assert_eq!(batch.last_offset_delta, 2);
    }

    #[test]
    fn pending_records_tombstone_omits_value() {
        let p = PendingRecords {
            member_metadata: vec![("m1".into(), None)],
            ..Default::default()
        };
        let batch = p.into_batch("g", 0);
        assert_eq!(batch.records.len(), 1);
        assert!(batch.records[0].value.is_none());
    }

    // ---------------------------------------------------------------------
    // custom assignor registry
    // ---------------------------------------------------------------------

    use crate::coordinator::next_gen::assignor::{
        Assignment, Assignor, MemberSubscription, TopicMetadata,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CountingAssignor {
        calls: Arc<AtomicUsize>,
    }
    impl Assignor for CountingAssignor {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn assign(&self, _members: &[MemberSubscription], _topics: &TopicMetadata) -> Assignment {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::collections::HashMap::new()
        }
    }

    #[test]
    fn pick_assignor_skips_unregistered_member_preference() {
        let config = NextGenConfig::default();
        let mut state = crate::coordinator::next_gen::group_state::GroupState::new("g");
        let mut m = build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest::default(),
            "h",
            Instant::now(),
        );
        m.server_assignor = Some("ghost".into());
        state.members.insert("m1".into(), m);

        let picked = pick_assignor(&state, &config);
        assert_eq!(picked.name(), "uniform");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_assignor_invoked_when_requested() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut config = NextGenConfig::default();
        config
            .register_assignor(Arc::new(CountingAssignor {
                calls: calls.clone(),
            }))
            .unwrap();

        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(NextGenCoordinator::new(config, empty_metadata(), log));
        let handle = coord.get_or_create("g");

        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    server_assignor: Some("counting".into()),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let resp = rx.await.unwrap();
        assert_eq!(resp.error_code, 0);
        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "custom assignor must be invoked at least once",
        );
    }
}
