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
    ) -> Self {
        let (tx, rx) = mpsc::channel(64);
        let task = tokio::spawn(actor_loop(group_id, config, metadata_provider, rx));
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
                        let resp = handle_heartbeat(&mut state, &config, &*metadata, &request, &client_host);
                        let _ = reply.send(resp);
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
                        let view = build_describe(&state);
                        let _ = reply.send(view);
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
                let evicted = state.evict_expired(Instant::now(), config.session_timeout);
                if !evicted.is_empty() {
                    state.bump_epoch();
                    run_reconcile(&mut state, &config, &*metadata);
                }
            }
        }
    }
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
            // Persisted MemberMetadataValue doesn't carry the regex yet
            // (slice 64a deferred), so we hydrate with None; the client's
            // next heartbeat re-supplies it within a few seconds.
            subscribed_topic_regex: None,
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

fn handle_heartbeat(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    client_host: &str,
) -> ConsumerGroupHeartbeatResponse {
    let now = Instant::now();
    if req.member_epoch == -1 {
        state.remove_member(&req.member_id);
        state.bump_epoch();
        return base_resp(0, req.member_epoch, config);
    }
    if let Some(name) = req.server_assignor.as_deref()
        && !config.assignor_enabled(name)
    {
        return error_resp(codes::UNSUPPORTED_ASSIGNOR, config);
    }
    if req.member_epoch == 0 && req.member_id.is_empty() {
        let new_member_id = uuid::Uuid::new_v4().to_string();
        if let Some(iid) = req.instance_id.as_deref()
            && let Some(existing) = state.current_member_for_instance(iid)
            && state
                .members
                .get(existing)
                .is_some_and(|m| m.member_epoch != 0)
        {
            return error_resp(codes::UNRELEASED_INSTANCE_ID, config);
        }
        let m = build_member(&new_member_id, req, client_host, now);
        state.add_or_update_member(m);
        run_reconcile(state, config, metadata);
        state.advance_member_epoch(&new_member_id);
        return build_assignment_resp(state, &new_member_id, config);
    }
    let cur_epoch = state
        .members
        .get(&req.member_id)
        .map_or(-2, |m| m.member_epoch);
    if cur_epoch == -2 {
        return error_resp(codes::UNKNOWN_MEMBER_ID, config);
    }
    if req.member_epoch < cur_epoch {
        return error_resp(codes::STALE_MEMBER_EPOCH, config);
    }
    if req.member_epoch > cur_epoch {
        return error_resp(codes::FENCED_MEMBER_EPOCH, config);
    }
    if let Some(m) = state.members.get_mut(&req.member_id) {
        m.last_seen = now;
        if let Some(ref names) = req.subscribed_topic_names {
            let set: std::collections::HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                state.dirty = true;
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
    run_reconcile(state, config, metadata);
    if state.target.epoch > cur_epoch {
        state.advance_member_epoch(&req.member_id);
    }
    build_assignment_resp(state, &req.member_id, config)
}

fn run_reconcile(state: &mut GroupState, config: &NextGenConfig, metadata: &dyn MetadataProvider) {
    let input = metadata.snapshot();
    let assignor_name = pick_assignor(state, config);
    reconciler::reconcile_if_dirty(state, &input, &assignor_name);
}

fn pick_assignor(state: &GroupState, config: &NextGenConfig) -> String {
    state
        .members
        .values()
        .find_map(|m| m.server_assignor.clone())
        .unwrap_or_else(|| {
            config
                .assignors
                .first()
                .cloned()
                .unwrap_or_else(|| "uniform".into())
        })
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
