//! KIP-1071 streams-group coordinator actor: per-group tokio task driving the
//! heartbeat epoch dance, reconciliation, and persistence.
//!
//! Mirrors the KIP-932 share-group actor ([`super::super::share::actor`]) in
//! overall shape — a `tokio::select!` loop over an mpsc message channel plus a
//! `heartbeat_interval` session tick, the `Pending*Records` → `RecordBatch` →
//! `OffsetsLog::append` flush, and a last-known-good cache hand-off via
//! `GroupCoordinator::update_streams_cache` — but assigns *tasks*
//! `(subtopology, partition)` across the active/standby/warmup roles rather than
//! topic partitions, and reconciles against a full `MetadataImage` (topology
//! resolution + internal-topic creation) via the [`MetadataSource`] instead of
//! the consumer `MetadataProvider`.
//!
//! Reconciliation is gated on a wired [`MetadataSource`]: in the pure-coordinator
//! unit tests (no source) the group stays `NotReady` with empty assignments —
//! members still mint a `member_id` and advance their epoch, but no tasks are
//! assigned.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use crabka_log::Offset;
use crabka_protocol::owned::{
    common::streams_group_heartbeat_response::{status::Status, task_ids::TaskIds as RespTaskIds},
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use super::{
    assignor::{self, AssignorInput, AssignorMember},
    config::StreamsGroupConfig,
    persistence::{
        PendingStreamsRecords, StreamsEndpoint, StreamsGroupCurrentMemberAssignmentValue,
        StreamsGroupMemberMetadataValue, StreamsGroupMetadataValue,
        StreamsGroupPartitionMetadataValue, StreamsGroupTargetAssignmentMemberValue,
        StreamsGroupTargetAssignmentMetadataValue, StreamsGroupTopologyValue,
    },
    state::{
        StoredTopologyHandle, StreamsGroupState, StreamsGroupStatePhase,
        StreamsMemberAssignmentState, StreamsMemberState, StreamsTargetAssignment,
    },
    topology::{self, status as topo_status},
};
use crate::{
    codes,
    coordinator::unified::{first_join_member_id, offsets_log::OffsetsLog, validate_member_epoch},
    metadata_source::MetadataSource,
};

/// Messages accepted by a [`StreamsGroupActorHandle`].
#[derive(Debug)]
pub enum StreamsGroupActorMessage {
    Heartbeat {
        request: Box<StreamsGroupHeartbeatRequest>,
        client_id: String,
        client_host: String,
        reply: oneshot::Sender<StreamsGroupHeartbeatResponse>,
    },
    Describe {
        reply: oneshot::Sender<StreamsDescribeView>,
    },
    /// Validate an `OffsetCommit` / `TxnOffsetCommit` against the streams
    /// group's membership (KIP-1071 fences by `member_epoch`, like a KIP-848
    /// consumer group). `Ok(())` = allowed; `Err(code)` = reject. A
    /// simple-consumer commit (empty `member_id`, `member_epoch == -1`) is not
    /// fenced. Mirrors the consumer-group `ValidateCommit`.
    ValidateCommit {
        member_id: String,
        /// The request's `generation_id_or_member_epoch` field, interpreted as
        /// the streams `member_epoch`.
        member_epoch: i32,
        reply: oneshot::Sender<Result<(), i16>>,
    },
    Seed(super::super::StreamsGroupSeed),
    Shutdown(oneshot::Sender<()>),
}

/// Read-only projection of [`StreamsGroupState`], consumed by the (later)
/// `StreamsGroupDescribe` handler.
#[derive(Debug, Clone)]
pub struct StreamsDescribeView {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub topology_epoch: i32,
    pub group_state: String,
    /// The group's resolved topology (subtopologies + their topics). The real
    /// JVM `DescribeStreamsGroupsHandler` rejects a describe response whose
    /// topology is absent, so this must be populated once a member has supplied
    /// one. `None` only before any topology has been initialized.
    pub topology: Option<StreamsGroupTopologyValue>,
    pub members: Vec<StreamsDescribeMember>,
}

#[derive(Debug, Clone)]
pub struct StreamsDescribeMember {
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub process_id: String,
    pub active: BTreeMap<String, Vec<i32>>,
    pub standby: BTreeMap<String, Vec<i32>>,
    pub warmup: BTreeMap<String, Vec<i32>>,
}

#[derive(Debug)]
pub struct StreamsGroupActorHandle {
    pub tx: mpsc::Sender<StreamsGroupActorMessage>,
    _task: JoinHandle<()>,
}

impl StreamsGroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<StreamsGroupConfig>,
        offsets_log: Arc<dyn OffsetsLog>,
        metadata_source: Option<Arc<dyn MetadataSource>>,
        coordinator: Arc<super::super::GroupCoordinator>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.actor_mailbox_capacity);
        let task = tokio::spawn(actor_loop(
            group_id,
            config,
            offsets_log,
            metadata_source,
            coordinator,
            rx,
        ));
        Self { tx, _task: task }
    }
}

/// Validate an `OffsetCommit` / `TxnOffsetCommit` against a streams group's
/// membership by messaging its actor. Returns `Some(error_code)` to reject,
/// `None` to allow. KIP-447: a streams group fences offset commits by
/// `member_epoch` (the request's `generation_id_or_member_epoch`), exactly as a
/// KIP-848 consumer group does. The shared `validate_group_commit` only knows
/// about the classic/consumer `GroupActorHandle`, so a streams-group consumer
/// (whose membership lives in the streams actor, not a classic one) must be
/// validated here instead — otherwise its commit is fenced against an empty
/// classic actor and rejected with `UNKNOWN_MEMBER_ID`.
pub(crate) async fn validate_streams_group_commit(
    handle: &StreamsGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
) -> Option<i16> {
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(StreamsGroupActorMessage::ValidateCommit {
            member_id: member_id.to_string(),
            member_epoch,
            reply: tx,
        })
        .await
        .is_err()
    {
        return Some(codes::UNKNOWN_SERVER_ERROR);
    }
    match rx.await {
        Ok(Ok(())) => None,
        Ok(Err(code)) => Some(code),
        Err(_) => Some(codes::UNKNOWN_SERVER_ERROR),
    }
}

/// The actor's full mutable state: the in-memory state machine plus the
/// in-flight `StreamsGroupTopologyValue` (the resolved topology kept for
/// persistence + reconcile, since [`StreamsGroupState`] only tracks presence +
/// epoch) and the last-derived partition metadata.
struct ActorState {
    state: StreamsGroupState,
    /// The full stored topology, kept alongside `state.topology` (which only
    /// carries the epoch). `None` until the first member supplies one.
    topology: Option<StreamsGroupTopologyValue>,
    /// Partition metadata derived by the most recent reconcile, persisted as
    /// the group's `StreamsGroupPartitionMetadataValue`.
    partition_metadata: Option<StreamsGroupPartitionMetadataValue>,
}

impl ActorState {
    fn new(group_id: String) -> Self {
        Self {
            state: StreamsGroupState::new(group_id),
            topology: None,
            partition_metadata: None,
        }
    }
}

async fn actor_loop(
    group_id: String,
    config: Arc<StreamsGroupConfig>,
    offsets_log: Arc<dyn OffsetsLog>,
    metadata_source: Option<Arc<dyn MetadataSource>>,
    coordinator: Arc<super::super::GroupCoordinator>,
    mut rx: mpsc::Receiver<StreamsGroupActorMessage>,
) {
    let mut actor = ActorState::new(group_id);
    let mut tick = tokio::time::interval(config.heartbeat_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    StreamsGroupActorMessage::Heartbeat { request, client_id, client_host, reply } => {
                        match handle_heartbeat(
                            &mut actor,
                            &config,
                            &*offsets_log,
                            metadata_source.as_ref(),
                            &coordinator,
                            &request,
                            ClientContext {
                                id: &client_id,
                                host: &client_host,
                            },
                        )
                        .await
                        {
                            Ok(resp) => {
                                let _ = reply.send(resp);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    group_id = %actor.state.group_id,
                                    error = %e,
                                    "streams-group actor exiting after log-write failure",
                                );
                                let _ = reply.send(StreamsGroupHeartbeatResponse {
                                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                                    ..Default::default()
                                });
                                break;
                            }
                        }
                    }
                    StreamsGroupActorMessage::Describe { reply } => {
                        let _ = reply.send(build_describe(&actor.state, actor.topology.as_ref()));
                    }
                    StreamsGroupActorMessage::ValidateCommit { member_id, member_epoch, reply } => {
                        // KIP-447 fencing for a streams group: member_epoch must
                        // match the member's current epoch, mirroring the KIP-848
                        // consumer-group check. A simple-consumer commit (empty
                        // member_id, member_epoch == -1) is not fenced.
                        let result: Result<(), i16> = if member_id.is_empty() {
                            Ok(())
                        } else {
                            validate_member_epoch(
                                actor.state.members.get(&member_id).map(|m| m.member_epoch),
                                member_epoch,
                            )
                            .map(|_| ())
                        };
                        let _ = reply.send(result);
                    }
                    StreamsGroupActorMessage::Seed(seed) => {
                        apply_seed(&mut actor, seed);
                    }
                    StreamsGroupActorMessage::Shutdown(reply) => {
                        let _ = reply.send(());
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                if handle_session_tick(&mut actor, &config, &*offsets_log, metadata_source.as_ref(), &coordinator).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// Evict members silent past the session timeout, reconcile, and persist the
/// resulting tombstones. Returns `Err` if the log write fails (the actor exits).
async fn handle_session_tick(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &super::super::GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let evicted = actor
        .state
        .evict_expired(Instant::now(), config.session_timeout);
    if evicted.is_empty() {
        return Ok(());
    }
    // `evict_expired` set `dirty`; reconcile owns the single `bump_epoch`.
    reconcile(actor, config, metadata_source).await;
    let mut pending = snapshot_pending_after_change(actor, &[]);
    for mid in &evicted {
        pending.member_metadata.push((mid.clone(), None));
        pending.target_per_member.push((mid.clone(), None));
        pending.current_per_member.push((mid.clone(), None));
    }
    let now_ms = chrono_now_ms();
    flush_pending(actor, pending, offsets_log, coordinator, now_ms).await
}

#[derive(Clone, Copy)]
struct ClientContext<'a> {
    id: &'a str,
    host: &'a str,
}

async fn handle_heartbeat(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &super::super::GroupCoordinator,
    req: &StreamsGroupHeartbeatRequest,
    client: ClientContext<'_>,
) -> Result<StreamsGroupHeartbeatResponse, crate::error::BrokerError> {
    let ClientContext {
        id: client_id,
        host: client_host,
    } = client;
    let now = Instant::now();
    let now_ms = chrono_now_ms();

    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return handle_leave(
            actor,
            config,
            offsets_log,
            metadata_source,
            coordinator,
            req,
            now_ms,
        )
        .await;
    }

    // ─── First-join path ─────────────────────────────────────────
    // KIP-1071 mirrors KIP-848: epoch 0 from an unknown member is a first
    // join. The client may supply its own id; an empty id mints a server UUID.
    if req.member_epoch == 0 && !actor.state.members.contains_key(&req.member_id) {
        let new_member_id = first_join_member_id(&req.member_id);
        let m = build_member(&new_member_id, req, client_id, client_host, now);
        actor.state.add_or_update_member(m);
        // Topology supplied on first join is accepted before reconcile.
        if let Some(topo) = &req.topology {
            accept_topology(actor, topo);
        }
        apply_shutdown_application(actor, req);
        reconcile(actor, config, metadata_source).await;
        actor.state.advance_member_epoch(&new_member_id);
        let pending = snapshot_pending_after_change(actor, std::slice::from_ref(&new_member_id));
        flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
        return Ok(build_assignment_resp(&actor.state, &new_member_id, config));
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    let cur_epoch = match validate_member_epoch(
        actor
            .state
            .members
            .get(&req.member_id)
            .map(|m| m.member_epoch),
        req.member_epoch,
    ) {
        Ok(epoch) => epoch,
        Err(error_code) => return Ok(error_resp(error_code, config)),
    };

    // ─── Steady state ────────────────────────────────────────────
    let mut changed = update_member_steady_state(actor, req, now);
    // Topology handling: newer epoch is accepted, older is flagged STALE.
    if let Some(topo) = &req.topology {
        let cur_topo_epoch = actor.state.topology_epoch;
        if topo.epoch > cur_topo_epoch {
            accept_topology(actor, topo);
            changed = true;
        } else if topo.epoch < cur_topo_epoch {
            set_status(
                actor,
                topo_status::STALE_TOPOLOGY,
                "member reported a stale topology",
            );
        }
    }
    if apply_shutdown_application(actor, req) {
        changed = true;
    }

    if actor.state.dirty {
        reconcile(actor, config, metadata_source).await;
        changed = true;
    }
    // If the member's target advanced past its current epoch, hand it over.
    if actor.state.target.epoch > cur_epoch {
        actor.state.advance_member_epoch(&req.member_id);
        changed = true;
    }

    if changed {
        let pending = snapshot_pending_after_change(actor, std::slice::from_ref(&req.member_id));
        flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
    }
    Ok(build_assignment_resp(&actor.state, &req.member_id, config))
}

/// Update a steady-state member's reported ownership + catch-up offsets +
/// `last_seen`. Returns `true` if anything that needs persisting changed.
fn update_member_steady_state(
    actor: &mut ActorState,
    req: &StreamsGroupHeartbeatRequest,
    now: Instant,
) -> bool {
    let Some(m) = actor.state.members.get_mut(&req.member_id) else {
        return false;
    };
    m.last_seen = now;
    let mut changed = false;

    if let Some(active) = &req.active_tasks {
        let map = task_ids_to_map(active);
        if map != m.active {
            m.active = map;
            changed = true;
        }
    }
    if let Some(standby) = &req.standby_tasks {
        let map = task_ids_to_map(standby);
        if map != m.standby {
            m.standby = map;
            changed = true;
        }
    }
    if let Some(warmup) = &req.warmup_tasks {
        let map = task_ids_to_map(warmup);
        if map != m.warmup {
            m.warmup = map;
            changed = true;
        }
    }
    if let Some(offsets) = &req.task_offsets {
        let map = task_offsets_to_map(offsets);
        if map != m.task_offsets {
            m.task_offsets = map;
            changed = true;
        }
    }
    if let Some(end_offsets) = &req.task_end_offsets {
        let map = task_offsets_to_map(end_offsets);
        if map != m.task_end_offsets {
            m.task_end_offsets = map;
            changed = true;
        }
    }
    changed
}

/// Handle a leave-group heartbeat (`member_epoch == -1`).
async fn handle_leave(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &super::super::GroupCoordinator,
    req: &StreamsGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<StreamsGroupHeartbeatResponse, crate::error::BrokerError> {
    let was_member = actor.state.members.contains_key(&req.member_id);
    actor.state.remove_member(&req.member_id);
    // `remove_member` set `dirty`; reconcile owns the single `bump_epoch`. If
    // the leaver was unknown (not a member) the group is clean, so force a
    // reconcile to still re-stamp/bump as the leave path expects.
    actor.state.dirty = true;
    reconcile(actor, config, metadata_source).await;
    let mut pending = snapshot_pending_after_change(actor, &[]);
    if was_member {
        pending.member_metadata.push((req.member_id.clone(), None));
        pending
            .target_per_member
            .push((req.member_id.clone(), None));
        pending
            .current_per_member
            .push((req.member_id.clone(), None));
    }
    flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
    Ok(base_resp(codes::NONE, -1, config))
}

/// Accept a client-supplied topology: store the resolved value for persistence
/// + reconcile, stamp the epoch on the state handle, and mark the group dirty.
fn accept_topology(
    actor: &mut ActorState,
    wire_topology: &crabka_protocol::owned::streams_group_heartbeat_request::Topology,
) {
    let stored = topology::to_stored_topology(wire_topology);
    actor.state.topology = Some(StoredTopologyHandle {
        epoch: stored.epoch,
    });
    actor.state.topology_epoch = stored.epoch;
    actor.topology = Some(stored);
    actor.state.dirty = true;
}

/// KIP-1071 shutdown-application: any member can signal the whole group to shut
/// down. Record it as a group status so subsequent responses carry it. Returns
/// `true` if the status was newly added.
fn apply_shutdown_application(actor: &mut ActorState, req: &StreamsGroupHeartbeatRequest) -> bool {
    if !req.shutdown_application {
        return false;
    }
    set_status(
        actor,
        topo_status::SHUTDOWN_APPLICATION,
        "a member requested application shutdown",
    )
}

/// Add a `(code, detail)` to the group status if no entry with that code is
/// already present. Returns `true` if it was added.
fn set_status(actor: &mut ActorState, code: i8, detail: &str) -> bool {
    if actor.state.status.iter().any(|(c, _)| *c == code) {
        return false;
    }
    actor.state.status.push((code, detail.to_string()));
    true
}

/// Recompute the target assignment when the group is dirty.
///
/// Without a wired [`MetadataSource`] (unit tests) or before any topology is
/// supplied, the group stays `NotReady` with an empty target — members still
/// advance their epoch but receive no tasks. Otherwise: validate the topology,
/// derive task counts + partition metadata, ensure internal topics exist, and
/// run the assignor.
async fn reconcile(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) {
    if !actor.state.dirty {
        return;
    }

    let (Some(source), Some(topology)) = (metadata_source, actor.topology.clone()) else {
        // No metadata source or no topology yet: cannot assign. Bump the epoch
        // and install an empty target so members still advance (to an empty
        // assignment) and the group sits in NotReady.
        install_empty_target(&mut actor.state, StreamsGroupStatePhase::NotReady);
        return;
    };

    let image = source.current_image();

    // 1. Validation status (missing source / copartition mismatch).
    let mut status = topology::validate_topology(&topology, &image);

    // 2. Derive task counts + the external-topic partition snapshot.
    let derived = topology::derive_tasks(&topology, &image);
    actor.partition_metadata = Some(derived.partition_metadata.clone());

    // 3. Materialize required internal topics; any still-missing → status.
    let specs = topology::required_internal_topics(&topology, &derived.num_tasks);
    if !specs.is_empty() {
        match topology::ensure_internal_topics(
            source,
            &specs,
            config.internal_topic_replication_factor,
        )
        .await
        {
            Ok(still_missing) => {
                if !still_missing.is_empty() {
                    status.push((
                        topo_status::MISSING_INTERNAL_TOPICS,
                        format!(
                            "internal topics not yet created: {}",
                            still_missing.join(", ")
                        ),
                    ));
                }
            }
            Err(e) => {
                status.push((
                    topo_status::MISSING_INTERNAL_TOPICS,
                    format!("internal-topic creation failed: {e}"),
                ));
            }
        }
    }

    // Preserve any non-topology status (e.g. SHUTDOWN_APPLICATION) the actor
    // already recorded; topology-derived status replaces the rest.
    let preserved: Vec<(i8, String)> = actor
        .state
        .status
        .iter()
        .filter(|(c, _)| *c == topo_status::SHUTDOWN_APPLICATION)
        .cloned()
        .collect();

    let blocking = status.iter().any(|(c, _)| {
        *c == topo_status::MISSING_SOURCE_TOPICS
            || *c == topo_status::INCORRECTLY_PARTITIONED_TOPICS
            || *c == topo_status::MISSING_INTERNAL_TOPICS
    });

    status.extend(preserved);
    actor.state.status = status;

    if blocking {
        install_empty_target(&mut actor.state, StreamsGroupStatePhase::NotReady);
        return;
    }

    // 4. Build assignor inputs, compute the target, and install it.
    compute_and_install_target(actor, config, &topology, &derived.num_tasks);
}

/// Run the assignor over the resolved topology and install its output as the
/// new target. Bumps the group epoch, installs the target (which computes the
/// active revoke-split), and sets the phase to `Reconciling` while any member
/// still owns un-revoked active tasks, else `Stable`.
fn compute_and_install_target(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    topology: &StreamsGroupTopologyValue,
    num_tasks: &BTreeMap<String, i32>,
) {
    let members: Vec<AssignorMember> = actor
        .state
        .members
        .values()
        .map(|m| AssignorMember {
            member_id: m.member_id.clone(),
            process_id: m.process_id.clone(),
            rack_id: m.rack_id.clone(),
            current_active: m.active.clone(),
            current_standby: m.standby.clone(),
            current_warmup: m.warmup.clone(),
            task_lag: task_lag(m),
        })
        .collect();

    let stateful: BTreeSet<String> = topology
        .subtopologies
        .iter()
        .filter(|s| !s.state_changelog_topics.is_empty())
        .map(|s| s.subtopology_id.clone())
        .collect();

    let input = AssignorInput {
        tasks: topology::task_set(num_tasks),
        stateful,
        num_standby_replicas: config.num_standby_replicas,
        num_warmup_replicas: config.num_warmup_replicas,
        acceptable_recovery_lag: config.acceptable_recovery_lag,
        kind: config.assignor,
    };
    let assignment = assignor::assign(&members, &input);

    let target = StreamsTargetAssignment {
        epoch: 0,
        active: assignment.active,
        standby: assignment.standby,
        warmup: assignment.warmup,
    };
    actor.state.bump_epoch();
    actor.state.install_target(target);

    let pending_revocation = actor
        .state
        .members
        .values()
        .any(|m| m.assignment_state == StreamsMemberAssignmentState::UnrevokedActiveTasks);
    actor.state.phase = if pending_revocation {
        StreamsGroupStatePhase::Reconciling
    } else {
        StreamsGroupStatePhase::Stable
    };
    actor.state.dirty = false;
}

/// Bump the group epoch and install an empty target assignment, transitioning
/// to `phase`. Members still advance to the new (empty) assignment epoch on
/// their next `advance_member_epoch`. Clears `dirty`.
fn install_empty_target(state: &mut StreamsGroupState, phase: StreamsGroupStatePhase) {
    state.bump_epoch();
    state.install_target(StreamsTargetAssignment::default());
    state.phase = phase;
    state.dirty = false;
}

/// Per-task changelog lag for the assignor: `end_offset - offset` keyed by
/// `(subtopology, partition)`, only where both endpoints are reported.
fn task_lag(m: &StreamsMemberState) -> BTreeMap<(String, i32), i64> {
    let mut lag = BTreeMap::new();
    for (key, &end) in &m.task_end_offsets {
        if let Some(&pos) = m.task_offsets.get(key) {
            // Lag is the delta between two offsets — a record count (i64),
            // compared against `acceptable_recovery_lag`, not an offset.
            lag.insert(key.clone(), end.0 - pos.0);
        }
    }
    lag
}

// ---------------------------------------------------------------------------
// Wire <-> state conversions
// ---------------------------------------------------------------------------

/// Convert request `TaskIds` (subtopology + partitions) into the in-memory
/// `subtopology -> partitions` task map.
fn task_ids_to_map(
    tasks: &[crabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds],
) -> BTreeMap<String, Vec<i32>> {
    let mut map: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for t in tasks {
        let entry = map.entry(t.subtopology_id.clone()).or_default();
        entry.extend_from_slice(&t.partitions);
    }
    for parts in map.values_mut() {
        parts.sort_unstable();
        parts.dedup();
    }
    map
}

/// Convert request `TaskOffset` entries into a `(subtopology, partition) ->
/// offset` map. The wire `o.offset` field stays a raw `i64`; wrap it as an
/// `Offset` for the in-memory changelog-position map.
fn task_offsets_to_map(
    offsets: &[crabka_protocol::owned::common::streams_group_heartbeat_request::task_offset::TaskOffset],
) -> BTreeMap<(String, i32), Offset> {
    offsets
        .iter()
        .map(|o| ((o.subtopology_id.clone(), o.partition), Offset(o.offset)))
        .collect()
}

/// Render a `subtopology -> partitions` task map as a response `Vec<TaskIds>`.
fn map_to_task_ids(map: &BTreeMap<String, Vec<i32>>) -> Vec<RespTaskIds> {
    map.iter()
        .map(|(sub, parts)| RespTaskIds {
            subtopology_id: sub.clone(),
            partitions: parts.clone(),
            ..Default::default()
        })
        .collect()
}

fn build_member(
    member_id: &str,
    req: &StreamsGroupHeartbeatRequest,
    client_id: &str,
    host: &str,
    now: Instant,
) -> StreamsMemberState {
    let mut m = StreamsMemberState::joining(member_id, client_id, host);
    if let Some(pid) = &req.process_id
        && !pid.is_empty()
    {
        m.process_id.clone_from(pid);
    }
    m.rack_id.clone_from(&req.rack_id);
    m.instance_id.clone_from(&req.instance_id);
    m.user_endpoint = req
        .user_endpoint
        .as_ref()
        .map(|ep| (ep.host.clone(), u32::from(ep.port)));
    if let Some(tags) = &req.client_tags {
        m.client_tags = tags
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
    }
    m.rebalance_timeout_ms = req.rebalance_timeout_ms;
    if let Some(topo) = &req.topology {
        m.topology_epoch = topo.epoch;
    }
    m.last_seen = now;
    m
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

fn base_resp(
    error_code: i16,
    member_epoch: i32,
    config: &StreamsGroupConfig,
) -> StreamsGroupHeartbeatResponse {
    StreamsGroupHeartbeatResponse {
        error_code,
        member_epoch,
        heartbeat_interval_ms: duration_ms(config.heartbeat_interval, 5_000),
        acceptable_recovery_lag: i32::try_from(config.acceptable_recovery_lag).unwrap_or(i32::MAX),
        task_offset_interval_ms: duration_ms(config.task_offset_interval, 30_000),
        ..Default::default()
    }
}

fn error_resp(error_code: i16, config: &StreamsGroupConfig) -> StreamsGroupHeartbeatResponse {
    base_resp(error_code, 0, config)
}

fn build_assignment_resp(
    state: &StreamsGroupState,
    member_id: &str,
    config: &StreamsGroupConfig,
) -> StreamsGroupHeartbeatResponse {
    let m = state
        .members
        .get(member_id)
        .expect("member exists at build_assignment_resp");
    let status = if state.status.is_empty() {
        None
    } else {
        Some(
            state
                .status
                .iter()
                .map(|(code, detail)| Status {
                    status_code: *code,
                    status_detail: detail.clone(),
                    ..Default::default()
                })
                .collect(),
        )
    };
    StreamsGroupHeartbeatResponse {
        error_code: codes::NONE,
        member_id: member_id.to_string(),
        member_epoch: m.member_epoch,
        heartbeat_interval_ms: duration_ms(config.heartbeat_interval, 5_000),
        acceptable_recovery_lag: i32::try_from(config.acceptable_recovery_lag).unwrap_or(i32::MAX),
        task_offset_interval_ms: duration_ms(config.task_offset_interval, 30_000),
        status,
        active_tasks: Some(map_to_task_ids(&m.active)),
        standby_tasks: Some(map_to_task_ids(&m.standby)),
        warmup_tasks: Some(map_to_task_ids(&m.warmup)),
        ..Default::default()
    }
}

fn build_describe(
    state: &StreamsGroupState,
    topology: Option<&StreamsGroupTopologyValue>,
) -> StreamsDescribeView {
    StreamsDescribeView {
        group_id: state.group_id.clone(),
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        topology_epoch: state.topology_epoch,
        group_state: state.phase.as_str().to_string(),
        topology: topology.cloned(),
        members: state
            .members
            .values()
            .map(|m| StreamsDescribeMember {
                member_id: m.member_id.clone(),
                member_epoch: m.member_epoch,
                instance_id: m.instance_id.clone(),
                rack_id: m.rack_id.clone(),
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                process_id: m.process_id.clone(),
                active: m.active.clone(),
                standby: m.standby.clone(),
                warmup: m.warmup.clone(),
            })
            .collect(),
    }
}

fn duration_ms(d: std::time::Duration, fallback: i32) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(fallback)
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

/// Build a `PendingStreamsRecords` reflecting the changes for `affected_members`.
/// Always includes the current group epoch; includes topology + partition
/// metadata when both are present, and target metadata when the target has been
/// installed (`epoch > 0`).
fn snapshot_pending_after_change(
    actor: &ActorState,
    affected_members: &[String],
) -> PendingStreamsRecords {
    let state = &actor.state;
    let mut pending = PendingStreamsRecords {
        group_metadata: Some(StreamsGroupMetadataValue {
            epoch: state.group_epoch,
        }),
        ..Default::default()
    };
    if let Some(topology) = &actor.topology {
        pending.topology = Some(topology.clone());
    }
    if let Some(pm) = &actor.partition_metadata {
        pending.partition_metadata = Some(pm.clone());
    }
    if state.target.epoch > 0 {
        pending.target_metadata = Some(StreamsGroupTargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    for mid in affected_members {
        if let Some(m) = state.members.get(mid) {
            pending
                .member_metadata
                .push((mid.clone(), Some(member_metadata_value(m))));
            pending
                .current_per_member
                .push((mid.clone(), Some(current_assignment_value(m))));
            if let Some(tv) = target_member_value(state, mid) {
                pending.target_per_member.push((mid.clone(), Some(tv)));
            }
        }
    }
    pending
}

fn member_metadata_value(m: &StreamsMemberState) -> StreamsGroupMemberMetadataValue {
    StreamsGroupMemberMetadataValue {
        instance_id: m.instance_id.clone(),
        rack_id: m.rack_id.clone(),
        client_id: m.client_id.clone(),
        client_host: m.client_host.clone(),
        process_id: m.process_id.clone(),
        user_endpoint: m
            .user_endpoint
            .as_ref()
            .map(|(host, port)| StreamsEndpoint {
                host: host.clone(),
                port: *port,
            }),
        client_tags: m.client_tags.clone(),
        rebalance_timeout_ms: m.rebalance_timeout_ms,
        topology_epoch: m.topology_epoch,
    }
}

fn current_assignment_value(m: &StreamsMemberState) -> StreamsGroupCurrentMemberAssignmentValue {
    StreamsGroupCurrentMemberAssignmentValue {
        member_epoch: m.member_epoch,
        previous_member_epoch: m.previous_member_epoch,
        state: m.assignment_state.as_i8(),
        active: m.active.clone(),
        standby: m.standby.clone(),
        warmup: m.warmup.clone(),
        active_pending_revocation: m.active_pending_revocation.clone(),
    }
}

fn target_member_value(
    state: &StreamsGroupState,
    member_id: &str,
) -> Option<StreamsGroupTargetAssignmentMemberValue> {
    let active = state.target.active.get(member_id).cloned();
    let standby = state.target.standby.get(member_id).cloned();
    let warmup = state.target.warmup.get(member_id).cloned();
    if active.is_none() && standby.is_none() && warmup.is_none() {
        return None;
    }
    Some(StreamsGroupTargetAssignmentMemberValue {
        active: active.unwrap_or_default(),
        standby: standby.unwrap_or_default(),
        warmup: warmup.unwrap_or_default(),
    })
}

async fn flush_pending(
    actor: &ActorState,
    pending: PendingStreamsRecords,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::super::GroupCoordinator,
    now_ms: i64,
) -> Result<(), crate::error::BrokerError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = pending.into_batch(&actor.state.group_id, now_ms);
    offsets_log.append(batch).await?;
    coordinator.update_streams_cache(&actor.state.group_id, snapshot_seed(actor));
    Ok(())
}

/// Snapshot the full actor state into a `StreamsGroupSeed` for the cache (and a
/// respawned actor). Mirrors what bootstrap replay produces.
fn snapshot_seed(actor: &ActorState) -> super::super::StreamsGroupSeed {
    let state = &actor.state;
    let mut members = std::collections::HashMap::new();
    let mut target_per_member = std::collections::HashMap::new();
    let mut current_per_member = std::collections::HashMap::new();
    for (mid, m) in &state.members {
        members.insert(mid.clone(), member_metadata_value(m));
        current_per_member.insert(mid.clone(), current_assignment_value(m));
        if let Some(tv) = target_member_value(state, mid) {
            target_per_member.insert(mid.clone(), tv);
        }
    }
    super::super::StreamsGroupSeed {
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        topology: actor.topology.clone(),
        partition_metadata: actor.partition_metadata.clone(),
        members,
        target_per_member,
        current_per_member,
    }
}

/// Hydrate the actor from a `StreamsGroupSeed` (bootstrap replay or respawn).
fn apply_seed(actor: &mut ActorState, seed: super::super::StreamsGroupSeed) {
    let state = &mut actor.state;
    state.group_epoch = seed.group_epoch;
    state.target.epoch = seed.assignment_epoch;
    state.assignment_epoch = seed.assignment_epoch;
    if let Some(topology) = &seed.topology {
        state.topology = Some(StoredTopologyHandle {
            epoch: topology.epoch,
        });
        state.topology_epoch = topology.epoch;
    }
    actor.topology = seed.topology;
    actor.partition_metadata = seed.partition_metadata;

    for (mid, meta) in seed.members {
        let mut m = StreamsMemberState::joining(mid.clone(), meta.client_id, meta.client_host);
        m.instance_id = meta.instance_id;
        m.rack_id = meta.rack_id;
        m.process_id = meta.process_id;
        m.user_endpoint = meta.user_endpoint.map(|ep| (ep.host, ep.port));
        m.client_tags = meta.client_tags;
        m.rebalance_timeout_ms = meta.rebalance_timeout_ms;
        m.topology_epoch = meta.topology_epoch;
        state.members.insert(mid, m);
    }
    for (mid, cur) in seed.current_per_member {
        if let Some(m) = state.members.get_mut(&mid) {
            m.member_epoch = cur.member_epoch;
            m.previous_member_epoch = cur.previous_member_epoch;
            m.assignment_state =
                StreamsMemberAssignmentState::from_i8(cur.state).unwrap_or_default();
            m.active = cur.active;
            m.standby = cur.standby;
            m.warmup = cur.warmup;
            m.active_pending_revocation = cur.active_pending_revocation;
        }
    }
    for (mid, tv) in seed.target_per_member {
        state.target.active.insert(mid.clone(), tv.active);
        state.target.standby.insert(mid.clone(), tv.standby);
        state.target.warmup.insert(mid, tv.warmup);
    }
    state.phase = if state.members.is_empty() {
        StreamsGroupStatePhase::Empty
    } else if actor.topology.is_some() {
        StreamsGroupStatePhase::Stable
    } else {
        StreamsGroupStatePhase::NotReady
    };
    state.dirty = false;
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::{
        GroupCoordinator, actor::MetadataProvider, config::NextGenConfig,
        offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
        share::config::ShareGroupConfig,
    };

    #[derive(Debug)]
    struct EmptyMetadata;
    impl MetadataProvider for EmptyMetadata {
        fn snapshot(&self) -> ReconcileInput {
            ReconcileInput::default()
        }
    }

    /// Build a coordinator with no `MetadataSource` wired (reconcile no-ops to
    /// `NotReady`) and a fake offsets log.
    fn make_coordinator() -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
        let log = Arc::new(InMemoryOffsetsLog::default());
        let metadata: Arc<dyn MetadataProvider> = Arc::new(EmptyMetadata);
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            ShareGroupConfig::default(),
            metadata,
            log.clone(),
            StreamsGroupConfig::default(),
        ));
        (coord, log)
    }

    async fn heartbeat(
        handle: &StreamsGroupActorHandle,
        req: StreamsGroupHeartbeatRequest,
    ) -> StreamsGroupHeartbeatResponse {
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(StreamsGroupActorMessage::Heartbeat {
                request: Box::new(req),
                client_id: "client".into(),
                client_host: "/127.0.0.1".into(),
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_join_mints_id_advances_epoch_not_ready() {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        let resp = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                ..Default::default()
            },
        )
        .await;
        check!(resp.error_code == codes::NONE);
        check!(!resp.member_id.is_empty(), "server mints a member id");
        // No metadata source / no topology → NotReady, empty assignment, but the
        // member still advances to the (bumped) group epoch.
        check!(resp.member_epoch == 1);
        check!(resp.active_tasks == Some(vec![]));
        check!(resp.standby_tasks == Some(vec![]));
        check!(resp.warmup_tasks == Some(vec![]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn second_heartbeat_at_right_epoch_accepted() {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        let join = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                ..Default::default()
            },
        )
        .await;
        assert!(join.error_code == codes::NONE);
        let epoch = join.member_epoch;
        let resp = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: epoch,
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == codes::NONE);
        assert!(resp.member_epoch == epoch);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_epoch_is_rejected() {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        let join = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                ..Default::default()
            },
        )
        .await;
        assert!(join.member_epoch == 1);
        // member_epoch below the server's view → STALE_MEMBER_EPOCH (the member
        // is known at epoch 1, so re-sending epoch 0 is treated as a stale
        // existing member, not a first-join).
        let resp = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: -2,
                ..Default::default()
            },
        )
        .await;
        // -2 < 1 → stale. (member_epoch 0 from a *known* member is the
        // first-join guard's `!contains_key` miss, so we use a clearly-stale
        // value here.)
        assert!(resp.error_code == codes::STALE_MEMBER_EPOCH);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn known_member_epoch_zero_is_stale_not_first_join() {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        let join = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                ..Default::default()
            },
        )
        .await;
        assert!(join.member_epoch == 1);

        let resp = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                ..Default::default()
            },
        )
        .await;

        assert!(resp.error_code == codes::STALE_MEMBER_EPOCH);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fenced_epoch_is_rejected() {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        let join = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                ..Default::default()
            },
        )
        .await;
        assert!(join.member_epoch == 1);
        let resp = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 99,
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == codes::FENCED_MEMBER_EPOCH);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leave_removes_member() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        let join = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                ..Default::default()
            },
        )
        .await;
        let mid = join.member_id.clone();
        let pre_leave = log.batches().await.len();

        let resp = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: mid,
                member_epoch: -1,
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == codes::NONE);
        assert!(resp.member_epoch == -1);
        let batches = log.batches().await;
        assert!(batches.len() == pre_leave + 1);
        let leave_batch = &batches[batches.len() - 1];
        assert!(
            leave_batch.records.iter().any(|r| r.value.is_none()),
            "leave batch must contain at least one tombstone"
        );
    }

    #[test]
    fn seed_hydrates_state() {
        let mut actor = ActorState::new("g".into());
        let mut members = std::collections::HashMap::new();
        members.insert(
            "m1".to_string(),
            StreamsGroupMemberMetadataValue {
                instance_id: Some("i1".into()),
                rack_id: Some("r1".into()),
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                process_id: "p1".into(),
                user_endpoint: Some(StreamsEndpoint {
                    host: "h".into(),
                    port: 9092,
                }),
                client_tags: vec![],
                rebalance_timeout_ms: 60_000,
                topology_epoch: 2,
            },
        );
        let mut current = std::collections::HashMap::new();
        current.insert(
            "m1".to_string(),
            StreamsGroupCurrentMemberAssignmentValue {
                member_epoch: 4,
                previous_member_epoch: 3,
                state: 0,
                active: BTreeMap::from([("0".to_string(), vec![0, 1])]),
                standby: BTreeMap::new(),
                warmup: BTreeMap::new(),
                active_pending_revocation: BTreeMap::new(),
            },
        );
        let mut target = std::collections::HashMap::new();
        target.insert(
            "m1".to_string(),
            StreamsGroupTargetAssignmentMemberValue {
                active: BTreeMap::from([("0".to_string(), vec![0, 1])]),
                standby: BTreeMap::new(),
                warmup: BTreeMap::new(),
            },
        );
        let seed = super::super::super::StreamsGroupSeed {
            group_epoch: 4,
            assignment_epoch: 4,
            topology: Some(StreamsGroupTopologyValue {
                epoch: 2,
                subtopologies: vec![],
            }),
            partition_metadata: None,
            members,
            target_per_member: target,
            current_per_member: current,
        };
        apply_seed(&mut actor, seed);

        check!(actor.state.group_epoch == 4);
        check!(actor.state.target.epoch == 4);
        check!(actor.state.topology_epoch == 2);
        let m = actor.state.members.get("m1").expect("member restored");
        check!(m.member_epoch == 4);
        check!(m.previous_member_epoch == 3);
        check!(m.process_id == "p1");
        check!(m.active == BTreeMap::from([("0".to_string(), vec![0, 1])]));
        check!(actor.state.target.active["m1"] == BTreeMap::from([("0".to_string(), vec![0, 1])]));
        check!(actor.state.phase == StreamsGroupStatePhase::Stable);
    }

    #[test]
    fn task_lag_is_end_minus_offset_only_when_both_reported() {
        let mut m = StreamsMemberState::joining("m1", "client", "/127.0.0.1");
        // Two tasks with both endpoints reported → lag = end - offset.
        m.task_end_offsets = BTreeMap::from([
            (("sub-a".to_string(), 0), Offset(10)),
            (("sub-a".to_string(), 1), Offset(5)),
            // A task with an end offset but NO reported position is dropped.
            (("sub-b".to_string(), 0), Offset(99)),
        ]);
        m.task_offsets = BTreeMap::from([
            (("sub-a".to_string(), 0), Offset(3)),
            (("sub-a".to_string(), 1), Offset(5)),
        ]);
        let lag = task_lag(&m);
        // 10 - 3 = 7 (kills `-`→`+` which is 13, and `-`→`/` which is 3).
        check!(lag[&("sub-a".to_string(), 0)] == 7);
        // 5 - 5 = 0 (kills `-`→`/` which would be 1).
        check!(lag[&("sub-a".to_string(), 1)] == 0);
        // sub-b has no reported position, so it is absent (pins the filter and
        // kills the fixed-map replacements that inject sub-b / xyzzy keys).
        check!(!lag.contains_key(&("sub-b".to_string(), 0)));
        check!(lag.len() == 2);
    }

    #[test]
    fn task_offsets_to_map_wraps_each_wire_entry() {
        use crabka_protocol::owned::common::streams_group_heartbeat_request::task_offset::TaskOffset;
        let wire = vec![
            TaskOffset {
                subtopology_id: "sub-a".to_string(),
                partition: 0,
                offset: 42,
                ..Default::default()
            },
            TaskOffset {
                subtopology_id: "sub-a".to_string(),
                partition: 1,
                offset: 7,
                ..Default::default()
            },
        ];
        let map = task_offsets_to_map(&wire);
        check!(
            map == BTreeMap::from([
                (("sub-a".to_string(), 0), Offset(42)),
                (("sub-a".to_string(), 1), Offset(7)),
            ])
        );
    }
}
