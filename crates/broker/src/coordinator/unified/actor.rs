//! Per-group tokio actor. Owns one unified `Group` — either the classic
//! 5-state machine or the next-gen epoch machine. Next-gen heartbeats are
//! non-parking mpsc messages with `oneshot` replies; classic
//! `JoinGroup`/`SyncGroup` parking is re-expressed as a park/wake message
//! protocol where the actor holds the reply `oneshot::Sender` in a parked
//! registry and resolves it at the rebalance boundary (the rebalance-deadline
//! timer, an all-members-joined early-complete, or the leader's `SyncGroup`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::consumer_group_heartbeat_response::{
    Assignment as RespAssignment, ConsumerGroupHeartbeatResponse,
};
use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::join_group_request::JoinGroupRequest;
use crabka_protocol::owned::leave_group_request::LeaveGroupRequest;
use crabka_protocol::owned::leave_group_response::MemberResponse;
use crabka_protocol::owned::sync_group_request::SyncGroupRequest;
use crabka_protocol::primitives::uuid::Uuid;

use crate::codes;
use crate::coordinator::{GroupSnapshot, MemberSnapshot};

use super::classic_ops;
use super::classic_state::{Group as ClassicState, GroupState as ClassicGroupState, OffsetEntry};
use super::config::NextGenConfig;
use super::consumer_state::{GroupState, MemberState};
use super::group::{Group, GroupKind};
use super::migration;
use super::offsets_log::OffsetsLog;
use super::persistence_next_gen::MemberAssignmentState;
use super::reconciler::{self, ReconcileInput};

/// Which protocol an actor's `Group` speaks. Fixed at spawn; exposed on the
/// handle so the coordinator can route/reject cross-protocol RPCs and filter
/// admin views without messaging the actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupKindTag {
    Classic,
    Consumer,
}

#[derive(Debug)]
pub enum GroupActorMessage {
    // ── next-gen consumer protocol (non-parking) ──
    Heartbeat {
        request: ConsumerGroupHeartbeatRequest,
        client_host: String,
        reply: oneshot::Sender<ConsumerGroupHeartbeatResponse>,
    },
    /// Validate an `OffsetCommit` against the group's LIVE protocol. The actor
    /// dispatches on `group.kind`: next-gen checks `member_epoch`, classic checks
    /// member/instance/generation. `Ok(())` = allowed; `Err(code)` = reject.
    ValidateCommit {
        member_id: String,
        group_instance_id: Option<String>,
        /// The request's `generation_id_or_member_epoch` field; interpreted as the
        /// consumer `member_epoch` or the classic generation per the live kind.
        generation_or_epoch: i32,
        reply: oneshot::Sender<Result<(), i16>>,
    },
    Describe {
        reply: oneshot::Sender<DescribeView>,
    },

    // ── classic protocol (parking) ──
    ClassicJoin {
        req: JoinGroupRequest,
        client_host: String,
        reply: oneshot::Sender<JoinResult>,
    },
    ClassicSync {
        req: SyncGroupRequest,
        reply: oneshot::Sender<SyncResult>,
    },
    ClassicHeartbeat {
        req: HeartbeatRequest,
        reply: oneshot::Sender<i16>,
    },
    ClassicLeave {
        req: LeaveGroupRequest,
        version: i16,
        reply: oneshot::Sender<Vec<MemberResponse>>,
    },
    /// Read-only classic snapshot for the admin/offset-delete handlers.
    ClassicInspect {
        reply: oneshot::Sender<ClassicView>,
    },
    /// Kind-agnostic admin snapshot for the classic `ListGroups`/`DescribeGroups`
    /// path. Projects the LIVE group — classic OR consumer (hosted-classic
    /// included, post-migration) — into a `GroupSnapshot`, so a migrated group
    /// reports coherently regardless of the handle's spawn-time `kind` hint.
    InspectAny {
        reply: oneshot::Sender<GroupSnapshot>,
    },

    // ── committed offsets (protocol-agnostic; on `Group.committed_offsets`) ──
    UpdateCommitted {
        entries: Vec<((String, i32), OffsetEntry)>,
        reply: oneshot::Sender<()>,
    },
    FetchCommitted {
        reply: oneshot::Sender<HashMap<(String, i32), OffsetEntry>>,
    },
    RemoveCommitted {
        keys: Vec<(String, i32)>,
        reply: oneshot::Sender<()>,
    },

    // ── bootstrap / lifecycle ──
    Seed(super::GroupSeed),
    /// Replace this (classic) actor's whole `Group` with replayed state.
    ClassicSeed(Box<Group>),
    Shutdown(oneshot::Sender<()>),

    /// Test-only: flip the live `Group` to a fresh empty consumer group in
    /// place, exercising the tick's dispatch on the live `group.kind`.
    #[cfg(test)]
    TestForceConsumerKind,
}

/// Structured `JoinGroup` result handed back to the handler, which encodes it
/// for the wire version. Mirrors the fields of `JoinGroupResponse`.
#[derive(Debug, Default, Clone)]
pub struct JoinResult {
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinResultMember>,
}

#[derive(Debug, Clone)]
pub struct JoinResultMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Bytes,
}

/// Structured `SyncGroup` result handed back to the handler.
#[derive(Debug, Default, Clone)]
pub struct SyncResult {
    pub error_code: i16,
    pub assignment: Bytes,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
}

/// Read-only projection of a classic `Group` for the admin / offset-delete
/// handlers (which need member subscriptions the next-gen `DescribeView`
/// doesn't carry).
#[derive(Debug, Clone)]
pub struct ClassicView {
    pub group_id: String,
    pub state: ClassicGroupState,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub generation_id: i32,
    pub members: Vec<ClassicMemberView>,
}

#[derive(Debug, Clone)]
pub struct ClassicMemberView {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub group_instance_id: Option<String>,
    pub protocol_metadata: Bytes,
    pub assignment: Option<Bytes>,
}

impl ClassicView {
    /// Build the admin `GroupSnapshot` (`ListGroups`/`DescribeGroups`).
    #[must_use]
    pub fn snapshot(&self) -> GroupSnapshot {
        GroupSnapshot {
            group_id: self.group_id.clone(),
            state: self.state,
            protocol_type: self.protocol_type.clone(),
            protocol_name: self.protocol_name.clone(),
            generation_id: self.generation_id,
            members: self
                .members
                .iter()
                .map(|m| MemberSnapshot {
                    member_id: m.member_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.host.clone(),
                    assignment: m
                        .assignment
                        .as_ref()
                        .map(|b| b.to_vec())
                        .unwrap_or_default(),
                    protocol_metadata: m.protocol_metadata.to_vec(),
                })
                .collect(),
        }
    }
}

/// Project a next-gen consumer `GroupState` into the classic admin
/// `GroupSnapshot` (`ListGroups`/`DescribeGroups`). KIP-848 consumer groups are
/// reported to the classic admin path with `protocol_type = "consumer"`, the
/// classic `Stable` state (the value the classic path uses for a settled group —
/// Kafka shows a healthy consumer group as `Stable`), and `generation_id` set to
/// the group epoch (the next-gen analogue of the classic generation). Each
/// member's assignment is its reconciler TARGET translated to a
/// `ConsumerProtocolAssignment` blob — the same source `serve_classic_sync` /
/// the heartbeat response use — so an assigned member (hosted classic included)
/// reports a non-empty assignment.
fn build_consumer_snapshot(state: &GroupState, image: &ReconcileInput) -> GroupSnapshot {
    GroupSnapshot {
        group_id: state.group_id.clone(),
        state: ClassicGroupState::Stable,
        protocol_type: Some("consumer".into()),
        // Next-gen (KIP-848) members carry no classic JoinGroup protocol
        // name; `DescribeGroups` is the classic API, so leave it empty.
        protocol_name: None,
        generation_id: state.group_epoch,
        members: state
            .members
            .values()
            .map(|m| {
                let target = state
                    .target
                    .per_member
                    .get(&m.member_id)
                    .cloned()
                    .unwrap_or_default();
                let assignment = migration::target_to_consumer_assignment(&target, image).to_vec();
                MemberSnapshot {
                    member_id: m.member_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    assignment,
                    // Next-gen members carry no classic JoinGroup metadata.
                    protocol_metadata: Vec::new(),
                }
            })
            .collect(),
    }
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
    /// `true` iff this is a classic member hosted in an upgraded group (its
    /// `ClassicMemberFacade` is set). Distinguishes a classic-protocol member
    /// served through the next-gen machinery from a native consumer member.
    pub is_classic: bool,
}

#[derive(Debug)]
pub struct GroupActorHandle {
    pub tx: mpsc::Sender<GroupActorMessage>,
    /// Spawn-time protocol hint. Fixed for the actor's lifetime — a KIP-848 live
    /// migration can flip the group's kind in place after spawn, leaving this
    /// stale. It is therefore used ONLY for spawn-time wiring (the initial
    /// `Group::new_classic`/`new_consumer`) and replay assertions; every
    /// routing/validation decision dispatches on the actor's LIVE `group.kind`
    /// inside the actor, never on this field.
    pub kind: GroupKindTag,
    _task: JoinHandle<()>,
}

impl GroupActorHandle {
    pub fn spawn(
        group_id: String,
        kind: GroupKindTag,
        config: Arc<NextGenConfig>,
        metadata_provider: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
        coordinator: Arc<super::GroupCoordinator>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(64);
        let task = tokio::spawn(actor_loop(
            group_id,
            kind,
            config,
            metadata_provider,
            offsets_log,
            coordinator,
            rx,
        ));
        Self {
            tx,
            kind,
            _task: task,
        }
    }
}

pub trait MetadataProvider: Send + Sync + std::fmt::Debug {
    fn snapshot(&self) -> ReconcileInput;
}

/// Parked classic-protocol waiters for one group.
#[derive(Default)]
struct ParkedWaiters {
    /// Parked `JoinGroup` handlers, keyed by `member_id` → reply sender.
    joiners: HashMap<String, oneshot::Sender<JoinResult>>,
    /// Parked `SyncGroup` followers, keyed by `member_id` → reply sender.
    followers: HashMap<String, oneshot::Sender<SyncResult>>,
}

#[allow(clippy::too_many_lines)] // one match arm per message variant; splitting hurts readability
async fn actor_loop(
    group_id: String,
    kind: GroupKindTag,
    config: Arc<NextGenConfig>,
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
    coordinator: Arc<super::GroupCoordinator>,
    mut rx: mpsc::Receiver<GroupActorMessage>,
) {
    let mut group = match kind {
        GroupKindTag::Classic => Group::new_classic(group_id),
        GroupKindTag::Consumer => Group::new_consumer(group_id),
    };
    let mut parked = ParkedWaiters::default();
    // A single 1-second session-expiry tick, kind-agnostic. The tick arm
    // dispatches on the live `group.kind`, so the cadence must not depend on
    // the spawn-time kind. Expiry is a
    // `last_seen`-vs-`session_timeout` comparison, so ticking once a second
    // (rather than on the heartbeat interval for consumer groups) only changes
    // how often we check, never the outcome.
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let deadline = classic_deadline(&group);
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    // ── next-gen ──
                    GroupActorMessage::Heartbeat { request, client_host, reply } => {
                        // KIP-848 live migration: a `ConsumerGroupHeartbeat` for a
                        // classic group triggers an in-place UPGRADE to a next-gen
                        // consumer group hosting the existing classic members,
                        // provided the policy allows it and every classic member's
                        // subscription survives translation. The upgrade batch is
                        // atomic (k2 tombstone + full next-gen record set); the
                        // reconcile + per-member target write happens on the
                        // `handle_heartbeat` call below (the joining consumer's
                        // first-join reconciles ALL members, classic facades
                        // included, since the converted state is dirty).
                        if group.is_classic() {
                            let convertible = group
                                .as_classic()
                                .is_some_and(migration::classic_is_convertible);
                            if config.migration_policy.allows_upgrade() && convertible {
                                let classic = group.as_classic().expect("classic kind");
                                let new_state = migration::convert_classic_to_consumer(classic);
                                let pending = migration::upgrade_pending_records(&new_state);
                                if flush_pending(
                                    &new_state, pending, &*offsets_log, &coordinator,
                                    chrono_now_ms(),
                                )
                                .await
                                .is_err()
                                {
                                    let _ = reply.send(ConsumerGroupHeartbeatResponse {
                                        error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                                        ..Default::default()
                                    });
                                    break;
                                }
                                *group.kind_mut() = GroupKind::Consumer(new_state);
                            } else {
                                // TODO(kip-848): confirm against apache/kafka:4.0.0 — an
                                // un-upgradable/disallowed classic group appears to surface as
                                // GROUP_ID_NOT_FOUND to a ConsumerGroupHeartbeat.
                                let _ = reply.send(ConsumerGroupHeartbeatResponse {
                                    error_code: codes::GROUP_ID_NOT_FOUND,
                                    ..Default::default()
                                });
                                continue;
                            }
                        }
                        let Some(state) = group.as_consumer_mut() else {
                            let _ = reply.send(ConsumerGroupHeartbeatResponse {
                                error_code: codes::GROUP_ID_NOT_FOUND,
                                ..Default::default()
                            });
                            continue;
                        };
                        match handle_heartbeat(
                            state, &config, &*metadata, &*offsets_log, &coordinator,
                            &request, &client_host,
                        ).await {
                            Ok(resp) => { let _ = reply.send(resp); }
                            Err(e) => {
                                tracing::warn!(
                                    group_id = %group.group_id, error = %e,
                                    "next-gen actor exiting after log-write failure",
                                );
                                let _ = reply.send(ConsumerGroupHeartbeatResponse {
                                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                                    ..Default::default()
                                });
                                break;
                            }
                        }
                        // KIP-848 DOWNGRADE: only a leave can remove the last native
                        // consumer member, but we re-check after every heartbeat;
                        // maybe_downgrade is a cheap no-op while any native member
                        // remains. The reply for this RPC is already sent above.
                        if let Err(e) = maybe_downgrade(
                            &mut group, &config, &*metadata, &*offsets_log, &coordinator,
                        ).await {
                            tracing::warn!(
                                group_id = %group.group_id, error = %e,
                                "next-gen actor exiting after downgrade log-write failure",
                            );
                            break;
                        }
                    }
                    GroupActorMessage::ValidateCommit { member_id, group_instance_id, generation_or_epoch, reply } => {
                        let result: Result<(), i16> = if let Some(s) = group.as_consumer() {
                            s.validate_commit_decision(&member_id, generation_or_epoch)
                        } else if let Some(s) = group.as_classic() {
                            match classic_ops::validate_commit(s, &member_id, group_instance_id.as_deref(), generation_or_epoch) {
                                None => Ok(()),
                                Some(code) => Err(code),
                            }
                        } else {
                            Ok(())
                        };
                        let _ = reply.send(result);
                    }
                    GroupActorMessage::Describe { reply } => {
                        if let Some(state) = group.as_consumer() {
                            let _ = reply.send(build_describe(state));
                        }
                    }

                    // ── classic ──
                    GroupActorMessage::ClassicJoin { req, client_host, reply } => {
                        if let Some(state) = group.as_classic_mut() {
                            match classic_ops::handle_join(state, &req, &client_host) {
                                classic_ops::JoinAction::Immediate(result) => {
                                    let _ = reply.send(result);
                                }
                                classic_ops::JoinAction::Park => {
                                    parked.joiners.insert(req.member_id, reply);
                                }
                                classic_ops::JoinAction::CompleteNow => {
                                    parked.joiners.insert(req.member_id, reply);
                                    complete_classic_rebalance(
                                        state,
                                        &mut parked.joiners,
                                        &mut parked.followers,
                                    );
                                }
                            }
                        } else if group.as_consumer().is_some() {
                            // KIP-848 live migration: a classic member joins (or
                            // rejoins) an upgraded consumer group. Upsert it into the
                            // next-gen state; if its subscription is new/changed the
                            // group is dirty → reconcile + persist the membership
                            // change via the SAME path handle_heartbeat's first-join
                            // uses (run_reconcile → advance_member_epoch →
                            // snapshot_pending_after_change → flush_pending). Then
                            // hand back a server-assigned single-member JoinGroup
                            // result; the real assignment arrives on the next Sync.
                            if classic_join_hosted(
                                &mut group, &config, &*metadata, &*offsets_log,
                                &coordinator, &req, &client_host, reply, chrono_now_ms(),
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                        } else {
                            let _ = reply.send(JoinResult {
                                error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                                member_id: req.member_id,
                                ..JoinResult::default()
                            });
                        }
                    }
                    GroupActorMessage::ClassicSync { req, reply } => {
                        let Some(state) = group.as_classic_mut() else {
                            // KIP-848 live migration: serve a hosted classic member's
                            // SyncGroup off the reconciler's target, translated to a
                            // ConsumerProtocolAssignment blob.
                            if let Some(state) = group.as_consumer_mut() {
                                let result = migration::serve_classic_sync(
                                    state, &req.member_id, &metadata.snapshot(),
                                );
                                let _ = reply.send(result);
                            } else {
                                let _ = reply.send(SyncResult {
                                    error_code: codes::UNKNOWN_MEMBER_ID,
                                    ..SyncResult::default()
                                });
                            }
                            continue;
                        };
                        match classic_ops::handle_sync(state, &req) {
                            classic_ops::SyncAction::Immediate(result) => {
                                let _ = reply.send(result);
                            }
                            classic_ops::SyncAction::Park => {
                                parked.followers.insert(req.member_id, reply);
                            }
                            classic_ops::SyncAction::LeaderInstalled(result) => {
                                let _ = reply.send(result);
                                drain_parked_followers(state, &mut parked.followers);
                            }
                        }
                    }
                    GroupActorMessage::ClassicHeartbeat { req, reply } => {
                        if let Some(state) = group.as_classic_mut() {
                            let code = classic_ops::handle_heartbeat(state, &req);
                            let _ = reply.send(code);
                        } else if let Some(state) = group.as_consumer_mut() {
                            // KIP-848 live migration: a classic member hosted in an
                            // upgraded consumer group is served off the reconciler's
                            // target. `REBALANCE_IN_PROGRESS` while its target differs
                            // from what it last synced, else `NONE`.
                            let code = migration::serve_classic_heartbeat(
                                state, &req.member_id, &metadata.snapshot(),
                            );
                            let _ = reply.send(code);
                        } else {
                            let _ = reply.send(codes::UNKNOWN_MEMBER_ID);
                        }
                    }
                    GroupActorMessage::ClassicLeave { req, version, reply } => {
                        if let Some(state) = group.as_classic_mut() {
                            let before_members: Vec<String> = state.members.keys().cloned().collect();
                            let responses = classic_ops::handle_leave(state, &req, version);
                            let removed: Vec<String> = before_members
                                .into_iter()
                                .filter(|member_id| !state.members.contains_key(member_id))
                                .collect();
                            drain_removed_classic_waiters(
                                &removed,
                                &mut parked.joiners,
                                &mut parked.followers,
                            );
                            let _ = reply.send(responses);
                            // A leave can make every survivor "joined this round"
                            // true; complete the rebalance early if so (mirrors the
                            // old `join_complete.notify_waiters()`).
                            maybe_complete_classic(
                                state,
                                &mut parked.joiners,
                                &mut parked.followers,
                            );
                        } else {
                            let _ = reply.send(Vec::new());
                        }
                    }
                    GroupActorMessage::ClassicInspect { reply } => {
                        if let Some(state) = group.as_classic() {
                            let _ = reply.send(build_classic_view(state));
                        }
                    }
                    GroupActorMessage::InspectAny { reply } => {
                        // Project the LIVE group into a `GroupSnapshot`. A classic
                        // group reuses the exact projection `ClassicInspect` uses;
                        // a consumer group (hosted-classic members included) is
                        // projected with each member's reconciler target translated
                        // to a `ConsumerProtocolAssignment` blob, so assigned
                        // members report a non-empty assignment.
                        if let Some(state) = group.as_classic() {
                            let _ = reply.send(build_classic_view(state).snapshot());
                        } else if let Some(state) = group.as_consumer() {
                            let _ = reply.send(build_consumer_snapshot(state, &metadata.snapshot()));
                        }
                    }

                    // ── committed offsets ──
                    GroupActorMessage::UpdateCommitted { entries, reply } => {
                        for (k, v) in entries {
                            group.committed_offsets.insert(k, v);
                        }
                        let _ = reply.send(());
                    }
                    GroupActorMessage::FetchCommitted { reply } => {
                        let _ = reply.send(group.committed_offsets.clone());
                    }
                    GroupActorMessage::RemoveCommitted { keys, reply } => {
                        for k in &keys {
                            group.committed_offsets.remove(k);
                        }
                        let _ = reply.send(());
                    }

                    // ── lifecycle ──
                    GroupActorMessage::Seed(seed) => {
                        if let Some(state) = group.as_consumer_mut() {
                            apply_seed(state, seed);
                        }
                    }
                    GroupActorMessage::ClassicSeed(seeded) => {
                        group = *seeded;
                    }
                    GroupActorMessage::Shutdown(reply) => {
                        let _ = reply.send(());
                        break;
                    }
                    #[cfg(test)]
                    GroupActorMessage::TestForceConsumerKind => {
                        group = Group::new_consumer(group.group_id.clone());
                    }
                }
            }
            _ = tick.tick() => {
                // Dispatch on the LIVE `group.kind`; the captured spawn-time
                // `kind` is not a reliable discriminator after migration.
                let gid = group.group_id.clone();
                if let Some(state) = group.as_consumer_mut() {
                    if handle_session_tick(state, &config, &*metadata, &*offsets_log, &coordinator)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    // KIP-848 DOWNGRADE: an eviction may have removed the last
                    // native consumer member, leaving only hosted classic
                    // members → flip back to classic in place.
                    if let Err(e) = maybe_downgrade(
                        &mut group, &config, &*metadata, &*offsets_log, &coordinator,
                    ).await {
                        tracing::warn!(
                            group_id = %gid, error = %e,
                            "next-gen actor exiting after tick downgrade log-write failure",
                        );
                        break;
                    }
                } else if let Some(state) = group.as_classic_mut() {
                    let dropped = state.expire_dead_members(Instant::now());
                        if !dropped.is_empty() {
                            tracing::info!(
                                group = %gid, dropped = ?dropped,
                                "expired members; waking joiners",
                            );
                            drain_removed_classic_waiters(
                                &dropped,
                                &mut parked.joiners,
                                &mut parked.followers,
                            );
                            maybe_complete_classic(
                                state,
                                &mut parked.joiners,
                            &mut parked.followers,
                        );
                    }
                }
            }
            () = opt_sleep(deadline) => {
                // Classic rebalance deadline fired: complete with whoever is here.
                if let Some(state) = group.as_classic_mut() {
                    complete_classic_rebalance(
                        state,
                        &mut parked.joiners,
                        &mut parked.followers,
                    );
                }
            }
        }
    }
}

/// The classic rebalance-completion deadline, if a rebalance is open.
fn classic_deadline(group: &Group) -> Option<Instant> {
    group.as_classic().and_then(|s| s.rebalance_deadline)
}

/// A future that resolves at `deadline`, or never if `None`.
async fn opt_sleep(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d.into()).await,
        None => std::future::pending::<()>().await,
    }
}

/// Run the rebalance vote and resolve every parked joiner. Mirrors the old
/// `join_group.rs` block 5 + `notify_waiters()`.
///
/// Also drains any stale parked followers with `REBALANCE_IN_PROGRESS`: they
/// belong to a previous `CompletingRebalance` whose leader was dead and never
/// sent `SyncGroup`. Notifying them lets the client rejoin immediately rather
/// than waiting for the 30-second request timeout.
fn complete_classic_rebalance(
    state: &mut ClassicState,
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    for (_, sender) in followers.drain() {
        let _ = sender.send(SyncResult {
            error_code: codes::REBALANCE_IN_PROGRESS,
            ..SyncResult::default()
        });
    }
    let inconsistent = classic_ops::try_complete(state).is_err();
    if inconsistent {
        state.rebalance_deadline = None;
        state.joined_this_round.clear();
    }
    for (member_id, sender) in joiners.drain() {
        let result = if inconsistent {
            JoinResult {
                error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                member_id: member_id.clone(),
                protocol_type: state.protocol_type.clone(),
                ..JoinResult::default()
            }
        } else {
            classic_ops::build_join_result(state, &member_id)
        };
        let _ = sender.send(result);
    }
}

fn drain_removed_classic_waiters(
    removed: &[String],
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    for member_id in removed {
        if let Some(sender) = joiners.remove(member_id) {
            let _ = sender.send(JoinResult {
                error_code: codes::UNKNOWN_MEMBER_ID,
                member_id: member_id.clone(),
                ..JoinResult::default()
            });
        }
        if let Some(sender) = followers.remove(member_id) {
            let _ = sender.send(SyncResult {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..SyncResult::default()
            });
        }
    }
}

/// Complete the rebalance early iff every still-live member has joined this
/// round and the group has rebalanced before (mirrors `wake_other_joiners`).
fn maybe_complete_classic(
    state: &mut ClassicState,
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    let should = state.generation_id > 0
        && matches!(state.state, ClassicGroupState::PreparingRebalance)
        && state.all_members_joined_this_round();
    if should {
        complete_classic_rebalance(state, joiners, followers);
    }
}

/// Deliver each parked follower its installed assignment (post leader-sync).
fn drain_parked_followers(
    state: &ClassicState,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    let protocol_type = state.protocol_type.clone();
    let protocol_name = state.protocol_name.clone();
    for (member_id, sender) in followers.drain() {
        let result = classic_ops::read_sync_result(
            state,
            &member_id,
            protocol_type.clone(),
            protocol_name.clone(),
        );
        let _ = sender.send(result);
    }
}

/// Build the read-only classic view for the admin / offset-delete handlers.
fn build_classic_view(state: &ClassicState) -> ClassicView {
    ClassicView {
        group_id: state.group_id.clone(),
        state: state.state,
        protocol_type: state.protocol_type.clone(),
        protocol_name: state.protocol_name.clone(),
        generation_id: state.generation_id,
        members: state
            .members
            .values()
            .map(|m| ClassicMemberView {
                member_id: m.member_id.clone(),
                client_id: m.client_id.clone(),
                host: m.host.clone(),
                group_instance_id: m.group_instance_id.clone(),
                protocol_metadata: m.protocol_metadata.clone(),
                assignment: m.assignment.clone(),
            })
            .collect(),
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
    coordinator: &super::GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let evicted = state.evict_expired(Instant::now(), config.session_timeout);
    if evicted.is_empty() {
        return Ok(());
    }
    // `evict_expired` → `remove_member` already set `dirty`. Let the
    // reconciler own the single `bump_epoch` (via `reconcile_if_dirty`); an
    // explicit pre-bump here would double-advance `group_epoch` per eviction.
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
    if let Err(e) = flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
        tracing::warn!(
            group_id = %state.group_id,
            error = %e,
            "next-gen actor exiting after tick log-write failure",
        );
        return Err(e);
    }
    Ok(())
}

/// KIP-848 DOWNGRADE trigger. After a membership change on a consumer-kind
/// group, flip it back to classic in place when no NATIVE consumer member
/// remains, there ARE hosted classic members, and policy allows it. The flip
/// is one atomic batch: tombstone the next-gen k3 + k6 (both group-level) +
/// every member's k5/k7/k8, and write the classic k2 `GroupMetadata`. Returns `Ok(true)` if a flip
/// happened, `Ok(false)` if the conditions weren't met, `Err` on a log-write
/// failure (the caller exits the actor loop).
// TODO(kip-848): confirm exact downgrade trigger boundary against apache/kafka:4.0.0
async fn maybe_downgrade(
    group: &mut Group,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::GroupCoordinator,
) -> Result<bool, crate::error::BrokerError> {
    let Some(state) = group.as_consumer() else {
        return Ok(false);
    };
    if !config.migration_policy.allows_downgrade() {
        return Ok(false);
    }
    if state.members.is_empty() {
        // Fully empty: normal cleanup (a tombstoned next-gen group), not a
        // downgrade — there are no hosted classic members to re-express.
        return Ok(false);
    }
    if state.members.values().any(|m| m.classic.is_none()) {
        // A native consumer member is still present: the group stays next-gen.
        return Ok(false);
    }
    // Spec fidelity: a server-managed consumer group is always re-expressible as
    // a classic group (members become classic members; the server target
    // becomes the seed assignment). Documents the predicate the downgrade rests
    // on; always `true` today.
    if !migration::consumer_is_convertible() {
        return Ok(false);
    }

    let image = metadata.snapshot();

    // The departed native member's membership change shrank the group, but the
    // server `target` was last computed while it was still present, so it does
    // NOT yet cover its partitions. Re-reconcile over the SURVIVING members
    // first — exactly the way the heartbeat path does (`run_reconcile`) — so
    // the remaining classic members absorb the orphaned partitions before we
    // freeze the target into the classic group's seed assignments. Without this
    // the group would land `Stable` with a partition gap and never rebalance,
    // violating the migration spec's "no partition gap" guarantee.
    {
        let state = group
            .as_consumer_mut()
            .expect("consumer-kind verified above");
        state.dirty = true;
        run_reconcile(state, config, metadata);
    }

    // Re-borrow immutably to read the freshly-reconciled target.
    let state = group.as_consumer().expect("consumer-kind verified above");
    let classic = migration::convert_consumer_to_classic(state, &image);
    let pending = migration::downgrade_pending_records(state, &classic);
    let group_id = group.group_id.clone();
    let batch = pending.into_batch(&group_id, chrono_now_ms());
    offsets_log.append(batch).await?;
    coordinator.mark_classic_after_downgrade(&group_id);
    *group.kind_mut() = GroupKind::Classic(classic);
    Ok(true)
}

fn apply_seed(state: &mut GroupState, seed: super::GroupSeed) {
    use super::consumer_state::ClassicMemberFacade;
    state.group_epoch = seed.group_epoch;
    state.target.epoch = seed.target_epoch;
    let group_generation = seed.group_epoch;
    for (mid, meta) in seed.members {
        let mut sub = std::collections::HashSet::new();
        for n in meta.subscribed_topic_names {
            sub.insert(n);
        }
        // KIP-848 migration: a k5 record carrying a `classic` block describes a
        // classic-protocol member hosted in an upgraded group. Rebuild its
        // `ClassicMemberFacade` so the member keeps speaking
        // `JoinGroup`/`SyncGroup`/`Heartbeat` after a coordinator failover; a
        // native consumer-protocol member has `classic == None`.
        let classic = meta.classic.as_ref().map(|c| ClassicMemberFacade {
            generation_id: group_generation,
            supported_protocols: c.supported_protocols.clone(),
            session_timeout: Duration::from_millis(
                u64::try_from(c.session_timeout_ms.max(0)).unwrap_or(30_000),
            ),
            last_synced_assignment: c.last_synced_assignment.clone(),
            awaiting_sync: true,
        });
        state.add_or_update_member(MemberState {
            member_id: mid.clone(),
            instance_id: meta.instance_id,
            rack_id: meta.rack_id,
            client_id: meta.client_id,
            client_host: meta.client_host,
            subscribed_topic_names: sub,
            subscribed_topic_regex: meta.subscribed_topic_regex,
            compiled_regex: None,
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
            classic,
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

/// KIP-848 live migration: serve a classic `JoinGroup` for a member hosted in
/// an upgraded consumer group. Upserts the member into the next-gen state and,
/// if its subscription is new or changed (the group went dirty), reconciles and
/// persists the membership change exactly the way `handle_heartbeat`'s
/// first-join path does — `run_reconcile` → `advance_member_epoch` →
/// `snapshot_pending_after_change` → `flush_pending`. Replies on `reply` with a
/// server-assigned single-member `JoinResult`; the assignment is delivered on
/// the member's next `SyncGroup`. Returns `Err` only on a log-write failure (so
/// the actor exits), after replying with the same failure code the heartbeat
/// path uses.
#[allow(clippy::too_many_arguments)]
async fn classic_join_hosted(
    group: &mut Group,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::GroupCoordinator,
    req: &JoinGroupRequest,
    client_host: &str,
    reply: oneshot::Sender<JoinResult>,
    now_ms: i64,
) -> Result<(), crate::error::BrokerError> {
    // Decode the subscription from the first protocol whose metadata is a valid
    // `ConsumerProtocolSubscription` (mirrors `convert_classic_to_consumer`,
    // which derives topics from a member's selected protocol metadata). The
    // matching protocol's name is echoed back as the result's `protocol_name`.
    let decoded = req.protocols.iter().find_map(|p| {
        migration::decode_consumer_subscription(&p.metadata).map(|sub| (p.name.clone(), sub.topics))
    });
    let (protocol_name, topics) = match decoded {
        Some((name, topics)) => (Some(name), topics.into_iter().collect()),
        None => (
            req.protocols.first().map(|p| p.name.clone()),
            std::collections::HashSet::new(),
        ),
    };
    let protocols: Vec<(String, Bytes)> = req
        .protocols
        .iter()
        .map(|p| (p.name.clone(), p.metadata.clone()))
        .collect();
    let session_timeout =
        Duration::from_millis(u64::try_from(req.session_timeout_ms.max(0)).unwrap_or(30_000));
    let rebalance_timeout =
        Duration::from_millis(u64::try_from(req.rebalance_timeout_ms.max(0)).unwrap_or(60_000));

    let state = group
        .as_consumer_mut()
        .expect("caller verified consumer kind");
    migration::upsert_classic_member(
        state,
        &req.member_id,
        topics,
        protocols,
        String::new(), // client_id is header-level only (matches classic_ops::handle_join)
        client_host.to_string(),
        session_timeout,
        rebalance_timeout,
        req.group_instance_id.clone(),
    );
    if state.dirty {
        run_reconcile(state, config, metadata);
        state.advance_member_epoch(&req.member_id);
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&req.member_id));
        if let Err(e) = flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
            tracing::warn!(
                group_id = %state.group_id, error = %e,
                "next-gen actor exiting after hosted classic-join log-write failure",
            );
            let _ = reply.send(JoinResult {
                error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                member_id: req.member_id.clone(),
                ..JoinResult::default()
            });
            return Err(e);
        }
    }
    let result = migration::build_hosted_classic_join_result(state, &req.member_id, protocol_name);
    let _ = reply.send(result);
    Ok(())
}

/// Outcome of the pure heartbeat decision phase: the response to return to the
/// client and the records the async caller must append to the offsets log.
pub(crate) struct HeartbeatStep {
    pub response: ConsumerGroupHeartbeatResponse,
    pub pending: PendingRecords,
}

/// The pure, synchronous heartbeat decision core: assignor-selection and epoch
/// validation, member upsert / leave, `update_member_state`, `run_reconcile`,
/// `advance_member_epoch`, and response build. Contains no `.await` and performs
/// no I/O — `handle_heartbeat` calls this, then flushes `pending` to the log.
/// Extracted so the reconciliation policy is independently model-checkable.
pub(crate) fn step_heartbeat(
    state: &mut super::consumer_state::GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    client_host: &str,
    now: Instant,
) -> HeartbeatStep {
    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return leave_step(state, config, req);
    }

    // ─── Validate assignor selection ─────────────────────────────
    if req
        .server_assignor
        .as_deref()
        .is_some_and(|name| !config.assignor_enabled(name))
    {
        return HeartbeatStep {
            response: error_resp(codes::UNSUPPORTED_ASSIGNOR, config),
            pending: PendingRecords::default(),
        };
    }

    // ─── First-join path ─────────────────────────────────────────
    // KIP-848 (finalized): the consumer generates its own member UUID and
    // sends it with `member_epoch == 0` on first join. Treat epoch 0 from a
    // member we don't yet know as a first-join, adopting the client-supplied
    // id. An empty `member_id` is tolerated as a fallback (raw-RPC / older
    // callers) by minting a server-side UUID.
    if req.member_epoch == 0 && !state.members.contains_key(&req.member_id) {
        let new_member_id = if req.member_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            req.member_id.clone()
        };
        if let Some(iid) = req.instance_id.as_deref()
            && state
                .current_member_for_instance(iid)
                .and_then(|existing| state.members.get(existing))
                .is_some_and(|m| m.member_epoch != 0)
        {
            return HeartbeatStep {
                response: error_resp(codes::UNRELEASED_INSTANCE_ID, config),
                pending: PendingRecords::default(),
            };
        }
        let m = build_member(&new_member_id, req, client_host, now);
        state.add_or_update_member(m);
        run_reconcile(state, config, metadata);
        state.advance_member_epoch(&new_member_id);
        // Compute the new member's current assignment (grants free target
        // partitions, withholds those still held by others) before responding.
        let owned = reported_owned(req);
        state.reconcile_member(&new_member_id, &owned);
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&new_member_id));
        let response = build_assignment_resp(state, &new_member_id, config);
        return HeartbeatStep { response, pending };
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    let cur_epoch = state
        .members
        .get(&req.member_id)
        .map_or(-2, |m| m.member_epoch);
    if cur_epoch == -2 {
        return HeartbeatStep {
            response: error_resp(codes::UNKNOWN_MEMBER_ID, config),
            pending: PendingRecords::default(),
        };
    }
    if req.member_epoch < cur_epoch {
        return HeartbeatStep {
            response: error_resp(codes::STALE_MEMBER_EPOCH, config),
            pending: PendingRecords::default(),
        };
    }
    if req.member_epoch > cur_epoch {
        return HeartbeatStep {
            response: error_resp(codes::FENCED_MEMBER_EPOCH, config),
            pending: PendingRecords::default(),
        };
    }

    // ─── Steady-state: update last_seen / subscription / owned ───
    let any_change = update_member_state(state, config, metadata, req, now, cur_epoch);
    let pending = if any_change {
        snapshot_pending_after_change(state, std::slice::from_ref(&req.member_id))
    } else {
        PendingRecords::default()
    };
    let response = build_assignment_resp(state, &req.member_id, config);
    HeartbeatStep { response, pending }
}

/// Pure form of the leave path (`member_epoch == -1`): remove the member, bump
/// the group epoch, and build the tombstone + group-epoch records. The async
/// caller flushes the returned `pending`.
fn leave_step(
    state: &mut super::consumer_state::GroupState,
    config: &NextGenConfig,
    req: &ConsumerGroupHeartbeatRequest,
) -> HeartbeatStep {
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
    HeartbeatStep {
        response: base_resp(0, req.member_epoch, config),
        pending,
    }
}

async fn handle_heartbeat(
    state: &mut super::consumer_state::GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::GroupCoordinator,
    req: &ConsumerGroupHeartbeatRequest,
    client_host: &str,
) -> Result<ConsumerGroupHeartbeatResponse, crate::error::BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();
    let step = step_heartbeat(state, config, metadata, req, client_host, now);
    flush_pending(state, step.pending, offsets_log, coordinator, now_ms).await?;
    Ok(step.response)
}

/// The partitions a member reports owning in its heartbeat. Absent
/// `topic_partitions` means "unchanged" — the caller substitutes the member's
/// current assignment so a keepalive can still pick up newly-freed partitions.
fn reported_owned(req: &ConsumerGroupHeartbeatRequest) -> HashMap<Uuid, Vec<i32>> {
    req.topic_partitions
        .as_ref()
        .map(|tp| {
            tp.iter()
                .map(|t| (t.topic_id, t.partitions.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Apply steady-state member updates and run reconciliation.
/// Returns `true` if any change occurred that requires a log write.
fn update_member_state(
    state: &mut super::consumer_state::GroupState,
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
            // Recompile the cached regex only here — the one place the
            // pattern actually changes (the client re-sends the same regex
            // every heartbeat while the subscription is stable).
            m.set_regex(req.subscribed_topic_regex.clone());
            state.dirty = true;
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
    // Reconcile this member's current assignment against the (possibly new)
    // target and what it reports owning: grant free target partitions, mark
    // revocations, and withhold partitions still held by another member. A
    // heartbeat without `topic_partitions` is a keepalive — reuse the member's
    // current assignment as its owned set so it can still pick up freed partitions.
    let owned = if req.topic_partitions.is_some() {
        reported_owned(req)
    } else {
        state
            .members
            .get(&req.member_id)
            .map(|m| m.assigned_partitions.clone())
            .unwrap_or_default()
    };
    let assignment_changed = state.reconcile_member(&req.member_id, &owned);
    subscription_changed || was_dirty || epoch_advanced || assignment_changed
}

fn run_reconcile(state: &mut GroupState, config: &NextGenConfig, metadata: &dyn MetadataProvider) {
    // `metadata.snapshot()` rebuilds HashMaps over every cluster topic /
    // partition — far too expensive to run on a steady-state no-op
    // heartbeat. `reconcile_if_dirty` early-returns when `!dirty`, so gate
    // the snapshot on the same condition: only pay for it when we will
    // actually recompute. Behavior when dirty is identical to before.
    if !state.dirty {
        return;
    }
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
        compiled_regex: None,
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
        classic: None,
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
    // KIP-848: the `assignment` field carries the member's *current* assignment —
    // the partitions it may own right now — NOT the raw target. `reconcile_member`
    // computes this each heartbeat, withholding any target partition still held by
    // another member until that member revokes it. Returning the current
    // assignment (`assigned_partitions`) rather than the target is what prevents
    // two members from owning the same partition during a handoff.
    let target_partitions = m.assigned_partitions.clone();
    let assignment = Some(RespAssignment {
        topic_partitions: target_partitions
            .iter()
            .map(
                |(tid, parts)| crabka_protocol::owned::common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions {
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
                is_classic: m.is_classic(),
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// PendingRecords — collects mutations for one group-state transition and
// encodes them as a single RecordBatch ready for OffsetsLog::append.
// ---------------------------------------------------------------------------

use crabka_protocol::records::{Record, RecordBatch};

use super::persistence_next_gen::{
    CurrentMemberAssignmentValue, GroupMetadataValue, MemberMetadataValue, NextGenKey,
    TargetAssignmentMemberValue, TargetAssignmentMetadataValue, encode_key,
};

#[derive(Debug, Default)]
pub(crate) struct PendingRecords {
    pub group_metadata: Option<GroupMetadataValue>,
    /// `Some(value)` writes the record; `None` writes a tombstone (null value).
    pub member_metadata: Vec<(String, Option<MemberMetadataValue>)>,
    pub target_metadata: Option<TargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<TargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<CurrentMemberAssignmentValue>)>,
    /// When set, the batch also tombstones the classic k2 `GroupMetadata` record
    /// for this group (used by an upgrade flip).
    pub classic_group_metadata_tombstone: bool,
    /// Tombstone the next-gen k3 `GroupMetadata` (downgrade flip).
    pub next_gen_group_metadata_tombstone: bool,
    /// Tombstone the next-gen k6 `TargetAssignmentMetadata` (downgrade flip).
    pub next_gen_target_metadata_tombstone: bool,
    /// Write the classic k2 `GroupMetadata` value (downgrade flip).
    pub classic_group_metadata:
        Option<crate::coordinator::unified::persistence::GroupMetadataValue>,
}

impl PendingRecords {
    pub fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
            && !self.classic_group_metadata_tombstone
            && !self.next_gen_group_metadata_tombstone
            && !self.next_gen_target_metadata_tombstone
            && self.classic_group_metadata.is_none()
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
        if self.classic_group_metadata_tombstone {
            push(
                crate::coordinator::unified::persistence::encode_key(
                    &crate::coordinator::unified::persistence::Key::GroupMetadata {
                        group_id: group_id.into(),
                    },
                ),
                None,
            );
        }
        if self.next_gen_group_metadata_tombstone {
            push(
                encode_key(&NextGenKey::GroupMetadata {
                    group_id: group_id.into(),
                }),
                None,
            );
        }
        if self.next_gen_target_metadata_tombstone {
            push(
                encode_key(&NextGenKey::TargetAssignmentMetadata {
                    group_id: group_id.into(),
                }),
                None,
            );
        }
        if let Some(v) = self.classic_group_metadata {
            push(
                crate::coordinator::unified::persistence::encode_key(
                    &crate::coordinator::unified::persistence::Key::GroupMetadata {
                        group_id: group_id.into(),
                    },
                ),
                Some(v.encode_value()),
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
}

/// Snapshot a `GroupState` into a `GroupSeed` suitable for restoring a
/// Map a member's in-memory classic facade (if any) into the persisted k5
/// `ClassicMemberMetadata` sub-block. Single source of truth for both the cache
/// snapshot (`snapshot_seed`) and the log-write path (`snapshot_pending_after_change`)
/// so the two cannot drift.
fn classic_member_metadata(
    m: &super::consumer_state::MemberState,
) -> Option<super::persistence_next_gen::ClassicMemberMetadata> {
    m.classic
        .as_ref()
        .map(|f| super::persistence_next_gen::ClassicMemberMetadata {
            session_timeout_ms: i32::try_from(f.session_timeout.as_millis()).unwrap_or(30_000),
            supported_protocols: f.supported_protocols.clone(),
            last_synced_assignment: f.last_synced_assignment.clone(),
        })
}

/// freshly-respawned actor. Mirrors what bootstrap replay would produce.
// PERF: this deep-clones EVERY member's subscriptions/assignments into a
// fresh `GroupSeed` on every persisted heartbeat, even when only one member
// changed. An incremental cache update — applying just the affected-member
// delta computed by `snapshot_pending_after_change` — would avoid the
// full-group re-clone, but `GroupCoordinator::update_cache` (mod.rs) only
// exposes a whole-seed replace and `GroupSeed` is consumed wholesale by
// replay/scrub. Adding a delta-apply API would ripple into mod.rs, which is
// out of scope here; left as the remaining full-clone for a follow-up.
pub(crate) fn snapshot_seed(state: &super::consumer_state::GroupState) -> super::GroupSeed {
    use crate::coordinator::unified::persistence_next_gen as p;
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
            classic: classic_member_metadata(m),
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
    state: &super::consumer_state::GroupState,
    affected_members: &[String],
) -> PendingRecords {
    use crate::coordinator::unified::persistence_next_gen as p;
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
                    classic: classic_member_metadata(m),
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

/// Build a `PendingRecords` set describing the WHOLE consumer group: the group
/// epoch, the target epoch (if non-zero), and every member's k5 member-metadata
/// (facade included), k8 current-assignment, and k7 target (if present). Used by
/// the upgrade flip to atomically write the full converted group in one batch.
pub(crate) fn full_pending_records(state: &super::consumer_state::GroupState) -> PendingRecords {
    let all_member_ids: Vec<String> = state.members.keys().cloned().collect();
    snapshot_pending_after_change(state, &all_member_ids)
}

/// Build a wire-faithful classic k2 `GroupMetadataValue` from a downgraded
/// [`super::classic_state::Group`]. Every classic member is persisted with its
/// `subscription` (the selected `protocol_metadata`) and `assignment` (the seed
/// the downgrade computed from the next-gen target), so bootstrap replay
/// reconstructs the classic group with its members and their assignments intact
/// (see `apply_group_metadata` in `coordinator::bootstrap`). Used by the
/// downgrade flip.
pub(crate) fn classic_group_metadata_record(
    state: &super::classic_state::Group,
) -> crate::coordinator::unified::persistence::GroupMetadataValue {
    use crate::coordinator::unified::persistence::{GroupMetadataValue, MemberMetadata};
    let members = state
        .members
        .values()
        .map(|m| MemberMetadata {
            member_id: m.member_id.clone(),
            group_instance_id: m.group_instance_id.clone(),
            client_id: m.client_id.clone(),
            client_host: m.host.clone(),
            rebalance_timeout_ms: i32::try_from(m.rebalance_timeout.as_millis()).unwrap_or(60_000),
            session_timeout_ms: i32::try_from(m.session_timeout.as_millis()).unwrap_or(30_000),
            subscription: m.protocol_metadata.clone(),
            assignment: m.assignment.clone().unwrap_or_default(),
        })
        .collect();
    GroupMetadataValue {
        protocol_type: state
            .protocol_type
            .clone()
            .unwrap_or_else(|| "consumer".into()),
        generation: state.generation_id,
        protocol_name: state.protocol_name.clone(),
        leader: state.leader_id.clone(),
        current_state_timestamp_ms: chrono_now_ms(),
        members,
    }
}

async fn flush_pending(
    state: &super::consumer_state::GroupState,
    pending: PendingRecords,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::GroupCoordinator,
    now_ms: i64,
) -> Result<(), crate::error::BrokerError> {
    if pending.is_empty() {
        return Ok(());
    }
    // Consume `pending` by value: `into_batch` moves the per-member
    // record vectors straight into the batch instead of deep-cloning them.
    let batch = pending.into_batch(&state.group_id, now_ms);
    offsets_log.append(batch).await?;
    coordinator.update_cache(&state.group_id, snapshot_seed(state));
    Ok(())
}

/// Validate an offset commit — regular or transactional — against the group's
/// membership and generation (classic) or member epoch (KIP-848 next-gen).
/// Returns `Some(error_code)` if the commit must be rejected, `None` if it may
/// proceed.
///
/// Shared by `OffsetCommit` and `TxnOffsetCommit` so the two paths fence
/// identically — KIP-447 requires transactional offset fencing to be
/// "consistent with normal offset fencing". For a simple consumer (empty
/// `member_id`, no `group_instance_id`) the classic path no-ops, so a producer
/// that supplies no group metadata is never fenced.
///
/// Dispatch happens inside the actor on the LIVE `group.kind` (the single
/// `ValidateCommit` message), not on the spawn-time `handle.kind` hint — a
/// KIP-848 migration may have flipped the protocol in place after spawn.
pub(crate) async fn validate_group_commit(
    handle: &GroupActorHandle,
    member_id: &str,
    generation_or_epoch: i32,
    group_instance_id: Option<&str>,
) -> Option<i16> {
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::ValidateCommit {
            member_id: member_id.to_string(),
            group_instance_id: group_instance_id.map(str::to_string),
            generation_or_epoch,
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

#[cfg(test)]
#[path = "reconciler_model.rs"]
mod reconciler_model;

/// Compositional model: the KIP-848 reconciliation engine composed with a
/// modeled offset-commit fencing + fetch layer (consumer delivery correctness
/// through rebalances).
#[cfg(test)]
#[path = "consumer_group_composition_model.rs"]
mod consumer_group_composition_model;

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    use crate::coordinator::unified::GroupCoordinator;
    use crate::coordinator::unified::config::NextGenConfig;
    use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;
    use crate::coordinator::unified::reconciler::ReconcileInput;
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

    fn make_coordinator() -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            empty_metadata(),
            log.clone(),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        (coord, log)
    }

    #[test]
    fn inconsistent_classic_completion_clears_deadline() {
        use super::super::classic_state::{Group as ClassicState, Member};

        let mut state = ClassicState::new("g");
        state.protocol_type = Some("consumer".into());
        state.add_member(Member::new(
            "m1",
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), Bytes::new())],
        ));
        state.add_member(Member::new(
            "m2",
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("cooperative-sticky".into(), Bytes::new())],
        ));
        state.rebalance_deadline = Some(
            Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now),
        );
        assert!(state.state == ClassicGroupState::PreparingRebalance);

        let (tx1, _rx1) = tokio::sync::oneshot::channel();
        let (tx2, _rx2) = tokio::sync::oneshot::channel();
        let mut joiners = HashMap::from([("m1".to_string(), tx1), ("m2".to_string(), tx2)]);
        let mut followers = HashMap::new();

        complete_classic_rebalance(&mut state, &mut joiners, &mut followers);

        assert!(
            state.rebalance_deadline.is_none(),
            "a failed protocol vote must not leave an already-fired deadline armed"
        );
    }

    #[tokio::test]
    async fn removed_classic_members_drain_parked_waiters() {
        let (join_tx, join_rx) = tokio::sync::oneshot::channel();
        let (sync_tx, sync_rx) = tokio::sync::oneshot::channel();
        let mut joiners = HashMap::from([("m1".to_string(), join_tx)]);
        let mut followers = HashMap::from([("m1".to_string(), sync_tx)]);

        drain_removed_classic_waiters(&["m1".to_string()], &mut joiners, &mut followers);

        assert!(joiners.is_empty());
        assert!(followers.is_empty());
        assert!(join_rx.await.unwrap().error_code == codes::UNKNOWN_MEMBER_ID);
        assert!(sync_rx.await.unwrap().error_code == codes::UNKNOWN_MEMBER_ID);
    }

    /// A coordinator whose metadata image holds one topic `t` with `partitions`
    /// partitions, so the reconciler can resolve a `t` subscription to real
    /// topic-id/partitions and compute a target assignment.
    fn make_coordinator_with_topic(
        topic: &str,
        partitions: i32,
    ) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
        make_coordinator_with_topic_policy(
            topic,
            partitions,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::default(),
        )
    }

    /// As [`make_coordinator_with_topic`], but with an explicit migration
    /// policy. Hosted-classic tests pin `Upgrade` so the native
    /// member's leave in `seed_and_upgrade` does NOT trigger a downgrade back
    /// to classic (which would strand them on the wrong RPC path); the
    /// downgrade trigger itself is exercised with `Bidirectional`/`Downgrade`.
    fn make_coordinator_with_topic_policy(
        topic: &str,
        partitions: i32,
        policy: crate::coordinator::unified::config::ConsumerGroupMigrationPolicy,
    ) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
        let topic_id = Uuid([7; 16]);
        let input = ReconcileInput {
            topic_id_by_name: [(topic.to_string(), topic_id)].into(),
            partitions_per_topic: [(topic_id, partitions)].into(),
            ..Default::default()
        };
        let metadata: Arc<dyn MetadataProvider> = Arc::new(StaticMetadata { input });
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig {
                migration_policy: policy,
                ..NextGenConfig::default()
            },
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            metadata,
            log.clone(),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        (coord, log)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_join_emits_one_batch() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_consumer("g");
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
        assert!(resp.error_code == 0);
        let batches = log.batches().await;
        assert!(
            batches.len() == 1,
            "first join should write exactly one batch"
        );
        // Minimum: k3 (group metadata) + k5 (member metadata) + k8 (current).
        assert!(batches[0].records.len() >= 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_join_adopts_client_member_id() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_consumer("g");
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "client-uuid-1".into(),
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
        assert!(resp.error_code == 0, "client-id first-join must succeed");
        assert!(
            resp.member_id.as_deref() == Some("client-uuid-1"),
            "response must echo the client-supplied member id"
        );
        assert!(resp.member_epoch >= 1, "epoch must advance off 0 on join");
        // The client-id first-join takes the same flush path as the empty-id case
        // and persists exactly one batch.
        assert!(
            log.batches().await.len() == 1,
            "client-id first join writes exactly one batch"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn known_member_id_epoch_zero_is_stale() {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_consumer("g");
        // First join with a client id, epoch 0 → succeeds, epoch advances.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "client-uuid-2".into(),
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
        assert!(rx.await.unwrap().error_code == 0);

        // Same id re-sending epoch 0 is now a known member at a higher epoch →
        // stale, not a re-join.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "client-uuid-2".into(),
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
        assert!(rx.await.unwrap().error_code == codes::STALE_MEMBER_EPOCH);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unchanged_heartbeat_emits_no_batch() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_consumer("g");
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
        assert!(
            batches_after_steady == batches_after_join,
            "steady-state heartbeat should not write"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leave_emits_tombstone_batch() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_consumer("g");
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
        assert!(batches.len() == pre_leave + 1);
        let leave_batch = &batches[batches.len() - 1];
        assert!(
            leave_batch.records.iter().any(|r| r.value.is_none()),
            "leave batch must contain at least one tombstone"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actor_exits_on_append_error() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_consumer("g");
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
        assert!(resp.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);

        // Wait briefly for the actor to drain.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            handle.tx.is_closed(),
            "actor mpsc should be closed after exit"
        );

        // get_or_create should respawn a fresh actor.
        let fresh = coord.get_or_create_consumer("g");
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
                    classic: None,
                }),
            )],
            target_metadata: Some(TargetAssignmentMetadataValue {
                assignment_epoch: 1,
            }),
            ..Default::default()
        };
        let batch = p.into_batch("g", 0);
        assert!(batch.records.len() == 3);
        let deltas: Vec<i32> = batch.records.iter().map(|r| r.offset_delta).collect();
        assert!(deltas == vec![0, 1, 2]);
        assert!(batch.last_offset_delta == 2);
    }

    #[test]
    fn pending_records_tombstone_omits_value() {
        let p = PendingRecords {
            member_metadata: vec![("m1".into(), None)],
            ..Default::default()
        };
        let batch = p.into_batch("g", 0);
        assert!(batch.records.len() == 1);
        assert!(batch.records[0].value.is_none());
    }

    /// Regression for the epoch double-bump: a single session-timeout
    /// eviction must advance `group_epoch` by exactly 1. The fix removed the
    /// explicit `state.bump_epoch()` in `handle_session_tick`, leaving the
    /// reconciler (`reconcile_if_dirty`) as the sole bump.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_eviction_advances_epoch_by_one() {
        use crate::coordinator::unified::consumer_state::GroupState;

        let (coord, log) = make_coordinator();
        // Tiny session timeout so a member whose `last_seen` is a few ms in
        // the past counts as expired — avoids subtracting a large duration
        // from `Instant::now()`, which `checked_sub` rejects on low-uptime
        // CI runners (e.g. a freshly-booted Windows agent).
        let config = NextGenConfig {
            session_timeout: Duration::from_millis(1),
            ..NextGenConfig::default()
        };
        let metadata = empty_metadata();

        // Seed a member and reconcile once so the join settles into a clean
        // (non-dirty) baseline epoch.
        let mut state = GroupState::new("g");
        let mut m = build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest {
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            "h",
            Instant::now(),
        );
        // Force the member to look session-expired. 50ms is always within
        // `Instant`'s range (no underflow on any host) yet far exceeds the
        // 1ms `session_timeout` set above.
        m.last_seen = Instant::now()
            .checked_sub(Duration::from_millis(50))
            .expect("50ms is always within Instant range");
        state.add_or_update_member(m);
        run_reconcile(&mut state, &config, &*metadata);
        assert!(!state.dirty, "baseline must be clean before eviction");
        let epoch_before = state.group_epoch;

        // One eviction tick.
        handle_session_tick(&mut state, &config, &*metadata, &*log, &coord)
            .await
            .expect("tick should succeed");

        assert!(
            state.members.is_empty(),
            "expired member must have been evicted"
        );
        assert!(
            state.group_epoch == epoch_before + 1,
            "a single eviction must advance the group epoch by exactly 1"
        );
    }

    // ---------------------------------------------------------------------
    // custom assignor registry
    // ---------------------------------------------------------------------

    use crate::coordinator::unified::assignor::{
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
        let mut state = crate::coordinator::unified::consumer_state::GroupState::new("g");
        let mut m = build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest::default(),
            "h",
            Instant::now(),
        );
        m.server_assignor = Some("ghost".into());
        state.members.insert("m1".into(), m);

        let picked = pick_assignor(&state, &config);
        assert!(picked.name() == "uniform");
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
        let coord = Arc::new(GroupCoordinator::new(
            config,
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            empty_metadata(),
            log,
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        let handle = coord.get_or_create_consumer("g");

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
        assert!(resp.error_code == 0);
        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "custom assignor must be invoked at least once",
        );
    }

    // ── classic actor arms + coordinator admin surface ──────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_admin_surface_and_immediate_join() {
        use crabka_protocol::owned::join_group_request::JoinGroupRequest;
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_classic("g");

        // Empty member_id → immediate MEMBER_ID_REQUIRED (no member added).
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicJoin {
                req: JoinGroupRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    protocol_type: "consumer".into(),
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let r = rx.await.unwrap();
        assert!(r.error_code == codes::MEMBER_ID_REQUIRED);

        // ClassicInspect → empty view.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicInspect { reply: tx })
            .await
            .unwrap();
        let view = rx.await.unwrap();
        assert!(view.group_id == "g" && view.members.is_empty());

        // Admin surface lists/describes the classic group, then deletes it (empty).
        let listed = coord.list_groups().await;
        assert!(listed.iter().any(|s| s.group_id == "g"));
        assert!(coord.describe_group("g").await.is_some());
        assert!(coord.delete_group("g").await.is_ok());
        assert!(coord.describe_group("g").await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_offset_validate_heartbeat_arms() {
        use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;

        use super::super::classic_state::OffsetEntry;
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_classic("g");

        // UpdateCommitted then FetchCommitted round-trips on the kind-agnostic Group.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::UpdateCommitted {
                entries: vec![(
                    ("t".to_string(), 0),
                    OffsetEntry {
                        offset: 42,
                        leader_epoch: 1,
                        metadata: String::new(),
                        commit_timestamp_ms: 0,
                    },
                )],
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchCommitted { reply: tx })
            .await
            .unwrap();
        let committed = rx.await.unwrap();
        assert!(committed.get(&("t".to_string(), 0)).unwrap().offset == 42);

        // Classic offset-commit validate: a simple consumer (no member/instance)
        // is allowed. `ValidateCommit` dispatches on the live (classic) kind.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ValidateCommit {
                member_id: String::new(),
                group_instance_id: None,
                generation_or_epoch: -1,
                reply: tx,
            })
            .await
            .unwrap();
        assert!(rx.await.unwrap() == Ok(()));

        // Classic Heartbeat for an unknown member on an empty group.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicHeartbeat {
                req: HeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "ghost".into(),
                    generation_id: 0,
                    ..Default::default()
                },
                reply: tx,
            })
            .await
            .unwrap();
        assert!(rx.await.unwrap() == codes::UNKNOWN_MEMBER_ID);

        // RemoveCommitted clears the entry.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::RemoveCommitted {
                keys: vec![("t".to_string(), 0)],
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchCommitted { reply: tx })
            .await
            .unwrap();
        assert!(rx.await.unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_seed_hydrates_group_and_blocks_delete_when_nonempty() {
        use std::time::Duration;

        use super::super::classic_state::{Group as ClassicState, Member, OffsetEntry};
        use super::super::group::{Group, GroupKind};
        let (coord, _log) = make_coordinator();

        let mut cs = ClassicState::new("g");
        cs.add_member(Member::new(
            "m1",
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), bytes::Bytes::new())],
        ));
        let group = Box::new(Group {
            group_id: "g".into(),
            kind: GroupKind::Classic(cs),
            committed_offsets: [(
                ("t".to_string(), 0),
                OffsetEntry {
                    offset: 7,
                    leader_epoch: 0,
                    metadata: String::new(),
                    commit_timestamp_ms: 0,
                },
            )]
            .into(),
        });
        coord.seed_classic("g", group);

        // Seeded committed offsets and member are visible.
        let handle = coord.find("g").expect("seeded actor");
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchCommitted { reply: tx })
            .await
            .unwrap();
        assert!(rx.await.unwrap().get(&("t".to_string(), 0)).unwrap().offset == 7);
        // Non-empty group cannot be deleted.
        assert!(
            coord.delete_group("g").await == Err(crate::coordinator::DeleteGroupError::NonEmpty)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_protocol_get_or_create_returns_the_one_actor() {
        // KIP-848 live migration: the registry no longer pins a group to its
        // spawn kind. Both getters return the SAME actor for an id; the per-group
        // kind lock now lives in the actor's message arms, not the registry.
        let (coord, _log) = make_coordinator();
        // Consumer owns "c" → a classic get-or-create returns that same actor.
        let c_consumer = coord.get_or_create_consumer("c");
        let c_classic = coord.get_or_create_classic("c");
        assert!(Arc::ptr_eq(&c_consumer, &c_classic));
        // Classic owns "k" → a consumer get-or-create returns that same actor.
        let k_classic = coord.get_or_create_classic("k");
        let k_consumer = coord.get_or_create_consumer("k");
        assert!(Arc::ptr_eq(&k_classic, &k_consumer));
    }

    /// KIP-848 live migration: the tick must dispatch on the LIVE `group.kind`,
    /// not the captured spawn-time kind. Spawn a classic actor, flip it to a
    /// consumer group in place, and let a tick fire — the actor must keep
    /// running rather than panic on a kind-mismatched `expect(...)`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actor_tick_does_not_panic_after_in_place_flip() {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_group("g", GroupKindTag::Classic);
        handle
            .tx
            .send(GroupActorMessage::TestForceConsumerKind)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        assert!(!handle.tx.is_closed());
    }

    /// KIP-848 upgrade trigger: a `ConsumerGroupHeartbeat` for a *classic* group
    /// (default `bidirectional` policy) converts it in place to a next-gen
    /// consumer group hosting the classic member, atomically tombstoning the
    /// classic k2 `GroupMetadata` and writing the full next-gen record set.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn consumer_heartbeat_upgrades_a_classic_group() {
        use super::super::classic_state::{Group as ClassicState, Member};
        use super::super::group::{Group, GroupKind};

        let (coord, log) = make_coordinator_with_topic("t", 2);

        // Seed a classic group with one classic consumer member subscribed to
        // "t". Seeding (vs a JoinGroup round-trip) keeps the test deterministic
        // and timing-free; `classic_is_convertible` only inspects protocol_type
        // and each member's protocol_metadata, both set here.
        let mut cs = ClassicState::new("g");
        cs.protocol_type = Some("consumer".into());
        cs.generation_id = 1;
        cs.add_member(Member::new(
            "m-classic",
            "client",
            "127.0.0.1",
            std::time::Duration::from_secs(30),
            std::time::Duration::from_mins(1),
            vec![("range".into(), subscription_blob(&["t"]))],
        ));
        let group = Box::new(Group {
            group_id: "g".into(),
            kind: GroupKind::Classic(cs),
            committed_offsets: HashMap::new(),
        });
        coord.seed_classic("g", group);
        let handle = coord.find("g").expect("seeded classic actor");

        // A native consumer-protocol heartbeat for the same group → upgrade.
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
        assert!(resp.error_code == codes::NONE);

        // Describe now reports 2 members: the hosted classic member and the new
        // native consumer member.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        let describe = rx.await.unwrap();
        assert!(describe.members.len() == 2);
        assert!(
            describe.members.iter().any(|m| m.is_classic),
            "the hosted classic member must survive the upgrade"
        );
        assert!(
            describe.members.iter().any(|m| !m.is_classic),
            "the new native consumer member must be present"
        );

        // The upgrade batch tombstoned the classic k2 GroupMetadata record.
        assert!(log.has_classic_group_metadata_tombstone("g").await);
    }

    // ── KIP-848: serving hosted classic members off the reconciler ─────

    /// A real classic consumer client's `JoinGroup` protocol metadata: a
    /// `ConsumerProtocolSubscription` with the leading version-negotiation
    /// prefix.
    fn subscription_blob(topics: &[&str]) -> Bytes {
        use bytes::{BufMut, BytesMut};
        use crabka_protocol::Encode;
        use crabka_protocol::owned::consumer_protocol_subscription::ConsumerProtocolSubscription;
        let sub = ConsumerProtocolSubscription {
            topics: topics.iter().map(|s| (*s).to_string()).collect(),
            ..Default::default()
        };
        let mut out = BytesMut::new();
        out.put_i16(0);
        sub.encode(&mut out, 0).unwrap();
        out.freeze()
    }

    /// Decode a `SyncGroup` assignment blob (version prefix + body) back into a
    /// `ConsumerProtocolAssignment`.
    fn decode_assignment(
        blob: &Bytes,
    ) -> crabka_protocol::owned::consumer_protocol_assignment::ConsumerProtocolAssignment {
        use bytes::Buf;
        use crabka_protocol::Decode;
        use crabka_protocol::owned::consumer_protocol_assignment::ConsumerProtocolAssignment;
        let mut cur = &blob[..];
        let version = cur.get_i16();
        ConsumerProtocolAssignment::decode(&mut cur, version).expect("assignment decodes")
    }

    /// Seed a classic consumer group with member `m-classic` subscribed to
    /// `topic`, then upgrade it in place via a native consumer heartbeat. After
    /// this returns, the group is consumer-kind and `m-classic` has a target.
    async fn seed_and_upgrade(coord: &Arc<GroupCoordinator>, topic: &str) -> Arc<GroupActorHandle> {
        use super::super::classic_state::{Group as ClassicState, Member};
        use super::super::group::{Group, GroupKind};

        let mut cs = ClassicState::new("g");
        cs.protocol_type = Some("consumer".into());
        cs.generation_id = 1;
        cs.add_member(Member::new(
            "m-classic",
            "client",
            "127.0.0.1",
            std::time::Duration::from_secs(30),
            std::time::Duration::from_mins(1),
            vec![("range".into(), subscription_blob(&[topic]))],
        ));
        let group = Box::new(Group {
            group_id: "g".into(),
            kind: GroupKind::Classic(cs),
            committed_offsets: HashMap::new(),
        });
        coord.seed_classic("g", group);
        let handle = coord.find("g").expect("seeded classic actor");

        // Native consumer heartbeat triggers the in-place upgrade and the
        // reconcile that gives m-classic a target.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec![topic.into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let resp = rx.await.unwrap();
        assert!(resp.error_code == codes::NONE);

        // The native heartbeat minted a transient consumer member to drive the
        // upgrade. Have it leave so the group hosts only the classic member(s)
        // under test — otherwise it would claim a share of the partitions.
        let native_id = resp.member_id.expect("native member id");
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: native_id,
                    member_epoch: -1,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        assert!(rx.await.unwrap().error_code == codes::NONE);
        handle
    }

    async fn classic_join(handle: &GroupActorHandle, member_id: &str, topic: &str) -> JoinResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicJoin {
                req: JoinGroupRequest {
                    group_id: "g".into(),
                    member_id: member_id.into(),
                    protocol_type: "consumer".into(),
                    protocols: vec![
                        crabka_protocol::owned::join_group_request::JoinGroupRequestProtocol {
                            name: "range".into(),
                            metadata: subscription_blob(&[topic]),
                            ..Default::default()
                        },
                    ],
                    session_timeout_ms: 30_000,
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: "127.0.0.1".into(),
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    async fn classic_sync(
        handle: &GroupActorHandle,
        member_id: &str,
        generation: i32,
    ) -> SyncResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicSync {
                req: SyncGroupRequest {
                    group_id: "g".into(),
                    member_id: member_id.into(),
                    generation_id: generation,
                    ..Default::default()
                },
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    async fn classic_heartbeat(handle: &GroupActorHandle, member_id: &str) -> i16 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicHeartbeat {
                req: HeartbeatRequest {
                    group_id: "g".into(),
                    member_id: member_id.into(),
                    generation_id: 0,
                    ..Default::default()
                },
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hosted_classic_member_syncs_translated_assignment() {
        // `Upgrade` policy: the native member's leave in `seed_and_upgrade`
        // must NOT downgrade the group back to classic — this test exercises
        // serving a hosted classic member from the consumer-kind reconciler.
        let (coord, _log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        let handle = seed_and_upgrade(&coord, "t").await;

        // 1. Heartbeat: the upgrade gave m-classic a target that differs from
        //    its (empty) last-synced assignment → it owes a re-sync.
        assert!(
            classic_heartbeat(&handle, "m-classic").await == codes::REBALANCE_IN_PROGRESS,
            "post-upgrade heartbeat must signal a re-sync"
        );

        // 2. JoinGroup (rejoin of the existing member, unchanged subscription):
        //    success, server-assigned single-member view at group_epoch, self leader.
        let join = classic_join(&handle, "m-classic", "t").await;
        assert!(join.error_code == codes::NONE);
        assert!(join.leader == "m-classic");
        assert!(join.member_id == "m-classic");
        // Generation equals the group epoch (read it back from Describe).
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        let describe = rx.await.unwrap();
        assert!(join.generation_id == describe.group_epoch);

        // 3. SyncGroup: returns the translated target assignment for "t".
        let sync = classic_sync(&handle, "m-classic", join.generation_id).await;
        assert!(sync.error_code == codes::NONE);
        assert!(sync.protocol_type.as_deref() == Some("consumer"));
        let asn = decode_assignment(&sync.assignment);
        let t_assign = asn
            .assigned_partitions
            .iter()
            .find(|tp| tp.topic == "t")
            .expect("assignment contains topic t");
        assert!(
            !t_assign.partitions.is_empty(),
            "m-classic must own partitions of t"
        );

        // 4. Heartbeat again: now in sync → NONE.
        assert!(
            classic_heartbeat(&handle, "m-classic").await == codes::NONE,
            "after sync the member is in sync → NONE"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn new_classic_member_joins_upgraded_group_and_gets_assignment() {
        // `Upgrade` policy: keep the group consumer-kind after the native
        // member leaves in `seed_and_upgrade` (see the note above).
        let (coord, _log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        let handle = seed_and_upgrade(&coord, "t").await;

        // First bring m-classic fully in sync so it holds a stable assignment.
        let join_c = classic_join(&handle, "m-classic", "t").await;
        let _ = classic_sync(&handle, "m-classic", join_c.generation_id).await;

        // A brand-new classic member m2 joins the already-upgraded group.
        let join2 = classic_join(&handle, "m2", "t").await;
        assert!(join2.error_code == codes::NONE);
        assert!(join2.leader == "m2");

        // Both members re-sync at the (new) group epoch to pick up the
        // rebalanced two-way split.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        let epoch = rx.await.unwrap().group_epoch;
        let sync_c = classic_sync(&handle, "m-classic", epoch).await;
        let sync2 = classic_sync(&handle, "m2", epoch).await;
        assert!(sync_c.error_code == codes::NONE);
        assert!(sync2.error_code == codes::NONE);

        // Collect each member's partitions of "t".
        let parts = |s: &SyncResult| -> Vec<i32> {
            decode_assignment(&s.assignment)
                .assigned_partitions
                .iter()
                .find(|tp| tp.topic == "t")
                .map(|tp| tp.partitions.clone())
                .unwrap_or_default()
        };
        let p_c = parts(&sync_c);
        let p_2 = parts(&sync2);
        assert!(!p_2.is_empty(), "the new member must receive an assignment");

        // Disjoint, and together cover {0, 1}.
        let set_c: std::collections::HashSet<i32> = p_c.iter().copied().collect();
        let set_2: std::collections::HashSet<i32> = p_2.iter().copied().collect();
        assert!(
            set_c.is_disjoint(&set_2),
            "the two members must hold disjoint partitions"
        );
        let mut union: Vec<i32> = set_c.union(&set_2).copied().collect();
        union.sort_unstable();
        assert!(
            union == vec![0, 1],
            "the union of partitions must be {{0, 1}}"
        );
    }

    /// KIP-848 DOWNGRADE trigger: a consumer group that hosts a classic member
    /// must flip back to classic in place when the LAST native consumer member
    /// leaves (default `Bidirectional` policy). The flip tombstones the
    /// next-gen k3 `GroupMetadata`, writes a classic k2, and re-expresses the
    /// hosted classic member as a classic member.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn last_consumer_member_leaving_downgrades_to_classic() {
        use super::super::classic_state::{Group as ClassicState, Member};
        use super::super::group::{Group, GroupKind};

        // Default policy is Bidirectional → downgrade is allowed.
        let (coord, log) = make_coordinator_with_topic("t", 2);

        // Seed a classic group with one classic member subscribed to "t".
        let mut cs = ClassicState::new("g");
        cs.protocol_type = Some("consumer".into());
        cs.generation_id = 1;
        cs.add_member(Member::new(
            "m-classic",
            "client",
            "127.0.0.1",
            std::time::Duration::from_secs(30),
            std::time::Duration::from_mins(1),
            vec![("range".into(), subscription_blob(&["t"]))],
        ));
        let group = Box::new(Group {
            group_id: "g".into(),
            kind: GroupKind::Classic(cs),
            committed_offsets: HashMap::new(),
        });
        coord.seed_classic("g", group);
        let handle = coord.find("g").expect("seeded classic actor");

        // A native consumer heartbeat upgrades the group in place; it now hosts
        // the classic member AND the native consumer member.
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
        assert!(resp.error_code == codes::NONE);
        let native_id = resp.member_id.expect("native member id");

        // The native consumer member leaves (member_epoch == -1). It was the
        // only native member, so the group downgrades back to classic.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: native_id,
                    member_epoch: -1,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        assert!(rx.await.unwrap().error_code == codes::NONE);

        // The group is now classic again. `describe_group` only returns
        // classic groups; it must surface "g" with the hosted classic member
        // re-expressed as a classic member.
        let snap = coord
            .describe_group("g")
            .await
            .expect("group downgraded to classic");
        // Only the hosted classic member remains; the departed native member
        // is gone.
        assert!(
            snap.members.len() == 1,
            "exactly the hosted classic member must remain after downgrade"
        );
        assert!(
            snap.members.iter().any(|m| m.member_id == "m-classic"),
            "the hosted classic member must survive the downgrade"
        );

        // The downgrade batch tombstoned the next-gen k3 GroupMetadata record.
        assert!(log.has_next_gen_group_metadata_tombstone("g").await);
        // ...and the group-level k6 TargetAssignmentMetadata record, which would
        // otherwise survive log compaction and resurrect the group as next-gen.
        assert!(log.has_next_gen_target_metadata_tombstone("g").await);
        // ...and wrote a classic k2 GroupMetadata (non-tombstone) for "g".
        assert!(log_has_classic_group_metadata_write(&log, "g").await);
    }

    /// KIP-848 admin coherence: after an in-place UPGRADE the group is
    /// consumer-kind, yet the classic `kafka-consumer-groups --list`/`--describe`
    /// path must still report it. `describe_group` (which used to return `None`
    /// for any non-classic handle) now inspects the LIVE group and projects the
    /// consumer state into a `GroupSnapshot`; `list_groups` includes it too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_reports_an_upgraded_consumer_group() {
        // Pin `Upgrade` policy so the transient native member's leave in
        // `seed_and_upgrade` does NOT downgrade the group back to classic —
        // we want it to STAY consumer-kind for this test.
        let (coord, _log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        // Seed classic "m-classic" subscribed to "t", then upgrade in place via a
        // native consumer heartbeat. The group is now consumer-kind, hosting the
        // classic member (the native member left in the helper).
        let _handle = seed_and_upgrade(&coord, "t").await;

        let snap = coord
            .describe_group("g")
            .await
            .expect("describe must surface an upgraded consumer group");
        assert!(snap.group_id == "g");
        // The hosted classic member survives the upgrade and is reported.
        assert!(
            !snap.members.is_empty(),
            "an upgraded consumer group must report its members"
        );
        assert!(
            snap.members.iter().any(|m| m.member_id == "m-classic"),
            "the hosted classic member must appear in the describe snapshot"
        );
        // KIP-848 next-gen consumer groups report protocol_type "consumer".
        assert!(snap.protocol_type.as_deref() == Some("consumer"));
        // The assignment is projected from the member's reconciler target, so an
        // assigned hosted-classic member has non-empty assignment bytes.
        assert!(
            snap.members
                .iter()
                .any(|m| m.member_id == "m-classic" && !m.assignment.is_empty()),
            "the assigned hosted classic member must carry a translated assignment"
        );
        // generation_id mirrors the group epoch (the next-gen analogue of a
        // classic group's generation).
        assert!(
            snap.generation_id >= 1,
            "an upgraded group's generation must have advanced off 0"
        );

        // `list_groups` produces the wire `group_type="classic"` rows; an
        // upgraded (consumer-kind) group is NOT a classic row, so it does not
        // appear here. The `ListGroups` handler surfaces it separately via
        // `consumer_group_ids()` tagged `group_type="consumer"` (so it is neither
        // double-counted nor mislabeled). Assert both halves of that contract.
        let listed = coord.list_groups().await;
        assert!(
            !listed.iter().any(|s| s.group_id == "g"),
            "an upgraded consumer group must not be reported as a classic row"
        );
        assert!(
            coord.consumer_group_ids().contains(&"g".to_string()),
            "the upgraded consumer group must be listed for the wire `consumer` pass"
        );
    }

    /// `true` iff some appended record WRITES (non-null value) a classic k2
    /// `GroupMetadata` for `group_id`.
    async fn log_has_classic_group_metadata_write(
        log: &InMemoryOffsetsLog,
        group_id: &str,
    ) -> bool {
        use crate::coordinator::unified::persistence::{Key, parse_key};
        log.batches().await.iter().any(|batch| {
            batch.records.iter().any(|rec| {
                rec.value.is_some()
                    && rec.key.as_ref().is_some_and(|k| {
                        matches!(
                            parse_key(k),
                            Ok(Key::GroupMetadata { group_id: ref gid }) if gid == group_id
                        )
                    })
            })
        })
    }

    // ── KIP-848 bidirectional migration integration suite ──────────
    //
    // These scenarios exercise the in-process classic↔next-gen migration end to
    // end (upgrade, downgrade, the two together, and the kind-agnostic state
    // that must survive a flip). They reuse the crate-internal harness above
    // (`make_coordinator_with_topic[_policy]`, `subscription_blob`, the actor
    // message arms) rather than a `tests/` integration file, which could not
    // see this scaffolding.

    /// Seed a classic consumer group "g" with a single classic member
    /// `member_id` subscribed to `topic`, optionally carrying a KIP-345 static
    /// `group_instance_id`. Mirrors the inline seeding the upgrade/downgrade
    /// tests use, but parameterized so a static-identity test can attach an
    /// instance id (which `seed_and_upgrade`'s fixed `m-classic` cannot).
    fn seed_classic_member(
        coord: &Arc<GroupCoordinator>,
        member_id: &str,
        topic: &str,
        instance_id: Option<&str>,
    ) -> Arc<GroupActorHandle> {
        use super::super::classic_state::{Group as ClassicState, Member};
        use super::super::group::{Group, GroupKind};

        let mut cs = ClassicState::new("g");
        cs.protocol_type = Some("consumer".into());
        cs.generation_id = 1;
        cs.add_member(
            Member::new(
                member_id,
                "client",
                "127.0.0.1",
                std::time::Duration::from_secs(30),
                std::time::Duration::from_mins(1),
                vec![("range".into(), subscription_blob(&[topic]))],
            )
            .with_instance_id(instance_id.map(str::to_string)),
        );
        let group = Box::new(Group {
            group_id: "g".into(),
            kind: GroupKind::Classic(cs),
            committed_offsets: HashMap::new(),
        });
        coord.seed_classic("g", group);
        coord.find("g").expect("seeded classic actor")
    }

    /// Send a native consumer `Heartbeat` and return the response. A `member_id`
    /// of `""`/epoch 0 is a first-join (which triggers an upgrade when the group
    /// is a convertible classic group under an upgrade-allowing policy).
    async fn consumer_heartbeat(
        handle: &GroupActorHandle,
        member_id: &str,
        member_epoch: i32,
        topic: Option<&str>,
    ) -> ConsumerGroupHeartbeatResponse {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: member_id.into(),
                    member_epoch,
                    subscribed_topic_names: topic.map(|t| vec![t.into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    /// Read the live `ClassicInspect` view (only a classic-kind group replies).
    async fn classic_inspect(handle: &GroupActorHandle) -> ClassicView {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicInspect { reply: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    /// A classic member leaves the group (v3 single-member leave list).
    async fn classic_leave(handle: &GroupActorHandle, member_id: &str) -> Vec<MemberResponse> {
        use crabka_protocol::owned::leave_group_request::MemberIdentity;
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicLeave {
                req: LeaveGroupRequest {
                    group_id: "g".into(),
                    members: vec![MemberIdentity {
                        member_id: member_id.into(),
                        group_instance_id: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                version: 3,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    /// Validate an offset commit against the group's LIVE kind via the single
    /// `ValidateCommit` message.
    async fn validate_commit(
        handle: &GroupActorHandle,
        member_id: &str,
        generation_or_epoch: i32,
    ) -> Result<(), i16> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ValidateCommit {
                member_id: member_id.into(),
                group_instance_id: None,
                generation_or_epoch,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    /// Round-trip the kind-agnostic committed-offset store.
    async fn fetch_committed(
        handle: &GroupActorHandle,
    ) -> HashMap<(String, i32), super::super::classic_state::OffsetEntry> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchCommitted { reply: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    /// Scenario 1: a full upgrade→downgrade round trip under `Bidirectional`. A
    /// classic member "m1" joins; a native consumer "c1" heartbeats → upgrade;
    /// c1 leaves → downgrade. The group must end CLASSIC with "m1" still present
    /// and still assigned (its partitions preserved across both flips).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upgrade_then_downgrade_round_trip() {
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
        let (coord, _log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);
        let handle = seed_classic_member(&coord, "m1", "t", None);

        // A native consumer "c1" heartbeats → in-place UPGRADE; the group is now
        // consumer-kind and hosts both m1 (classic facade) and c1.
        let up = consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(up.error_code == codes::NONE);
        let c1 = up.member_id.expect("native member id");
        let describe = {
            let (tx, rx) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(GroupActorMessage::Describe { reply: tx })
                .await
                .unwrap();
            rx.await.unwrap()
        };
        assert!(
            describe.members.len() == 2,
            "upgraded group hosts both m1 and c1"
        );

        // c1 leaves (member_epoch -1). It was the only native member → DOWNGRADE
        // back to classic.
        let leave = consumer_heartbeat(&handle, &c1, -1, None).await;
        assert!(leave.error_code == codes::NONE);

        // The group is classic again, with m1 restored and still assigned.
        let snap = coord
            .describe_group("g")
            .await
            .expect("group downgraded back to classic");
        assert!(
            snap.members.iter().any(|m| m.member_id == "m1"),
            "m1 must survive the upgrade→downgrade round trip"
        );
        let m1 = snap
            .members
            .iter()
            .find(|m| m.member_id == "m1")
            .expect("m1 present");
        // Decode the assignment blob and verify m1's partitions are preserved.
        // `!is_empty()` is insufficient — the blob always has a 2-byte version
        // prefix even if the partition list were empty.
        //
        // After upgrade, the range assignor splits {0,1} between m1 and c1;
        // m1 is assigned partition [1] (range assignor gives the higher range
        // to the lexicographically-later member when both subscribe to "t").
        // On downgrade, c1 (the only native member) has departed, so the
        // downgrade RE-RECONCILES over the surviving members BEFORE converting
        // to classic. m1 is now the sole member subscribed to "t", so the range
        // assignor gives it BOTH partitions — no partition is orphaned. Its
        // seed assignment in the restored classic group is therefore [0, 1].
        let assignment_bytes = bytes::Bytes::from(m1.assignment.clone());
        let decoded = decode_assignment(&assignment_bytes);
        let tp = decoded
            .assigned_partitions
            .iter()
            .find(|tp| tp.topic == "t")
            .expect("decoded assignment must contain topic t");
        let mut parts = tp.partitions.clone();
        parts.sort_unstable();
        assert!(
            parts == vec![0, 1],
            "m1 (sole surviving member) must own BOTH partitions after the downgrade re-reconcile; got {parts:?}"
        );
    }

    /// Scenario 2: KIP-345 static identity must survive both flips. A classic
    /// member with `group.instance.id = "inst-a"` joins; upgrade; then downgrade.
    /// The restored classic member must still carry `group_instance_id ==
    /// Some("inst-a")` (read from the classic inspect view — `MemberSnapshot`
    /// does not carry the instance id, but `ClassicMemberView` does).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn static_member_identity_survives_both_flips() {
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
        let (coord, _log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);
        let handle = seed_classic_member(&coord, "m1", "t", Some("inst-a"));

        // Upgrade via a native consumer heartbeat, then downgrade by having that
        // native member leave.
        let up = consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(up.error_code == codes::NONE);
        let native = up.member_id.expect("native member id");
        let leave = consumer_heartbeat(&handle, &native, -1, None).await;
        assert!(leave.error_code == codes::NONE);

        // The group is classic again; the restored member must still carry the
        // static identity (convert_classic_to_consumer maps instance_id and
        // convert_consumer_to_classic restores it).
        let view = classic_inspect(&handle).await;
        let m1 = view
            .members
            .iter()
            .find(|m| m.member_id == "m1")
            .expect("m1 restored as a classic member");
        assert!(
            m1.group_instance_id.as_deref() == Some("inst-a"),
            "the static identity must survive both flips"
        );
    }

    /// Scenario 3: under `Disabled` policy a classic group stays classic — a
    /// native consumer heartbeat for the same group is REJECTED rather than
    /// upgrading it. This reproduces today's hard classic/next-gen separation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn policy_disabled_keeps_group_classic() {
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
        let (coord, _log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Disabled);
        let handle = seed_classic_member(&coord, "m1", "t", None);

        // A native consumer heartbeat must be rejected (no upgrade is allowed).
        let resp = consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(
            resp.error_code != codes::NONE,
            "Disabled policy must reject the upgrade heartbeat"
        );
        assert!(
            resp.error_code == codes::GROUP_ID_NOT_FOUND,
            "an un-upgradable classic group surfaces as GROUP_ID_NOT_FOUND"
        );

        // The group is untouched: still classic, still hosting m1.
        let view = classic_inspect(&handle).await;
        assert!(
            view.members.iter().any(|m| m.member_id == "m1"),
            "the group must remain classic with m1 intact"
        );
        assert!(handle.kind == GroupKindTag::Classic);
    }

    /// Scenario 4: committed offsets live on the kind-agnostic `Group` container
    /// and must survive both flips untouched. Commit an offset for ("t", 0) on a
    /// classic group, upgrade, assert it's still readable, downgrade, assert it's
    /// STILL there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn committed_offsets_survive_a_flip() {
        use super::super::classic_state::OffsetEntry;
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
        let (coord, _log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);
        let handle = seed_classic_member(&coord, "m1", "t", None);

        // Record a committed offset for ("t", 0) via the kind-agnostic path.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::UpdateCommitted {
                entries: vec![(
                    ("t".to_string(), 0),
                    OffsetEntry {
                        offset: 99,
                        leader_epoch: 3,
                        metadata: String::new(),
                        commit_timestamp_ms: 0,
                    },
                )],
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap();

        // Upgrade → the offset must still be readable.
        let up = consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(up.error_code == codes::NONE);
        let native = up.member_id.expect("native member id");
        let after_upgrade = fetch_committed(&handle).await;
        assert!(
            after_upgrade.get(&("t".to_string(), 0)).map(|e| e.offset) == Some(99),
            "committed offset must survive the upgrade"
        );

        // Downgrade → the offset must STILL be there.
        let leave = consumer_heartbeat(&handle, &native, -1, None).await;
        assert!(leave.error_code == codes::NONE);
        let after_downgrade = fetch_committed(&handle).await;
        assert!(
            after_downgrade.get(&("t".to_string(), 0)).map(|e| e.offset) == Some(99),
            "committed offset must survive the downgrade too"
        );
    }

    /// Regression for the stale-`handle.kind` defect (KIP-848 live migration):
    /// a group SPAWNED as a consumer group (its first RPC was a native
    /// `ConsumerGroupHeartbeat`, so `handle.kind == Consumer`) that later hosts a
    /// classic member and then DOWNGRADES in place when the last native member
    /// leaves. The handle's spawn-time `kind` stays `Consumer` and is now stale.
    ///
    /// The defect: `offset_commit::validate` pre-dispatched on a per-handle kind
    /// mirror, so a downgraded classic member's offset commit was at risk of
    /// being routed to the next-gen epoch path (`group.as_consumer()` is now
    /// `None`, so it would reject with `UNKNOWN_MEMBER_ID`). With the
    /// single-source-of-truth fix the one `ValidateCommit` message dispatches on
    /// the actor's LIVE `group.kind` — now classic — and `classic_ops::
    /// validate_commit` finds the re-expressed classic member and accepts the
    /// commit (`Ok(())`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawned_consumer_group_downgrade_allows_classic_offset_commit() {
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
        let (coord, _log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);

        // SPAWN the actor as a consumer group: the first RPC is a native
        // ConsumerGroupHeartbeat, so the handle's spawn-time `kind == Consumer`.
        let handle = coord.get_or_create_consumer("g");
        assert!(
            handle.kind == GroupKindTag::Consumer,
            "the group must be spawned consumer-kind"
        );

        let up = consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(up.error_code == codes::NONE);
        let native = up.member_id.expect("native member id");

        // A CLASSIC member joins the (consumer-kind) group as a hosted member.
        let join = classic_join(&handle, "m-classic", "t").await;
        assert!(join.error_code == codes::NONE);

        // The native consumer member leaves (member_epoch -1). It was the only
        // native member and a hosted classic member remains → DOWNGRADE in
        // place. The group is now live-Classic but the handle was spawned
        // Consumer (its `kind` field stays stale). `maybe_downgrade` runs inside
        // the Heartbeat handler AFTER the reply is sent, so we round-trip one
        // more message (the `classic_inspect` below) to be sure the in-place
        // flip has completed before validating.
        let leave = consumer_heartbeat(&handle, &native, -1, None).await;
        assert!(leave.error_code == codes::NONE);

        // The hosted classic member was re-expressed as a classic member. Read
        // the restored classic generation it must commit against. This
        // `ClassicInspect` round-trip is also the barrier that guarantees the
        // downgrade completed (only a classic-kind group answers it; the actor
        // processes it strictly after the leave's `maybe_downgrade`).
        let view = classic_inspect(&handle).await;
        // The handle's spawn-time `kind` is unchanged (and stale) — validation
        // must NOT consult it.
        assert!(
            handle.kind == GroupKindTag::Consumer,
            "spawn-time kind unchanged"
        );
        assert!(
            view.members.iter().any(|m| m.member_id == "m-classic"),
            "the hosted classic member must survive the downgrade"
        );
        let generation = view.generation_id;

        // Prove the fix at the routing boundary `offset_commit::validate` uses:
        // the single `ValidateCommit` message dispatches on the actor's LIVE
        // `group.kind` (now classic) and accepts the downgraded classic member's
        // commit (`Ok(())`). Pre-refactor, a handle-side mirror could route this
        // to the consumer epoch path and reject with `UNKNOWN_MEMBER_ID`.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ValidateCommit {
                member_id: "m-classic".into(),
                group_instance_id: None,
                generation_or_epoch: generation,
                reply: tx,
            })
            .await
            .unwrap();
        let result = rx.await.unwrap();
        assert!(
            result == Ok(()),
            "ValidateCommit must dispatch on the live (classic) kind and accept \
             the downgraded member (got {result:?})"
        );
    }

    /// Regression (user-requested): a group that downgraded in place becomes
    /// deletable. Spawn a CONSUMER group (first RPC a `ConsumerGroupHeartbeat`),
    /// host a classic member, then downgrade (the native consumer leaves). The
    /// handle's spawn-time `kind` is stale `Consumer`, but `delete_group` now
    /// dispatches on `ClassicInspect`'s live-kind reply — the downgraded group
    /// answers as classic, so a non-empty group reports `NonEmpty` (NOT
    /// `NotFound`), proving delete sees it as classic. Pre-refactor the stale
    /// `handle.kind == Consumer` gate short-circuited to `NotFound`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn downgraded_group_is_deletable_once_empty() {
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
        let (coord, _log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);

        // SPAWN consumer-kind; host a classic member; downgrade.
        let handle = coord.get_or_create_consumer("g");
        assert!(handle.kind == GroupKindTag::Consumer);
        let up = consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(up.error_code == codes::NONE);
        let native = up.member_id.expect("native member id");
        let join = classic_join(&handle, "m-classic", "t").await;
        assert!(join.error_code == codes::NONE);
        let leave = consumer_heartbeat(&handle, &native, -1, None).await;
        assert!(leave.error_code == codes::NONE);

        // Barrier: only a classic-kind group answers `ClassicInspect`, so this
        // round-trip guarantees the downgrade completed. The lone hosted classic
        // member keeps it non-empty.
        let view = classic_inspect(&handle).await;
        assert!(view.members.iter().any(|m| m.member_id == "m-classic"));

        // The spawn-time kind is the stale `Consumer`; delete must not consult
        // it. A non-empty downgraded (live-classic) group reports `NonEmpty`,
        // NOT `NotFound` — proving delete sees it as classic.
        assert!(handle.kind == GroupKindTag::Consumer);
        assert!(
            coord.delete_group("g").await == Err(crate::coordinator::DeleteGroupError::NonEmpty),
            "a downgraded non-empty group must report NonEmpty (seen as classic), \
             not the stale-handle.kind NotFound"
        );

        // Drain the last hosted classic member so the group is empty, then it
        // must be deletable.
        let resp = classic_leave(&handle, "m-classic").await;
        assert!(!resp.is_empty());
        let view = classic_inspect(&handle).await;
        assert!(
            view.members.is_empty(),
            "the group must be empty after the classic member leaves"
        );
        assert!(
            coord.delete_group("g").await == Ok(()),
            "an empty downgraded group must be deletable"
        );
    }

    /// Regression (user-requested): an UPGRADED group runs the consumer epoch
    /// fence on a native consumer member's commit. A classic group upgrades (a
    /// native consumer heartbeats in), so the handle's spawn-time `kind` is the
    /// stale `Classic`. `ValidateCommit` for that native member must dispatch on
    /// the LIVE (consumer) kind and apply the epoch fence: a STALE epoch (<
    /// current) → `STALE_MEMBER_EPOCH`, a FENCED epoch (> current) →
    /// `FENCED_MEMBER_EPOCH`. Pre-refactor a spawned-Classic upgraded group sent
    /// the classic validate path and SKIPPED the epoch check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upgraded_group_fences_stale_native_consumer_commit() {
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
        let (coord, _log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);

        // SPAWN classic-kind via a seeded classic member, then UPGRADE by having
        // a native consumer heartbeat in. The handle's spawn-time `kind` stays
        // the stale `Classic`.
        let handle = seed_classic_member(&coord, "m1", "t", None);
        assert!(handle.kind == GroupKindTag::Classic);
        let up = consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(up.error_code == codes::NONE);
        let native = up.member_id.expect("native member id");
        let current_epoch = up.member_epoch;

        // The handle's spawn-time kind is the stale `Classic`; validation must
        // not consult it — it must run the consumer epoch fence.
        assert!(handle.kind == GroupKindTag::Classic);

        // STALE epoch (< current) → STALE_MEMBER_EPOCH.
        let stale = validate_commit(&handle, &native, current_epoch - 1).await;
        assert!(
            stale == Err(codes::STALE_MEMBER_EPOCH),
            "an upgraded group must run the consumer epoch fence (stale); got {stale:?}"
        );

        // FENCED epoch (> current) → FENCED_MEMBER_EPOCH.
        let fenced = validate_commit(&handle, &native, current_epoch + 1).await;
        assert!(
            fenced == Err(codes::FENCED_MEMBER_EPOCH),
            "an upgraded group must run the consumer epoch fence (fenced); got {fenced:?}"
        );

        // The current epoch is accepted.
        let ok = validate_commit(&handle, &native, current_epoch).await;
        assert!(
            ok == Ok(()),
            "the current epoch must be accepted; got {ok:?}"
        );
    }

    #[test]
    fn step_heartbeat_first_join_targets_all_partitions() {
        use crate::coordinator::unified::consumer_state::GroupState;
        let topic_id = Uuid([7; 16]);
        let metadata = StaticMetadata {
            input: ReconcileInput {
                topic_id_by_name: [("t".to_string(), topic_id)].into(),
                partitions_per_topic: [(topic_id, 2)].into(),
                ..Default::default()
            },
        };
        let config = NextGenConfig::default();
        let mut group = GroupState::new("g");
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        };
        let step = step_heartbeat(&mut group, &config, &metadata, &req, "", Instant::now());
        assert!(step.response.error_code == 0);
        assert!(
            step.response.member_epoch == 1,
            "first join advances to group epoch 1"
        );
        assert!(group.target.per_member["m1"][&topic_id] == vec![0, 1]);
        assert!(!step.pending.is_empty(), "first join must persist records");
    }
}
