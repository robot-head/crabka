//! Exhaustive Stateright model for the range-0 timestamp oracle protocol.
//!
//! The model keeps the protocol vocabulary deliberately close to the production
//! seams: a request becomes a [`GrantLease`] reply, `durable_max_ts` represents
//! [`crabka_gres_ranges::tso::oracle::MAX_TS_KEY`], and an oracle epoch is admitted only by the same liveness
//! decision as [`HeartbeatVerdict`]. It explores two clients, variable grant
//! sizes, delayed requests and replies, crash recovery, and a live fenced
//! oracle that still has a client connection.
//!
//! Epoch liveness is amortized behind a certificate window rather than a
//! per-grant heartbeat: a fenced replica whose liveness certificate has not yet
//! lapsed keeps serving within-stride grants from memory, but it can never
//! extend its reservation because the stride persist is epoch-fenced. The
//! `successor_grace` knob models the successor grace rule: a successor may not
//! serve its first grant while any older-epoch replica is still running inside
//! its certificate window. To keep the exhaustive state space affordable the
//! model reserves each recovering replica's first stride up front (see
//! [`TsoModel::reserve_first_stride`]); every lazily-persisting production
//! trace is order-isomorphic to a modeled trace.

use std::num::NonZeroU64;

use assert2::assert;
use crabka_gres_ranges::{GrantLease, HeartbeatVerdict, TsoTimestamp};
use stateright::{Checker, Model, Property};

const CLIENTS: u8 = 2;
const MAX_EPOCH: u8 = 2;
// Protocol-domain bounds that make this configuration finite. They are deliberately
// not Stateright exploration budgets: the checker must drain its own work queue.
const MAX_PENDING_REQUESTS: usize = 2;
const MAX_PENDING_REPLIES: usize = 2;
const MAX_ISSUED_GRANTS: usize = 2;
const EXHAUSTIVE_STATE_COUNT: usize = 11_358_628;
const EXHAUSTIVE_UNIQUE_STATE_COUNT: usize = 3_805_290;
const EXHAUSTIVE_MAX_DEPTH: usize = 16;

#[derive(Clone, Copy)]
struct TsoModel {
    stride: NonZeroU64,
    stride_persistence: StridePersistence,
    epoch_liveness: EpochLiveness,
    successor_grace: SuccessorGrace,
    client_visible_max: ClientVisibleMax,
}

/// Whether grants persist a stride-ahead durable horizon before being served.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StridePersistence {
    Ahead,
    Missing,
}

/// Whether serving requires a live epoch (fresh heartbeat or an unexpired
/// liveness certificate).
#[derive(Clone, Copy, PartialEq, Eq)]
enum EpochLiveness {
    Required,
    Unchecked,
}

/// Whether a successor waits out the certificate windows of every older-epoch
/// replica that is still running before serving its first grant.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SuccessorGrace {
    WaitsOutCertificate,
    ServesImmediately,
}

/// How a delivered reply updates the client-visible timestamp.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ClientVisibleMax {
    Preserved,
    Overwritten,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// Inclusive durable range-0 horizon stored at `MAX_TS_KEY`.
    durable_max_ts: u8,
    active_epoch: u8,
    replicas: Vec<Replica>,
    requests: Vec<Request>,
    replies: Vec<Reply>,
    issued_grants: Vec<IssuedGrant>,
    fence_horizons: Vec<FenceHorizon>,
    clients: [Client; CLIENTS as usize],
    acknowledged_commit_ts: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Replica {
    epoch: u8,
    next_ts: u8,
    running: bool,
    /// Whether the amortized liveness certificate is still unexpired. Every
    /// replica is created holding a live certificate (an active oracle renews
    /// it on each stride persist or heartbeat, so the flag only becomes
    /// meaningful once the replica is fenced).
    certificate_live: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Request {
    client: u8,
    target_epoch: u8,
    count: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Reply {
    client: u8,
    first_ts: u8,
    count: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct IssuedGrant {
    client: u8,
    epoch: u8,
    first_ts: u8,
    count: u8,
    durable_after_grant: u8,
    acknowledged_commit_before_grant: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct FenceHorizon {
    successor_epoch: u8,
    durable_max_ts: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Client {
    /// The greatest timestamp the client has made visible to its caller.
    visible_ts: u8,
    /// The greatest timestamp in every reply delivered to this client.
    delivered_reply_max_ts: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    SendRequest {
        client: u8,
        target_epoch: u8,
        count: u8,
    },
    ProcessRequest(usize),
    DeliverReply(usize),
    AcknowledgeCommit,
    CrashActive,
    RestartAfterCrash,
    FenceWithLiveZombie,
    ExpireCertificate(usize),
}

impl Model for TsoModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let mut state = State {
            durable_max_ts: 0,
            active_epoch: 0,
            replicas: vec![Replica {
                epoch: 0,
                next_ts: 1,
                running: true,
                certificate_live: true,
            }],
            requests: Vec::new(),
            replies: Vec::new(),
            issued_grants: Vec::new(),
            fence_horizons: Vec::new(),
            clients: std::array::from_fn(|_| Client {
                visible_ts: 0,
                delivered_reply_max_ts: 0,
            }),
            acknowledged_commit_ts: 0,
        };
        self.reserve_first_stride(&mut state);
        vec![state]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.requests.len() < MAX_PENDING_REQUESTS {
            for replica in state.replicas.iter().filter(|replica| replica.running) {
                for client in 0..CLIENTS {
                    for count in [1, 2] {
                        actions.push(Action::SendRequest {
                            client,
                            target_epoch: replica.epoch,
                            count,
                        });
                    }
                }
            }
        }
        actions.extend((0..state.requests.len()).map(Action::ProcessRequest));
        actions.extend((0..state.replies.len()).map(Action::DeliverReply));
        if !state.issued_grants.is_empty() {
            actions.push(Action::AcknowledgeCommit);
        }
        if active_replica(state).is_some_and(|replica| replica.running) {
            actions.push(Action::CrashActive);
            if state.active_epoch < MAX_EPOCH {
                actions.push(Action::FenceWithLiveZombie);
            }
        } else if state.active_epoch < MAX_EPOCH {
            actions.push(Action::RestartAfterCrash);
        }
        for (index, replica) in state.replicas.iter().enumerate() {
            if replica.running && replica.epoch < state.active_epoch && replica.certificate_live {
                actions.push(Action::ExpireCertificate(index));
            }
        }
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();

        match action {
            Action::SendRequest {
                client,
                target_epoch,
                count,
            } => next.requests.push(Request {
                client,
                target_epoch,
                count,
            }),
            Action::ProcessRequest(index) => self.process_request(&mut next, index)?,
            Action::DeliverReply(index) => self.deliver_reply(&mut next, index)?,
            Action::AcknowledgeCommit => acknowledge_latest_grant(&mut next)?,
            Action::CrashActive => active_replica_mut(&mut next)?.running = false,
            Action::RestartAfterCrash => self.restart_active(&mut next)?,
            Action::FenceWithLiveZombie => self.fence_with_live_zombie(&mut next)?,
            Action::ExpireCertificate(index) => expire_certificate(&mut next, index)?,
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "tso_grants_are_globally_unique_and_strictly_monotone",
                |_, state| grants_are_globally_ordered(state),
            ),
            Property::always(
                "tso_never_reuses_a_timestamp_after_crash_or_fence",
                |_, state| grants_after_each_fence_start_past_durable_horizon(state),
            ),
            Property::always(
                "tso_grants_are_covered_by_the_durable_stride_fence",
                |_, state| grants_are_durably_reserved(state),
            ),
            Property::always(
                "tso_client_visible_timestamp_never_regresses_after_reply_delivery",
                |_, state| clients_never_regress(state),
            ),
            Property::always(
                "tso_grant_never_precedes_a_commit_acknowledged_before_it",
                |_, state| grants_follow_acknowledged_commits(state),
            ),
            Property::sometimes("both_clients_receive_grants", |_, state: &State| {
                (0..CLIENTS).all(|client| {
                    state
                        .issued_grants
                        .iter()
                        .any(|grant| grant.client == client)
                })
            }),
            Property::sometimes(
                "request_is_delayed_while_a_reply_is_in_flight",
                |_, state: &State| !state.requests.is_empty() && !state.replies.is_empty(),
            ),
            Property::sometimes("crashed_epoch_is_restarted", |_, state: &State| {
                state.replicas.iter().any(|replica| !replica.running)
                    && active_replica(state).is_some_and(|replica| replica.running)
            }),
            Property::sometimes(
                "fence_leaves_a_live_zombie_connected",
                |_, state: &State| {
                    state
                        .replicas
                        .iter()
                        .any(|replica| replica.epoch < state.active_epoch && replica.running)
                },
            ),
            Property::sometimes(
                "fenced_zombie_can_serve_inside_its_certificate_window",
                |_, state: &State| {
                    state.replicas.iter().any(|replica| {
                        replica.running
                            && replica.epoch < state.active_epoch
                            && replica.certificate_live
                            && reservation_ceiling(state, replica.epoch, true)
                                .is_some_and(|ceiling| replica.next_ts <= ceiling)
                    })
                },
            ),
        ]
    }
}

impl TsoModel {
    fn deliver_reply(&self, state: &mut State, index: usize) -> Option<()> {
        let reply = state.replies.get(index)?.clone();
        let client = state.clients.get_mut(usize::from(reply.client))?;
        let reply_last_ts = checked_last(reply.first_ts, reply.count)?;
        client.delivered_reply_max_ts = client.delivered_reply_max_ts.max(reply_last_ts);
        if self.client_visible_max == ClientVisibleMax::Preserved {
            client.visible_ts = client.visible_ts.max(reply_last_ts);
        } else {
            client.visible_ts = reply_last_ts;
        }
        state.replies.remove(index);
        Some(())
    }

    fn process_request(&self, state: &mut State, index: usize) -> Option<()> {
        let request = state.requests.get(index)?.clone();
        let replica_index = state
            .replicas
            .iter()
            .position(|replica| replica.epoch == request.target_epoch && replica.running)?;
        let replica_epoch = state.replicas[replica_index].epoch;
        let fenced = epoch_heartbeat(replica_epoch, state.active_epoch) != HeartbeatVerdict::Live;
        if self.epoch_liveness == EpochLiveness::Required
            && fenced
            && !state.replicas[replica_index].certificate_live
        {
            return None;
        }
        if self.successor_grace == SuccessorGrace::WaitsOutCertificate
            && state
                .replicas
                .iter()
                .any(|older| older.epoch < replica_epoch && older.running && older.certificate_live)
        {
            return None;
        }
        if state.replies.len() >= MAX_PENDING_REPLIES
            || state.issued_grants.len() >= MAX_ISSUED_GRANTS
        {
            return None;
        }

        let first_ts = state.replicas[replica_index].next_ts;
        let last_ts = checked_last(first_ts, request.count)?;
        if self.stride_persistence == StridePersistence::Ahead
            && last_ts > reservation_ceiling(state, replica_epoch, fenced)?
        {
            // Extending the reservation persists a new stride through the
            // epoch-fenced range-0 write; a deposed epoch's persist is refused,
            // so it can only serve what its last stride already reserved.
            if fenced {
                return None;
            }
            let stride_last = checked_last(first_ts, u8::try_from(self.stride.get()).ok()?)?;
            state.durable_max_ts = state.durable_max_ts.max(last_ts.max(stride_last));
        }
        state.replicas[replica_index].next_ts = last_ts.checked_add(1)?;
        state.requests.remove(index);
        state.replies.push(Reply {
            client: request.client,
            first_ts,
            count: request.count,
        });
        state.issued_grants.push(IssuedGrant {
            client: request.client,
            epoch: replica_epoch,
            first_ts,
            count: request.count,
            durable_after_grant: state.durable_max_ts,
            acknowledged_commit_before_grant: state.acknowledged_commit_ts,
        });
        Some(())
    }

    fn restart_active(&self, state: &mut State) -> Option<()> {
        let old_active = active_replica(state)?;
        if old_active.running || state.active_epoch >= MAX_EPOCH {
            return None;
        }
        self.start_successor(state);
        Some(())
    }

    fn fence_with_live_zombie(&self, state: &mut State) -> Option<()> {
        if state.active_epoch >= MAX_EPOCH || !active_replica(state)?.running {
            return None;
        }
        self.start_successor(state);
        Some(())
    }

    fn start_successor(&self, state: &mut State) {
        state.active_epoch += 1;
        state.fence_horizons.push(FenceHorizon {
            successor_epoch: state.active_epoch,
            durable_max_ts: state.durable_max_ts,
        });
        state.replicas.push(Replica {
            epoch: state.active_epoch,
            next_ts: state.durable_max_ts.saturating_add(1),
            running: true,
            certificate_live: true,
        });
        self.reserve_first_stride(state);
    }

    /// Reserve the newly recovered active replica's first stride.
    ///
    /// Production persists strides lazily on grant, but a lazy model needs an
    /// extra warm-up grant before every fence just to open the deposed
    /// replica's certificate window, which pushes the issued-grant history
    /// bound (and the reachable state space) far past what exhaustive checking
    /// affords. Reserving the first stride at recovery keeps every lazy trace
    /// order-isomorphic to a modeled trace while only widening what a fenced
    /// replica may serve, so `MAX_ISSUED_GRANTS` can stay a protocol-domain
    /// bound of two.
    fn reserve_first_stride(&self, state: &mut State) {
        if self.stride_persistence != StridePersistence::Ahead {
            return;
        }
        if let Some(replica) = active_replica(state)
            && let Ok(stride) = u8::try_from(self.stride.get())
            && let Some(stride_last) = checked_last(replica.next_ts, stride)
        {
            state.durable_max_ts = state.durable_max_ts.max(stride_last);
        }
    }
}

fn epoch_heartbeat(replica_epoch: u8, active_epoch: u8) -> HeartbeatVerdict {
    if replica_epoch == active_epoch {
        return HeartbeatVerdict::Live;
    }

    HeartbeatVerdict::Fenced
}

/// Inclusive horizon the replica may serve from memory: the shared durable
/// horizon while it is the active writer, or the horizon captured when it was
/// deposed. A fenced epoch can no longer persist, so its reservation stays
/// frozen at fence time even though successors keep advancing `durable_max_ts`.
fn reservation_ceiling(state: &State, replica_epoch: u8, fenced: bool) -> Option<u8> {
    if !fenced {
        return Some(state.durable_max_ts);
    }
    state
        .fence_horizons
        .iter()
        .find(|fence| fence.successor_epoch == replica_epoch + 1)
        .map(|fence| fence.durable_max_ts)
}

fn acknowledge_latest_grant(state: &mut State) -> Option<()> {
    let latest = state.issued_grants.last()?;
    if latest.epoch != state.active_epoch {
        return None;
    }
    state.acknowledged_commit_ts = state
        .acknowledged_commit_ts
        .max(checked_last(latest.first_ts, latest.count)?);
    Some(())
}

fn expire_certificate(state: &mut State, index: usize) -> Option<()> {
    let active_epoch = state.active_epoch;
    let replica = state.replicas.get_mut(index)?;
    if !replica.running || replica.epoch >= active_epoch || !replica.certificate_live {
        return None;
    }
    replica.certificate_live = false;
    Some(())
}

fn active_replica(state: &State) -> Option<&Replica> {
    state
        .replicas
        .iter()
        .find(|replica| replica.epoch == state.active_epoch)
}

fn active_replica_mut(state: &mut State) -> Option<&mut Replica> {
    state
        .replicas
        .iter_mut()
        .find(|replica| replica.epoch == state.active_epoch)
}

fn checked_last(first_ts: u8, count: u8) -> Option<u8> {
    let count = NonZeroU64::new(u64::from(count))?;
    let first_ts = TsoTimestamp::new(NonZeroU64::new(u64::from(first_ts))?);
    let lease = GrantLease::new(first_ts, count);
    u8::try_from(lease.last_ts().ok()?.get()).ok()
}

fn grants_are_globally_ordered(state: &State) -> bool {
    state
        .issued_grants
        .iter()
        .enumerate()
        .all(|(index, grant)| {
            state.issued_grants[..index].iter().all(|prior| {
                checked_last(prior.first_ts, prior.count)
                    .is_some_and(|prior_last| prior_last < grant.first_ts)
            })
        })
}

fn grants_after_each_fence_start_past_durable_horizon(state: &State) -> bool {
    state.fence_horizons.iter().all(|fence| {
        state
            .issued_grants
            .iter()
            .filter(|grant| grant.epoch >= fence.successor_epoch)
            .all(|grant| grant.first_ts > fence.durable_max_ts)
    })
}

fn grants_are_durably_reserved(state: &State) -> bool {
    state.issued_grants.iter().all(|grant| {
        checked_last(grant.first_ts, grant.count)
            .is_some_and(|last_ts| last_ts <= grant.durable_after_grant)
    })
}

fn clients_never_regress(state: &State) -> bool {
    state
        .clients
        .iter()
        .all(|client| client.visible_ts >= client.delivered_reply_max_ts)
}

fn grants_follow_acknowledged_commits(state: &State) -> bool {
    state
        .issued_grants
        .iter()
        .all(|grant| grant.first_ts >= grant.acknowledged_commit_before_grant)
}

fn checker(model: TsoModel) -> impl Checker<TsoModel> {
    model.checker().spawn_bfs().join()
}

fn assert_exhaustive(checker: &impl Checker<TsoModel>) {
    eprintln!(
        "[tso_monotonicity_model] exhaustive traversal: states={} unique_states={} max_depth={}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
    );
    assert!(
        checker.is_done(),
        "TSO model traversal was truncated: states={} unique_states={} max_depth={}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
    );
    assert!(
        checker.state_count() == EXHAUSTIVE_STATE_COUNT,
        "TSO model traversal generated an unexpected number of states; a checker cutoff or model-bound change may have truncated the proof",
    );
    assert!(
        checker.unique_state_count() == EXHAUSTIVE_UNIQUE_STATE_COUNT,
        "TSO model traversal visited an unexpected number of unique states; a checker cutoff or model-bound change may have truncated the proof",
    );
    assert!(
        checker.max_depth() == EXHAUSTIVE_MAX_DEPTH,
        "TSO model traversal stopped at an unexpected depth; a checker cutoff or model-bound change may have truncated the proof",
    );
}

#[test]
fn stride_ahead_epoch_liveness_and_successor_grace_preserve_tso_safety() {
    let checker = checker(TsoModel {
        stride: NonZeroU64::new(3).expect("model stride is non-zero"),
        stride_persistence: StridePersistence::Ahead,
        epoch_liveness: EpochLiveness::Required,
        successor_grace: SuccessorGrace::WaitsOutCertificate,
        client_visible_max: ClientVisibleMax::Preserved,
    });

    assert_exhaustive(&checker);
    checker.assert_properties();
    assert!(checker.unique_state_count() > 1);
}

#[test]
fn missing_stride_ahead_has_a_crash_reuse_counterexample() {
    let checker = checker(TsoModel {
        stride: NonZeroU64::new(3).expect("model stride is non-zero"),
        stride_persistence: StridePersistence::Missing,
        epoch_liveness: EpochLiveness::Required,
        successor_grace: SuccessorGrace::WaitsOutCertificate,
        client_visible_max: ClientVisibleMax::Preserved,
    });

    assert!(
        checker
            .discoveries()
            .contains_key("tso_grants_are_globally_unique_and_strictly_monotone")
    );
}

#[test]
fn missing_epoch_liveness_has_a_live_zombie_freshness_counterexample() {
    let checker = checker(TsoModel {
        stride: NonZeroU64::new(3).expect("model stride is non-zero"),
        stride_persistence: StridePersistence::Ahead,
        epoch_liveness: EpochLiveness::Unchecked,
        successor_grace: SuccessorGrace::WaitsOutCertificate,
        client_visible_max: ClientVisibleMax::Preserved,
    });

    assert!(
        checker
            .discoveries()
            .contains_key("tso_grant_never_precedes_a_commit_acknowledged_before_it")
    );
}

#[test]
fn missing_successor_grace_has_a_stale_certificate_window_counterexample() {
    let checker = checker(TsoModel {
        stride: NonZeroU64::new(3).expect("model stride is non-zero"),
        stride_persistence: StridePersistence::Ahead,
        epoch_liveness: EpochLiveness::Required,
        successor_grace: SuccessorGrace::ServesImmediately,
        client_visible_max: ClientVisibleMax::Preserved,
    });

    assert!(
        checker
            .discoveries()
            .contains_key("tso_grant_never_precedes_a_commit_acknowledged_before_it")
    );
}

#[test]
fn direct_reply_assignment_has_a_client_visible_timestamp_regression_counterexample() {
    let checker = checker(TsoModel {
        stride: NonZeroU64::new(3).expect("model stride is non-zero"),
        stride_persistence: StridePersistence::Ahead,
        epoch_liveness: EpochLiveness::Required,
        successor_grace: SuccessorGrace::WaitsOutCertificate,
        client_visible_max: ClientVisibleMax::Overwritten,
    });

    assert!(
        checker
            .discoveries()
            .contains_key("tso_client_visible_timestamp_never_regresses_after_reply_delivery")
    );
}
