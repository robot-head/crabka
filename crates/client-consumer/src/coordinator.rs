//! Background coordinator task. It owns the join/sync/heartbeat/rebalance
//! lifecycle for a [`Consumer`](crate::consumer::Consumer).
//!
//! On each tick the task either sends a steady-state `Heartbeat` or runs a
//! full `JoinGroup` + `SyncGroup` round when `needs_rejoin` is set. The broker
//! signals a rebalance with `error_code = 27 (REBALANCE_IN_PROGRESS)`
//! on heartbeat. `25 (UNKNOWN_MEMBER_ID)` forces a from-scratch
//! handshake, which clears `member_id` and sets `generation_id = -1`.
//!
//! Cooperative rebalance (KIP-429) runs phase-1 and phase-2 in place. Phase 1
//! reduces the owned set to the partitions the member kept. The task then
//! re-Joins and re-Syncs at once, so the leader can place the freshly
//! freed partitions onto whoever needs them. Eager (`range`) drops the
//! whole assignment and reinstalls it in a single round.
//!
//! While a rejoin is in flight the task deliberately does *not* heartbeat.
//! `JoinGroup` resets the broker-side session timer.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::FindCoordinatorResponse,
        heartbeat_request::HeartbeatRequest,
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::JoinGroupResponse,
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        offset_commit_request::OffsetCommitRequest,
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use crabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt as _},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    assignor::{Assignor, RebalanceProtocol},
    builder::{
        AutoOffsetReset, decode_assignment, decode_subscription, encode_assignment,
        encode_subscription,
    },
    consumer::{ConsumerRetryPolicy, reset_starting_offset, starting_offset},
    error::ConsumerError,
    offset_wire::{build_commit_topics, build_offset_fetch, id_to_name, parse_offset_fetch},
};

/// Retriable group-coordinator error codes. The coordinator is loading its
/// state (`14`), not yet available (`15`), or has moved to another broker
/// (`16`). Kafka clients retry these with backoff rather than failing.
pub(crate) const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
pub(crate) const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub(crate) const NOT_COORDINATOR: i16 = 16;

const UNKNOWN_EPOCH: i32 = -1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CoordinatorRetryPolicy {
    pub timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl From<ConsumerRetryPolicy> for CoordinatorRetryPolicy {
    fn from(value: ConsumerRetryPolicy) -> Self {
        Self {
            timeout: value.coordinator_retry_timeout().to_std(),
            initial_backoff: value.coordinator_initial_backoff().to_std(),
            max_backoff: value.coordinator_max_backoff().to_std(),
        }
    }
}

pub(crate) fn is_retriable_coordinator_code(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

pub(crate) fn is_retriable_transport_error(e: &crabka_client_core::ClientError) -> bool {
    matches!(
        e,
        crabka_client_core::ClientError::Connect { .. }
            | crabka_client_core::ClientError::Disconnected
            | crabka_client_core::ClientError::Io(_)
    )
}

/// Read the effective `error_code` from a `FindCoordinatorResponse` across wire
/// shapes.
///
/// v4+ carries per-key rows in `coordinators`, and this function uses the
/// first row. v0-v3 uses the top-level field. crabka's broker populates both,
/// so either read is correct against it. This function stays correct against
/// real Kafka at any negotiated version.
fn coordinator_error_code(r: &FindCoordinatorResponse) -> i16 {
    r.coordinators
        .first()
        .map_or(r.error_code, |c| c.error_code)
}

/// Read the coordinator `node_id` from a `FindCoordinatorResponse` across wire
/// shapes: v4+ uses `coordinators[0].node_id`, and older versions use the
/// top-level `node_id`.
fn coordinator_node_id(r: &FindCoordinatorResponse) -> i32 {
    r.coordinators.first().map_or(r.node_id, |c| c.node_id)
}

/// Discover the broker that currently coordinates `group_id` and return its
/// node id.
///
/// This function sends `FindCoordinator(key = group_id)` over the bootstrap
/// connection, because any broker can answer `FindCoordinator`. It retries the
/// cold and loading coordinator codes (14/15/16) with backoff. On success it
/// calls `refresh_metadata`, so the pool learns the coordinator broker's
/// address from the cluster's broker list. Without that refresh,
/// [`Client::broker`](crabka_client_core::Client::broker) for the coordinator
/// id would fail with `Disconnected`.
///
/// This function returns the coordinator's `node_id`. It errors with
/// `Server(code)` if the lookup keeps returning a non-zero, non-retriable code.
/// It errors with `CoordinatorUnavailable` if, after the refresh, the pool
/// still has no dialable address for that id.
#[tracing::instrument(
    name = "consumer.find_coordinator",
    level = "info",
    skip_all,
    fields(group_id = %group_id, coordinator_id = tracing::field::Empty),
    err
)]
pub(crate) async fn find_coordinator(
    client: &Client,
    group_id: &str,
    retry: CoordinatorRetryPolicy,
) -> Result<i32, ConsumerError> {
    let resp = with_coordinator_retry(retry, coordinator_error_code, || {
        let group_id = group_id.to_string();
        async move {
            match client.send(build_find_coordinator_request(group_id)).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if is_retriable_transport_error(&e) {
                        client.reconnect_bootstrap().await;
                    }
                    Err(ConsumerError::from(e))
                }
            }
        }
    })
    .await?;

    let code = coordinator_error_code(&resp);
    if code != 0 {
        return Err(ConsumerError::Server(code));
    }
    let node_id = coordinator_node_id(&resp);

    // Refresh the pool's (id → addr) registry so a multi-broker cluster learns
    // the coordinator broker's real address and `client.broker(node_id)` dials
    // it directly. A single-broker cluster advertises the coordinator on port 0
    // (deliberately skipped by `refresh_brokers`), leaving the id unknown — but
    // `BrokerHandle::send` then falls back to the bootstrap connection, which on
    // a single-broker cluster IS the coordinator. So we no longer hard-fail when
    // the coordinator isn't a separately dialable broker.
    client.refresh_metadata().await?;
    tracing::Span::current().record("coordinator_id", node_id);
    Ok(node_id)
}

fn build_find_coordinator_request(group_id: String) -> FindCoordinatorRequest {
    FindCoordinatorRequest {
        key: group_id.clone(),
        // v4+ carries the key(s) in `coordinator_keys`; older versions ignore
        // it and use `key`. Populating both keeps us version-agnostic on the
        // negotiated wire form.
        coordinator_keys: vec![group_id],
        ..Default::default()
    }
}

fn retry_deadline_elapsed(start: tokio::time::Instant, timeout: Duration) -> bool {
    start.elapsed() >= timeout
}

fn next_backoff(backoff: Duration, max_backoff: Duration) -> Duration {
    (backoff * 2).min(max_backoff)
}

pub(crate) fn build_leave_group_request(
    group_id: String,
    member_id: String,
    group_instance_id: Option<String>,
) -> LeaveGroupRequest {
    LeaveGroupRequest {
        group_id,
        member_id: member_id.clone(),
        members: vec![MemberIdentity {
            member_id,
            group_instance_id,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn build_heartbeat_request(
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
) -> HeartbeatRequest {
    HeartbeatRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        ..Default::default()
    }
}

/// Send a group-coordinator RPC to the *current* coordinator broker, and
/// re-discover the coordinator before a retry on a cold or relocating
/// coordinator code (14/15/16).
///
/// This chases a moved coordinator to its new home instead of looping forever
/// on the stale id. Real Kafka returns `NOT_COORDINATOR` when an RPC reaches
/// the wrong broker. The plain `with_coordinator_retry` re-sends the identical
/// request to the same broker.
///
/// `coordinator_id` is the shared cell that `make` reads, so each retry targets
/// the latest id. Re-discovery updates that cell in place on success. `make`
/// does the `client.broker(id).send(...)` routing itself. This function mirrors
/// the deadline and backoff of `with_coordinator_retry`. The only addition is
/// the re-find between retriable attempts.
pub(crate) async fn with_coordinator_refind<R, F, Fut>(
    client: &Client,
    group_id: &str,
    coordinator_id: &AtomicI32,
    retry: CoordinatorRetryPolicy,
    code: impl Fn(&R) -> i16,
    make: F,
) -> Result<R, ConsumerError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<R, ConsumerError>>,
{
    let start = tokio::time::Instant::now();
    let mut backoff = retry.initial_backoff;
    loop {
        let needs_refind = match make().await {
            Ok(r) if !is_retriable_coordinator_code(code(&r)) => return Ok(r),
            Ok(r) => {
                if retry_deadline_elapsed(start, retry.timeout) {
                    return Ok(r);
                }
                // Retriable broker code: the coordinator likely moved.
                true
            }
            Err(ConsumerError::Client(e)) if is_retriable_transport_error(&e) => {
                if retry_deadline_elapsed(start, retry.timeout) {
                    return Err(ConsumerError::CoordinatorUnavailable);
                }
                // The socket to the coordinator is gone (bounced / failed
                // over); evict it so re-discovery + the next attempt reconnect
                // to the current coordinator's address.
                client.evict_broker(coordinator_id.load(Ordering::Relaxed));
                true
            }
            Err(e) => return Err(e),
        };
        if needs_refind {
            // Best-effort re-discovery. A transient failure here just means the
            // next attempt reuses the last-known id; the outer deadline (and
            // find_coordinator's own retry) still bound us.
            match find_coordinator(client, group_id, retry).await {
                Ok(id) => coordinator_id.store(id, Ordering::Relaxed),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "coordinator re-discovery failed; retrying with last-known id"
                    );
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff, retry.max_backoff);
    }
}

/// Send a group-coordinator RPC and retry on the cold-coordinator codes
/// (14/15/16) and on transient transport errors.
///
/// The retry uses capped exponential backoff until `timeout` elapses. `make`
/// rebuilds the request on each attempt, so the function can re-send it. `code`
/// reads the response's `error_code`. On the deadline this function returns the
/// last response, so the caller's `error_code` handling runs. It returns
/// `CoordinatorUnavailable` instead if the last attempt was a transport
/// failure.
pub(crate) async fn with_coordinator_retry<R, F, Fut>(
    retry: CoordinatorRetryPolicy,
    code: impl Fn(&R) -> i16,
    make: F,
) -> Result<R, ConsumerError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<R, ConsumerError>>,
{
    let start = tokio::time::Instant::now();
    let mut backoff = retry.initial_backoff;
    loop {
        match make().await {
            Ok(r) if !is_retriable_coordinator_code(code(&r)) => return Ok(r),
            Ok(r) => {
                if retry_deadline_elapsed(start, retry.timeout) {
                    return Ok(r);
                }
            }
            Err(ConsumerError::Client(e)) if is_retriable_transport_error(&e) => {
                if retry_deadline_elapsed(start, retry.timeout) {
                    return Err(ConsumerError::CoordinatorUnavailable);
                }
            }
            Err(e) => return Err(e),
        }
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff, retry.max_backoff);
    }
}

/// Mutable state owned exclusively by the coordinator task.
///
/// The `Arc<Mutex<...>>` fields are shared with the parent `Consumer`, so
/// `poll()` and `assignment()` see live updates as rebalances land. The plain
/// non-`Arc` fields belong to the coordinator alone, and it can mutate them
/// freely. `member_id` and `generation_id` change on a from-scratch rejoin.
pub(crate) struct CoordinatorState {
    pub client: Client,
    pub group_id: String,
    /// Node id of the broker that currently coordinates this group, discovered
    /// with `FindCoordinator`. Every group RPC (Join/Sync/Heartbeat/Commit/
    /// Fetch/Leave) routes here with `client.broker(coordinator_id)`. A
    /// coordinator RPC that returns 14/15/16 triggers re-discovery.
    ///
    /// This `Arc<AtomicI32>` is shared with the parent `Consumer`, so its commit
    /// path (`commit.rs`, on the data-path client) routes `OffsetCommit` to the
    /// same coordinator and sees re-discovery updates the moment they land.
    pub coordinator_id: Arc<AtomicI32>,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub generation_id: i32,
    /// Published copy of `generation_id`. This `Arc<AtomicI32>` is shared with
    /// the parent `Consumer`, so its commit path (`commit.rs`) stamps the
    /// CURRENT generation onto `OffsetCommit`. The coordinator is the sole
    /// writer. Always update both fields together with [`set_generation`], so a
    /// commit after a rebalance never carries the stale generation that the
    /// broker rejects with `ILLEGAL_GENERATION`.
    pub current_generation: Arc<AtomicI32>,
    pub assignor: Assignor,
    pub subscribed_topics: Vec<String>,
    pub assigned: Arc<Mutex<Vec<(String, i32)>>>,
    pub next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub positions: Arc<Mutex<HashMap<(String, i32), crate::position::PartitionPosition>>>,
    pub topic_ids: Arc<Mutex<HashMap<String, WireUuid>>>,
    pub session_timeout: Time,
    pub rebalance_timeout: Time,
    pub heartbeat_interval: Time,
    pub subscription_metadata_refresh_interval: Time,
    pub leave_group_timeout: Time,
    pub auto_offset_reset: AutoOffsetReset,
    pub client_rack: Option<String>,
    /// Subscribed-topic partition counts that the INITIAL assignment was
    /// computed against. This is the metadata snapshot that `start_once` already
    /// fetched. The coordinator seeds its rejoin baseline from this and not from
    /// a fresh post-spawn `Metadata` fetch. A fresh fetch could already include
    /// a topic created in the window between the initial assignment and the
    /// start of this task. It would then compare equal to the baseline forever
    /// and strand the empty cold-start assignment permanently.
    pub initial_subscribed_counts: HashMap<String, i32>,
    pub retry_policy: CoordinatorRetryPolicy,
}

/// Set the coordinator's working generation AND publish it to the shared atomic
/// that the commit path reads.
///
/// Use this for EVERY generation change: join, rejoin, and from-scratch reset.
/// The parent `Consumer`'s `OffsetCommit` then always stamps the current
/// generation. The broker rejects a stale generation with
/// `ILLEGAL_GENERATION`.
fn set_generation(state: &mut CoordinatorState, generation_id: i32) {
    state.generation_id = generation_id;
    state
        .current_generation
        .store(generation_id, Ordering::Relaxed);
}

/// Outcome of a single heartbeat RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeartbeatOutcome {
    /// `error_code == 0`.
    Ok,
    /// `REBALANCE_IN_PROGRESS (27)` or `ILLEGAL_GENERATION (22)`. Rejoin with
    /// the current `member_id`. `ILLEGAL_GENERATION` fires when the heartbeat
    /// tick lands after the broker has already advanced past the generation the
    /// member last synced on, for example when a rebalance completed between
    /// two heartbeat windows. Without a rejoin the member would keep
    /// heartbeating the dead generation forever and would never pick up the new
    /// assignment.
    NeedRejoin,
    /// `UNKNOWN_MEMBER_ID (25)`. Clear `member_id` and rejoin from scratch.
    RejoinFromScratch,
    /// Transport error or unexpected non-fatal broker code. Retry on the next tick.
    Transient,
}

fn heartbeat_outcome(error_code: i16) -> HeartbeatOutcome {
    match error_code {
        0 => HeartbeatOutcome::Ok,
        27 | 22 => HeartbeatOutcome::NeedRejoin,
        25 => HeartbeatOutcome::RejoinFromScratch,
        _ => HeartbeatOutcome::Transient,
    }
}

/// Drive the heartbeat + rebalance loop until `shutdown` fires.
///
/// On entry the caller has already done one initial Join+Sync, so the loop
/// begins in steady-state heartbeating. `needs_rejoin` becomes `true` as soon
/// as the broker signals a rebalance. The next tick then does the rejoin in
/// place of the heartbeat.
#[cfg_attr(test, mutants::skip)] // cargo-mutants: long-running I/O event loop, exercised by integration tests
fn subscription_metadata_refresh_due(last_check: tokio::time::Instant, interval: Time) -> bool {
    last_check.elapsed().as_time() >= interval
}

/// Current partition count of each subscribed topic that exists in broker
/// metadata.
///
/// A subscribed topic that does not exist yet is absent from the map. It shows
/// up later as growth once someone creates the topic.
#[tracing::instrument(
    name = "consumer.subscribed_partition_counts",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, topics = tracing::field::Empty),
    err
)]
async fn subscribed_partition_counts(
    state: &CoordinatorState,
) -> Result<HashMap<String, i32>, ConsumerError> {
    let md = state.client.refresh_metadata().await?;
    let mut counts = HashMap::new();
    for t in &md.topics {
        let Some(name) = &t.name else { continue };
        if state.subscribed_topics.iter().any(|s| s == name) {
            counts.insert(
                name.clone(),
                i32::try_from(t.partitions.len()).unwrap_or(i32::MAX),
            );
        }
    }
    tracing::Span::current().record("topics", counts.len());
    Ok(counts)
}

/// True when any subscribed topic now has more partitions than the assignment
/// was last computed against, which means a topic appeared or grew.
///
/// Such a change means the group must rejoin, so the assignor redistributes the
/// new partitions. Without the rejoin, a consumer that joined before its WAL
/// topic existed keeps an EMPTY assignment forever. The broker never sends a
/// rebalance to a single-member Stable group, and that rebalance is the only
/// other thing that sets `needs_rejoin`.
fn subscribed_topics_grew(known: &HashMap<String, i32>, current: &HashMap<String, i32>) -> bool {
    current
        .iter()
        .any(|(topic, count)| *count > known.get(topic).copied().unwrap_or(0))
}

/// Fold `current` into `known` and take the per-topic max.
///
/// Kafka partition counts are monotonic, because a topic never loses
/// partitions. The rejoin baseline must therefore only ever ADVANCE. A
/// transient metadata under-report from a controller failover or a partial
/// response must never lower it and re-trigger a spurious rejoin. A non-leader
/// rejoin, whose snapshot is empty, must leave the baseline untouched and must
/// not erase it.
fn merge_counts(known: &mut HashMap<String, i32>, current: &HashMap<String, i32>) {
    for (topic, &count) in current {
        let entry = known.entry(topic.clone()).or_insert(0);
        *entry = (*entry).max(count);
    }
}

pub(crate) async fn run(mut state: CoordinatorState, shutdown: CancellationToken) {
    // The coordinator task runs on its own `Client` (separate pool from the
    // build/data-path client), so its pool's (id → addr) registry starts empty.
    // Populate it once up front so the very first heartbeat's
    // `client.broker(coordinator_id)` resolves an address instead of failing
    // `Disconnected` and burning a heartbeat interval on re-discovery. The id
    // was already discovered at build time; this just teaches *this* pool where
    // that broker lives. Best-effort: a failure here is recovered by the
    // heartbeat path's Disconnected → re-discover handling.
    if let Err(e) = state.client.refresh_metadata().await {
        tracing::warn!(error = %e, "coordinator client metadata refresh failed at startup");
    }

    let mut ticker = tokio::time::interval(state.heartbeat_interval.to_std());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut needs_rejoin = false;
    // Subscribed-topic partition counts the current assignment was computed
    // against. We rejoin when these GROW (a topic created after we joined, or a
    // topic that gains partitions) so the assignor distributes the new
    // partitions. Seeded from the snapshot the INITIAL assignment used (threaded
    // in as `initial_subscribed_counts`), NOT a fresh fetch here: a fresh fetch
    // could already include a topic created in the window between that initial
    // Metadata and this task starting, comparing equal to the baseline forever
    // and stranding the empty cold-start assignment permanently. Advanced only
    // ever via `merge_counts` (monotonic max), so a transient metadata blip
    // can't lower it. This is what lets a consumer that joined before its WAL
    // topic existed (empty assignment) recover once the topic is created.
    let mut known_counts = std::mem::take(&mut state.initial_subscribed_counts);
    let mut last_meta_check = tokio::time::Instant::now();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }

        // Detect a subscribed topic appearing / gaining partitions after we
        // joined (the cold-start race that otherwise strands an empty
        // assignment) and rejoin to distribute it. Throttled, and only when not
        // already rejoining. Best-effort: a failed metadata RPC just retries.
        if !needs_rejoin
            && subscription_metadata_refresh_due(
                last_meta_check,
                state.subscription_metadata_refresh_interval,
            )
        {
            last_meta_check = tokio::time::Instant::now();
            if let Ok(current) = subscribed_partition_counts(&state).await
                && subscribed_topics_grew(&known_counts, &current)
            {
                tracing::info!(
                    group = %state.group_id,
                    "subscribed-topic partitions changed; rejoining to update assignment"
                );
                // Don't advance `known_counts` here against this fresh fetch —
                // it could record partitions the rejoin doesn't end up assigning
                // (e.g. a leader whose Metadata lags this read). Advance only
                // once the rejoin lands, from the snapshot its assignment was
                // actually computed against (the Ok branch below).
                needs_rejoin = true;
            }
        }

        // Race the per-tick RPCs against shutdown so `close()` returns
        // promptly even when we're mid-rebalance and the broker is holding
        // a JoinGroup / SyncGroup open. Without this, cancellation is only
        // observed *between* ticks, so a `rejoin()` in flight against an
        // open broker call would stall `close()` for up to session_timeout.
        // The RPC futures are cancellation-safe: `Client` multiplexes on
        // correlation ids, so dropping an in-flight send only abandons its
        // pending response — it can't corrupt the connection.
        if needs_rejoin {
            tokio::select! {
                () = shutdown.cancelled() => break,
                result = rejoin(&mut state) => match result {
                    Ok(snapshot) => {
                        needs_rejoin = false;
                        // Re-baseline from the metadata the rejoin's assignment
                        // was actually computed against (the leader's snapshot;
                        // empty for a non-leader, which `merge_counts` leaves
                        // untouched) — NOT a third independent fetch, which could
                        // read a newer count than was assigned and strand the
                        // difference. Monotonic max-merge: only ever advance.
                        merge_counts(&mut known_counts, &snapshot);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "rejoin failed; will retry on next tick");
                    }
                },
            }
        } else {
            tokio::select! {
                () = shutdown.cancelled() => break,
                outcome = heartbeat_once(&state) => match outcome {
                    HeartbeatOutcome::Ok | HeartbeatOutcome::Transient => {}
                    HeartbeatOutcome::NeedRejoin => needs_rejoin = true,
                    HeartbeatOutcome::RejoinFromScratch => {
                        state.member_id.clear();
                        set_generation(&mut state, -1);
                        needs_rejoin = true;
                    }
                },
            }
        }
    }

    // Graceful departure: tell the broker to evict us *now* rather than
    // waiting out `session_timeout`. This MUST use `state.member_id`, which
    // is the live id — a from-scratch rejoin (`UNKNOWN_MEMBER_ID`) replaces
    // it mid-life, so the `Consumer`'s build-time copy can be stale. Leaving
    // with a stale id is a silent no-op that orphans the real member until
    // its session expires, stalling the rest of the group's rebalance.
    // Best-effort and bounded: a hung broker must not block `close()`.
    leave_group(&state).await;
}

/// Best-effort `LeaveGroup` for the coordinator's *current* member id.
///
/// The task sends it once, on shutdown, with a short timeout. A broker that
/// falls back to session-timeout eviction is harmless on close, but a stalled
/// send would hang `close()`, which awaits this task. This mirrors the Java
/// client, which leaves the group on close for dynamic members. This function
/// skips a cleared id, which comes from a from-scratch rejoin that never
/// re-completed, and which the broker would not recognize anyway.
#[cfg_attr(test, mutants::skip)] // cargo-mutants: best-effort shutdown I/O, exercised by integration tests
#[tracing::instrument(
    name = "consumer.leave_group",
    level = "info",
    skip_all,
    fields(group_id = %state.group_id, member_id = %state.member_id)
)]
async fn leave_group(state: &CoordinatorState) {
    if state.member_id.is_empty() {
        return;
    }
    // `member_id` is populated for both the v0–v2 (top-level) and v3+
    // (`members` array) wire shapes so the negotiated version picks up
    // whichever it serializes. Routed to the coordinator broker like every
    // other group RPC; on close it's best-effort, so a stale/unknown
    // coordinator id (Disconnected) just falls back to session-timeout
    // eviction — no re-discovery is worth the wall-clock on shutdown.
    let coordinator = state
        .client
        .broker(state.coordinator_id.load(Ordering::Relaxed));
    let send = coordinator.send(build_leave_group_request(
        state.group_id.clone(),
        state.member_id.clone(),
        state.group_instance_id.clone(),
    ));
    let _ = tokio::time::timeout(state.leave_group_timeout.to_std(), send).await;
}

/// Send one `Heartbeat` to the coordinator broker and translate the response
/// into a directive.
///
/// A cold or relocating coordinator code (14/15/16) triggers in-place
/// re-discovery, because the coordinator moved and the next tick's heartbeat or
/// rejoin must target the new broker. This function reports such a code as
/// `Transient`, so the task simply retries on the next tick.
#[tracing::instrument(
    name = "consumer.heartbeat",
    level = "debug",
    skip_all,
    fields(
        group_id = %state.group_id,
        member_id = %state.member_id,
        generation = state.generation_id,
    )
)]
async fn heartbeat_once(state: &CoordinatorState) -> HeartbeatOutcome {
    let result = state
        .client
        .broker(state.coordinator_id.load(Ordering::Relaxed))
        .send(build_heartbeat_request(
            state.group_id.clone(),
            state.generation_id,
            state.member_id.clone(),
            state.group_instance_id.clone(),
        ))
        .await;
    match result {
        Ok(r) => {
            let outcome = heartbeat_outcome(r.error_code);
            if matches!(outcome, HeartbeatOutcome::Transient) {
                if is_retriable_coordinator_code(r.error_code) {
                    refind_after(state, "heartbeat").await;
                } else {
                    tracing::warn!(error_code = r.error_code, "unexpected heartbeat error");
                }
            }
            outcome
        }
        Err(e) if is_retriable_transport_error(&e) => {
            // Lost the socket to the coordinator (bounced / failed over):
            // evict + re-discover so the next tick reconnects to its current
            // address.
            state
                .client
                .evict_broker(state.coordinator_id.load(Ordering::Relaxed));
            refind_after(state, "heartbeat").await;
            HeartbeatOutcome::Transient
        }
        Err(e) => {
            tracing::warn!(error = %e, "heartbeat send failed");
            HeartbeatOutcome::Transient
        }
    }
}

/// Best-effort coordinator re-discovery for use off the heartbeat path, which
/// cannot surface an error.
///
/// On success this function publishes the new id into the shared
/// `coordinator_id` cell. On failure it logs and keeps the last-known id, and
/// the next tick retries.
#[cfg_attr(test, mutants::skip)] // cargo-mutants: best-effort discovery I/O, exercised by integration tests
async fn refind_after(state: &CoordinatorState, ctx: &str) {
    match find_coordinator(&state.client, &state.group_id, state.retry_policy).await {
        Ok(id) => state.coordinator_id.store(id, Ordering::Relaxed),
        Err(e) => {
            tracing::warn!(error = %e, context = ctx, "coordinator re-discovery failed");
        }
    }
}

/// Run one complete rebalance round, Join and Sync, then mutate the shared
/// `assigned` and `next_offsets` snapshots in place.
///
/// For [`RebalanceProtocol::Cooperative`] this can issue *two* Join+Sync rounds
/// back-to-back. The first installs the kept partitions only. The second, phase
/// 2, receives the freshly placed ones. See KIP-429.
#[tracing::instrument(
    name = "consumer.rejoin",
    level = "info",
    skip_all,
    fields(
        group_id = %state.group_id,
        member_id = %state.member_id,
        protocol = ?state.assignor.rebalance_protocol(),
        generation = tracing::field::Empty,
        revoked = tracing::field::Empty,
        added = tracing::field::Empty,
    ),
    err
)]
async fn rejoin(state: &mut CoordinatorState) -> Result<HashMap<String, i32>, ConsumerError> {
    let owned: Vec<(String, i32)> = state.assigned.lock().await.clone();
    let JoinOutcome {
        assignment: new_assignment,
        generation: new_generation,
        topic_partitions,
        ..
    } = join_and_sync(state, &owned).await?;

    let old_set: HashSet<(String, i32)> = owned.iter().cloned().collect();
    let new_set: HashSet<(String, i32)> = new_assignment.iter().cloned().collect();
    let revoked: Vec<(String, i32)> = old_set.difference(&new_set).cloned().collect();
    let added: Vec<(String, i32)> = new_set.difference(&old_set).cloned().collect();
    let span = tracing::Span::current();
    span.record("generation", new_generation);
    span.record("revoked", revoked.len());
    span.record("added", added.len());

    // The subscribed-topic partition snapshot the FINAL published assignment was
    // computed against — returned so the coordinator re-baselines against exactly
    // what it assigned (eager / pure-add use the round-1 snapshot; a cooperative
    // revoke uses phase 2's).
    let final_counts = match state.assignor.rebalance_protocol() {
        RebalanceProtocol::Eager => {
            // Drop everything and reinstall in a single round. Prime the
            // added partitions' fetch offsets *before* publishing the new
            // assignment: `poll()` defaults an assigned-but-unprimed
            // partition to offset 0 (poll.rs's `unwrap_or(0)`), so a poll
            // racing between the `assigned` publish and the prime would
            // re-fetch from 0 and re-deliver already-consumed records. Prime
            // first → a partition is only visible in `assigned` once its
            // next_offset is established.
            prime_offsets(state, &added).await?;
            {
                let mut a = state.assigned.lock().await;
                a.clone_from(&new_assignment);
            }
            {
                let mut off = state.next_offsets.lock().await;
                off.retain(|k, _| new_set.contains(k));
                // Prune the KIP-320 position sidecar in lockstep so stale
                // epoch metadata for dropped partitions doesn't accumulate.
                let mut pos = state.positions.lock().await;
                pos.retain(|k, _| new_set.contains(k));
            }
            set_generation(state, new_generation);
            topic_partitions
        }
        RebalanceProtocol::Cooperative => {
            if revoked.is_empty() {
                // Pure additions: merge into the existing assigned set.
                // No phase 2 needed because no member needed to revoke.
                //
                // Prime the added partitions' fetch offsets *before*
                // publishing them into `assigned`: a `poll()` racing the
                // rebalance would otherwise see an assigned-but-unprimed
                // partition and fetch it from offset 0 (poll.rs's
                // `unwrap_or(0)`), re-delivering records the previous owner
                // already committed past at revoke time.
                prime_offsets(state, &added).await?;
                {
                    let mut a = state.assigned.lock().await;
                    for p in &added {
                        if !a.contains(p) {
                            a.push(p.clone());
                        }
                    }
                }
                set_generation(state, new_generation);
                topic_partitions
            } else {
                // Phase 1: drop the partitions we're losing, then
                // immediately rejoin so the leader can place them on
                // whoever needs them in phase 2. Keeping kept partitions
                // active throughout is the whole point of KIP-429.
                {
                    let mut a = state.assigned.lock().await;
                    a.retain(|p| !revoked.contains(p));
                }
                // Adopt the generation from round 1 *before* committing: the
                // broker advanced the group epoch when we rejoined above, so
                // an OffsetCommit carrying the pre-rebalance generation is
                // rejected with ILLEGAL_GENERATION. Commit the revoked
                // partitions' positions under the current generation so the
                // member that picks them up in phase 2 primes from the offset
                // we'd consumed to, rather than re-delivering records we
                // already saw (KIP-429 onPartitionsRevoked semantics).
                set_generation(state, new_generation);
                commit_revoked(state, &revoked).await;
                {
                    let mut off = state.next_offsets.lock().await;
                    let mut pos = state.positions.lock().await;
                    for p in &revoked {
                        off.remove(p);
                        // Prune the KIP-320 position sidecar in lockstep.
                        pos.remove(p);
                    }
                }

                // Phase 2: rejoin with the reduced owned-set.
                let owned_after_revoke: Vec<(String, i32)> = state.assigned.lock().await.clone();
                let JoinOutcome {
                    assignment: assignment2,
                    generation: gen2,
                    topic_partitions: topic_partitions2,
                    ..
                } = join_and_sync(state, &owned_after_revoke).await?;
                let owned_after_revoke_set: HashSet<(String, i32)> =
                    owned_after_revoke.iter().cloned().collect();
                let added2: Vec<(String, i32)> = assignment2
                    .iter()
                    .filter(|p| !owned_after_revoke_set.contains(*p))
                    .cloned()
                    .collect();
                // Prime the freshly placed partitions *before* publishing the
                // phase-2 assignment, so a poll racing the rebalance can't
                // observe them in `assigned` without a primed next_offset and
                // fetch from 0 (poll.rs). That primed value is the offset the
                // revoking member committed at revoke time; fetching from 0
                // instead would re-deliver the records it already consumed.
                prime_offsets(state, &added2).await?;
                {
                    let mut a = state.assigned.lock().await;
                    *a = assignment2;
                }
                set_generation(state, gen2);
                topic_partitions2
            }
        }
    };
    Ok(final_counts)
}

/// Best-effort `OffsetCommit` for the partitions that a cooperative rebalance
/// revokes. It uses the current, pre-rebalance generation.
///
/// This function logs and swallows failures. A revoke-time commit that races
/// the generation bump can return `ILLEGAL_GENERATION`, and surfacing that into
/// `poll()` would break the KIP-429 transparency guarantee. In the worst case
/// the new owner re-delivers a few records, which is at-least-once.
#[cfg_attr(test, mutants::skip)] // cargo-mutants: best-effort revoke-time commit I/O, exercised by integration tests
#[tracing::instrument(
    name = "consumer.commit_revoked",
    level = "debug",
    skip_all,
    fields(
        group_id = %state.group_id,
        generation = state.generation_id,
        revoked = revoked.len(),
    )
)]
async fn commit_revoked(state: &CoordinatorState, revoked: &[(String, i32)]) {
    let revoked_set: HashSet<&(String, i32)> = revoked.iter().collect();
    let offsets: HashMap<(String, i32), (i64, i32)> = {
        let off = state.next_offsets.lock().await;
        let pos = state.positions.lock().await;
        off.iter()
            // Only commit partitions where we actually consumed something. A
            // next_offset still at its reset baseline (0 = Earliest, i64::MAX =
            // Latest) means no records were polled, so there is no progress to
            // preserve — committing it just adds a blocking round-trip that
            // widens the mid-rebalance generation-race window.
            .filter(|(k, v)| should_commit_revoked_offset(revoked_set.contains(k), **v))
            .map(|(k, v)| {
                // Unwrap the position's leader epoch to raw wire `int32` for the
                // revoke-time OffsetCommit `committed_leader_epoch` field.
                let epoch = pos.get(k).map_or(UNKNOWN_EPOCH, |p| p.offset_epoch.get());
                (k.clone(), (*v, epoch))
            })
            .collect()
    };
    if offsets.is_empty() {
        return;
    }
    let topic_ids = state.topic_ids.lock().await.clone();
    let topics = build_commit_topics(offsets, &topic_ids);
    let res = state
        .client
        .broker(state.coordinator_id.load(Ordering::Relaxed))
        .send(build_revoked_commit_request(
            state.group_id.clone(),
            state.generation_id,
            state.member_id.clone(),
            state.group_instance_id.clone(),
            topics,
        ))
        .await;
    match res {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, "revoke-time offset commit failed; partitions may re-deliver");
        }
    }
}

fn should_commit_revoked_offset(is_revoked: bool, next_offset: i64) -> bool {
    is_revoked && next_offset > 0 && next_offset != i64::MAX
}

fn build_revoked_commit_request(
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
    topics: Vec<crabka_protocol::owned::offset_commit_request::OffsetCommitRequestTopic>,
) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id,
        generation_id_or_member_epoch: generation_id,
        member_id,
        group_instance_id,
        topics,
        ..Default::default()
    }
}

fn build_join_group_request(
    group_id: String,
    member_id: String,
    group_instance_id: Option<String>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    protocol_name: String,
    subscription_bytes: Bytes,
) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id,
        protocol_type: "consumer".into(),
        member_id,
        group_instance_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        protocols: vec![JoinGroupRequestProtocol {
            name: protocol_name,
            metadata: subscription_bytes,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn build_sync_group_assignment(
    member_id: String,
    partitions: &[(String, i32)],
) -> SyncGroupRequestAssignment {
    SyncGroupRequestAssignment {
        member_id,
        assignment: encode_assignment(partitions),
        ..Default::default()
    }
}

fn build_sync_group_request(
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
    chosen_protocol: String,
    assignments: Vec<SyncGroupRequestAssignment>,
) -> SyncGroupRequest {
    SyncGroupRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        protocol_type: Some("consumer".into()),
        protocol_name: Some(chosen_protocol),
        assignments,
        ..Default::default()
    }
}

struct JoinOutcome {
    assignment: Vec<(String, i32)>,
    generation: i32,
    topic_partitions: HashMap<String, i32>,
}

async fn perform_join(
    state: &mut CoordinatorState,
    owned: &[(String, i32)],
) -> Result<JoinGroupResponse, ConsumerError> {
    // Truncating, not rounding: these are `JoinGroupRequest` `int32`
    // milliseconds the coordinator range-checks, and `Duration::as_millis`
    // truncated here before the conversion.
    let session_timeout_ms = crate::consumer::protocol_millis_i32(state.session_timeout);
    let rebalance_timeout_ms = crate::consumer::protocol_millis_i32(state.rebalance_timeout);

    let subscription_bytes = encode_subscription(
        &state.subscribed_topics,
        owned,
        state.generation_id,
        state.client_rack.as_deref(),
    );
    let protocol_name = state.assignor.protocol_name().to_string();

    // Pull the pieces every retry closure needs out of `&mut state` into locals
    // so `with_coordinator_refind` can borrow the shared coordinator cell
    // alongside the closures' borrows without aliasing `state`. `Client` and
    // the `Arc<AtomicI32>` are both cheap to clone; the atomic is the same cell
    // `state.coordinator_id` points at, so re-discovery updates are visible to
    // the rest of the task and the parent `Consumer` immediately.
    let client = state.client.clone();
    let group_id = state.group_id.clone();
    let group_instance_id = state.group_instance_id.clone();
    let coordinator_id = Arc::clone(&state.coordinator_id);

    // First join: if we have no member_id, expect MEMBER_ID_REQUIRED (79) and
    // capture the broker-assigned id, then issue a second join. Retry a cold or
    // relocating coordinator (14/15/16) with backoff, re-discovering the
    // coordinator before each retry so a moved coordinator is chased rather
    // than re-hit on the stale broker.
    let r1 = with_coordinator_refind(
        &client,
        &group_id,
        &coordinator_id,
        state.retry_policy,
        |r: &JoinGroupResponse| r.error_code,
        || {
            let group_id = group_id.clone();
            let member_id = state.member_id.clone();
            let protocol_name = protocol_name.clone();
            let subscription_bytes = subscription_bytes.clone();
            let group_instance_id = group_instance_id.clone();
            let client = &client;
            let target = coordinator_id.load(Ordering::Relaxed);
            async move {
                client
                    .broker(target)
                    .send(build_join_group_request(
                        group_id,
                        member_id,
                        group_instance_id.clone(),
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        protocol_name,
                        subscription_bytes,
                    ))
                    .await
                    .map_err(ConsumerError::from)
            }
        },
    )
    .await?;
    let join_resp = if r1.error_code == 0 {
        r1
    } else if r1.error_code == 79 {
        let assigned_id = r1.member_id.clone();
        if assigned_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed(
                "broker did not assign a member_id".into(),
            ));
        }
        state.member_id.clone_from(&assigned_id);
        let r2 = with_coordinator_refind(
            &client,
            &group_id,
            &coordinator_id,
            state.retry_policy,
            |r: &JoinGroupResponse| r.error_code,
            || {
                let group_id = group_id.clone();
                let assigned_id = assigned_id.clone();
                let protocol_name = protocol_name.clone();
                let subscription_bytes = subscription_bytes.clone();
                let group_instance_id = group_instance_id.clone();
                let client = &client;
                let target = coordinator_id.load(Ordering::Relaxed);
                async move {
                    client
                        .broker(target)
                        .send(build_join_group_request(
                            group_id,
                            assigned_id,
                            group_instance_id.clone(),
                            session_timeout_ms,
                            rebalance_timeout_ms,
                            protocol_name,
                            subscription_bytes,
                        ))
                        .await
                        .map_err(ConsumerError::from)
                }
            },
        )
        .await?;
        if r2.error_code != 0 {
            return Err(ConsumerError::Server(r2.error_code));
        }
        r2
    } else {
        return Err(ConsumerError::Server(r1.error_code));
    };

    Ok(join_resp)
}

/// Issue `JoinGroup`, assign as leader if this member won the election, then
/// issue `SyncGroup`.
///
/// This function handles the `MEMBER_ID_REQUIRED` two-step when `member_id` is
/// empty. It returns `(assignment, generation_id, protocol_name)`.
// Sequential join/sync state machine; splitting fragments the linear
// MEMBER_ID_REQUIRED → leader-assign → SyncGroup flow.
#[tracing::instrument(
    name = "consumer.join_and_sync",
    level = "info",
    skip_all,
    fields(
        group_id = %state.group_id,
        member_id = tracing::field::Empty,
        generation = tracing::field::Empty,
        is_leader = tracing::field::Empty,
        protocol = tracing::field::Empty,
        assigned_partitions = tracing::field::Empty,
    ),
    err
)]
async fn join_and_sync(
    state: &mut CoordinatorState,
    owned: &[(String, i32)],
) -> Result<JoinOutcome, ConsumerError> {
    let join_resp = perform_join(state, owned).await?;
    // The broker may have refreshed our member_id on this join too.
    if !join_resp.member_id.is_empty() {
        state.member_id.clone_from(&join_resp.member_id);
    }
    let chosen_protocol = join_resp
        .protocol_name
        .clone()
        .unwrap_or_else(|| state.assignor.protocol_name().to_string());
    let generation_id = join_resp.generation_id;

    // Leader: resolve partition counts via Metadata and run the assignor.
    let is_leader = join_resp.leader == state.member_id;
    {
        let span = tracing::Span::current();
        span.record("member_id", state.member_id.as_str());
        span.record("generation", generation_id);
        span.record("is_leader", is_leader);
        span.record("protocol", chosen_protocol.as_str());
    }
    // Subscribed-topic partition counts the assignment is computed against,
    // captured from the SAME Metadata the leader runs the assignor on (empty for
    // a non-leader). Returned so the coordinator's rejoin baseline tracks exactly
    // what was assigned rather than a divergent later fetch.
    let leader = compute_leader_assignment(state, &join_resp, is_leader).await?;
    let my_assignment =
        sync_assignment(state, generation_id, &chosen_protocol, leader.assignments).await?;
    tracing::Span::current().record("assigned_partitions", my_assignment.len());
    Ok(JoinOutcome {
        assignment: my_assignment,
        generation: generation_id,
        topic_partitions: leader.topic_partitions,
    })
}

struct LeaderAssignment {
    assignments: Vec<SyncGroupRequestAssignment>,
    topic_partitions: HashMap<String, i32>,
}

async fn compute_leader_assignment(
    state: &CoordinatorState,
    response: &JoinGroupResponse,
    is_leader: bool,
) -> Result<LeaderAssignment, ConsumerError> {
    if !is_leader {
        return Ok(LeaderAssignment {
            assignments: Vec::new(),
            topic_partitions: HashMap::new(),
        });
    }
    let metadata = state.client.refresh_metadata().await?;
    let mut topic_partitions = HashMap::new();
    let mut resolved_ids = HashMap::new();
    for topic in &metadata.topics {
        let Some(name) = &topic.name else { continue };
        if state
            .subscribed_topics
            .iter()
            .any(|subscribed| subscribed == name)
        {
            topic_partitions.insert(
                name.clone(),
                i32::try_from(topic.partitions.len()).unwrap_or(i32::MAX),
            );
            resolved_ids.insert(name.clone(), topic.topic_id);
        }
    }
    state.topic_ids.lock().await.extend(resolved_ids);
    let decoded: Vec<(String, crate::builder::DecodedSubscription)> = response
        .members
        .iter()
        .map(|member| {
            (
                member.member_id.clone(),
                decode_subscription(&member.metadata),
            )
        })
        .collect();
    let assignments = match state.assignor {
        Assignor::Range => {
            let inputs: Vec<(String, Vec<String>)> = decoded
                .into_iter()
                .map(|(id, subscription)| (id, subscription.topics))
                .collect();
            crate::assignor::range::assign(inputs, &topic_partitions)
        }
        Assignor::CooperativeSticky => {
            let inputs: Vec<crate::assignor::cooperative_sticky::MemberInput> = decoded
                .into_iter()
                .map(|(id, subscription)| {
                    (
                        id,
                        subscription.topics,
                        subscription.owned,
                        subscription.generation_id,
                    )
                })
                .collect();
            crate::assignor::cooperative_sticky::assign(&inputs, &topic_partitions)
        }
    };
    Ok(LeaderAssignment {
        assignments: assignments
            .into_iter()
            .map(|(member, partitions)| build_sync_group_assignment(member, &partitions))
            .collect(),
        topic_partitions,
    })
}

async fn sync_assignment(
    state: &CoordinatorState,
    generation_id: i32,
    protocol: &str,
    assignments: Vec<SyncGroupRequestAssignment>,
) -> Result<Vec<(String, i32)>, ConsumerError> {
    let response = with_coordinator_refind(
        &state.client,
        &state.group_id,
        &state.coordinator_id,
        state.retry_policy,
        |response: &SyncGroupResponse| response.error_code,
        || {
            let request = build_sync_group_request(
                state.group_id.clone(),
                generation_id,
                state.member_id.clone(),
                state.group_instance_id.clone(),
                protocol.to_string(),
                assignments.clone(),
            );
            let target = state.coordinator_id.load(Ordering::Relaxed);
            async move {
                state
                    .client
                    .broker(target)
                    .send(request)
                    .await
                    .map_err(ConsumerError::from)
            }
        },
    )
    .await?;
    if response.error_code != 0 {
        return Err(ConsumerError::Server(response.error_code));
    }
    Ok(decode_assignment(&response.assignment))
}

/// Populate `next_offsets` for newly added partitions with a batch fetch of the
/// committed offsets.
///
/// When no commit exists, this function falls back to `auto.offset.reset`
/// semantics. It mirrors the initial prime in `consumer.rs::start` step 5.
#[tracing::instrument(
    name = "consumer.prime_offsets",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, partitions = partitions.len()),
    err
)]
async fn prime_offsets(
    state: &CoordinatorState,
    partitions: &[(String, i32)],
) -> Result<(), ConsumerError> {
    if partitions.is_empty() {
        return Ok(());
    }
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for (t, p) in partitions {
        by_topic.entry(t.clone()).or_default().push(*p);
    }
    let topic_ids = state.topic_ids.lock().await.clone();
    // OffsetFetch is a coordinator RPC. `prime_offsets` only runs right after a
    // successful join/sync (which just discovered/refreshed `coordinator_id`),
    // so the id is fresh; route straight to it.
    let of = state
        .client
        .broker(state.coordinator_id.load(Ordering::Relaxed))
        .send(build_offset_fetch(&state.group_id, &by_topic, &topic_ids))
        .await?;

    let id_to_name = id_to_name(&topic_ids);
    let mut offsets = state.next_offsets.lock().await;
    let mut positions = state.positions.lock().await;
    let mut seen: HashSet<(String, i32)> = HashSet::new();
    for (name, partition_index, committed, committed_epoch) in parse_offset_fetch(&of, &id_to_name)
    {
        let starting = starting_offset(committed, state.auto_offset_reset);
        let key = (name, partition_index);
        seen.insert(key.clone());
        offsets.insert(key.clone(), starting);
        // Wrap the committed leader epoch (raw wire `int32` from OffsetFetch) at
        // the decode boundary.
        positions.entry(key).or_default().offset_epoch = crabka_ids::LeaderEpoch(committed_epoch);
    }
    // The broker may omit partitions that have no commit record at all;
    // ensure every requested partition has an entry so poll() can find it.
    for tp in partitions {
        if should_prime_missing_partition(seen.contains(tp)) {
            let starting = reset_starting_offset(state.auto_offset_reset);
            offsets.insert(tp.clone(), starting);
            positions.entry(tp.clone()).or_default();
        }
    }
    Ok(())
}

fn should_prime_missing_partition(seen: bool) -> bool {
    !seen
}

#[cfg(test)]
mod retry_tests {
    use std::{
        io,
        net::SocketAddr,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use crabka_client_core::MockBroker;
    use crabka_protocol::{
        Encode, UnknownTaggedFields,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            leave_group_request,
        },
    };
    use crabka_units::{millis, minutes, secs};

    use super::*;

    fn retry(timeout: Duration) -> CoordinatorRetryPolicy {
        CoordinatorRetryPolicy {
            timeout,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }

    struct Resp {
        error_code: i16,
    }

    fn refused_connect_error() -> ConsumerError {
        ConsumerError::Client(crabka_client_core::ClientError::Connect {
            addr: SocketAddr::from(([127, 0, 0, 1], 9092)),
            source: io::Error::new(io::ErrorKind::ConnectionRefused, "refused"),
        })
    }

    fn api_versions_for_leave_group() -> Vec<u8> {
        let response = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 3,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: leave_group_request::API_KEY,
                    min_version: 0,
                    max_version: leave_group_request::MAX_VERSION,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buffer = bytes::BytesMut::new();
        response
            .encode(&mut buffer, 0)
            .expect("encode API versions");
        buffer.to_vec()
    }

    #[tokio::test(start_paused = true)]
    async fn subscription_metadata_refresh_due_uses_configured_inclusive_boundary() {
        let last_check = tokio::time::Instant::now();
        let interval = millis(37);

        tokio::time::advance(Duration::from_millis(36)).await;
        assert2::assert!(!subscription_metadata_refresh_due(last_check, interval));

        tokio::time::advance(Duration::from_millis(1)).await;
        assert2::assert!(subscription_metadata_refresh_due(last_check, interval));
    }

    #[tokio::test]
    async fn coordinator_leave_group_uses_configured_timeout() {
        let saw_leave = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_leave_in_mock = Arc::clone(&saw_leave);
        let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                Some(api_versions_for_leave_group())
            } else if api_key == leave_group_request::API_KEY {
                saw_leave_in_mock.store(true, Ordering::SeqCst);
                None
            } else {
                None
            }
        })
        .await;
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(crabka_units::secs(5))
            .build()
            .await
            .expect("client");
        let state = CoordinatorState {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            member_id: "member-a".into(),
            group_instance_id: None,
            generation_id: 1,
            current_generation: Arc::new(AtomicI32::new(1)),
            assignor: Assignor::Range,
            subscribed_topics: vec!["topic".into()],
            assigned: Arc::new(Mutex::new(Vec::new())),
            next_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::new())),
            session_timeout: secs(45),
            rebalance_timeout: minutes(1),
            heartbeat_interval: secs(3),
            subscription_metadata_refresh_interval: millis(37),
            leave_group_timeout: millis(37),
            auto_offset_reset: AutoOffsetReset::Latest,
            client_rack: None,
            initial_subscribed_counts: HashMap::new(),
            retry_policy: retry(Duration::from_secs(30)),
        };

        tokio::time::timeout(Duration::from_secs(1), leave_group(&state))
            .await
            .expect("configured leave deadline bounds coordinator shutdown");
        mock.stop();
        assert2::assert!(saw_leave.load(Ordering::SeqCst));
    }

    #[test]
    fn find_coordinator_request_populates_legacy_and_batched_group_keys() {
        let req = build_find_coordinator_request("group-a".into());

        assert2::assert!(
            req == FindCoordinatorRequest {
                key: "group-a".into(),
                key_type: 0,
                coordinator_keys: vec!["group-a".into()],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn subscribed_topics_grew_detects_appearance_and_partition_growth() {
        let empty: HashMap<String, i32> = HashMap::new();
        let one: HashMap<String, i32> = [("logs".to_string(), 1)].into_iter().collect();
        let three: HashMap<String, i32> = [("logs".to_string(), 3)].into_iter().collect();

        for (_name, known, current, expected) in [
            // Cold-start race: topic absent at join, created later -> growth -> rejoin.
            ("topic appears", &empty, &one, true),
            // Topic gained partitions -> rejoin to (re)distribute them.
            ("partition count grows", &one, &three, true),
            // Steady state: unchanged -> no spurious rejoin.
            ("steady state", &one, &one, false),
            // A topic shrinking/disappearing is not "growth" -> no rejoin.
            ("partition count shrinks", &three, &one, false),
            ("topic disappears", &one, &empty, false),
        ] {
            assert2::assert!(subscribed_topics_grew(known, current) == expected);
        }
    }

    #[test]
    fn merge_counts_advances_monotonically_and_ignores_transient_under_reports() {
        let one: HashMap<String, i32> = [("logs".to_string(), 1)].into_iter().collect();
        let three: HashMap<String, i32> = [("logs".to_string(), 3)].into_iter().collect();
        let five: HashMap<String, i32> = [("logs".to_string(), 5)].into_iter().collect();

        // Empty baseline + a topic appears -> baseline records it.
        let mut known: HashMap<String, i32> = HashMap::new();
        merge_counts(&mut known, &one);
        assert2::assert!(known.get("logs") == Some(&1));

        // Growth advances the baseline; after merging the new count the SAME
        // count is no longer seen as growth (so the rejoin doesn't re-fire).
        merge_counts(&mut known, &three);
        assert2::assert!(
            (known.get("logs"), subscribed_topics_grew(&known, &three)) == (Some(&3), false)
        );

        // A transient metadata under-report (controller failover / partial
        // response) must NOT lower the baseline: Kafka partition counts are
        // monotonic, so dropping to 1 then recovering to 3 would otherwise churn
        // a spurious rejoin. max-merge pins it at 3.
        merge_counts(&mut known, &one);
        assert2::assert!(
            (known.get("logs"), subscribed_topics_grew(&known, &three)) == (Some(&3), false)
        );

        // A non-leader rejoin's snapshot is empty -> max-merge is a no-op, so the
        // baseline survives (the next tick sees no phantom growth).
        merge_counts(&mut known, &HashMap::new());
        assert2::assert!(known.get("logs") == Some(&3));

        // A genuinely larger count still advances.
        merge_counts(&mut known, &five);
        assert2::assert!(known.get("logs") == Some(&5));
    }

    #[tokio::test(start_paused = true)]
    async fn retry_deadline_elapsed_uses_elapsed_timeout_boundary() {
        let start = tokio::time::Instant::now();

        assert2::assert!(!retry_deadline_elapsed(start, Duration::from_millis(1)));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert2::assert!(retry_deadline_elapsed(start, Duration::from_millis(1)));
    }

    #[test]
    fn next_backoff_doubles_until_cap() {
        for (_name, backoff, max_backoff, expected) in [
            (
                "doubling below cap",
                Duration::from_millis(100),
                Duration::from_secs(1),
                Duration::from_millis(200),
            ),
            (
                "doubling reaches cap",
                Duration::from_millis(800),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
            (
                "already capped",
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        ] {
            assert2::assert!(next_backoff(backoff, max_backoff) == expected);
        }
    }

    #[test]
    fn leave_group_request_populates_legacy_and_batched_member_fields() {
        let req = build_leave_group_request(
            "group-a".into(),
            "member-a".into(),
            Some("instance-a".into()),
        );

        assert2::assert!(
            req == LeaveGroupRequest {
                group_id: "group-a".into(),
                member_id: "member-a".into(),
                members: vec![MemberIdentity {
                    member_id: "member-a".into(),
                    group_instance_id: Some("instance-a".into()),
                    reason: None,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn revoked_commit_helpers_preserve_filter_boundaries_and_request_fields() {
        for (_name, is_revoked, next_offset, expected) in [
            ("revoked positive", true, 1, true),
            ("not revoked", false, 1, false),
            ("zero offset", true, 0, false),
            ("negative offset", true, -1, false),
            ("latest sentinel", true, i64::MAX, false),
        ] {
            assert2::assert!(should_commit_revoked_offset(is_revoked, next_offset) == expected);
        }

        let topics = build_commit_topics(
            HashMap::from([(("topic-a".to_string(), 2), (42, 7))]),
            &HashMap::new(),
        );
        let req = build_revoked_commit_request(
            "group-a".into(),
            3,
            "member-a".into(),
            Some("instance-a".into()),
            topics.clone(),
        );

        assert2::assert!(
            req == OffsetCommitRequest {
                group_id: "group-a".into(),
                generation_id_or_member_epoch: 3,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                retention_time_ms: -1,
                topics,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
        assert2::assert!(UNKNOWN_EPOCH == -1);
    }

    #[test]
    fn join_group_request_preserves_group_member_timeouts_and_protocol() {
        let req = build_join_group_request(
            "group-a".into(),
            "member-a".into(),
            Some("instance-a".into()),
            10_000,
            30_000,
            "range".into(),
            vec![1, 2, 3].into(),
        );

        assert2::assert!(
            req == JoinGroupRequest {
                group_id: "group-a".into(),
                session_timeout_ms: 10_000,
                rebalance_timeout_ms: 30_000,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                protocol_type: "consumer".into(),
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: vec![1, 2, 3].into(),
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                reason: None,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn sync_group_request_preserves_member_generation_protocol_and_assignments() {
        let assignment = build_sync_group_assignment(
            "member-a".into(),
            &[("topic-a".to_string(), 0), ("topic-a".to_string(), 1)],
        );
        assert2::assert!(
            (
                assignment.member_id.as_str(),
                decode_assignment(&assignment.assignment),
            ) == (
                "member-a",
                vec![("topic-a".to_string(), 0), ("topic-a".to_string(), 1)],
            )
        );

        let req = build_sync_group_request(
            "group-a".into(),
            7,
            "member-a".into(),
            Some("instance-a".into()),
            "range".into(),
            vec![assignment.clone()],
        );

        assert2::assert!(
            req == SyncGroupRequest {
                group_id: "group-a".into(),
                generation_id: 7,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                assignments: vec![assignment],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn heartbeat_request_preserves_group_generation_and_member() {
        let req = build_heartbeat_request(
            "group-a".into(),
            42,
            "member-a".into(),
            Some("instance-a".into()),
        );

        assert2::assert!(
            req == HeartbeatRequest {
                group_id: "group-a".into(),
                generation_id: 42,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn prime_offset_helpers_preserve_committed_and_reset_boundaries() {
        for (_name, committed, reset, expected) in [
            ("committed positive", 12, AutoOffsetReset::Earliest, 12),
            ("committed zero", 0, AutoOffsetReset::Latest, 0),
            ("missing earliest", -1, AutoOffsetReset::Earliest, 0),
            ("missing latest", -1, AutoOffsetReset::Latest, i64::MAX),
            ("missing none", -1, AutoOffsetReset::None, i64::MAX),
        ] {
            assert2::assert!(starting_offset(committed, reset) == expected);
        }

        for (_name, reset, expected) in [
            ("earliest", AutoOffsetReset::Earliest, 0),
            ("latest", AutoOffsetReset::Latest, i64::MAX),
            ("none", AutoOffsetReset::None, i64::MAX),
        ] {
            assert2::assert!(reset_starting_offset(reset) == expected);
        }

        for (_name, has_position, expected) in [
            ("missing position", false, true),
            ("existing position", true, false),
        ] {
            assert2::assert!(should_prime_missing_partition(has_position) == expected);
        }
    }

    #[test]
    fn heartbeat_outcome_classifies_success_rejoin_and_transient_errors() {
        for (_name, error_code, expected) in [
            ("success", 0, HeartbeatOutcome::Ok),
            ("rebalance in progress", 27, HeartbeatOutcome::NeedRejoin),
            ("illegal generation", 22, HeartbeatOutcome::NeedRejoin),
            ("unknown member", 25, HeartbeatOutcome::RejoinFromScratch),
            ("loading coordinator", 14, HeartbeatOutcome::Transient),
            ("unknown transient", 99, HeartbeatOutcome::Transient),
        ] {
            assert2::assert!(heartbeat_outcome(error_code) == expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_coordinator_finishes_loading() {
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_retry(
            retry(Duration::from_secs(30)),
            |r: &Resp| r.error_code,
            || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    // COORDINATOR_LOAD_IN_PROGRESS (14) thrice, then success.
                    Ok::<_, ConsumerError>(Resp {
                        error_code: if n < 3 { 14 } else { 0 },
                    })
                }
            },
        )
        .await
        .unwrap();
        assert2::assert!(r.error_code == 0);
        assert2::assert!(calls.load(Ordering::SeqCst) == 4);
    }

    #[tokio::test(start_paused = true)]
    async fn configured_coordinator_retry_policy_controls_backoff_and_timeout() {
        let calls = AtomicUsize::new(0);
        let retry = CoordinatorRetryPolicy {
            timeout: Duration::from_millis(35),
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(10),
        };
        let r = with_coordinator_retry(
            retry,
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, ConsumerError>(Resp { error_code: 15 }) }
            },
        )
        .await
        .unwrap();
        assert2::assert!(r.error_code == 15);
        assert2::assert!(calls.load(Ordering::SeqCst) == 5);
    }

    #[tokio::test(start_paused = true)]
    async fn surfaces_last_response_after_deadline() {
        let r = with_coordinator_retry(
            retry(Duration::from_secs(1)),
            |r: &Resp| r.error_code,
            || async { Ok::<_, ConsumerError>(Resp { error_code: 15 }) },
        )
        .await
        .unwrap();
        // Deadline hit while still retriable: return the last response so the
        // caller's `error_code != 0` handling surfaces it.
        assert2::assert!(r.error_code == 15);
    }

    #[tokio::test(start_paused = true)]
    async fn non_retriable_code_returns_immediately() {
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_retry(
            retry(Duration::from_secs(30)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok::<_, ConsumerError>(Resp { error_code: 25 }) } // UNKNOWN_MEMBER_ID
            },
        )
        .await
        .unwrap();
        assert2::assert!(r.error_code == 25);
        assert2::assert!(calls.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test(start_paused = true)]
    async fn disconnect_past_deadline_surfaces_coordinator_unavailable() {
        let r = with_coordinator_retry(
            retry(Duration::from_secs(1)),
            |r: &Resp| r.error_code,
            || async {
                Err::<Resp, _>(ConsumerError::Client(
                    crabka_client_core::ClientError::Disconnected,
                ))
            },
        )
        .await;
        assert2::assert!(matches!(r, Err(ConsumerError::CoordinatorUnavailable)));
    }

    #[tokio::test(start_paused = true)]
    async fn connect_past_deadline_surfaces_coordinator_unavailable() {
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_retry(
            retry(Duration::from_millis(1)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<Resp, _>(refused_connect_error()) }
            },
        )
        .await;

        assert2::assert!(matches!(r, Err(ConsumerError::CoordinatorUnavailable)));
        assert2::assert!(calls.load(Ordering::SeqCst) > 1);
    }
}

#[cfg(test)]
mod find_coordinator_parse_tests {

    use crabka_protocol::owned::find_coordinator_response::Coordinator;

    use super::*;

    #[test]
    fn parses_legacy_and_batched_coordinator_shapes() {
        for (_name, resp, expected) in [
            (
                "batched success",
                FindCoordinatorResponse {
                    node_id: -1,
                    error_code: 99,
                    coordinators: vec![Coordinator {
                        key: "g".into(),
                        node_id: 7,
                        host: "h".into(),
                        port: 9092,
                        error_code: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                (7, 0, false),
            ),
            (
                "legacy success",
                FindCoordinatorResponse {
                    node_id: 3,
                    error_code: 0,
                    coordinators: vec![],
                    ..Default::default()
                },
                (3, 0, false),
            ),
            (
                "batched not coordinator",
                FindCoordinatorResponse {
                    node_id: 1,
                    error_code: 0,
                    coordinators: vec![Coordinator {
                        key: "g".into(),
                        node_id: -1,
                        error_code: NOT_COORDINATOR,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                (-1, NOT_COORDINATOR, true),
            ),
        ] {
            let code = coordinator_error_code(&resp);
            assert2::assert!(
                (
                    coordinator_node_id(&resp),
                    code,
                    is_retriable_coordinator_code(code)
                ) == expected
            );
        }
    }
}

#[cfg(test)]
mod refind_tests {
    use std::sync::atomic::AtomicUsize;

    use assert2::check;

    use super::*;

    fn retry(timeout: Duration) -> CoordinatorRetryPolicy {
        CoordinatorRetryPolicy {
            timeout,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }

    struct Resp {
        error_code: i16,
    }

    // Without a live broker we can't exercise the real re-find (it sends RPCs),
    // but we can prove the retry/backoff/deadline behaviour matches
    // `with_coordinator_retry` for the no-broker code paths. A purely
    // successful response returns immediately without touching the
    // coordinator cell.
    #[tokio::test(start_paused = true)]
    async fn returns_immediately_on_success_without_refind() {
        // 127.0.0.1:1 is unroutable, so any re-find attempt would fail — but a
        // success on the first attempt must never re-find.
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .build()
            .await
            .unwrap();
        let coord = AtomicI32::new(5);
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_refind(
            &client,
            "g",
            &coord,
            retry(Duration::from_secs(30)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, ConsumerError>(Resp { error_code: 0 }) }
            },
        )
        .await
        .unwrap();
        check!(r.error_code == 0);
        check!(calls.load(Ordering::SeqCst) == 1);
        // Coordinator cell untouched — no re-find on success.
        check!(coord.load(Ordering::Relaxed) == 5);
    }

    // A non-retriable broker code (e.g. UNKNOWN_MEMBER_ID 25) is returned to the
    // caller on the first attempt, no re-find.
    #[tokio::test(start_paused = true)]
    async fn non_retriable_code_returns_without_refind() {
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .build()
            .await
            .unwrap();
        let coord = AtomicI32::new(2);
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_refind(
            &client,
            "g",
            &coord,
            retry(Duration::from_secs(30)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, ConsumerError>(Resp { error_code: 25 }) }
            },
        )
        .await
        .unwrap();
        check!(r.error_code == 25);
        check!(calls.load(Ordering::SeqCst) == 1);
        check!(coord.load(Ordering::Relaxed) == 2);
    }

    #[tokio::test(start_paused = true)]
    async fn connect_error_refinds_until_deadline() {
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .connect_timeout(crabka_units::millis(10))
            .request_timeout(crabka_units::millis(10))
            .build()
            .await
            .unwrap();
        let coord = AtomicI32::new(5);
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_refind(
            &client,
            "g",
            &coord,
            retry(Duration::from_millis(1)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async {
                    Err::<Resp, _>(ConsumerError::Client(
                        crabka_client_core::ClientError::Connect {
                            addr: "127.0.0.1:9092".parse().unwrap(),
                            source: std::io::Error::new(
                                std::io::ErrorKind::ConnectionRefused,
                                "refused",
                            ),
                        },
                    ))
                }
            },
        )
        .await;

        assert2::assert!(matches!(r, Err(ConsumerError::CoordinatorUnavailable)));
        assert2::assert!(calls.load(Ordering::SeqCst) > 1);
    }
}
