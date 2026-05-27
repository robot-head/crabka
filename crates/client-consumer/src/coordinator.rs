//! Background coordinator task — owns the join/sync/heartbeat/rebalance
//! lifecycle for a [`Consumer`](crate::consumer::Consumer). Replaces the
//! slice-5 standalone heartbeat task.
//!
//! On each tick we either send a `Heartbeat` (steady-state) or run a
//! full `JoinGroup` + `SyncGroup` round (`needs_rejoin`). The broker
//! signals a rebalance via `error_code = 27 (REBALANCE_IN_PROGRESS)`
//! on heartbeat; `25 (UNKNOWN_MEMBER_ID)` forces a from-scratch
//! handshake (clear `member_id`, `generation_id = -1`).
//!
//! Cooperative rebalance (KIP-429) runs phase-1 + phase-2 in place:
//! phase 1 reduces the owned set to the partitions we kept, then we
//! immediately re-Join + re-Sync so the leader can place the freshly
//! freed partitions onto whoever needs them. Eager (`range`) drops the
//! whole assignment and reinstalls in a single round.
//!
//! During a rejoin in flight we deliberately do *not* heartbeat —
//! `JoinGroup` resets the broker-side session timer.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic};
use crabka_protocol::owned::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::assignor::{Assignor, RebalanceProtocol};
use crate::builder::{AutoOffsetReset, decode_assignment, decode_subscription, encode_assignment,
    encode_subscription};
use crate::error::ConsumerError;

/// Mutable state owned exclusively by the coordinator task.
///
/// The `Arc<Mutex<...>>` fields are shared with the parent `Consumer`
/// so that `poll()` / `assignment()` see live updates as rebalances
/// land. Plain (non-`Arc`) fields are exclusive to the coordinator
/// and may be mutated freely — `member_id` and `generation_id` change
/// on a from-scratch rejoin.
pub(crate) struct CoordinatorState {
    pub client: Client,
    pub group_id: String,
    pub member_id: String,
    pub generation_id: i32,
    pub assignor: Assignor,
    pub subscribed_topics: Vec<String>,
    pub assigned: Arc<Mutex<Vec<(String, i32)>>>,
    pub next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub topic_ids: Arc<Mutex<HashMap<String, WireUuid>>>,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub auto_offset_reset: AutoOffsetReset,
    pub client_rack: Option<String>,
}

/// Outcome of a single heartbeat RPC.
enum HeartbeatOutcome {
    /// `error_code == 0`.
    Ok,
    /// `REBALANCE_IN_PROGRESS (27)` — rejoin with the current `member_id`.
    NeedRejoin,
    /// `UNKNOWN_MEMBER_ID (25)` — clear `member_id` + rejoin from scratch.
    RejoinFromScratch,
    /// Transport error or unexpected non-fatal broker code; retry on next tick.
    Transient,
}

/// Drive the heartbeat + rebalance loop until `shutdown` fires.
///
/// On entry the caller has already done one initial Join+Sync, so we
/// begin in steady-state heartbeating. `needs_rejoin` becomes `true`
/// as soon as the broker signals a rebalance; the next tick performs
/// the rejoin in place of heartbeating.
pub(crate) async fn run(mut state: CoordinatorState, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(state.heartbeat_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut needs_rejoin = false;

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                if needs_rejoin {
                    match rejoin(&mut state).await {
                        Ok(()) => needs_rejoin = false,
                        Err(e) => {
                            tracing::warn!(error = %e, "rejoin failed; will retry on next tick");
                        }
                    }
                    continue;
                }
                match heartbeat_once(&state).await {
                    HeartbeatOutcome::Ok => {}
                    HeartbeatOutcome::NeedRejoin => needs_rejoin = true,
                    HeartbeatOutcome::RejoinFromScratch => {
                        state.member_id.clear();
                        state.generation_id = -1;
                        needs_rejoin = true;
                    }
                    HeartbeatOutcome::Transient => {}
                }
            }
        }
    }
}

/// Send one `Heartbeat` and translate the response into a directive.
async fn heartbeat_once(state: &CoordinatorState) -> HeartbeatOutcome {
    let result = state
        .client
        .send(HeartbeatRequest {
            group_id: state.group_id.clone(),
            generation_id: state.generation_id,
            member_id: state.member_id.clone(),
            ..Default::default()
        })
        .await;
    match result {
        Ok(r) if r.error_code == 0 => HeartbeatOutcome::Ok,
        Ok(r) if r.error_code == 27 => HeartbeatOutcome::NeedRejoin,
        Ok(r) if r.error_code == 25 => HeartbeatOutcome::RejoinFromScratch,
        Ok(r) => {
            tracing::warn!(error_code = r.error_code, "unexpected heartbeat error");
            HeartbeatOutcome::Transient
        }
        Err(e) => {
            tracing::warn!(error = %e, "heartbeat send failed");
            HeartbeatOutcome::Transient
        }
    }
}

/// Run one complete rebalance round (Join + Sync), then mutate the
/// shared `assigned` / `next_offsets` snapshots in place.
///
/// For [`RebalanceProtocol::Cooperative`] this may issue *two* Join+Sync
/// rounds back-to-back: the first to install the kept partitions only,
/// the second (phase 2) to receive the freshly placed ones. See KIP-429.
async fn rejoin(state: &mut CoordinatorState) -> Result<(), ConsumerError> {
    let owned: Vec<(String, i32)> = state.assigned.lock().await.clone();
    let (new_assignment, new_generation, _protocol_name) =
        join_and_sync(state, &owned).await?;

    let old_set: HashSet<(String, i32)> = owned.iter().cloned().collect();
    let new_set: HashSet<(String, i32)> = new_assignment.iter().cloned().collect();
    let revoked: Vec<(String, i32)> = old_set.difference(&new_set).cloned().collect();
    let added: Vec<(String, i32)> = new_set.difference(&old_set).cloned().collect();

    match state.assignor.rebalance_protocol() {
        RebalanceProtocol::Eager => {
            // Drop everything and reinstall in a single round. We still
            // prime offsets explicitly for the new set so the first
            // `poll()` after rebalance doesn't restart from 0 when the
            // partition has a committed position.
            {
                let mut a = state.assigned.lock().await;
                *a = new_assignment.clone();
            }
            {
                let mut off = state.next_offsets.lock().await;
                off.retain(|k, _| new_set.contains(k));
            }
            prime_offsets(state, &added).await?;
            state.generation_id = new_generation;
        }
        RebalanceProtocol::Cooperative => {
            if !revoked.is_empty() {
                // Phase 1: drop the partitions we're losing, then
                // immediately rejoin so the leader can place them on
                // whoever needs them in phase 2. Keeping kept partitions
                // active throughout is the whole point of KIP-429.
                {
                    let mut a = state.assigned.lock().await;
                    a.retain(|p| !revoked.contains(p));
                }
                {
                    let mut off = state.next_offsets.lock().await;
                    for p in &revoked {
                        off.remove(p);
                    }
                }
                state.generation_id = new_generation;

                // Phase 2: rejoin with the reduced owned-set.
                let owned_after_revoke: Vec<(String, i32)> =
                    state.assigned.lock().await.clone();
                let (assignment2, gen2, _) =
                    join_and_sync(state, &owned_after_revoke).await?;
                let owned_after_revoke_set: HashSet<(String, i32)> =
                    owned_after_revoke.iter().cloned().collect();
                let added2: Vec<(String, i32)> = assignment2
                    .iter()
                    .filter(|p| !owned_after_revoke_set.contains(*p))
                    .cloned()
                    .collect();
                {
                    let mut a = state.assigned.lock().await;
                    *a = assignment2;
                }
                prime_offsets(state, &added2).await?;
                state.generation_id = gen2;
            } else {
                // Pure additions: merge into the existing assigned set.
                // No phase 2 needed because no member needed to revoke.
                {
                    let mut a = state.assigned.lock().await;
                    for p in &added {
                        if !a.contains(p) {
                            a.push(p.clone());
                        }
                    }
                }
                prime_offsets(state, &added).await?;
                state.generation_id = new_generation;
            }
        }
    }
    Ok(())
}

/// Issue `JoinGroup` (handling the `MEMBER_ID_REQUIRED` two-step when
/// our `member_id` is empty), assign as leader if we won the election,
/// then `SyncGroup`. Returns `(assignment, generation_id, protocol_name)`.
async fn join_and_sync(
    state: &mut CoordinatorState,
    owned: &[(String, i32)],
) -> Result<(Vec<(String, i32)>, i32, String), ConsumerError> {
    let session_timeout_ms =
        i32::try_from(state.session_timeout.as_millis()).unwrap_or(i32::MAX);
    let rebalance_timeout_ms =
        i32::try_from(state.rebalance_timeout.as_millis()).unwrap_or(i32::MAX);

    let subscription_bytes = encode_subscription(
        &state.subscribed_topics,
        owned,
        state.generation_id,
        state.client_rack.as_deref(),
    );
    let protocol_name = state.assignor.protocol_name().to_string();

    let make_join = |member_id: String| JoinGroupRequest {
        group_id: state.group_id.clone(),
        protocol_type: "consumer".into(),
        member_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        protocols: vec![JoinGroupRequestProtocol {
            name: protocol_name.clone(),
            metadata: subscription_bytes.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };

    // First join: if we have no member_id, expect MEMBER_ID_REQUIRED (79)
    // and capture the broker-assigned id, then issue a second join.
    let r1 = state.client.send(make_join(state.member_id.clone())).await?;
    let join_resp = if r1.error_code == 0 {
        r1
    } else if r1.error_code == 79 {
        let assigned_id = r1.member_id.clone();
        if assigned_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed(
                "broker did not assign a member_id".into(),
            ));
        }
        state.member_id = assigned_id.clone();
        let r2 = state.client.send(make_join(assigned_id)).await?;
        if r2.error_code != 0 {
            return Err(ConsumerError::Server(r2.error_code));
        }
        r2
    } else {
        return Err(ConsumerError::Server(r1.error_code));
    };

    // The broker may have refreshed our member_id on this join too.
    if !join_resp.member_id.is_empty() {
        state.member_id = join_resp.member_id.clone();
    }
    let chosen_protocol = join_resp
        .protocol_name
        .clone()
        .unwrap_or_else(|| protocol_name.clone());
    let generation_id = join_resp.generation_id;

    // Leader: resolve partition counts via Metadata and run the assignor.
    let is_leader = join_resp.leader == state.member_id;
    let assignments_for_sync: Vec<SyncGroupRequestAssignment> = if is_leader {
        let md = state.client.send(MetadataRequest::default()).await?;
        let mut topic_partitions: HashMap<String, i32> = HashMap::new();
        let mut resolved_ids: HashMap<String, WireUuid> = HashMap::new();
        for t in &md.topics {
            let Some(name) = &t.name else { continue };
            if state.subscribed_topics.iter().any(|s| s == name) {
                let count = i32::try_from(t.partitions.len()).unwrap_or(i32::MAX);
                topic_partitions.insert(name.clone(), count);
                resolved_ids.insert(name.clone(), t.topic_id);
            }
        }
        // Push the freshly resolved topic_ids into the shared map so
        // poll() can satisfy newly added partitions on Fetch v ≥ 13.
        {
            let mut ids = state.topic_ids.lock().await;
            for (k, v) in resolved_ids {
                ids.insert(k, v);
            }
        }

        let decoded: Vec<(String, crate::builder::DecodedSubscription)> = join_resp
            .members
            .iter()
            .map(|m| (m.member_id.clone(), decode_subscription(&m.metadata)))
            .collect();

        let assignments = match state.assignor {
            Assignor::Range => {
                let inputs: Vec<(String, Vec<String>)> = decoded
                    .into_iter()
                    .map(|(id, sub)| (id, sub.topics))
                    .collect();
                crate::assignor::range::assign(inputs, &topic_partitions)
            }
            Assignor::CooperativeSticky => {
                let inputs: Vec<(String, Vec<String>, Vec<(String, i32)>, i32)> = decoded
                    .into_iter()
                    .map(|(id, sub)| (id, sub.topics, sub.owned, sub.generation_id))
                    .collect();
                crate::assignor::cooperative_sticky::assign(inputs, &topic_partitions)
            }
        };
        assignments
            .into_iter()
            .map(|(m, partitions)| SyncGroupRequestAssignment {
                member_id: m,
                assignment: encode_assignment(&partitions),
                ..Default::default()
            })
            .collect()
    } else {
        Vec::new()
    };

    let sync = state
        .client
        .send(SyncGroupRequest {
            group_id: state.group_id.clone(),
            generation_id,
            member_id: state.member_id.clone(),
            protocol_type: Some("consumer".into()),
            protocol_name: Some(chosen_protocol.clone()),
            assignments: assignments_for_sync,
            ..Default::default()
        })
        .await?;
    if sync.error_code != 0 {
        return Err(ConsumerError::Server(sync.error_code));
    }
    let my_assignment = decode_assignment(&sync.assignment);
    Ok((my_assignment, generation_id, chosen_protocol))
}

/// Populate `next_offsets` for newly added partitions by batch-fetching
/// committed offsets, falling back to `auto.offset.reset` semantics
/// when no commit exists. Mirrors the slice-5 initial-prime in
/// `consumer.rs::start` step 5.
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
    let topics: Vec<OffsetFetchRequestTopic> = by_topic
        .into_iter()
        .map(|(name, partition_indexes)| OffsetFetchRequestTopic {
            name,
            partition_indexes,
            ..Default::default()
        })
        .collect();
    let of = state
        .client
        .send(OffsetFetchRequest {
            group_id: state.group_id.clone(),
            topics: Some(topics),
            ..Default::default()
        })
        .await?;

    let mut offsets = state.next_offsets.lock().await;
    let mut seen: HashSet<(String, i32)> = HashSet::new();
    for t in &of.topics {
        for p in &t.partitions {
            let committed = p.committed_offset;
            let starting = if committed >= 0 {
                committed
            } else {
                match state.auto_offset_reset {
                    AutoOffsetReset::Earliest => 0,
                    // Resolved lazily by poll::resolve_latest_sentinels.
                    AutoOffsetReset::Latest => i64::MAX,
                }
            };
            let key = (t.name.clone(), p.partition_index);
            seen.insert(key.clone());
            offsets.insert(key, starting);
        }
    }
    // The broker may omit partitions that have no commit record at all;
    // ensure every requested partition has an entry so poll() can find it.
    for tp in partitions {
        if !seen.contains(tp) {
            let starting = match state.auto_offset_reset {
                AutoOffsetReset::Earliest => 0,
                AutoOffsetReset::Latest => i64::MAX,
            };
            offsets.insert(tp.clone(), starting);
        }
    }
    Ok(())
}
