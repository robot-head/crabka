//! Background `ShareGroupHeartbeat` loop for a [`ShareConsumer`].
//!
//! Mirrors the classic [`coordinator::run`](crate::coordinator::run) shape:
//! an interval ticker plus a `tokio::select!` that races each heartbeat RPC
//! against a shutdown token so `close()` returns promptly. Each tick sends a
//! `ShareGroupHeartbeat` (API key 76) with the live member epoch; on success
//! it adopts the broker-returned epoch and (when present) the new assignment.
//!
//! Share-group membership has no Join/Sync handshake — the heartbeat *is* the
//! join. A from-scratch rejoin therefore just resets the member epoch to 0 and
//! re-sends `subscribed_topic_names`; the broker hands back a fresh epoch and
//! assignment on the next ok.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

/// `FENCED_MEMBER_EPOCH` — our epoch is behind the broker's; rejoin.
const FENCED_MEMBER_EPOCH: i16 = 110;
/// `UNKNOWN_MEMBER_ID` — the broker has forgotten us (session expired); rejoin.
const UNKNOWN_MEMBER_ID: i16 = 25;
/// `STALE_MEMBER_EPOCH` — same family as fenced; rejoin from scratch.
const STALE_MEMBER_EPOCH: i16 = 113;

/// State owned by the share-group heartbeat task.
///
/// The `Arc<Mutex<...>>` fields are shared with the parent [`ShareConsumer`]
/// so `poll()` sees the live member epoch / assignment as the broker rebalances
/// the group.
pub(crate) struct ShareCoordinatorState {
    pub client: Client,
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: Arc<Mutex<i32>>,
    /// Live assignment as `(topic_id, topic_name, partition)`.
    pub assignment: Arc<Mutex<Vec<(WireUuid, String, i32)>>>,
    pub topic_names: Arc<Mutex<HashMap<WireUuid, String>>>,
    pub subscribe: Vec<String>,
    pub heartbeat_interval: Duration,
}

/// Outcome of a single `ShareGroupHeartbeat` RPC.
enum HeartbeatOutcome {
    /// `error_code == 0` — steady state.
    Ok,
    /// Fenced / unknown-member / stale-epoch — reset to epoch 0 and re-send the
    /// subscription on the next tick so the broker re-admits us.
    RejoinFromScratch,
    /// Transport error or unexpected non-fatal code; retry next tick.
    Transient,
}

/// Drive the heartbeat loop until `shutdown` fires.
pub(crate) async fn run(state: ShareCoordinatorState, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(state.heartbeat_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // After a fence/unknown-member we must re-send `subscribed_topic_names` on
    // the next heartbeat (the broker treats epoch 0 + subscription as a join).
    let mut rejoining = false;

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }

        // Race the RPC against shutdown so `close()` is prompt even when the
        // broker is slow. The send is cancellation-safe: `Client` multiplexes
        // on correlation ids, so dropping an in-flight send only abandons its
        // pending response.
        tokio::select! {
            () = shutdown.cancelled() => break,
            outcome = heartbeat_once(&state, rejoining) => match outcome {
                HeartbeatOutcome::Ok => rejoining = false,
                HeartbeatOutcome::Transient => {}
                HeartbeatOutcome::RejoinFromScratch => {
                    *state.member_epoch.lock().await = 0;
                    rejoining = true;
                }
            },
        }
    }

    // Graceful departure: a leave heartbeat (`member_epoch = -1`) tells the
    // broker to evict us now rather than waiting out the session timeout.
    // Best-effort and bounded so a hung broker can't block `close()`.
    let leave = state.client.send(ShareGroupHeartbeatRequest {
        group_id: state.group_id.clone(),
        member_id: state.member_id.clone(),
        member_epoch: -1,
        ..Default::default()
    });
    let _ = tokio::time::timeout(Duration::from_secs(5), leave).await;
}

/// Send one `ShareGroupHeartbeat` and translate the response into a directive.
///
/// When `rejoining` we re-send `subscribed_topic_names` (the broker requires
/// the subscription on a fresh join); otherwise we send `None` (the broker
/// remembers it across steady-state heartbeats).
async fn heartbeat_once(state: &ShareCoordinatorState, rejoining: bool) -> HeartbeatOutcome {
    let epoch = *state.member_epoch.lock().await;
    let subscribed = if rejoining {
        Some(state.subscribe.clone())
    } else {
        None
    };
    let result = state
        .client
        .send(ShareGroupHeartbeatRequest {
            group_id: state.group_id.clone(),
            member_id: state.member_id.clone(),
            member_epoch: epoch,
            subscribed_topic_names: subscribed,
            ..Default::default()
        })
        .await;
    match result {
        Ok(r) if r.error_code == 0 => {
            *state.member_epoch.lock().await = r.member_epoch;
            if let Some(assignment) = r.assignment {
                update_assignment(state, assignment).await;
            }
            HeartbeatOutcome::Ok
        }
        Ok(r)
            if r.error_code == FENCED_MEMBER_EPOCH
                || r.error_code == UNKNOWN_MEMBER_ID
                || r.error_code == STALE_MEMBER_EPOCH =>
        {
            tracing::warn!(
                error_code = r.error_code,
                "share heartbeat fenced; rejoining from epoch 0"
            );
            HeartbeatOutcome::RejoinFromScratch
        }
        Ok(r) => {
            tracing::warn!(
                error_code = r.error_code,
                "unexpected share heartbeat error"
            );
            HeartbeatOutcome::Transient
        }
        Err(e) => {
            tracing::warn!(error = %e, "share heartbeat send failed");
            HeartbeatOutcome::Transient
        }
    }
}

/// Replace the shared assignment with the broker-returned topic/partition set,
/// resolving topic names from the cached `topic_names` map (topic ids the map
/// doesn't know yet fall back to the id's hex form so `poll()` still has a
/// stable display name; a later Metadata refresh fixes it).
async fn update_assignment(
    state: &ShareCoordinatorState,
    assignment: crabka_protocol::owned::share_group_heartbeat_response::Assignment,
) {
    let names = state.topic_names.lock().await;
    let mut next: Vec<(WireUuid, String, i32)> = Vec::new();
    for tp in &assignment.topic_partitions {
        let name = names
            .get(&tp.topic_id)
            .cloned()
            .unwrap_or_else(|| hex_topic_id(tp.topic_id));
        for &partition in &tp.partitions {
            next.push((tp.topic_id, name.clone(), partition));
        }
    }
    drop(names);
    *state.assignment.lock().await = next;
}

/// Hex display for a topic id whose name we haven't resolved yet. `Uuid` has no
/// `Display`; this gives `poll()` a stable, non-empty placeholder name until a
/// Metadata refresh fills the real one in.
fn hex_topic_id(id: WireUuid) -> String {
    use std::fmt::Write as _;
    id.0.iter().fold(String::with_capacity(32), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}
