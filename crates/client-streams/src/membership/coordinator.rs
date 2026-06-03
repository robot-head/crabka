//! Background `StreamsGroupHeartbeat` loop. Mirrors `share/coordinator.rs`: a
//! ticker + `select!` racing each heartbeat against shutdown. Adopts the
//! broker's epoch + assignment, echoes owned tasks back (adopt-and-echo
//! reconciliation), rejoins from epoch 0 on fence, and sends a leave heartbeat
//! (`member_epoch = -1`) on shutdown. Meaningful changes are emitted as
//! [`StreamsEvent`]s.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds;
use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds as RespTaskIds;
use crabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest;

use super::assignment::resolve;
use super::status::map_status;
use super::types::{StreamsAssignment, StreamsEvent};
use crate::topology::BuiltTopology;

const FENCED_MEMBER_EPOCH: i16 = 110;
const UNKNOWN_MEMBER_ID: i16 = 25;
const STALE_MEMBER_EPOCH: i16 = 113;

/// State owned by the heartbeat task.
pub(crate) struct CoordinatorState {
    pub client: Client,
    pub group_id: String,
    pub member_id: String,
    pub process_id: String,
    pub instance_id: Option<String>,
    pub rebalance_timeout_ms: i32,
    pub topology: Arc<BuiltTopology>,
    pub member_epoch: Arc<Mutex<i32>>,
    /// Owned tasks last adopted, echoed back as `active_tasks` next heartbeat.
    pub owned_active: Arc<Mutex<Vec<RespTaskIds>>>,
    pub heartbeat_interval: Duration,
    pub events: mpsc::UnboundedSender<StreamsEvent>,
}

enum Outcome {
    Ok,
    Rejoin,
    Transient,
}

/// Drive the loop until `shutdown` fires, then leave.
pub(crate) async fn run(state: CoordinatorState, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(state.heartbeat_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rejoining = false;

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }
        tokio::select! {
            () = shutdown.cancelled() => break,
            outcome = heartbeat_once(&state, rejoining) => match outcome {
                Outcome::Ok => rejoining = false,
                Outcome::Transient => {}
                Outcome::Rejoin => {
                    *state.member_epoch.lock().await = 0;
                    state.owned_active.lock().await.clear();
                    rejoining = true;
                    let _ = state.events.send(StreamsEvent::Fenced);
                }
            },
        }
    }

    let leave = state.client.send(StreamsGroupHeartbeatRequest {
        group_id: state.group_id.clone(),
        member_id: state.member_id.clone(),
        member_epoch: -1,
        ..Default::default()
    });
    let _ = tokio::time::timeout(Duration::from_secs(5), leave).await;
}

async fn heartbeat_once(state: &CoordinatorState, rejoining: bool) -> Outcome {
    let epoch = *state.member_epoch.lock().await;
    let owned = state.owned_active.lock().await.clone();
    let topology = if rejoining || epoch == 0 {
        Some(state.topology.to_wire())
    } else {
        None
    };
    let active_tasks = if owned.is_empty() {
        None
    } else {
        Some(owned.iter().map(resp_to_req).collect())
    };

    let req = StreamsGroupHeartbeatRequest {
        group_id: state.group_id.clone(),
        member_id: state.member_id.clone(),
        member_epoch: epoch,
        process_id: Some(state.process_id.clone()),
        instance_id: state.instance_id.clone(),
        rebalance_timeout_ms: state.rebalance_timeout_ms,
        topology,
        active_tasks,
        ..Default::default()
    };

    match state.client.send(req).await {
        Ok(r) if r.error_code == 0 => {
            *state.member_epoch.lock().await = r.member_epoch;
            emit_response(state, &r).await;
            Outcome::Ok
        }
        Ok(r)
            if r.error_code == FENCED_MEMBER_EPOCH
                || r.error_code == UNKNOWN_MEMBER_ID
                || r.error_code == STALE_MEMBER_EPOCH =>
        {
            tracing::warn!(
                error_code = r.error_code,
                "streams heartbeat fenced; rejoining"
            );
            Outcome::Rejoin
        }
        Ok(r) => {
            tracing::warn!(
                error_code = r.error_code,
                "unexpected streams heartbeat error"
            );
            Outcome::Transient
        }
        Err(e) => {
            tracing::warn!(error = %e, "streams heartbeat send failed");
            Outcome::Transient
        }
    }
}

/// Emit `NotReady` (status present) and/or `Assigned` (tasks present), and update
/// the owned-active set for the next echo.
async fn emit_response(
    state: &CoordinatorState,
    r: &crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
) {
    if let Some(statuses) = &r.status
        && !statuses.is_empty()
    {
        let mapped = statuses.iter().map(map_status).collect();
        let _ = state.events.send(StreamsEvent::NotReady(mapped));
    }
    if r.active_tasks.is_some() {
        if let Some(tasks) = &r.active_tasks {
            *state.owned_active.lock().await = tasks.clone();
        }
        let assignment = StreamsAssignment {
            active: resolve(r.active_tasks.as_ref(), &state.topology),
            standby: resolve(r.standby_tasks.as_ref(), &state.topology),
            warmup: resolve(r.warmup_tasks.as_ref(), &state.topology),
        };
        let _ = state.events.send(StreamsEvent::Assigned(assignment));
    }
}

fn resp_to_req(t: &RespTaskIds) -> ReqTaskIds {
    ReqTaskIds {
        subtopology_id: t.subtopology_id.clone(),
        partitions: t.partitions.clone(),
        ..Default::default()
    }
}
