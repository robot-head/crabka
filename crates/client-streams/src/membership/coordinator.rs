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
    /// The last assignment emitted, to suppress duplicate `Assigned` events
    /// (the broker re-sends `active_tasks: Some(...)` every heartbeat).
    pub last_assignment: tokio::sync::Mutex<StreamsAssignment>,
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
                    *state.last_assignment.lock().await = StreamsAssignment::default();
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

/// Emit `NotReady` (status present) and/or `Assigned` (tasks present and
/// changed since last emission), and update the owned-active set for the next
/// echo.
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
    if let Some(tasks) = &r.active_tasks {
        *state.owned_active.lock().await = tasks.clone();
    }
    let mut last = state.last_assignment.lock().await;
    if let Some(ev) = assignment_event(r, &state.topology, &mut last) {
        let _ = state.events.send(ev);
    }
}

/// Build the assignment from a heartbeat response and decide whether it
/// changed since `last`. Returns the event to emit, or `None` if unchanged.
fn assignment_event(
    r: &crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    topology: &BuiltTopology,
    last: &mut StreamsAssignment,
) -> Option<StreamsEvent> {
    let assignment = StreamsAssignment {
        active: resolve(r.active_tasks.as_ref(), topology),
        standby: resolve(r.standby_tasks.as_ref(), topology),
        warmup: resolve(r.warmup_tasks.as_ref(), topology),
    };
    if assignment == *last {
        None
    } else {
        *last = assignment.clone();
        Some(StreamsEvent::Assigned(assignment))
    }
}

fn resp_to_req(t: &RespTaskIds) -> ReqTaskIds {
    ReqTaskIds {
        subtopology_id: t.subtopology_id.clone(),
        partitions: t.partitions.clone(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Topology;
    use assert2::check;
    use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds;
    use crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse;

    fn built() -> BuiltTopology {
        let mut t = Topology::new();
        t.add_source("src", ["in"]);
        t.add_sink("snk", "out", ["src"]);
        t.build("app").unwrap()
    }

    fn resp(active: Vec<i32>) -> StreamsGroupHeartbeatResponse {
        StreamsGroupHeartbeatResponse {
            active_tasks: Some(vec![TaskIds {
                subtopology_id: "0".into(),
                partitions: active,
                ..Default::default()
            }]),
            ..Default::default()
        }
    }

    #[test]
    fn identical_assignment_is_not_re_emitted() {
        let topo = built();
        let mut last = StreamsAssignment::default();
        let r = resp(vec![0, 1]);
        // First time: changed → Some(Assigned)
        check!(assignment_event(&r, &topo, &mut last).is_some());
        // Second identical response: unchanged → None
        check!(assignment_event(&r, &topo, &mut last).is_none());
    }

    #[test]
    fn empty_assignment_is_not_emitted_from_default() {
        let topo = built();
        let mut last = StreamsAssignment::default();
        let empty = StreamsGroupHeartbeatResponse {
            active_tasks: Some(vec![]),
            ..Default::default()
        };
        check!(assignment_event(&empty, &topo, &mut last).is_none());
    }

    #[test]
    fn changed_assignment_is_re_emitted() {
        let topo = built();
        let mut last = StreamsAssignment::default();
        check!(assignment_event(&resp(vec![0]), &topo, &mut last).is_some());
        check!(assignment_event(&resp(vec![0, 1]), &topo, &mut last).is_some());
    }
}
