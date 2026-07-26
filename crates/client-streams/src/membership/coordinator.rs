//! Background `StreamsGroupHeartbeat` loop. Mirrors `share/coordinator.rs`: a
//! ticker + `select!` racing each heartbeat against shutdown. Adopts the
//! broker's epoch + assignment, echoes owned tasks back (adopt-and-echo
//! reconciliation), rejoins from epoch 0 on fence, and sends a leave heartbeat
//! (`member_epoch = -1`) on shutdown. Meaningful changes are emitted as
//! [`StreamsEvent`]s.

use std::{sync::Arc, time::Duration};

use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::{
    common::{
        streams_group_heartbeat_request::{
            task_ids::TaskIds as ReqTaskIds, task_offset::TaskOffset,
        },
        streams_group_heartbeat_response::task_ids::TaskIds as RespTaskIds,
    },
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    assignment::resolve,
    status::map_status,
    types::{StreamsAssignment, StreamsEvent, TaskOffsetTracker},
};
use crate::topology::BuiltTopology;

const FENCED_MEMBER_EPOCH: i16 = 110;
const UNKNOWN_MEMBER_ID: i16 = 25;
const STALE_MEMBER_EPOCH: i16 = 113;

/// The heartbeat RPC the coordinator depends on. Implemented by the real
/// [`Client`]; a fake is injected in tests so the loop is exercisable without a
/// broker.
#[async_trait::async_trait]
pub(crate) trait HeartbeatTransport: Send + Sync + 'static {
    async fn send_heartbeat(
        &self,
        req: StreamsGroupHeartbeatRequest,
    ) -> Result<StreamsGroupHeartbeatResponse, ClientError>;
}

#[async_trait::async_trait]
impl HeartbeatTransport for Client {
    async fn send_heartbeat(
        &self,
        req: StreamsGroupHeartbeatRequest,
    ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
        self.send(req).await
    }
}

/// State owned by the heartbeat task.
pub(crate) struct CoordinatorState<T: HeartbeatTransport> {
    pub client: T,
    pub group_id: String,
    pub member_id: String,
    pub process_id: String,
    pub instance_id: Option<String>,
    pub rebalance_timeout_ms: i32,
    pub topology: Arc<BuiltTopology>,
    pub member_epoch: Arc<Mutex<i32>>,
    /// Owned tasks last adopted, echoed back as `active_tasks` next heartbeat.
    pub owned_active: Arc<Mutex<Vec<RespTaskIds>>>,
    /// Owned standby tasks last adopted, echoed back next heartbeat.
    pub owned_standby: Arc<Mutex<Vec<RespTaskIds>>>,
    /// Owned warmup tasks last adopted, echoed back next heartbeat.
    pub owned_warmup: Arc<Mutex<Vec<RespTaskIds>>>,
    /// Tracker containing current and end offsets of all tasks.
    pub tracker: Arc<Mutex<TaskOffsetTracker>>,
    pub heartbeat_interval: Duration,
    pub leave_heartbeat_timeout: Duration,
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
#[tracing::instrument(
    name = "streams.coordinator.run",
    level = "info",
    skip_all,
    fields(group_id = %state.group_id, member_id = %state.member_id),
)]
pub(crate) async fn run<T: HeartbeatTransport>(
    state: CoordinatorState<T>,
    shutdown: CancellationToken,
) {
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
                    state.owned_standby.lock().await.clear();
                    state.owned_warmup.lock().await.clear();
                    {
                        let mut lock = state.tracker.lock().await;
                        lock.task_offsets.clear();
                        lock.task_end_offsets.clear();
                    }
                    *state.last_assignment.lock().await = StreamsAssignment::default();
                    rejoining = true;
                    let _ = state.events.send(StreamsEvent::Fenced);
                }
            },
        }
    }

    let leave = state.client.send_heartbeat(StreamsGroupHeartbeatRequest {
        group_id: state.group_id.clone(),
        member_id: state.member_id.clone(),
        member_epoch: -1,
        ..Default::default()
    });
    let _ = tokio::time::timeout(state.leave_heartbeat_timeout, leave).await;
}

#[tracing::instrument(
    name = "streams.coordinator.heartbeat_once",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, member_id = %state.member_id, rejoining),
)]
async fn heartbeat_once<T: HeartbeatTransport>(
    state: &CoordinatorState<T>,
    rejoining: bool,
) -> Outcome {
    let epoch = *state.member_epoch.lock().await;
    let owned = state.owned_active.lock().await.clone();
    let topology = if rejoining || epoch == 0 {
        Some(state.topology.to_wire_request())
    } else {
        None
    };
    let active_tasks = if owned.is_empty() {
        None
    } else {
        Some(owned.iter().map(resp_to_req).collect())
    };
    let owned_standby = state.owned_standby.lock().await.clone();
    let standby_tasks = if owned_standby.is_empty() {
        None
    } else {
        Some(owned_standby.iter().map(resp_to_req).collect())
    };
    let owned_warmup = state.owned_warmup.lock().await.clone();
    let warmup_tasks = if owned_warmup.is_empty() {
        None
    } else {
        Some(owned_warmup.iter().map(resp_to_req).collect())
    };

    let (task_offsets, task_end_offsets) = {
        let tracker = state.tracker.lock().await;
        let to_wire =
            |map: &std::collections::HashMap<(String, i32), i64>| -> Option<Vec<TaskOffset>> {
                if map.is_empty() {
                    None
                } else {
                    let mut list: Vec<TaskOffset> = map
                        .iter()
                        .map(|(key, &offset)| TaskOffset {
                            subtopology_id: key.0.clone(),
                            partition: key.1,
                            offset,
                            ..Default::default()
                        })
                        .collect();
                    list.sort_by(|a, b| match a.subtopology_id.cmp(&b.subtopology_id) {
                        std::cmp::Ordering::Equal => a.partition.cmp(&b.partition),
                        other => other,
                    });
                    Some(list)
                }
            };
        (
            to_wire(&tracker.task_offsets),
            to_wire(&tracker.task_end_offsets),
        )
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
        standby_tasks,
        warmup_tasks,
        task_offsets,
        task_end_offsets,
        ..Default::default()
    };

    match state.client.send_heartbeat(req).await {
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
#[tracing::instrument(
    name = "streams.coordinator.emit_response",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, member_id = %state.member_id, member_epoch = r.member_epoch),
)]
async fn emit_response<T: HeartbeatTransport>(
    state: &CoordinatorState<T>,
    r: &StreamsGroupHeartbeatResponse,
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
    if let Some(tasks) = &r.standby_tasks {
        *state.owned_standby.lock().await = tasks.clone();
    }
    if let Some(tasks) = &r.warmup_tasks {
        *state.owned_warmup.lock().await = tasks.clone();
    }
    let mut last = state.last_assignment.lock().await;
    if let Some(ev) = assignment_event(r, &state.topology, &mut last) {
        let _ = state.events.send(ev);
    }
}

/// Build the assignment from a heartbeat response and decide whether it
/// changed since `last`. Returns the event to emit, or `None` if unchanged.
fn assignment_event(
    r: &StreamsGroupHeartbeatResponse,
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
    use std::{collections::VecDeque, sync::Mutex as StdMutex};

    use assert2::check;
    use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds as RespTaskIds2;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        membership::DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT,
        topology::{NodeHandle, Topology},
    };

    // ---------------------------------------------------------------------------
    // Fake transport
    // ---------------------------------------------------------------------------

    struct FakeTransport {
        responses: StdMutex<VecDeque<Result<StreamsGroupHeartbeatResponse, ClientError>>>,
        sent: Arc<StdMutex<Vec<StreamsGroupHeartbeatRequest>>>,
    }

    impl FakeTransport {
        fn new(responses: Vec<StreamsGroupHeartbeatResponse>) -> Self {
            Self {
                responses: StdMutex::new(responses.into_iter().map(Ok).collect()),
                sent: Arc::new(StdMutex::new(Vec::new())),
            }
        }

        fn sent_arc(&self) -> Arc<StdMutex<Vec<StreamsGroupHeartbeatRequest>>> {
            Arc::clone(&self.sent)
        }
    }

    #[async_trait::async_trait]
    impl HeartbeatTransport for FakeTransport {
        async fn send_heartbeat(
            &self,
            req: StreamsGroupHeartbeatRequest,
        ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
            self.sent.lock().unwrap().push(req);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(ok_resp(7, vec![0])))
        }
    }

    // FakeTransport wrapped in Arc also implements the trait (for the run-loop test)
    #[async_trait::async_trait]
    impl HeartbeatTransport for Arc<FakeTransport> {
        async fn send_heartbeat(
            &self,
            req: StreamsGroupHeartbeatRequest,
        ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
            self.as_ref().send_heartbeat(req).await
        }
    }

    struct HangingLeaveTransport;

    #[async_trait::async_trait]
    impl HeartbeatTransport for HangingLeaveTransport {
        async fn send_heartbeat(
            &self,
            req: StreamsGroupHeartbeatRequest,
        ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
            if req.member_epoch == -1 {
                std::future::pending().await
            } else {
                Ok(ok_resp(7, vec![]))
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    fn built() -> Arc<BuiltTopology> {
        let mut t = Topology::new();
        let src: NodeHandle<bytes::Bytes, bytes::Bytes> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
        Arc::new(t.build("app").unwrap())
    }

    fn ok_resp(epoch: i32, active: Vec<i32>) -> StreamsGroupHeartbeatResponse {
        StreamsGroupHeartbeatResponse {
            error_code: 0,
            member_epoch: epoch,
            heartbeat_interval_ms: 1,
            active_tasks: Some(vec![RespTaskIds2 {
                subtopology_id: "0".into(),
                partitions: active,
                ..Default::default()
            }]),
            ..Default::default()
        }
    }

    fn err_resp(code: i16) -> StreamsGroupHeartbeatResponse {
        StreamsGroupHeartbeatResponse {
            error_code: code,
            ..Default::default()
        }
    }

    fn state_with<T: HeartbeatTransport>(
        client: T,
    ) -> (CoordinatorState<T>, mpsc::UnboundedReceiver<StreamsEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let st = CoordinatorState {
            client,
            group_id: "g".into(),
            member_id: "m".into(),
            process_id: "p".into(),
            instance_id: None,
            rebalance_timeout_ms: 30_000,
            topology: built(),
            member_epoch: Arc::new(Mutex::new(7)),
            owned_active: Arc::new(Mutex::new(Vec::new())),
            owned_standby: Arc::new(Mutex::new(Vec::new())),
            owned_warmup: Arc::new(Mutex::new(Vec::new())),
            tracker: Arc::new(Mutex::new(TaskOffsetTracker::default())),
            heartbeat_interval: Duration::from_millis(1),
            leave_heartbeat_timeout: DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT,
            events: tx,
            last_assignment: tokio::sync::Mutex::new(StreamsAssignment::default()),
        };
        (st, rx)
    }

    // ---------------------------------------------------------------------------
    // heartbeat_once tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn heartbeat_ok_adopts_epoch_and_emits_assignment() {
        let fake = FakeTransport::new(vec![ok_resp(9, vec![0, 1])]);
        let (st, mut rx) = state_with(fake);
        let outcome = heartbeat_once(&st, false).await;
        check!(matches!(outcome, Outcome::Ok));
        check!(*st.member_epoch.lock().await == 9);
        check!(matches!(rx.try_recv(), Ok(StreamsEvent::Assigned(_))));
    }

    #[tokio::test]
    async fn heartbeat_uses_configured_rebalance_timeout() {
        let fake = FakeTransport::new(vec![ok_resp(9, vec![0])]);
        let sent = fake.sent_arc();
        let (mut state, _rx) = state_with(fake);
        state.rebalance_timeout_ms = 45_000;

        check!(matches!(heartbeat_once(&state, false).await, Outcome::Ok));
        check!(sent.lock().unwrap()[0].rebalance_timeout_ms == 45_000);
    }

    #[tokio::test]
    async fn heartbeat_fenced_member_epoch_requests_rejoin() {
        let fake = FakeTransport::new(vec![err_resp(110)]);
        let (st, _rx) = state_with(fake);
        check!(matches!(heartbeat_once(&st, false).await, Outcome::Rejoin));
    }

    #[tokio::test]
    async fn heartbeat_unknown_member_id_requests_rejoin() {
        let fake = FakeTransport::new(vec![err_resp(25)]);
        let (st, _rx) = state_with(fake);
        check!(matches!(heartbeat_once(&st, false).await, Outcome::Rejoin));
    }

    #[tokio::test]
    async fn heartbeat_stale_member_epoch_requests_rejoin() {
        let fake = FakeTransport::new(vec![err_resp(113)]);
        let (st, _rx) = state_with(fake);
        check!(matches!(heartbeat_once(&st, false).await, Outcome::Rejoin));
    }

    #[tokio::test]
    async fn heartbeat_unexpected_code_is_transient() {
        let fake = FakeTransport::new(vec![err_resp(99)]);
        let (st, _rx) = state_with(fake);
        check!(matches!(
            heartbeat_once(&st, false).await,
            Outcome::Transient
        ));
    }

    #[tokio::test]
    async fn heartbeat_transport_error_is_transient() {
        struct ErrTransport;
        #[async_trait::async_trait]
        impl HeartbeatTransport for ErrTransport {
            async fn send_heartbeat(
                &self,
                _req: StreamsGroupHeartbeatRequest,
            ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
                Err(ClientError::Disconnected)
            }
        }
        let (st, _rx) = state_with(ErrTransport);
        check!(matches!(
            heartbeat_once(&st, false).await,
            Outcome::Transient
        ));
    }

    #[tokio::test]
    async fn heartbeat_sends_topology_when_rejoining() {
        let fake = FakeTransport::new(vec![ok_resp(1, vec![])]);
        let sent = fake.sent_arc();
        let (st, _rx) = state_with(fake);
        let _ = heartbeat_once(&st, true).await;
        let sent = sent.lock().unwrap();
        check!(sent[0].topology.is_some());
    }

    #[tokio::test]
    async fn heartbeat_sends_topology_when_epoch_zero() {
        let fake = FakeTransport::new(vec![ok_resp(1, vec![])]);
        let sent = fake.sent_arc();
        let (st, _rx) = state_with(fake);
        *st.member_epoch.lock().await = 0;
        let _ = heartbeat_once(&st, false).await;
        let sent = sent.lock().unwrap();
        check!(sent[0].topology.is_some());
    }

    #[tokio::test]
    async fn heartbeat_echoes_owned_active_tasks() {
        use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds as RespTids;
        let fake = FakeTransport::new(vec![ok_resp(8, vec![0])]);
        let sent = fake.sent_arc();
        let (st, _rx) = state_with(fake);
        // Pre-populate owned_active
        *st.owned_active.lock().await = vec![RespTids {
            subtopology_id: "0".into(),
            partitions: vec![0, 1],
            ..Default::default()
        }];
        let _ = heartbeat_once(&st, false).await;
        let sent = sent.lock().unwrap();
        check!(sent[0].active_tasks.is_some());
    }

    #[tokio::test]
    async fn emit_response_sends_not_ready_for_status() {
        use crabka_protocol::owned::common::streams_group_heartbeat_response::status::Status;
        let fake = FakeTransport::new(vec![]);
        let (st, mut rx) = state_with(fake);
        let resp = StreamsGroupHeartbeatResponse {
            error_code: 0,
            member_epoch: 1,
            status: Some(vec![Status {
                status_code: 0,
                status_detail: "topo-stale".into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        emit_response(&st, &resp).await;
        check!(matches!(rx.try_recv(), Ok(StreamsEvent::NotReady(_))));
    }

    // ---------------------------------------------------------------------------
    // run-loop tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn run_loop_heartbeats_then_leaves_on_shutdown() {
        let fake = Arc::new(FakeTransport::new(vec![ok_resp(8, vec![0, 1])]));
        let sent = fake.sent_arc();
        let (st, mut rx) = state_with(Arc::clone(&fake));
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(run(st, shutdown.clone()));
        // Deterministically wait for the Assigned event the first heartbeat
        // response produces, rather than racing a fixed sleep (which flakes
        // under the ~2-3x slowdown of coverage instrumentation).
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("an Assigned event within 5s")
            .expect("event channel stays open");
        check!(matches!(ev, StreamsEvent::Assigned(_)));
        shutdown.cancel();
        handle.await.unwrap();
        // A leave heartbeat (member_epoch == -1) must have been sent on shutdown.
        let sent = sent.lock().unwrap();
        check!(sent.iter().any(|r| r.member_epoch == -1));
    }

    #[tokio::test]
    async fn run_loop_fenced_emits_fenced_event_and_resets_epoch() {
        // First response fences, subsequent ones succeed.
        let fake = Arc::new(FakeTransport::new(vec![err_resp(110)]));
        let (st, mut rx) = state_with(Arc::clone(&fake));
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(run(st, shutdown.clone()));
        // Deterministically wait for the Fenced event the error response
        // produces, rather than racing a fixed sleep (which flakes under the
        // ~2-3x slowdown of coverage instrumentation).
        let saw_fenced = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(ev) = rx.recv().await {
                if matches!(ev, StreamsEvent::Fenced) {
                    return true;
                }
            }
            false
        })
        .await
        .expect("a Fenced event within 5s");
        check!(saw_fenced);
        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn run_loop_shutdown_immediately_sends_leave() {
        let fake = Arc::new(FakeTransport::new(vec![]));
        let sent = fake.sent_arc();
        let (st, _rx) = state_with(Arc::clone(&fake));
        let shutdown = CancellationToken::new();
        // Cancel immediately before any tick
        shutdown.cancel();
        run(st, shutdown).await;
        let sent = sent.lock().unwrap();
        // Even with immediate shutdown, leave heartbeat must be sent
        check!(sent.iter().any(|r| r.member_epoch == -1));
    }

    #[tokio::test]
    async fn run_loop_bounds_stalled_leave_with_configured_timeout() {
        let (mut state, _rx) = state_with(HangingLeaveTransport);
        state.leave_heartbeat_timeout = Duration::from_millis(37);
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(1), run(state, shutdown))
            .await
            .expect("configured leave timeout bounds shutdown");
    }

    // ---------------------------------------------------------------------------
    // assignment_event (existing tests preserved)
    // ---------------------------------------------------------------------------

    fn built_plain() -> BuiltTopology {
        let mut t = Topology::new();
        let src: NodeHandle<bytes::Bytes, bytes::Bytes> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
        t.build("app").unwrap()
    }

    fn resp_plain(active: Vec<i32>) -> StreamsGroupHeartbeatResponse {
        use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds;
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
        let topo = built_plain();
        let mut last = StreamsAssignment::default();
        let r = resp_plain(vec![0, 1]);
        check!(assignment_event(&r, &topo, &mut last).is_some());
        check!(assignment_event(&r, &topo, &mut last).is_none());
    }

    #[test]
    fn empty_assignment_is_not_emitted_from_default() {
        let topo = built_plain();
        let mut last = StreamsAssignment::default();
        let empty = StreamsGroupHeartbeatResponse {
            active_tasks: Some(vec![]),
            ..Default::default()
        };
        check!(assignment_event(&empty, &topo, &mut last).is_none());
    }

    #[test]
    fn changed_assignment_is_re_emitted() {
        let topo = built_plain();
        let mut last = StreamsAssignment::default();
        check!(assignment_event(&resp_plain(vec![0]), &topo, &mut last).is_some());
        check!(assignment_event(&resp_plain(vec![0, 1]), &topo, &mut last).is_some());
    }
}
