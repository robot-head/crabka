//! `StreamsMembership` — public handle for a KIP-1071 streams group.
//!
//! `start` generates a member id, sends the join heartbeat (epoch 0 with
//! topology), captures the broker-assigned epoch / heartbeat interval / initial
//! assignment, then spawns the background heartbeat loop on its own connection
//! (the broker serves a connection serially).
//!
//! `next_event` drains coordinator events; `close` leaves the group.

use std::{sync::Arc, time::Duration};

use crabka_client_core::Client;
use crabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest;
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::{
    coordinator::{self, CoordinatorState},
    status::map_status,
    types::{StreamsAssignment, StreamsEvent, TaskOffsetTracker},
};
use crate::{error::StreamsClientError, membership::assignment::resolve};

const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;

/// Hook invoked once at membership start to resolve schema ids before
/// processing. Implemented by `SchemaCache` under the `schema-serde` feature.
#[async_trait::async_trait]
pub trait SchemaPrewarm: Send + Sync {
    async fn prewarm(&self) -> Result<(), StreamsClientError>;
}

/// A live streams-group membership. Construct via [`StreamsMembership::builder`].
pub struct StreamsMembership {
    member_id: String,
    group_id: String,
    /// Shared with the coordinator loop; reads the live member epoch for
    /// [`group_metadata`](Self::group_metadata) (EOS `send_offsets_to_transaction`).
    member_epoch: Arc<Mutex<i32>>,
    events: mpsc::UnboundedReceiver<StreamsEvent>,
    shutdown: CancellationToken,
    hb_handle: Option<JoinHandle<()>>,
    tracker: Arc<Mutex<TaskOffsetTracker>>,
}

#[bon::bon]
impl StreamsMembership {
    /// Join a streams group and start heartbeating.
    #[builder(start_fn = builder, finish_fn = build)]
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(
        name = "streams.membership.start",
        level = "info",
        skip_all,
        fields(group_id = %group_id, member_id = tracing::field::Empty),
        err,
    )]
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "crabka-streams".to_string())] client_id: String,
        #[builder(into)] group_id: String,
        topology: std::sync::Arc<crate::topology::BuiltTopology>,
        #[builder(into)] process_id: Option<String>,
        #[builder(into)] instance_id: Option<String>,
        #[builder(default = Duration::from_secs(30))] rebalance_timeout: Duration,
        security: Option<crabka_client_core::security::ClientSecurity>,
        schema_prewarm: Option<std::sync::Arc<dyn SchemaPrewarm>>,
    ) -> Result<Self, StreamsClientError> {
        if group_id.is_empty() {
            return Err(StreamsClientError::Server(0));
        }
        if let Some(prewarm) = &schema_prewarm {
            prewarm.prewarm().await?;
        }
        let process_id = process_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let member_id = uuid::Uuid::new_v4().to_string();
        tracing::Span::current().record("member_id", tracing::field::display(&member_id));
        let rebalance_timeout_ms = i32::try_from(rebalance_timeout.as_millis()).unwrap_or(30_000);

        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .maybe_security(security.clone())
            .build()
            .await?;

        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let join = loop {
            let resp = client
                .send(build_join_heartbeat(
                    &group_id,
                    &member_id,
                    &process_id,
                    instance_id.clone(),
                    rebalance_timeout_ms,
                    &topology,
                ))
                .await?;
            if resp.error_code == COORDINATOR_LOAD_IN_PROGRESS {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            break map_error(resp)?;
        };

        let member_epoch_val = join.member_epoch;
        let hb_interval = heartbeat_interval(join.heartbeat_interval_ms);

        if should_emit_statuses(join.status.as_ref()) {
            let statuses = join.status.as_ref().expect("checked above");
            let _ = events_tx.send(StreamsEvent::NotReady(
                statuses.iter().map(map_status).collect(),
            ));
        }
        let owned_active = Arc::new(Mutex::new(join.active_tasks.clone().unwrap_or_default()));
        let owned_standby = Arc::new(Mutex::new(join.standby_tasks.clone().unwrap_or_default()));
        let owned_warmup = Arc::new(Mutex::new(join.warmup_tasks.clone().unwrap_or_default()));
        let tracker = Arc::new(Mutex::new(TaskOffsetTracker::default()));
        let initial = StreamsAssignment {
            active: resolve(join.active_tasks.as_ref(), &topology),
            standby: resolve(join.standby_tasks.as_ref(), &topology),
            warmup: resolve(join.warmup_tasks.as_ref(), &topology),
        };
        if initial != StreamsAssignment::default() {
            let _ = events_tx.send(StreamsEvent::Assigned(initial.clone()));
        }

        let coordinator_client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .maybe_security(security.clone())
            .build()
            .await?;
        let shutdown = CancellationToken::new();
        // Shared epoch handle: the coordinator advances it each heartbeat; the
        // membership reads it for EOS `group_metadata()`.
        let member_epoch = Arc::new(Mutex::new(member_epoch_val));
        let state = CoordinatorState {
            client: coordinator_client,
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            process_id,
            instance_id,
            rebalance_timeout_ms,
            topology: Arc::clone(&topology),
            member_epoch: Arc::clone(&member_epoch),
            owned_active,
            owned_standby,
            owned_warmup,
            tracker: tracker.clone(),
            heartbeat_interval: hb_interval,
            events: events_tx,
            last_assignment: tokio::sync::Mutex::new(initial),
        };
        let hb_handle = tokio::spawn(coordinator::run(state, shutdown.clone()));

        Ok(Self {
            member_id,
            group_id,
            member_epoch,
            events: events_rx,
            shutdown,
            hb_handle: Some(hb_handle),
            tracker,
        })
    }
}

impl StreamsMembership {
    /// The client-generated member id.
    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Get the shared task offset tracker.
    #[must_use]
    pub fn tracker(&self) -> Arc<Mutex<TaskOffsetTracker>> {
        self.tracker.clone()
    }

    /// The streams group id.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Streams group metadata for the EOS `send_offsets_to_transaction` call.
    ///
    /// The `generation_id` maps to the live member epoch (next-gen
    /// "generation"). The epoch lives behind the coordinator's async `Mutex`, so
    /// this reader is `async` (a sync accessor would have to `blocking_lock`,
    /// which panics inside the runtime's async supervisor).
    #[tracing::instrument(
        name = "streams.membership.group_metadata",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, member_id = %self.member_id),
    )]
    pub async fn group_metadata(&self) -> crate::runtime::eos::StreamsGroupMeta {
        let epoch = *self.member_epoch.lock().await;
        crate::runtime::eos::StreamsGroupMeta {
            group_id: self.group_id.clone(),
            generation_id: epoch,
            member_id: self.member_id.clone(),
            group_instance_id: None,
        }
    }

    /// Await the next membership event (assignment / not-ready / fenced).
    /// Returns [`StreamsClientError::Closed`] once the heartbeat loop has ended.
    pub async fn next_event(&mut self) -> Result<StreamsEvent, StreamsClientError> {
        self.events.recv().await.ok_or(StreamsClientError::Closed)
    }

    /// Leave the group and stop heartbeating.
    #[tracing::instrument(
        name = "streams.membership.close",
        level = "info",
        skip_all,
        fields(group_id = %self.group_id, member_id = %self.member_id),
        err,
    )]
    pub async fn close(&mut self) -> Result<(), StreamsClientError> {
        self.shutdown.cancel();
        if let Some(h) = self.hb_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }
}

fn build_join_heartbeat(
    group_id: &str,
    member_id: &str,
    process_id: &str,
    instance_id: Option<String>,
    rebalance_timeout_ms: i32,
    topology: &crate::topology::BuiltTopology,
) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group_id.to_string(),
        member_id: member_id.to_string(),
        process_id: Some(process_id.to_string()),
        instance_id,
        rebalance_timeout_ms,
        topology: Some(topology.to_wire_request()),
        ..Default::default()
    }
}

fn heartbeat_interval(heartbeat_interval_ms: i32) -> Duration {
    if heartbeat_interval_ms > 0 {
        Duration::from_millis(u64::try_from(heartbeat_interval_ms).unwrap_or(3000))
    } else {
        Duration::from_secs(3)
    }
}

fn should_emit_statuses<T>(statuses: Option<&Vec<T>>) -> bool {
    statuses.is_some_and(|statuses| !statuses.is_empty())
}

/// Map a join-response error code to a typed error (0 = ok).
fn map_error(
    resp: crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
) -> Result<
    crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    StreamsClientError,
> {
    // Kafka error codes for the STREAMS_INVALID_TOPOLOGY family (KIP-1071).
    // The broker coordinator surfaces topology problems via the response Status
    // list, but these are still valid top-level response codes per the
    // StreamsGroupHeartbeatResponse schema.
    const STREAMS_INVALID_TOPOLOGY: i16 = 130;
    const STREAMS_INVALID_TOPOLOGY_EPOCH: i16 = 131;
    const STREAMS_TOPOLOGY_FENCED: i16 = 132;
    // Verified against crates/broker/src/codes.rs:
    const GROUP_AUTHORIZATION_FAILED: i16 = 30; // codes::GROUP_AUTHORIZATION_FAILED
    const TOPIC_AUTHORIZATION_FAILED: i16 = 29; // codes::TOPIC_AUTHORIZATION_FAILED
    const GROUP_ID_NOT_FOUND: i16 = 69; // codes::GROUP_ID_NOT_FOUND
    match resp.error_code {
        0 => Ok(resp),
        c @ (STREAMS_INVALID_TOPOLOGY
        | STREAMS_INVALID_TOPOLOGY_EPOCH
        | STREAMS_TOPOLOGY_FENCED) => Err(StreamsClientError::InvalidTopology {
            code: c,
            message: resp.error_message.unwrap_or_default(),
        }),
        c @ (GROUP_AUTHORIZATION_FAILED | TOPIC_AUTHORIZATION_FAILED) => {
            Err(StreamsClientError::Authorization(c))
        }
        GROUP_ID_NOT_FOUND => Err(StreamsClientError::GroupIdNotFound),
        other => Err(StreamsClientError::Server(other)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse;
    use tokio::sync::{Mutex, mpsc};
    use tokio_util::sync::CancellationToken;

    use super::{build_join_heartbeat, heartbeat_interval, map_error, should_emit_statuses};
    use crate::{
        error::StreamsClientError, membership::types::TaskOffsetTracker, topology::Topology,
    };

    fn resp(code: i16) -> StreamsGroupHeartbeatResponse {
        StreamsGroupHeartbeatResponse {
            error_code: code,
            error_message: Some("detail".into()),
            ..Default::default()
        }
    }

    #[test]
    fn ok_code_passes_through() {
        check!(map_error(resp(0)).is_ok());
    }

    #[test]
    fn build_join_heartbeat_preserves_join_identity_and_topology() {
        let mut topology = Topology::new();
        let source = topology.add_source::<String, String>("source", ["input"]);
        topology.add_sink("sink", "output", [&source]);
        let topology = topology.build("streams-app").unwrap();

        let req = build_join_heartbeat(
            "streams-group",
            "member-1",
            "process-1",
            Some("instance-1".into()),
            45_000,
            &topology,
        );

        check!(
            (
                req.group_id.as_str(),
                req.member_id.as_str(),
                req.member_epoch,
                req.process_id.as_deref(),
                req.instance_id.as_deref(),
                req.rebalance_timeout_ms,
                req.topology.is_some(),
            ) == (
                "streams-group",
                "member-1",
                0,
                Some("process-1"),
                Some("instance-1"),
                45_000,
                true,
            )
        );
    }

    #[test]
    fn heartbeat_interval_uses_positive_broker_value_or_default() {
        for (name, broker_ms, expected) in [
            (
                "positive millisecond",
                1,
                std::time::Duration::from_millis(1),
            ),
            ("positive seconds", 3_000, std::time::Duration::from_secs(3)),
            ("zero defaults", 0, std::time::Duration::from_secs(3)),
            ("negative defaults", -1, std::time::Duration::from_secs(3)),
        ] {
            check!(heartbeat_interval(broker_ms) == expected, "case {name}");
        }
    }

    #[test]
    fn should_emit_statuses_only_for_non_empty_status_list() {
        for (name, statuses, expected) in [
            ("absent", None, false),
            ("empty", Some(vec![]), false),
            ("present", Some(vec![1]), true),
        ] {
            check!(
                should_emit_statuses(statuses.as_ref()) == expected,
                "case {name}"
            );
        }
    }

    #[tokio::test]
    async fn accessors_return_membership_identity_and_tracker() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let member_epoch = Arc::new(Mutex::new(42));
        let tracker = Arc::new(Mutex::new(TaskOffsetTracker::default()));
        let membership = super::StreamsMembership {
            member_id: "member-1".into(),
            group_id: "group-1".into(),
            member_epoch,
            events: rx,
            shutdown: CancellationToken::new(),
            hb_handle: None,
            tracker: tracker.clone(),
        };

        check!((membership.member_id(), membership.group_id()) == ("member-1", "group-1"));
        check!(Arc::ptr_eq(&membership.tracker(), &tracker));

        let meta = membership.group_metadata().await;
        check!(
            (
                meta.member_id.as_str(),
                meta.group_id.as_str(),
                meta.generation_id,
                meta.group_instance_id.as_ref(),
            ) == ("member-1", "group-1", 42, None)
        );
    }

    #[test]
    fn invalid_topology_family_maps() {
        for (name, code) in [
            ("invalid topology", 130i16),
            ("invalid subscription", 131),
            ("invalid repartition", 132),
        ] {
            check!(
                matches!(
                    map_error(resp(code)),
                    Err(StreamsClientError::InvalidTopology { code: c, .. }) if c == code
                ),
                "case {name}"
            );
        }
    }

    #[test]
    fn auth_not_found_and_unknown_codes_map() {
        enum Expected {
            Authorization(i16),
            NotFound,
            Server(i16),
        }
        for (name, code, expected) in [
            ("cluster authorization", 30, Expected::Authorization(30)),
            ("topic authorization", 29, Expected::Authorization(29)),
            ("group not found", 69, Expected::NotFound),
            ("unknown server error", 99, Expected::Server(99)),
        ] {
            let actual = map_error(resp(code));
            let matches = match expected {
                Expected::Authorization(expected) => {
                    matches!(actual, Err(StreamsClientError::Authorization(code)) if code == expected)
                }
                Expected::NotFound => matches!(actual, Err(StreamsClientError::GroupIdNotFound)),
                Expected::Server(expected) => {
                    matches!(actual, Err(StreamsClientError::Server(code)) if code == expected)
                }
            };
            check!(matches, "case {name}");
        }
    }
}
