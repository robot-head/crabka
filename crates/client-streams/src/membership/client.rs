//! `StreamsMembership` — public handle for a KIP-1071 streams group.
//!
//! `start` generates a member id, sends the join heartbeat (epoch 0 with
//! topology), captures the broker-assigned epoch / heartbeat interval / initial
//! assignment, then spawns the background heartbeat loop on its own connection
//! (the broker serves a connection serially).
//!
//! `next_event` drains coordinator events; `close` leaves the group.

use std::{sync::Arc, time::Duration};

use crabka_client_core::{Client, ClientDnsTimeout};
use crabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest;
use refined_type::rule::MinMaxU128;
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

/// Default Client Streams rebalance timeout.
pub const DEFAULT_STREAMS_REBALANCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default delay between Client Streams initial join retries.
pub const DEFAULT_STREAMS_JOIN_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Default deadline for the final Client Streams leave heartbeat.
pub const DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

fn validate_positive_whole_milliseconds(field: &str, value: Duration) -> Result<u64, String> {
    let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
        .map_err(|error| format!("{field}: {error}"))?
        .into_value();
    let milliseconds = u64::try_from(milliseconds).map_err(|error| format!("{field}: {error}"))?;
    if Duration::from_millis(milliseconds) != value {
        return Err(format!("{field} must be a whole number of milliseconds"));
    }
    Ok(milliseconds)
}

/// Positive, whole-millisecond delay between Client Streams initial join retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsJoinRetryBackoff(Duration);

impl StreamsJoinRetryBackoff {
    /// Validate a Client Streams initial join retry backoff.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        validate_positive_whole_milliseconds("streams join retry backoff", value)?;
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated whole milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if the validated duration no longer fits in `u64` milliseconds.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis()).expect("validated streams join retry backoff fits u64")
    }
}

impl Default for StreamsJoinRetryBackoff {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_JOIN_RETRY_BACKOFF)
            .expect("default streams join retry backoff is valid")
    }
}

/// Positive, whole-millisecond deadline for the final leave heartbeat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsLeaveHeartbeatTimeout(Duration);

impl StreamsLeaveHeartbeatTimeout {
    /// Validate a final leave-heartbeat timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        validate_positive_whole_milliseconds("streams leave heartbeat timeout", value)?;
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated whole milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if the validated timeout no longer fits in `u64` milliseconds.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .expect("validated streams leave heartbeat timeout fits u64")
    }
}

impl Default for StreamsLeaveHeartbeatTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT)
            .expect("default streams leave heartbeat timeout is valid")
    }
}

fn join_retry_delay(error_code: i16, backoff: StreamsJoinRetryBackoff) -> Option<Duration> {
    (error_code == COORDINATOR_LOAD_IN_PROGRESS).then(|| backoff.duration())
}

/// Positive, whole-millisecond rebalance timeout representable on the Kafka wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsRebalanceTimeout(Duration);

impl StreamsRebalanceTimeout {
    /// Validate a Client Streams rebalance timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value greater
    /// than `i32::MAX` milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if a positive `i32` cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        let milliseconds = MinMaxU128::<1, { i32::MAX as u128 }>::new(value.as_millis())
            .map_err(|error| format!("streams rebalance timeout: {error}"))?
            .into_value();
        let milliseconds = i32::try_from(milliseconds)
            .map_err(|error| format!("streams rebalance timeout: {error}"))?;
        let whole = Duration::from_millis(
            u64::try_from(milliseconds).expect("positive i32 milliseconds fit u64"),
        );
        if whole != value {
            return Err(
                "streams rebalance timeout must be a whole number of milliseconds".to_owned(),
            );
        }
        Ok(Self(value))
    }

    #[must_use]
    /// Return the validated duration.
    pub const fn duration(self) -> Duration {
        self.0
    }

    #[must_use]
    /// Return the validated signed wire milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if the validated timeout no longer fits in `i32`.
    pub fn milliseconds(self) -> i32 {
        i32::try_from(self.0.as_millis()).expect("validated streams rebalance timeout fits i32")
    }
}

impl Default for StreamsRebalanceTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_REBALANCE_TIMEOUT)
            .expect("default streams rebalance timeout is valid")
    }
}

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
    #[tracing::instrument(
        name = "streams.membership.start",
        level = "info",
        skip_all,
        fields(group_id = %group_id, member_id = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "crabka-streams".to_string())] client_id: String,
        #[builder(into)] group_id: String,
        topology: std::sync::Arc<crate::topology::BuiltTopology>,
        #[builder(into)] process_id: Option<String>,
        #[builder(into)] instance_id: Option<String>,
        #[builder(default = DEFAULT_STREAMS_REBALANCE_TIMEOUT)] rebalance_timeout: Duration,
        #[builder(default = DEFAULT_STREAMS_JOIN_RETRY_BACKOFF)] join_retry_backoff: Duration,
        #[builder(default = DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT)]
        leave_heartbeat_timeout: Duration,
        #[builder(default)] broker_dns_timeout: ClientDnsTimeout,
        security: Option<crabka_client_core::security::ClientSecurity>,
        schema_prewarm: Option<std::sync::Arc<dyn SchemaPrewarm>>,
    ) -> Result<Self, StreamsClientError> {
        if group_id.is_empty() {
            return Err(StreamsClientError::Server(0));
        }
        let rebalance_timeout =
            StreamsRebalanceTimeout::new(rebalance_timeout).map_err(StreamsClientError::Runtime)?;
        let join_retry_backoff = StreamsJoinRetryBackoff::new(join_retry_backoff)
            .map_err(StreamsClientError::Runtime)?;
        let leave_heartbeat_timeout = StreamsLeaveHeartbeatTimeout::new(leave_heartbeat_timeout)
            .map_err(StreamsClientError::Runtime)?;
        if let Some(prewarm) = &schema_prewarm {
            prewarm.prewarm().await?;
        }
        let process_id = process_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let member_id = uuid::Uuid::new_v4().to_string();
        tracing::Span::current().record("member_id", tracing::field::display(&member_id));
        let rebalance_timeout_ms = rebalance_timeout.milliseconds();

        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .dns_timeout(broker_dns_timeout.duration())
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
            if let Some(delay) = join_retry_delay(resp.error_code, join_retry_backoff) {
                tokio::time::sleep(delay).await;
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
            .dns_timeout(broker_dns_timeout.duration())
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
            leave_heartbeat_timeout: leave_heartbeat_timeout.duration(),
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
            group: self.group_id.clone(),
            generation: epoch,
            member: self.member_id.clone(),
            group_instance: None,
        }
    }

    /// Await the next membership event (assignment / not-ready / fenced).
    /// Returns [`StreamsClientError::Closed`] once the heartbeat loop has ended.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
    use std::{sync::Arc, time::Duration};

    use assert2::check;
    use crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse;
    use tokio::sync::{Mutex, mpsc};
    use tokio_util::sync::CancellationToken;

    use super::{
        COORDINATOR_LOAD_IN_PROGRESS, StreamsJoinRetryBackoff, StreamsLeaveHeartbeatTimeout,
        StreamsRebalanceTimeout, build_join_heartbeat, heartbeat_interval, join_retry_delay,
        map_error, should_emit_statuses,
    };
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

        check!(req.group_id == "streams-group");
        check!(req.member_id == "member-1");
        check!(req.member_epoch == 0);
        check!(req.process_id.as_deref() == Some("process-1"));
        check!(req.instance_id.as_deref() == Some("instance-1"));
        check!(req.rebalance_timeout_ms == 45_000);
        check!(req.topology.is_some());
    }

    #[test]
    #[allow(clippy::duration_suboptimal_units)]
    fn rebalance_timeout_uses_default_and_valid_override() {
        let default = StreamsRebalanceTimeout::default();
        check!(default.duration() == Duration::from_secs(30));
        check!(default.milliseconds() == 30_000);

        let timeout = StreamsRebalanceTimeout::new(Duration::from_millis(45_000))
            .expect("valid rebalance timeout");
        check!(timeout.duration() == Duration::from_secs(45));
        check!(timeout.milliseconds() == 45_000);
    }

    #[test]
    fn rebalance_timeout_rejects_invalid_wire_values() {
        check!(StreamsRebalanceTimeout::new(Duration::ZERO).is_err());
        check!(
            StreamsRebalanceTimeout::new(Duration::from_millis(1) + Duration::from_nanos(1))
                .is_err()
        );
        check!(
            StreamsRebalanceTimeout::new(Duration::from_millis(
                u64::try_from(i32::MAX).expect("i32 max fits u64") + 1,
            ))
            .is_err()
        );
    }

    #[test]
    fn join_retry_backoff_uses_default_and_valid_override() {
        let default = StreamsJoinRetryBackoff::default();
        check!(default.duration() == Duration::from_millis(200));
        check!(default.milliseconds() == 200);

        let backoff = StreamsJoinRetryBackoff::new(Duration::from_millis(37))
            .expect("positive whole milliseconds");
        check!(backoff.duration() == Duration::from_millis(37));
        check!(backoff.milliseconds() == 37);
    }

    #[test]
    fn join_retry_backoff_validates_millisecond_boundaries() {
        check!(StreamsJoinRetryBackoff::new(Duration::ZERO).is_err());
        check!(
            StreamsJoinRetryBackoff::new(Duration::from_millis(1) + Duration::from_nanos(1))
                .is_err()
        );
        check!(StreamsJoinRetryBackoff::new(Duration::from_millis(u64::MAX)).is_ok());
        check!(
            StreamsJoinRetryBackoff::new(
                Duration::from_millis(u64::MAX) + Duration::from_millis(1)
            )
            .is_err()
        );
    }

    #[test]
    fn leave_heartbeat_timeout_uses_default_and_valid_override() {
        let default = StreamsLeaveHeartbeatTimeout::default();
        check!(default.duration() == Duration::from_secs(5));
        check!(default.milliseconds() == 5_000);

        let timeout = StreamsLeaveHeartbeatTimeout::new(Duration::from_millis(37))
            .expect("positive whole milliseconds");
        check!(timeout.duration() == Duration::from_millis(37));
        check!(timeout.milliseconds() == 37);
    }

    #[test]
    fn leave_heartbeat_timeout_validates_millisecond_boundaries() {
        check!(StreamsLeaveHeartbeatTimeout::new(Duration::ZERO).is_err());
        check!(
            StreamsLeaveHeartbeatTimeout::new(Duration::from_millis(1) + Duration::from_nanos(1))
                .is_err()
        );
        check!(StreamsLeaveHeartbeatTimeout::new(Duration::from_millis(u64::MAX)).is_ok());
        check!(StreamsLeaveHeartbeatTimeout::new(Duration::from_secs(u64::MAX)).is_err());
    }

    struct CountingPrewarm(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait::async_trait]
    impl super::SchemaPrewarm for CountingPrewarm {
        async fn prewarm(&self) -> Result<(), StreamsClientError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn invalid_leave_heartbeat_timeout_fails_before_prewarm_or_broker_lookup() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut topology = Topology::new();
        let source = topology.add_source::<String, String>("source", ["input"]);
        topology.add_sink("sink", "output", [&source]);
        let topology = Arc::new(topology.build("leave-validation").expect("topology"));

        let error = super::StreamsMembership::builder()
            .bootstrap("invalid.invalid:9092")
            .group_id("leave-validation")
            .topology(topology)
            .leave_heartbeat_timeout(Duration::ZERO)
            .schema_prewarm(Arc::new(CountingPrewarm(Arc::clone(&calls))))
            .build()
            .await
            .err()
            .expect("invalid configuration");

        check!(
            error
                .to_string()
                .contains("streams leave heartbeat timeout")
        );
        check!(calls.load(std::sync::atomic::Ordering::Relaxed) == 0);
    }

    #[test]
    fn join_retry_path_uses_configured_backoff_only_while_coordinator_loads() {
        let backoff = StreamsJoinRetryBackoff::new(Duration::from_millis(37))
            .expect("positive whole milliseconds");
        check!(
            join_retry_delay(COORDINATOR_LOAD_IN_PROGRESS, backoff)
                == Some(Duration::from_millis(37))
        );
        check!(join_retry_delay(0, backoff).is_none());
        check!(join_retry_delay(15, backoff).is_none());
    }

    #[test]
    fn heartbeat_interval_uses_positive_broker_value_or_default() {
        check!(heartbeat_interval(1) == std::time::Duration::from_millis(1));
        check!(heartbeat_interval(3_000) == std::time::Duration::from_secs(3));
        check!(heartbeat_interval(0) == std::time::Duration::from_secs(3));
        check!(heartbeat_interval(-1) == std::time::Duration::from_secs(3));
    }

    #[test]
    fn should_emit_statuses_only_for_non_empty_status_list() {
        check!(!should_emit_statuses::<i32>(None));
        check!(!should_emit_statuses(Some(&Vec::<i32>::new())));
        check!(should_emit_statuses(Some(&vec![1])));
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

        check!(membership.member_id() == "member-1");
        check!(membership.group_id() == "group-1");
        check!(Arc::ptr_eq(&membership.tracker(), &tracker));

        let meta = membership.group_metadata().await;
        check!(meta.member == "member-1");
        check!(meta.group == "group-1");
        check!(meta.generation == 42);
        check!(meta.group_instance.is_none());
    }

    #[test]
    fn invalid_topology_family_maps() {
        for code in [130i16, 131, 132] {
            check!(matches!(
                map_error(resp(code)),
                Err(StreamsClientError::InvalidTopology { code: c, .. }) if c == code
            ));
        }
    }

    #[test]
    fn auth_not_found_and_unknown_codes_map() {
        check!(matches!(
            map_error(resp(30)),
            Err(StreamsClientError::Authorization(30))
        ));
        check!(matches!(
            map_error(resp(29)),
            Err(StreamsClientError::Authorization(29))
        ));
        check!(matches!(
            map_error(resp(69)),
            Err(StreamsClientError::GroupIdNotFound)
        ));
        check!(matches!(
            map_error(resp(99)),
            Err(StreamsClientError::Server(99))
        ));
    }
}
