//! `ShareConsumer`, the public lifecycle handle for a KIP-932 share group.
//!
//! Build it with [`ShareConsumer::builder`]. The constructor joins the share
//! group with one `ShareGroupHeartbeat` that carries an empty member id, epoch
//! 0, and the subscription. It captures the broker-assigned member id, epoch,
//! heartbeat interval, and assignment. It resolves the assignment's topic ids
//! to names with Metadata, then spawns the background heartbeat loop.

use std::{collections::HashMap, sync::Arc};

use crabka_client_core::Client;
use crabka_protocol::{
    owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    primitives::uuid::Uuid as WireUuid,
};
use crabka_units::{
    ByteSize, Time, bytes,
    convert::{ByteSizeExt as _, TimeExt as _},
    mebibytes, secs,
};
use refined_type::rule::{GreaterI32, GreaterI64};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use super::{
    coordinator::ShareCoordinatorState,
    types::{ShareAckMode, ShareAcquireMode},
};
use crate::error::ConsumerError;

/// Default deadline for the final best-effort `ShareGroup` leave heartbeat.
pub const DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT: Time = secs(5);

/// Default minimum response size for a `ShareFetch`.
pub const DEFAULT_SHARE_CONSUMER_FETCH_MIN: ByteSize = bytes(1);
/// Default maximum response size for a `ShareFetch`.
pub const DEFAULT_SHARE_CONSUMER_FETCH_MAX: ByteSize = mebibytes(50);
/// Default maximum records acquired by a `ShareFetch`.
pub const DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS: i32 = 500;

/// Validated minimum response bytes for a `ShareFetch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerFetchMinBytes(i32);

impl ShareConsumerFetchMinBytes {
    /// Validate a positive minimum byte count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("share consumer fetch min bytes: {error}"))
    }

    /// Return the validated byte count.
    #[must_use]
    pub const fn bytes(self) -> i32 {
        self.0
    }
}

impl TryFrom<ByteSize> for ShareConsumerFetchMinBytes {
    type Error = String;

    fn try_from(value: ByteSize) -> Result<Self, Self::Error> {
        Self::new(protocol_bytes("share consumer fetch min", value)?)
    }
}

impl Default for ShareConsumerFetchMinBytes {
    fn default() -> Self {
        Self::new(DEFAULT_SHARE_CONSUMER_FETCH_MIN.bytes_i32())
            .expect("default share consumer fetch min bytes is valid")
    }
}

/// Validated maximum response bytes for a `ShareFetch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerFetchMaxBytes(i32);

impl ShareConsumerFetchMaxBytes {
    /// Validate a positive maximum byte count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("share consumer fetch max bytes: {error}"))
    }

    /// Return the validated byte count.
    #[must_use]
    pub const fn bytes(self) -> i32 {
        self.0
    }
}

impl TryFrom<ByteSize> for ShareConsumerFetchMaxBytes {
    type Error = String;

    fn try_from(value: ByteSize) -> Result<Self, Self::Error> {
        Self::new(protocol_bytes("share consumer fetch max", value)?)
    }
}

impl Default for ShareConsumerFetchMaxBytes {
    fn default() -> Self {
        Self::new(DEFAULT_SHARE_CONSUMER_FETCH_MAX.bytes_i32())
            .expect("default share consumer fetch max bytes is valid")
    }
}

/// Validated maximum records acquired by a `ShareFetch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerFetchMaxRecords(i32);

impl ShareConsumerFetchMaxRecords {
    /// Validate a positive maximum record count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("share consumer fetch max records: {error}"))
    }

    /// Return the validated record count.
    #[must_use]
    pub const fn records(self) -> i32 {
        self.0
    }
}

impl Default for ShareConsumerFetchMaxRecords {
    fn default() -> Self {
        Self::new(DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS)
            .expect("default share consumer fetch max records is valid")
    }
}

/// Validated deadline for the final best-effort `ShareGroup` leave heartbeat.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ShareConsumerLeaveHeartbeatTimeout(Time);

impl ShareConsumerLeaveHeartbeatTimeout {
    /// Validate a positive, whole-millisecond timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for nonfinite, zero, negative, or fractional values.
    pub fn new(value: Time) -> Result<Self, String> {
        validated_time("share consumer leave-heartbeat timeout", value).map(Self)
    }

    /// Return the validated timeout.
    #[must_use]
    pub const fn time(self) -> Time {
        self.0
    }
}

impl Default for ShareConsumerLeaveHeartbeatTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT)
            .expect("default share consumer leave-heartbeat timeout is valid")
    }
}

fn build_join_heartbeat_request(
    group_id: String,
    subscribe: Vec<String>,
) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id,
        subscribed_topic_names: Some(subscribe),
        ..Default::default()
    }
}

fn response_has_error(error_code: i16) -> bool {
    error_code != 0
}

fn validated_time(name: &str, value: Time) -> Result<Time, String> {
    let milliseconds = GreaterI64::<0>::new(value.millis_i64())
        .map_err(|error| format!("{name}: {error}"))?
        .into_value();
    if value.secs_f64().is_finite() && Time::from_millis(milliseconds) == value {
        Ok(value)
    } else {
        Err(format!("{name} must be a whole number of milliseconds"))
    }
}

fn protocol_bytes(name: &str, value: ByteSize) -> Result<i32, String> {
    let bytes = value.bytes_f64();
    if bytes.is_finite() && bytes.fract() == 0.0 && (1.0..=f64::from(i32::MAX)).contains(&bytes) {
        Ok(value.bytes_i32())
    } else {
        Err(format!(
            "{name} must be a positive whole-byte value that fits i32"
        ))
    }
}

fn heartbeat_interval_from_response(heartbeat_interval_ms: i32, configured: Time) -> Time {
    if heartbeat_interval_ms > 0 {
        Time::from_millis(i64::from(heartbeat_interval_ms))
    } else {
        configured
    }
}

fn has_assignment_partitions(partitions_len: usize) -> bool {
    partitions_len > 0
}

fn should_stage_implicit_accepts(ack_mode: ShareAckMode) -> bool {
    ack_mode == ShareAckMode::Implicit
}

fn stage_implicit_accepts(
    prev_delivered: &mut Vec<(WireUuid, i32, i64, i64)>,
    pending_acks: &mut Vec<(WireUuid, i32, i64, i64, i8)>,
) {
    for (tid, partition, first, last) in std::mem::take(prev_delivered) {
        pending_acks.push((
            tid,
            partition,
            first,
            last,
            super::types::ShareAckType::Accept.wire(),
        ));
    }
}

/// A share-group consumer. Construct via [`ShareConsumer::builder`].
///
/// It joins the group and keeps the membership alive with a background
/// heartbeat. [`poll`](ShareConsumer::poll) issues `ShareFetch` over the live
/// assignment and returns the acquired records. Acknowledgement, implicit or
/// explicit per [`ShareAckMode`], travels back to the broker on the next
/// `ShareFetch` as a piggyback, or in a standalone `ShareAcknowledge` from
/// [`commit`](ShareConsumer::commit).
pub struct ShareConsumer {
    pub(crate) client: Client,
    pub(crate) group_id: String,
    pub(crate) member_id: String,
    /// The live member epoch. The background heartbeat loop owns and advances
    /// it, and holds the other `Arc`. The consumer keeps this clone so the
    /// shared cell outlives the heartbeat task. `poll()` does not read it,
    /// because the data path keys off the share-session epoch and not the
    /// member epoch.
    #[allow(dead_code)]
    pub(crate) member_epoch: Arc<Mutex<i32>>,
    /// Live assignment as `(topic_id, topic_name, partition)`. The heartbeat
    /// loop updates it.
    pub(crate) assignment: Arc<Mutex<Vec<(WireUuid, String, i32)>>>,
    pub(crate) topic_names: Arc<Mutex<HashMap<WireUuid, String>>>,
    /// `ShareFetch` session epoch: 0 opens the session, then 1, 2, … per
    /// successful fetch. `poll()` owns it.
    pub(crate) share_session_epoch: i32,
    pub(crate) fetch_min: ByteSize,
    pub(crate) fetch_max: ByteSize,
    pub(crate) fetch_max_records: i32,
    pub(crate) acquire_mode: ShareAcquireMode,
    pub(crate) ack_mode: ShareAckMode,
    /// Acks staged for the next `ShareFetch` / `ShareAcknowledge` as
    /// `(topic_id, partition, first_offset, last_offset, ack_type_wire)`.
    pub(crate) pending_acks: Vec<(WireUuid, i32, i64, i64, i8)>,
    /// Ranges delivered by the previous `poll()` as
    /// `(topic_id, partition, first_offset, last_offset)`, for implicit-accept.
    pub(crate) prev_delivered: Vec<(WireUuid, i32, i64, i64)>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) hb_handle: Option<JoinHandle<()>>,
}

#[bon::bon]
impl ShareConsumer {
    /// Join a share group and start heartbeating.
    ///
    /// This method sends one `ShareGroupHeartbeat` that carries an empty member
    /// id, epoch 0, and `subscribe`. It captures the assigned member id, epoch,
    /// heartbeat interval, and assignment. It resolves the assignment topic ids
    /// → names with Metadata, then spawns the heartbeat loop.
    #[builder(start_fn = builder, finish_fn = build)]
    #[tracing::instrument(
        name = "share_consumer.start",
        level = "info",
        skip_all,
        fields(
            group_id = %group_id,
            member_id = tracing::field::Empty,
            member_epoch = tracing::field::Empty,
            assigned_partitions = tracing::field::Empty,
        ),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "crabka-share-consumer".to_string())] client_id: String,
        #[builder(into)] group_id: String,
        #[builder(into)] subscribe: Vec<String>,
        #[builder(default = ShareAckMode::Implicit)] ack_mode: ShareAckMode,
        #[builder(default = ShareAcquireMode::BatchOptimized)] acquire_mode: ShareAcquireMode,
        #[builder(default = DEFAULT_SHARE_CONSUMER_FETCH_MIN)] fetch_min: ByteSize,
        #[builder(default = DEFAULT_SHARE_CONSUMER_FETCH_MAX)] fetch_max: ByteSize,
        #[builder(default = DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS)] fetch_max_records: i32,
        #[builder(default = secs(3))] heartbeat_interval: Time,
        #[builder(default = DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT)]
        leave_heartbeat_timeout: Time,
        #[builder(default = crabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY)]
        dispatch_queue_capacity: usize,
        #[builder(default = crabka_client_core::DEFAULT_CLIENT_FRAME_MAX)] frame_max: ByteSize,
        #[builder(default)]
        metadata_recovery_strategy: crabka_client_core::MetadataRecoveryStrategy,
        #[builder(default = crabka_client_core::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER)]
        metadata_recovery_rebootstrap_trigger: Time,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, ConsumerError> {
        if subscribe.is_empty() {
            return Err(ConsumerError::NotSubscribed);
        }
        if group_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed("group_id required".into()));
        }
        // The two fetch-size newtypes below still speak `i32`: both derive `Eq`,
        // which an `f64`-backed quantity cannot satisfy. Their whole job is to
        // police the positive-`int32` invariant, so the quantity meets them at
        // their own boundary and comes straight back.
        let fetch_min = ByteSize::from_bytes_i64(i64::from(
            ShareConsumerFetchMinBytes::try_from(fetch_min)
                .map_err(ConsumerError::RebalanceFailed)?
                .bytes(),
        ));
        let fetch_max = ByteSize::from_bytes_i64(i64::from(
            ShareConsumerFetchMaxBytes::try_from(fetch_max)
                .map_err(ConsumerError::RebalanceFailed)?
                .bytes(),
        ));
        let fetch_max_records = ShareConsumerFetchMaxRecords::new(fetch_max_records)
            .map_err(ConsumerError::RebalanceFailed)?
            .records();
        if fetch_min > fetch_max {
            return Err(ConsumerError::RebalanceFailed(
                "share consumer fetch min bytes must not exceed fetch max bytes".to_owned(),
            ));
        }
        let leave_heartbeat_timeout =
            ShareConsumerLeaveHeartbeatTimeout::new(leave_heartbeat_timeout)
                .map_err(ConsumerError::RebalanceFailed)?
                .time();
        let heartbeat_interval =
            validated_time("share consumer heartbeat interval", heartbeat_interval)
                .map_err(ConsumerError::RebalanceFailed)?;
        let dispatch_queue_capacity =
            crabka_client_core::ConnectionDispatchQueueCapacity::new(dispatch_queue_capacity)
                .map_err(ConsumerError::RebalanceFailed)?;
        let frame_max = crabka_client_core::ClientFrameMax::try_from(frame_max)
            .map_err(ConsumerError::RebalanceFailed)?;
        let metadata_recovery_rebootstrap_trigger =
            crabka_client_core::MetadataRecoveryRebootstrapTrigger::new(
                metadata_recovery_rebootstrap_trigger,
            )
            .map_err(ConsumerError::RebalanceFailed)?;

        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .dispatch_queue_capacity(dispatch_queue_capacity.get())
            .frame_max(frame_max.size())
            .metadata_recovery_strategy(metadata_recovery_strategy)
            .metadata_recovery_rebootstrap_trigger(metadata_recovery_rebootstrap_trigger.time())
            .maybe_security(security.clone())
            .build()
            .await?;

        // 1. Join: empty member id + epoch 0 + the subscription. The broker
        //    assigns a member id and bumps us to a live epoch.
        let join = client
            .send(build_join_heartbeat_request(
                group_id.clone(),
                subscribe.clone(),
            ))
            .await?;
        if response_has_error(join.error_code) {
            return Err(ConsumerError::Server(join.error_code));
        }
        let member_id = join.member_id.clone().unwrap_or_default();
        if member_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed(
                "broker did not assign a member_id".into(),
            ));
        }
        let member_epoch_val = join.member_epoch;
        {
            let span = tracing::Span::current();
            span.record("member_id", member_id.as_str());
            span.record("member_epoch", member_epoch_val);
        }
        // Honor the broker's heartbeat interval when it supplies one; else keep
        // the configured default.
        let hb_interval =
            heartbeat_interval_from_response(join.heartbeat_interval_ms, heartbeat_interval);

        // 2. Resolve assignment topic ids → names via Metadata.
        let md = client.refresh_metadata().await?;
        let mut topic_names: HashMap<WireUuid, String> = HashMap::new();
        for t in &md.topics {
            if let Some(name) = &t.name {
                topic_names.insert(t.topic_id, name.clone());
            }
        }

        // 3. Decode the initial assignment (if the broker placed us already).
        let mut assignment_vec: Vec<(WireUuid, String, i32)> = Vec::new();
        if let Some(assignment) = join.assignment {
            for tp in &assignment.topic_partitions {
                let name = topic_names.get(&tp.topic_id).cloned().unwrap_or_default();
                if has_assignment_partitions(tp.partitions.len()) {
                    for &partition in &tp.partitions {
                        assignment_vec.push((tp.topic_id, name.clone(), partition));
                    }
                }
            }
        }

        tracing::Span::current().record("assigned_partitions", assignment_vec.len());
        let member_epoch = Arc::new(Mutex::new(member_epoch_val));
        let assignment = Arc::new(Mutex::new(assignment_vec));
        let topic_names = Arc::new(Mutex::new(topic_names));
        let shutdown = CancellationToken::new();

        // 4. Spawn the heartbeat loop on its own connection so a parked
        //    request on the data path can't head-of-line-block heartbeats
        //    (the broker serves a connection's requests serially).
        let coordinator_client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .dispatch_queue_capacity(dispatch_queue_capacity.get())
            .frame_max(frame_max.size())
            .metadata_recovery_strategy(metadata_recovery_strategy)
            .metadata_recovery_rebootstrap_trigger(metadata_recovery_rebootstrap_trigger.time())
            .maybe_security(security.clone())
            .build()
            .await?;
        let state = ShareCoordinatorState {
            client: coordinator_client,
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            member_epoch: Arc::clone(&member_epoch),
            assignment: Arc::clone(&assignment),
            topic_names: Arc::clone(&topic_names),
            subscribe,
            heartbeat_interval: hb_interval,
            leave_heartbeat_timeout,
        };
        let hb_handle = tokio::spawn(super::coordinator::run(state, shutdown.clone()));

        Ok(ShareConsumer {
            client,
            group_id,
            member_id,
            member_epoch,
            assignment,
            topic_names,
            share_session_epoch: 0,
            fetch_min,
            fetch_max,
            fetch_max_records,
            acquire_mode,
            ack_mode,
            pending_acks: Vec::new(),
            prev_delivered: Vec::new(),
            shutdown,
            hb_handle: Some(hb_handle),
        })
    }
}

impl ShareConsumer {
    /// The share group id.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The member id that the broker assigned at join time.
    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Snapshot of the currently assigned `(topic, partition)` pairs.
    pub async fn assignment(&self) -> Vec<(String, i32)> {
        self.assignment
            .lock()
            .await
            .iter()
            .map(|(_, name, p)| (name.clone(), *p))
            .collect()
    }

    /// Stop heartbeating, acknowledge outstanding records, and leave the group.
    ///
    /// This method first flushes any outstanding acknowledgements in a
    /// standalone `ShareAcknowledge`. In Implicit mode it auto-`Accept`s the
    /// previous poll's delivered ranges. In Explicit mode it sends any staged
    /// `acknowledge()` calls. It then cancels the heartbeat task and awaits it.
    /// The task sends a best-effort leave heartbeat with `member_epoch = -1` on
    /// its way out, so the broker evicts this member promptly and does not wait
    /// out the session timeout. A flush failure is best-effort and logged, so
    /// close still leaves the group.
    #[tracing::instrument(
        name = "share_consumer.close",
        level = "info",
        skip_all,
        fields(group_id = %self.group_id, member_id = %self.member_id),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn close(&mut self) -> Result<(), ConsumerError> {
        // Roll the previous poll's implicit Accepts into the explicit ack queue
        // so the final flush below covers both modes in one ShareAcknowledge.
        if should_stage_implicit_accepts(self.ack_mode) {
            stage_implicit_accepts(&mut self.prev_delivered, &mut self.pending_acks);
        }
        if let Err(e) = self.flush_pending_acks().await {
            tracing::warn!(error = %e, "share consumer close: final acknowledge failed");
        }

        self.shutdown.cancel();
        if let Some(h) = self.hb_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_protocol::tagged_fields::UnknownTaggedFields;
    use crabka_units::millis;

    use super::*;

    fn id(n: u8) -> WireUuid {
        let mut b = [0u8; 16];
        b[15] = n;
        WireUuid(b)
    }

    #[test]
    fn leave_heartbeat_timeout_uses_default_and_valid_override() {
        let default = ShareConsumerLeaveHeartbeatTimeout::default();
        check!(default.time() == DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT);

        let configured = ShareConsumerLeaveHeartbeatTimeout::new(millis(37)).unwrap();
        check!(configured.time() == millis(37));
    }

    #[test]
    fn leave_heartbeat_timeout_validates_millisecond_boundaries() {
        assert2::assert!(
            ShareConsumerLeaveHeartbeatTimeout::new(secs(0))
                .unwrap_err()
                .contains("share consumer leave-heartbeat timeout")
        );
        assert2::assert!(
            ShareConsumerLeaveHeartbeatTimeout::new(Time::from_micros(1_001))
                .unwrap_err()
                .contains("whole number of milliseconds")
        );
        assert2::assert!(
            ShareConsumerLeaveHeartbeatTimeout::new(Time::from_secs_f64(f64::INFINITY))
                .unwrap_err()
                .contains("share consumer leave-heartbeat timeout")
        );
    }

    #[tokio::test]
    async fn invalid_leave_heartbeat_timeout_fails_before_broker_lookup() {
        let error = ShareConsumer::builder()
            .bootstrap("invalid.invalid:9092")
            .group_id("leave-validation")
            .subscribe(["topic".to_owned()])
            .leave_heartbeat_timeout(crabka_units::secs(0))
            .build()
            .await
            .err()
            .expect("zero leave-heartbeat timeout must fail");

        assert2::assert!(
            error
                .to_string()
                .contains("share consumer leave-heartbeat timeout")
        );
    }

    #[test]
    fn share_fetch_limits_use_defaults_and_valid_overrides() {
        check!(ShareConsumerFetchMinBytes::default().bytes() == 1);
        check!(ShareConsumerFetchMaxBytes::default().bytes() == 52_428_800);
        check!(DEFAULT_SHARE_CONSUMER_FETCH_MIN.bytes_i32() == 1);
        check!(DEFAULT_SHARE_CONSUMER_FETCH_MAX.bytes_i32() == 52_428_800);
        check!(
            ShareConsumerFetchMaxRecords::default().records()
                == DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS
        );

        check!(ShareConsumerFetchMinBytes::new(7).unwrap().bytes() == 7);
        check!(ShareConsumerFetchMaxBytes::new(65_536).unwrap().bytes() == 65_536);
        check!(ShareConsumerFetchMaxRecords::new(37).unwrap().records() == 37);
    }

    #[test]
    fn share_fetch_limits_validate_boundaries() {
        for invalid in [-1, 0] {
            check!(
                ShareConsumerFetchMinBytes::new(invalid)
                    .unwrap_err()
                    .contains("share consumer fetch min bytes")
            );
            check!(
                ShareConsumerFetchMaxBytes::new(invalid)
                    .unwrap_err()
                    .contains("share consumer fetch max bytes")
            );
            check!(
                ShareConsumerFetchMaxRecords::new(invalid)
                    .unwrap_err()
                    .contains("share consumer fetch max records")
            );
        }

        check!(ShareConsumerFetchMinBytes::new(i32::MAX).unwrap().bytes() == i32::MAX);
        check!(ShareConsumerFetchMaxBytes::new(i32::MAX).unwrap().bytes() == i32::MAX);
        check!(
            ShareConsumerFetchMaxRecords::new(i32::MAX)
                .unwrap()
                .records()
                == i32::MAX
        );
    }

    #[tokio::test]
    async fn invalid_share_fetch_limits_fail_before_broker_lookup() {
        let error = ShareConsumer::builder()
            .bootstrap("invalid.invalid:9092")
            .group_id("fetch-limit-validation")
            .subscribe(["topic".to_owned()])
            .fetch_min(bytes(2))
            .fetch_max(bytes(1))
            .build()
            .await
            .err()
            .expect("minimum above maximum must fail");

        check!(
            error
                .to_string()
                .contains("share consumer fetch min bytes must not exceed fetch max bytes")
        );
    }

    #[tokio::test]
    async fn fractional_share_fetch_limit_fails_before_broker_lookup() {
        let error = ShareConsumer::builder()
            .bootstrap("invalid.invalid:9092")
            .group_id("fetch-limit-validation")
            .subscribe(["topic".to_owned()])
            .fetch_min(ByteSize::from_bytes_f64(1.5))
            .build()
            .await
            .err()
            .expect("fractional fetch limit must fail");

        check!(error.to_string().contains("positive whole-byte"));
    }

    #[tokio::test]
    async fn invalid_client_resource_policy_fails_before_broker_lookup() {
        let error = ShareConsumer::builder()
            .bootstrap("invalid.invalid:9092")
            .group_id("client-policy-validation")
            .subscribe(["topic".to_owned()])
            .dispatch_queue_capacity(0)
            .build()
            .await
            .err()
            .expect("invalid client policy");

        check!(error.to_string().contains("client dispatch queue capacity"));
    }

    async fn test_consumer() -> ShareConsumer {
        ShareConsumer {
            client: Client::builder()
                .bootstrap("127.0.0.1:1")
                .client_id("share-test")
                .build()
                .await
                .unwrap(),
            group_id: "group-a".into(),
            member_id: "member-a".into(),
            member_epoch: Arc::new(Mutex::new(3)),
            assignment: Arc::new(Mutex::new(vec![(id(7), "topic-a".into(), 2)])),
            topic_names: Arc::new(Mutex::new(HashMap::new())),
            share_session_epoch: 0,
            fetch_min: DEFAULT_SHARE_CONSUMER_FETCH_MIN,
            fetch_max: DEFAULT_SHARE_CONSUMER_FETCH_MAX,
            fetch_max_records: DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS,
            acquire_mode: ShareAcquireMode::BatchOptimized,
            ack_mode: ShareAckMode::Explicit,
            pending_acks: Vec::new(),
            prev_delivered: Vec::new(),
            shutdown: CancellationToken::new(),
            hb_handle: None,
        }
    }

    #[test]
    fn join_heartbeat_request_preserves_group_member_epoch_and_subscription() {
        let req = build_join_heartbeat_request("group-a".into(), vec!["topic-a".into()]);

        assert2::assert!(
            req == ShareGroupHeartbeatRequest {
                group_id: "group-a".into(),
                member_id: String::new(),
                member_epoch: 0,
                rack_id: None,
                subscribed_topic_names: Some(vec!["topic-a".into()]),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }
        );
    }

    #[test]
    fn join_response_helpers_preserve_error_interval_and_assignment_boundaries() {
        for (name, code, expected) in [("success", 0, false), ("error", 17, true)] {
            check!(response_has_error(code) == expected, "case {name}");
        }
        for (name, broker_ms, expected) in [
            ("broker interval", 2500, crabka_units::millis(2500)),
            ("fallback interval", 0, crabka_units::secs(3)),
        ] {
            check!(
                heartbeat_interval_from_response(broker_ms, crabka_units::secs(3)) == expected,
                "case {name}"
            );
        }
        for (name, count, expected) in [("empty", 0, false), ("present", 1, true)] {
            check!(has_assignment_partitions(count) == expected, "case {name}");
        }
        for (name, mode, expected) in [
            ("implicit", ShareAckMode::Implicit, true),
            ("explicit", ShareAckMode::Explicit, false),
        ] {
            check!(
                should_stage_implicit_accepts(mode) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn stage_implicit_accepts_moves_delivered_ranges_to_pending_acks() {
        let mut prev = vec![(id(7), 2, 10, 12)];
        let mut pending = Vec::new();

        stage_implicit_accepts(&mut prev, &mut pending);

        assert2::assert!(prev.is_empty());
        assert2::assert!(
            pending
                == vec![(
                    id(7),
                    2,
                    10,
                    12,
                    crate::share::types::ShareAckType::Accept.wire()
                )]
        );
    }

    #[tokio::test]
    async fn accessors_return_share_identity_and_assignment() {
        let consumer = test_consumer().await;

        check!(
            (
                consumer.group_id(),
                consumer.member_id(),
                consumer.assignment().await
            ) == ("group-a", "member-a", vec![("topic-a".into(), 2)])
        );
    }

    #[tokio::test]
    async fn close_cancels_shutdown_token_without_spawned_handle() {
        let mut consumer = test_consumer().await;

        assert2::assert!(!consumer.shutdown.is_cancelled());
        consumer.close().await.unwrap();
        assert2::assert!(consumer.shutdown.is_cancelled());
    }
}
