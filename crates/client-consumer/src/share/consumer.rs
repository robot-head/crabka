//! `ShareConsumer` — public lifecycle handle for a KIP-932 share group.
//!
//! Built via [`ShareConsumer::builder`]. The constructor joins the share group
//! with one `ShareGroupHeartbeat` (empty member id, epoch 0, carrying the
//! subscription), captures the broker-assigned member id / epoch / heartbeat
//! interval / assignment, resolves the assignment's topic ids to names via
//! Metadata, then spawns the background heartbeat loop.

use std::{collections::HashMap, sync::Arc, time::Duration};

use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        metadata_request::MetadataRequest,
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use super::{coordinator::ShareCoordinatorState, types::ShareAckMode};
use crate::error::ConsumerError;

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

fn heartbeat_interval_from_response(heartbeat_interval_ms: i32, configured: Duration) -> Duration {
    if heartbeat_interval_ms > 0 {
        Duration::from_millis(u64::try_from(heartbeat_interval_ms).unwrap_or(0))
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
/// It joins the group and keeps the membership alive via a background
/// heartbeat; [`poll`](ShareConsumer::poll) issues `ShareFetch` over the live
/// assignment and returns acquired records, and acknowledgement (implicit or
/// explicit, per [`ShareAckMode`]) is carried back to the broker via the next
/// `ShareFetch` (piggybacked) or a standalone `ShareAcknowledge`
/// ([`commit`](ShareConsumer::commit)).
pub struct ShareConsumer {
    pub(crate) client: Client,
    pub(crate) group_id: String,
    pub(crate) member_id: String,
    /// The live member epoch, owned and advanced by the background heartbeat
    /// loop (which holds the other `Arc`). The consumer keeps this clone so the
    /// shared cell outlives the heartbeat task; `poll()` does not read it (the
    /// data path keys off the share-session epoch, not the member epoch).
    #[allow(dead_code)]
    pub(crate) member_epoch: Arc<Mutex<i32>>,
    /// Live assignment as `(topic_id, topic_name, partition)`, updated by the
    /// heartbeat loop.
    pub(crate) assignment: Arc<Mutex<Vec<(WireUuid, String, i32)>>>,
    pub(crate) topic_names: Arc<Mutex<HashMap<WireUuid, String>>>,
    /// `ShareFetch` session epoch: 0 opens the session, then 1, 2, … per
    /// successful fetch. Owned by `poll()`.
    pub(crate) share_session_epoch: i32,
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
    /// Sends one `ShareGroupHeartbeat` (empty member id, epoch 0, carrying
    /// `subscribe`), captures the assigned member id / epoch / heartbeat
    /// interval / assignment, resolves assignment topic ids → names via
    /// Metadata, then spawns the heartbeat loop.
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
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "crabka-share-consumer".to_string())] client_id: String,
        #[builder(into)] group_id: String,
        #[builder(into)] subscribe: Vec<String>,
        #[builder(default = ShareAckMode::Implicit)] ack_mode: ShareAckMode,
        #[builder(default = std::time::Duration::from_secs(45))] session_timeout: Duration,
        #[builder(default = std::time::Duration::from_secs(3))] heartbeat_interval: Duration,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, ConsumerError> {
        // `session_timeout` is reserved for a future tagged-field on the
        // share heartbeat (the broker derives it from group config today).
        let _ = session_timeout;

        if subscribe.is_empty() {
            return Err(ConsumerError::NotSubscribed);
        }
        if group_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed("group_id required".into()));
        }

        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
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
        let md = client.send(MetadataRequest::default()).await?;
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

    /// The member id assigned by the broker at join time.
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
    /// First flushes any outstanding acknowledgements via a standalone
    /// `ShareAcknowledge`: in Implicit mode the previous poll's delivered ranges
    /// are auto-`Accept`ed; in Explicit mode any staged `acknowledge()` calls are
    /// sent. Then cancels the heartbeat task and awaits it; the task sends a
    /// best-effort leave heartbeat (`member_epoch = -1`) on its way out so the
    /// broker evicts this member promptly rather than waiting out the session
    /// timeout. A flush failure is best-effort (logged) so close still leaves.
    #[tracing::instrument(
        name = "share_consumer.close",
        level = "info",
        skip_all,
        fields(group_id = %self.group_id, member_id = %self.member_id),
        err
    )]
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

    use super::*;

    fn id(n: u8) -> WireUuid {
        let mut b = [0u8; 16];
        b[15] = n;
        WireUuid(b)
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
            ("broker interval", 2500, Duration::from_millis(2500)),
            ("fallback interval", 0, Duration::from_secs(3)),
        ] {
            check!(
                heartbeat_interval_from_response(broker_ms, Duration::from_secs(3)) == expected,
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
