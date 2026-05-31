//! `ShareConsumer` — public lifecycle handle for a KIP-932 share group.
//!
//! Built via [`ShareConsumer::builder`]. The constructor joins the share group
//! with one `ShareGroupHeartbeat` (empty member id, epoch 0, carrying the
//! subscription), captures the broker-assigned member id / epoch / heartbeat
//! interval / assignment, resolves the assignment's topic ids to names via
//! Metadata, then spawns the background heartbeat loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use super::coordinator::ShareCoordinatorState;
use super::types::ShareAckMode;
use crate::error::ConsumerError;

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
            .send(ShareGroupHeartbeatRequest {
                group_id: group_id.clone(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(subscribe.clone()),
                ..Default::default()
            })
            .await?;
        if join.error_code != 0 {
            return Err(ConsumerError::Server(join.error_code));
        }
        let member_id = join.member_id.clone().unwrap_or_default();
        if member_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed(
                "broker did not assign a member_id".into(),
            ));
        }
        let member_epoch_val = join.member_epoch;
        // Honor the broker's heartbeat interval when it supplies one; else keep
        // the configured default.
        let hb_interval = if join.heartbeat_interval_ms > 0 {
            Duration::from_millis(u64::try_from(join.heartbeat_interval_ms).unwrap_or(0))
        } else {
            heartbeat_interval
        };

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
                for &partition in &tp.partitions {
                    assignment_vec.push((tp.topic_id, name.clone(), partition));
                }
            }
        }

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
    pub async fn close(&mut self) -> Result<(), ConsumerError> {
        // Roll the previous poll's implicit Accepts into the explicit ack queue
        // so the final flush below covers both modes in one ShareAcknowledge.
        if self.ack_mode == ShareAckMode::Implicit {
            for (tid, partition, first, last) in std::mem::take(&mut self.prev_delivered) {
                self.pending_acks.push((
                    tid,
                    partition,
                    first,
                    last,
                    super::types::ShareAckType::Accept.wire(),
                ));
            }
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
