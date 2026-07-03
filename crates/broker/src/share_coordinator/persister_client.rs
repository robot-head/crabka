//! `SharePersister` — the group-coordinator's client onto the share-state
//! persister (KIP-932). It bridges the share-group membership coordinator to
//! the durable [`ShareCoordinator`]:
//!
//! - When a share group joins a topic, the coordinator calls
//!   [`SharePersister::initialize`] for each newly-assigned `(topic, partition)`.
//! - When a topic leaves the subscription (or the group empties), it calls
//!   [`SharePersister::delete`].
//!
//! Routing mirrors [`crate::handlers::find_coordinator`]: the share key
//! `(group, topic_id, partition)` maps to a `__share_group_state` partition via
//! [`ShareCoordinator::state_partition_for`]. If this broker leads that state
//! partition the call dispatches into the local [`ShareCoordinator`] directly;
//! otherwise it resolves the partition leader from the metadata image and sends
//! the typed `InitializeShareGroupState` / `DeleteShareGroupState` RPC over the
//! inter-broker client (the same shape as
//! [`crate::txn::handlers::end_txn`]'s `WriteTxnMarkers` dial).
//!
//! Errors are returned to the caller — the lifecycle hook treats them as
//! best-effort and retries on the next heartbeat, never failing the heartbeat.

use std::sync::Arc;

use crabka_log::Offset;
use crabka_metadata::NodeId;
use crabka_protocol::{
    owned::{
        delete_share_group_state_request::{
            DeleteShareGroupStateRequest, DeleteStateData, PartitionData as DeletePartitionData,
        },
        initialize_share_group_state_request::{
            InitializeShareGroupStateRequest, InitializeStateData,
            PartitionData as InitPartitionData,
        },
        read_share_group_state_request::{
            PartitionData as ReadPartitionData, ReadShareGroupStateRequest, ReadStateData,
        },
        write_share_group_state_request::{
            PartitionData as WritePartitionData, StateBatch as ProtoStateBatch,
            WriteShareGroupStateRequest, WriteStateData,
        },
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use crabka_security::ListenerProtocol;

use crate::{
    error::BrokerError,
    metadata_source::MetadataSource,
    network::client::InterBrokerClient,
    share_coordinator::{
        bootstrap, coordinator::ShareCoordinator, persistence::StateBatch,
        state::SharePartitionState,
    },
};

/// Group-coordinator-side client for the share-state persister. Constructed in
/// `Broker::start` once both the [`ShareCoordinator`] and the `GroupCoordinator`
/// exist, and handed to the `GroupCoordinator` so its per-group share actors can
/// drive Initialize/Delete lifecycle calls.
pub(crate) struct SharePersister {
    node_id: NodeId,
    share_coordinator: Arc<ShareCoordinator>,
    controller: Arc<dyn MetadataSource>,
    inter_broker_client: Arc<InterBrokerClient>,
    inter_broker_listener_protocol: ListenerProtocol,
    inter_broker_listener_name: String,
}

impl std::fmt::Debug for SharePersister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharePersister")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl SharePersister {
    pub(crate) fn new(
        node_id: NodeId,
        share_coordinator: Arc<ShareCoordinator>,
        controller: Arc<dyn MetadataSource>,
        inter_broker_client: Arc<InterBrokerClient>,
        inter_broker_listener_protocol: ListenerProtocol,
        inter_broker_listener_name: String,
    ) -> Self {
        Self {
            node_id,
            share_coordinator,
            controller,
            inter_broker_client,
            inter_broker_listener_protocol,
            inter_broker_listener_name,
        }
    }

    /// Initialize the share state for `(group, topic_id, partition)` at
    /// `state_epoch` / `start_offset`. Local when this broker leads the target
    /// `__share_group_state` partition, else routed to the leader via RPC.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Share`] if the local coordinator fences/rejects
    /// the call, or [`BrokerError`] from the inter-broker dial/send on the
    /// remote path. The caller logs and retries — it must not fail a heartbeat.
    pub(crate) async fn initialize(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        state_epoch: i32,
        start_offset: Offset,
    ) -> Result<(), BrokerError> {
        // Lazily create `__share_group_state` and refresh local leadership —
        // the lifecycle hook is the first thing to touch the topic when no
        // client has issued FindCoordinator(SHARE) yet. Mirrors how the txn
        // handlers refresh before dispatching.
        self.ensure_topic_and_refresh().await?;

        let state_partition = self
            .share_coordinator
            .state_partition_for(group, &topic_id, partition);
        if self.share_coordinator.is_leader(state_partition).await {
            return self
                .share_coordinator
                .initialize(group, topic_id, partition, state_epoch, start_offset)
                .await
                .map_err(|code| {
                    BrokerError::Share(format!(
                        "InitializeShareGroupState {group}:{topic_id}:{partition} fenced (code {code})"
                    ))
                });
        }

        let req = InitializeShareGroupStateRequest {
            group_id: group.to_string(),
            topics: vec![InitializeStateData {
                topic_id: ProtoUuid(*topic_id.as_bytes()),
                partitions: vec![InitPartitionData {
                    partition,
                    state_epoch,
                    start_offset: start_offset.0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        self.send_to_leader(state_partition, req).await
    }

    /// Delete the share state for `(group, topic_id, partition)`. Local when
    /// this broker leads the target `__share_group_state` partition, else routed
    /// to the leader via RPC.
    ///
    /// # Errors
    ///
    /// As [`SharePersister::initialize`].
    pub(crate) async fn delete(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), BrokerError> {
        self.ensure_topic_and_refresh().await?;

        let state_partition = self
            .share_coordinator
            .state_partition_for(group, &topic_id, partition);
        if self.share_coordinator.is_leader(state_partition).await {
            return self
                .share_coordinator
                .delete(group, topic_id, partition)
                .await
                .map_err(|code| {
                    BrokerError::Share(format!(
                        "DeleteShareGroupState {group}:{topic_id}:{partition} failed (code {code})"
                    ))
                });
        }

        let req = DeleteShareGroupStateRequest {
            group_id: group.to_string(),
            topics: vec![DeleteStateData {
                topic_id: ProtoUuid(*topic_id.as_bytes()),
                partitions: vec![DeletePartitionData {
                    partition,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        self.send_to_leader(state_partition, req).await
    }

    /// Ensure `__share_group_state` exists and refresh this broker's view of
    /// which of its partitions it leads. The topic is created lazily (idempotent
    /// — tolerates an existing topic); the leadership refresh picks up any
    /// partitions already materialized locally by the replicator supervisor.
    async fn ensure_topic_and_refresh(&self) -> Result<(), BrokerError> {
        bootstrap::ensure_topic(&self.controller).await?;
        self.share_coordinator
            .refresh_leader_partitions(&self.controller.current_image())
            .await;
        Ok(())
    }

    /// Read the durable share state for `(group, topic_id, partition)`. Local
    /// when this broker leads the target `__share_group_state` partition, else
    /// routed to the leader via RPC and decoded from the typed response.
    ///
    /// # Errors
    ///
    /// As [`SharePersister::initialize`] (connect/send on the remote path).
    // Consumed by `SharePartitionLeaderManager::get_or_load`, which the
    // ShareFetch/ShareAcknowledge handlers drive.
    pub(crate) async fn read_state(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<SharePartitionState>, BrokerError> {
        self.ensure_topic_and_refresh().await?;

        let state_partition = self
            .share_coordinator
            .state_partition_for(group, &topic_id, partition);
        if self.share_coordinator.is_leader(state_partition).await {
            return Ok(self
                .share_coordinator
                .read(group, topic_id, partition)
                .await);
        }

        let req = ReadShareGroupStateRequest {
            group_id: group.to_string(),
            topics: vec![ReadStateData {
                topic_id: ProtoUuid(*topic_id.as_bytes()),
                partitions: vec![ReadPartitionData {
                    partition,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = self.send_to_leader_resp(state_partition, req).await?;
        // Map the per-partition result into a `SharePartitionState`. A
        // non-zero error_code or an absent partition entry is treated as
        // "no state" (the caller starts from an empty acquisition window).
        let part_result = resp
            .results
            .into_iter()
            .flat_map(|t| t.partitions)
            .find(|p| p.partition == partition);
        let Some(pr) = part_result else {
            return Ok(None);
        };
        if pr.error_code != 0 {
            return Ok(None);
        }
        Ok(Some(SharePartitionState {
            state_epoch: pr.state_epoch,
            leader_epoch: 0,
            start_offset: Offset(pr.start_offset),
            delivery_complete_count: 0,
            state_batches: pr
                .state_batches
                .into_iter()
                .map(|b| StateBatch {
                    first_offset: Offset(b.first_offset),
                    last_offset: Offset(b.last_offset),
                    delivery_state: b.delivery_state,
                    delivery_count: b.delivery_count,
                })
                .collect(),
            snapshot_epoch: 0,
            last_snapshot_offset: Offset(0),
            updates_since_snapshot: 0,
        }))
    }

    /// Persist a `WriteShareGroupState` delta for `(group, topic_id,
    /// partition)`. Local when this broker leads the target
    /// `__share_group_state` partition, else routed to the leader via RPC.
    ///
    /// # Errors
    ///
    /// As [`SharePersister::initialize`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn write_state(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        state_epoch: i32,
        leader_epoch: i32,
        start_offset: Offset,
        delivery_complete_count: i32,
        batches: Vec<StateBatch>,
    ) -> Result<(), BrokerError> {
        self.ensure_topic_and_refresh().await?;

        let state_partition = self
            .share_coordinator
            .state_partition_for(group, &topic_id, partition);
        if self.share_coordinator.is_leader(state_partition).await {
            return self
                .share_coordinator
                .write(
                    group,
                    topic_id,
                    partition,
                    state_epoch,
                    leader_epoch,
                    start_offset,
                    delivery_complete_count,
                    batches,
                )
                .await
                .map_err(|code| {
                    BrokerError::Share(format!(
                        "WriteShareGroupState {group}:{topic_id}:{partition} fenced (code {code})"
                    ))
                });
        }

        let req = WriteShareGroupStateRequest {
            group_id: group.to_string(),
            topics: vec![WriteStateData {
                topic_id: ProtoUuid(*topic_id.as_bytes()),
                partitions: vec![WritePartitionData {
                    partition,
                    state_epoch,
                    leader_epoch,
                    start_offset: start_offset.0,
                    delivery_complete_count,
                    state_batches: batches
                        .into_iter()
                        .map(|b| ProtoStateBatch {
                            first_offset: b.first_offset.0,
                            last_offset: b.last_offset.0,
                            delivery_state: b.delivery_state,
                            delivery_count: b.delivery_count,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        self.send_to_leader(state_partition, req).await
    }

    /// Resolve the leader of `__share_group_state`-`state_partition` from the
    /// metadata image and open an inter-broker connection to it. Mirrors the
    /// endpoint resolution in `txn::handlers::end_txn`.
    async fn connect_to_leader(
        &self,
        state_partition: i32,
    ) -> Result<crabka_client_core::Connection, BrokerError> {
        let image = self.controller.current_image();
        let pr = image
            .partition(bootstrap::TOPIC, state_partition)
            .ok_or_else(|| {
                BrokerError::Share(format!(
                    "{}-{state_partition} not present in metadata image",
                    bootstrap::TOPIC
                ))
            })?;
        let leader = pr.leader;
        let broker_info = image.broker(leader).ok_or_else(|| {
            BrokerError::Share(format!(
                "share-state leader node {leader} not in metadata image"
            ))
        })?;
        let (host, port) = broker_info
            .endpoints
            .iter()
            .find(|e| e.name == self.inter_broker_listener_name)
            .map_or_else(
                || (broker_info.host.clone(), broker_info.port),
                |e| (e.host.clone(), e.port),
            );

        let opts = crabka_client_core::ConnectionOptions {
            client_id: format!("crabka-broker-share-{}", self.node_id),
            ..crabka_client_core::ConnectionOptions::default()
        };
        self.inter_broker_client
            .connect_as_connection(
                &host,
                port,
                self.inter_broker_listener_protocol,
                "localhost",
                opts,
            )
            .await
            .map_err(|e| BrokerError::Share(format!("share-state connect to {host}:{port}: {e}")))
    }

    /// Send `req` to the `state_partition` leader, discarding the response.
    async fn send_to_leader<R>(&self, state_partition: i32, req: R) -> Result<(), BrokerError>
    where
        R: crabka_client_core::ProtocolRequest,
    {
        let conn = self.connect_to_leader(state_partition).await?;
        let _resp = conn
            .send(req)
            .await
            .map_err(|e| BrokerError::Share(format!("share-state RPC: {e}")))?;
        conn.close();
        Ok(())
    }

    /// Send `req` to the `state_partition` leader and return the typed
    /// response.
    async fn send_to_leader_resp<R>(
        &self,
        state_partition: i32,
        req: R,
    ) -> Result<R::Response, BrokerError>
    where
        R: crabka_client_core::ProtocolRequest,
    {
        let conn = self.connect_to_leader(state_partition).await?;
        let resp = conn
            .send(req)
            .await
            .map_err(|e| BrokerError::Share(format!("share-state RPC: {e}")))?;
        conn.close();
        Ok(resp)
    }
}
