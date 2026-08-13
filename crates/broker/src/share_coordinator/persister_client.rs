//! `SharePersister`, the group-coordinator's client onto the share-state
//! persister (KIP-932). It bridges the share-group membership coordinator to
//! the durable [`ShareCoordinator`]:
//!
//! - When a share group joins a topic, the coordinator calls
//!   [`SharePersister::initialize`] for each newly-assigned `(topic, partition)`.
//! - When a topic leaves the subscription, or the group empties, the
//!   coordinator calls [`SharePersister::delete`].
//!
//! Routing mirrors [`crate::handlers::find_coordinator`]:
//! [`ShareCoordinator::state_partition_for`] maps the share key
//! `(group, topic_id, partition)` to a `__share_group_state` partition. If
//! this broker leads that state partition, the call dispatches into the local
//! [`ShareCoordinator`] directly. If it does not, the client resolves the
//! partition leader from the metadata image and sends the typed
//! `InitializeShareGroupState` or `DeleteShareGroupState` RPC over the
//! inter-broker client. That is the same shape as the `WriteTxnMarkers` dial
//! in [`crate::txn::handlers::end_txn`].
//!
//! This module returns errors to the caller. The lifecycle hook treats them as
//! best-effort and retries on the next heartbeat. It never fails the
//! heartbeat.

use std::{sync::Arc, time::Duration};

use crabka_ids::PartitionIndex;
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

/// Group-coordinator-side client for the share-state persister. `Broker::start`
/// constructs it after both the [`ShareCoordinator`] and the
/// `GroupCoordinator` exist, and hands it to the `GroupCoordinator` so its
/// per-group share actors can drive Initialize and Delete lifecycle calls.
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
    /// `state_epoch` and `start_offset`. The call is local when this broker
    /// leads the target `__share_group_state` partition. If it does not, the
    /// client routes the call to the leader over RPC.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Share`] if the local coordinator fences or
    /// rejects the call, or [`BrokerError`] from the inter-broker dial or send
    /// on the remote path. The caller logs and retries. It must not fail a
    /// heartbeat.
    // cargo-mutants: the `topics` payload is only sent on the follower->leader
    // remote path (`send_to_leader` dials a real inter-broker socket); only the
    // live-broker integration suite exercises it, not in-file unit tests.
    #[cfg_attr(test, mutants::skip)]
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
        let state_partition = self
            .share_coordinator
            .state_partition_for(group, &topic_id, partition);
        self.ensure_topic_and_refresh(state_partition).await?;
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

    /// Delete the share state for `(group, topic_id, partition)`. The call is
    /// local when this broker leads the target `__share_group_state`
    /// partition. If it does not, the client routes the call to the leader
    /// over RPC.
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
        let state_partition = self
            .share_coordinator
            .state_partition_for(group, &topic_id, partition);
        self.ensure_topic_and_refresh(state_partition).await?;
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

    /// Make sure `__share_group_state` exists, and refresh this broker's view
    /// of which of its partitions it leads. This method creates the topic
    /// lazily. The creation is idempotent and accepts an existing topic. The
    /// leadership refresh picks up every partition that the replicator
    /// supervisor has already materialized locally.
    async fn ensure_topic_and_refresh(
        &self,
        state_partition: PartitionIndex,
    ) -> Result<(), BrokerError> {
        bootstrap::ensure_topic(
            &self.controller,
            self.share_coordinator.state_topic_num_partitions(),
            self.share_coordinator.state_topic_replication_factor(),
        )
        .await?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let image = self.controller.current_image();
            self.share_coordinator
                .refresh_leader_partitions(&image)
                .await;
            match image.partition(bootstrap::TOPIC, state_partition.get()) {
                Some(metadata) if metadata.leader != self.node_id => return Ok(()),
                Some(_)
                    if self
                        .share_coordinator
                        .partitions
                        .contains(bootstrap::TOPIC, state_partition) =>
                {
                    return Ok(());
                }
                _ if tokio::time::Instant::now() >= deadline => {
                    return Err(BrokerError::Share(format!(
                        "timed out waiting for {}-{state_partition} to materialize locally",
                        bootstrap::TOPIC
                    )));
                }
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    /// Read the durable share state for `(group, topic_id, partition)`. The
    /// call is local when this broker leads the target `__share_group_state`
    /// partition. If it does not, the client routes the call to the leader
    /// over RPC and decodes the typed response.
    ///
    /// # Errors
    ///
    /// As [`SharePersister::initialize`], from the connect or send on the
    /// remote path.
    // Consumed by `SharePartitionLeaderManager::get_or_load`, which the
    // ShareFetch/ShareAcknowledge handlers drive.
    pub(crate) async fn read_state(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<SharePartitionState>, BrokerError> {
        let state_partition = self
            .share_coordinator
            .state_partition_for(group, &topic_id, partition);
        self.ensure_topic_and_refresh(state_partition).await?;
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
        let Some(start_offset) = initialized_start_offset(pr.start_offset) else {
            return Ok(None);
        };
        Ok(Some(SharePartitionState {
            state_epoch: pr.state_epoch,
            leader_epoch: 0,
            start_offset,
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
    /// partition)`. The call is local when this broker leads the target
    /// `__share_group_state` partition. If it does not, the client routes the
    /// call to the leader over RPC.
    ///
    /// # Errors
    ///
    /// As [`SharePersister::initialize`].
    // cargo-mutants: the `topics` payload is only sent on the follower->leader
    // remote path (`send_to_leader` dials a real inter-broker socket); only the
    // live-broker integration suite exercises it, not in-file unit tests.
    #[cfg_attr(test, mutants::skip)]
    pub(crate) async fn write_state(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        epochs: (i32, i32),
        progress: (Offset, i32),
        batches: Vec<StateBatch>,
    ) -> Result<(), BrokerError> {
        let (state_epoch, leader_epoch) = epochs;
        let (start_offset, delivery_complete_count) = progress;
        let state_partition = self
            .share_coordinator
            .state_partition_for(group, &topic_id, partition);
        self.ensure_topic_and_refresh(state_partition).await?;
        if self.share_coordinator.is_leader(state_partition).await {
            return self
                .share_coordinator
                .write(group, topic_id, partition, epochs, progress, batches)
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
        state_partition: PartitionIndex,
    ) -> Result<crabka_client_core::Connection, BrokerError> {
        let image = self.controller.current_image();
        let pr = image
            .partition(bootstrap::TOPIC, state_partition.get())
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

    /// Send `req` to the `state_partition` leader and discard the response.
    async fn send_to_leader<R>(
        &self,
        state_partition: PartitionIndex,
        req: R,
    ) -> Result<(), BrokerError>
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
        state_partition: PartitionIndex,
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

fn initialized_start_offset(raw: i64) -> Option<Offset> {
    (raw >= 0).then_some(Offset(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negative_start_offset_is_uninitialized_state() {
        assert!(initialized_start_offset(-1).is_none());
        assert!(initialized_start_offset(0) == Some(Offset(0)));
    }
}
