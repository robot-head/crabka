//! `BrokerHeartbeat` (`api_key=63`). KIP-500 controller-side heartbeat handler.
//!
//! Only the openraft leader handles heartbeats. Non-leaders return
//! `NOT_CONTROLLER` so the broker client can redirect.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataImage, MetadataRecord};
use crabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use crabka_protocol::owned::broker_heartbeat_response::BrokerHeartbeatResponse;
use crabka_protocol::{Decode, Encode};
use crabka_raft::NodeId;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::heartbeat::controller_state::ControllerLivenessState;
use crate::leader_election::select_replacement_leader_for_shutdown;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let liveness = broker.liveness.clone();
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    // Check leadership: this broker is the controller leader iff the
    // watch channel reports a leader id equal to our own node_id.
    let is_leader = controller
        .watch_leader()
        .borrow()
        .is_some_and(|n| n == node_id);
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = BrokerHeartbeatRequest::decode(&mut cur, version)?;

        // Only the openraft leader handles heartbeats. NOT_CONTROLLER
        // tells the broker client to redirect.
        if !is_leader {
            let resp = BrokerHeartbeatResponse {
                throttle_time_ms: 0,
                error_code: codes::NOT_CONTROLLER,
                is_caught_up: false,
                is_fenced: true,
                should_shut_down: false,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        let broker_id_u64 = u64::try_from(req.broker_id).unwrap_or(0);

        // Record the heartbeat. If it's a revival, the liveness ticker
        // will pick up the transition next cycle and the heartbeat-side
        // wakeup is a no-op for slice 10b — slice 11's controlled-shutdown
        // path will add explicit on-revival handling.
        let _transition = liveness.record_heartbeat(broker_id_u64).await;

        // Slice 22: track want_shut_down state and drive leader transfer.
        liveness
            .set_wants_shutdown(broker_id_u64, req.want_shut_down)
            .await;

        let should_shut_down = if req.want_shut_down {
            drain_leaderships_for_shutdown(&controller, &liveness, broker_id_u64).await?
        } else {
            false
        };

        let resp = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            is_caught_up: true,
            is_fenced: false,
            should_shut_down,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

/// Scan partitions where `shutting_down` is currently leader, submit a
/// replacement-leader record for each one where a live ISR alternative
/// exists, and return `true` iff every partition has been re-led (i.e.
/// the broker is safe to shut down). Returns `false` while leadership
/// is still being transferred; the client retries on the next
/// heartbeat tick.
///
/// Pure-by-construction: `MetadataImage` is read-only, the controller
/// is the only side-effect channel. On submit failure we log and
/// return `Ok(false)` so the client will retry rather than crash.
async fn drain_leaderships_for_shutdown(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: &Arc<ControllerLivenessState>,
    shutting_down: u64,
) -> Result<bool, BrokerError> {
    let image: Arc<MetadataImage> = controller.current_image();
    let shutting_down_node: NodeId = shutting_down;

    let mut leader_count: usize = 0;
    let mut changes: Vec<MetadataRecord> = Vec::new();
    for topic in image.topics() {
        for pr in image.partitions_of(&topic.name) {
            if pr.leader != shutting_down_node {
                continue;
            }
            leader_count += 1;
            if let Ok(new_pr) = select_replacement_leader_for_shutdown(
                &image,
                liveness,
                &pr.topic,
                pr.partition,
                shutting_down_node,
            )
            .await
            {
                changes.push(MetadataRecord::V1Partition(new_pr));
            }
            // Else: no live alternative ISR member; leadership stays
            // here for now. The broker has to wait — possibly forever
            // if the cluster has no other replica.
        }
    }

    if !changes.is_empty()
        && let Err(e) = controller.submit_change(changes).await
    {
        tracing::warn!(error = %e, "controlled shutdown: submit_change failed");
        return Ok(false);
    }

    // `leader_count` was computed against the pre-submit image. The
    // submit above (if any) only takes effect on a subsequent
    // heartbeat once the new image is visible — so we report
    // `should_shut_down=true` only when this broker was already not
    // leading anything.
    Ok(leader_count == 0)
}
