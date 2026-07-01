//! `DescribeShareGroupOffsets` (`api_key` 90) — KIP-932. Returns the
//! share-partition start offset (SPSO), leader epoch, and best-effort lag for
//! each requested `(group, topic, partition)`, read from the share-state
//! persister.
//!
//! Intercepted inline in `network::dispatch` so the handler receives the
//! per-connection principal + peer `SocketAddr` for the per-group `Describe`
//! ACL gate.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest;
use crabka_protocol::owned::describe_share_group_offsets_response::{
    DescribeShareGroupOffsetsResponse, DescribeShareGroupOffsetsResponseGroup,
    DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
};
use crabka_protocol::primitives::uuid::Uuid;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_describe_share_group_offsets",
    level = "info",
    skip_all,
    fields(api = "DescribeShareGroupOffsets", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DescribeShareGroupOffsetsRequest::decode(&mut cur, version)?;

    // Feature gate: a broker with share groups disabled does not implement the
    // RPC. The response has no top-level error code, so mark every requested
    // group with UNSUPPORTED_VERSION.
    if !broker.config.share_group.enable {
        let groups = req
            .groups
            .iter()
            .map(|g| DescribeShareGroupOffsetsResponseGroup {
                group_id: g.group_id.clone(),
                error_code: codes::UNSUPPORTED_VERSION,
                ..Default::default()
            })
            .collect();
        let resp = DescribeShareGroupOffsetsResponse {
            groups,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        return Ok(buf.freeze());
    }

    let image = broker.controller.current_image();
    let ng_opt = Some(broker.group_coordinator.clone());

    let mut groups: Vec<DescribeShareGroupOffsetsResponseGroup> =
        Vec::with_capacity(req.groups.len());

    for group in req.groups {
        let gid = group.group_id;

        // ── ACL preamble ────────────────────────────────────
        // Per-group `Describe` check. On Deny → group `error_code = 30`.
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: gid.as_str(),
            operation: AclOperation::Describe,
        };
        if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
            groups.push(DescribeShareGroupOffsetsResponseGroup {
                group_id: gid,
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }

        // The persister is required to read SPSO. Absent (share groups
        // disabled / not yet bootstrapped) → coordinator-not-available.
        let Some(persister) = ng_opt.as_ref().and_then(|ng| ng.share_persister().cloned()) else {
            groups.push(DescribeShareGroupOffsetsResponseGroup {
                group_id: gid,
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                ..Default::default()
            });
            continue;
        };

        let metadata = ng_opt
            .as_ref()
            .and_then(|ng| ng.share_state_partition_metadata(&gid));

        let req_topics = group.topics.unwrap_or_default();
        let mut topics: Vec<DescribeShareGroupOffsetsResponseTopic> =
            Vec::with_capacity(req_topics.len());

        for rt in req_topics {
            topics.push(
                describe_topic(broker, &persister, &image, metadata.as_ref(), &gid, rt).await,
            );
        }

        groups.push(DescribeShareGroupOffsetsResponseGroup {
            group_id: gid,
            topics,
            error_code: codes::NONE,
            ..Default::default()
        });
    }

    let resp = DescribeShareGroupOffsetsResponse {
        groups,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Build one response topic: resolve `name → id` (unknown ⇒ per-partition
/// `UNKNOWN_TOPIC_OR_PARTITION`); enumerate initialized partitions when the
/// request omits an explicit list; then build a row per partition.
async fn describe_topic(
    broker: &Broker,
    persister: &crate::share_coordinator::persister_client::SharePersister,
    image: &crabka_metadata::MetadataImage,
    metadata: Option<
        &crate::coordinator::unified::share::persistence::ShareGroupStatePartitionMetadataValue,
    >,
    gid: &str,
    rt: crabka_protocol::owned::describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestTopic,
) -> DescribeShareGroupOffsetsResponseTopic {
    let topic_name = rt.topic_name;
    let Some(topic_id) = image.topic(&topic_name).map(|t| t.topic_id) else {
        let partitions = rt
            .partitions
            .into_iter()
            .map(|p| DescribeShareGroupOffsetsResponsePartition {
                partition_index: p,
                start_offset: -1,
                leader_epoch: -1,
                lag: -1,
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                ..Default::default()
            })
            .collect();
        return DescribeShareGroupOffsetsResponseTopic {
            topic_name,
            topic_id: Uuid::default(),
            partitions,
            ..Default::default()
        };
    };

    // Empty request partitions ⇒ enumerate the group's initialized partitions
    // for this topic_id.
    let part_indices: Vec<i32> = if rt.partitions.is_empty() {
        metadata
            .and_then(|m| {
                m.initialized
                    .iter()
                    .find(|(tid, _)| *tid == topic_id)
                    .map(|(_, parts)| parts.clone())
            })
            .unwrap_or_default()
    } else {
        rt.partitions
    };

    let mut partitions: Vec<DescribeShareGroupOffsetsResponsePartition> =
        Vec::with_capacity(part_indices.len());
    for p in part_indices {
        partitions.push(describe_partition(broker, persister, gid, &topic_name, topic_id, p).await);
    }

    DescribeShareGroupOffsetsResponseTopic {
        topic_name,
        topic_id: Uuid(*topic_id.as_bytes()),
        partitions,
        ..Default::default()
    }
}

/// Build one response partition: read SPSO from the persister, then compute
/// best-effort lag (HWM − SPSO) and leader epoch from the local data partition
/// when it's materialized here, else `-1`.
async fn describe_partition(
    broker: &Broker,
    persister: &crate::share_coordinator::persister_client::SharePersister,
    gid: &str,
    topic_name: &str,
    topic_id: uuid::Uuid,
    p: i32,
) -> DescribeShareGroupOffsetsResponsePartition {
    let (start_offset, error_code) = match persister.read_state(gid, topic_id, p).await {
        Ok(Some(state)) => (state.start_offset, codes::NONE),
        Ok(None) => (-1, codes::NONE),
        Err(_) => (-1, codes::COORDINATOR_NOT_AVAILABLE),
    };
    let (leader_epoch, lag) = if let Some(part) = broker.partitions.get(topic_name, p) {
        let hwm = part.high_watermark().await;
        let le = part
            .current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        let lag = if start_offset >= 0 {
            (hwm - start_offset).max(0)
        } else {
            -1
        };
        (le, lag)
    } else {
        (-1, -1)
    };
    DescribeShareGroupOffsetsResponsePartition {
        partition_index: p,
        start_offset,
        leader_epoch,
        lag,
        error_code,
        ..Default::default()
    }
}
