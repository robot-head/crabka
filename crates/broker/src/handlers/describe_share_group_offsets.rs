//! `DescribeShareGroupOffsets` (`api_key` 90), from KIP-932. It returns the
//! share-partition start offset (SPSO), leader epoch, and best-effort lag for
//! each requested `(group, topic, partition)`, read from the share-state
//! persister.
//!
//! `network::dispatch` intercepts it inline, so the handler receives the
//! per-connection principal and peer `SocketAddr` for the per-group `Describe`
//! ACL gate.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        describe_share_group_offsets_request::{
            DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsRequestTopic,
        },
        describe_share_group_offsets_response::{
            DescribeShareGroupOffsetsResponse, DescribeShareGroupOffsetsResponseGroup,
            DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    share_coordinator::coordinator::UNINITIALIZED_START_OFFSET,
};

#[tracing::instrument(
    name = "handle_describe_share_group_offsets",
    level = "info",
    skip_all,
    fields(api = "DescribeShareGroupOffsets", version, req_bytes = req_bytes.len()),
    err,
)]
// cargo-mutants: share-coordinator response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
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
        return crate::handlers::encode_response(&resp, version);
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
        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
            groups.push(DescribeShareGroupOffsetsResponseGroup {
                group_id: gid,
                error_code,
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

        let req_topics = requested_topics(group.topics, metadata.as_ref(), &image);
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
    crate::handlers::encode_response(&resp, version)
}

fn requested_topics(
    requested: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
    metadata: Option<
        &crate::coordinator::unified::share::persistence::ShareGroupStatePartitionMetadataValue,
    >,
    image: &crabka_metadata::MetadataImage,
) -> Vec<DescribeShareGroupOffsetsRequestTopic> {
    let Some(metadata) = metadata else {
        return requested.unwrap_or_default();
    };
    if let Some(topics) = requested {
        return topics;
    }
    let mut topics: Vec<_> = metadata
        .initialized
        .iter()
        .filter_map(|(topic_id, partitions)| {
            image.topic_name_by_id(topic_id).map(|topic_name| {
                DescribeShareGroupOffsetsRequestTopic {
                    topic_name: topic_name.into(),
                    partitions: partitions.clone(),
                    ..Default::default()
                }
            })
        })
        .collect();
    topics.sort_by(|a, b| a.topic_name.cmp(&b.topic_name));
    topics
}

/// Build one response topic. It resolves `name → id`, and an unknown name
/// gives per-partition `UNKNOWN_TOPIC_OR_PARTITION`. It enumerates the
/// initialized partitions when the request omits an explicit list. It then
/// builds one row per partition.
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
                start_offset: UNINITIALIZED_START_OFFSET,
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

/// Build one response partition. It reads the SPSO from the persister. It then
/// computes the best-effort lag (HWM − SPSO) and the leader epoch from the
/// local data partition when that partition is materialized here, and returns
/// `-1` for both otherwise.
async fn describe_partition(
    broker: &Broker,
    persister: &crate::share_coordinator::persister_client::SharePersister,
    gid: &str,
    topic_name: &str,
    topic_id: uuid::Uuid,
    p: i32,
) -> DescribeShareGroupOffsetsResponsePartition {
    let (start_offset, error_code) = match persister.read_state(gid, topic_id, p).await {
        Ok(Some(state)) => (state.start_offset.0, codes::NONE),
        Ok(None) => (UNINITIALIZED_START_OFFSET, codes::NONE),
        Err(_) => (UNINITIALIZED_START_OFFSET, codes::COORDINATOR_NOT_AVAILABLE),
    };
    let (leader_epoch, lag) = if let Some(part) = broker
        .partitions
        .get(topic_name, crabka_ids::PartitionIndex(p))
    {
        let hwm = part.high_watermark().await;
        let le = part
            .current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        let lag = if start_offset >= 0 {
            (hwm.0 - start_offset).max(0)
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

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use crabka_log::Offset;
    use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            describe_share_group_offsets_request::{
                DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
            },
            describe_share_group_offsets_response,
        },
    };
    use crabka_security::Principal;

    use super::*;
    use crate::{authorizer::Authorizer, test_support::DenyAll};

    type RequestTopic<'a> = (&'a str, Vec<i32>);
    type RequestGroup<'a> = (&'a str, Vec<RequestTopic<'a>>);

    fn request(groups: &[RequestGroup<'_>]) -> DescribeShareGroupOffsetsRequest {
        DescribeShareGroupOffsetsRequest {
            groups: groups
                .iter()
                .map(|(group_id, topics)| DescribeShareGroupOffsetsRequestGroup {
                    group_id: (*group_id).into(),
                    topics: Some(
                        topics
                            .iter()
                            .map(
                                |(topic_name, partitions)| DescribeShareGroupOffsetsRequestTopic {
                                    topic_name: (*topic_name).into(),
                                    partitions: partitions.clone(),
                                    ..Default::default()
                                },
                            )
                            .collect(),
                    ),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        DescribeShareGroupOffsetsRequest,
        DescribeShareGroupOffsetsResponse,
        version = describe_share_group_offsets_response::MAX_VERSION,
        client_id = "admin-client"
    );

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
        share_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = authorizer;
            cfg.share_group.enable = share_enabled;
        })
        .await
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    fn image_with_topic(name: &str, topic_id: uuid::Uuid) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id,
            partitions: 1,
            replication_factor: 1,
        }));
        image
    }

    #[tokio::test]
    async fn handle_error_scenarios_preserve_expected_rows() {
        type Case<'a> = (
            &'a str,
            Arc<dyn Authorizer>,
            bool,
            Vec<RequestGroup<'a>>,
            DescribeShareGroupOffsetsResponse,
        );
        let version = describe_share_group_offsets_response::MAX_VERSION;
        let cases: Vec<Case<'_>> = vec![
            (
                "disabled feature preserves group error rows",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                false,
                vec![("g1", vec![("t1", vec![0])]), ("g2", vec![("t2", vec![1])])],
                DescribeShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    groups: vec![
                        DescribeShareGroupOffsetsResponseGroup {
                            group_id: "g1".into(),
                            topics: Vec::new(),
                            error_code: codes::UNSUPPORTED_VERSION,
                            error_message: None,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        },
                        DescribeShareGroupOffsetsResponseGroup {
                            group_id: "g2".into(),
                            topics: Vec::new(),
                            error_code: codes::UNSUPPORTED_VERSION,
                            error_message: None,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        },
                    ],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
            (
                "denied group preserves group id and error code",
                Arc::new(DenyAll),
                true,
                vec![("g1", vec![("missing", vec![0])])],
                DescribeShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    groups: vec![DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g1".into(),
                        topics: Vec::new(),
                        error_code: codes::GROUP_AUTHORIZATION_FAILED,
                        error_message: None,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
            (
                "unknown topic preserves partition error rows",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                true,
                vec![("g1", vec![("missing-topic", vec![3, 5])])],
                DescribeShareGroupOffsetsResponse {
                    throttle_time_ms: 0,
                    groups: vec![DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g1".into(),
                        topics: vec![DescribeShareGroupOffsetsResponseTopic {
                            topic_name: "missing-topic".into(),
                            topic_id: Uuid::default(),
                            partitions: vec![
                                DescribeShareGroupOffsetsResponsePartition {
                                    partition_index: 3,
                                    start_offset: -1,
                                    leader_epoch: -1,
                                    lag: -1,
                                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                    error_message: None,
                                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                                },
                                DescribeShareGroupOffsetsResponsePartition {
                                    partition_index: 5,
                                    start_offset: -1,
                                    leader_epoch: -1,
                                    lag: -1,
                                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                    error_message: None,
                                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                                },
                            ],
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        }],
                        error_code: codes::NONE,
                        error_message: None,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ),
        ];
        for (case, authorizer, share_enabled, groups, expected) in cases {
            let (broker_handle, _dir) = start_broker(authorizer, share_enabled).await;
            let broker = broker_handle.broker_arc_for_test();
            let principal = principal();
            let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let ctx = test_context(&principal, &peer);
            let req_bytes = encode_request(&request(&groups));

            let resp = handle(&broker, version, 1, &req_bytes, &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&resp);

            assert!(resp == expected, "case: {case}");
            broker_handle.shutdown().await;
        }
    }

    #[tokio::test]
    async fn describe_topic_reads_persisted_partition_state() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let persister = broker
            .group_coordinator
            .share_persister()
            .cloned()
            .expect("share persister");
        let topic_id = uuid::Uuid::from_u128(0xD5C0);
        let image = image_with_topic("orders", topic_id);
        persister
            .initialize("g-desc", topic_id, 0, 1, Offset(33))
            .await
            .expect("seed state");

        let topic = describe_topic(
            &broker,
            &persister,
            &image,
            None,
            "g-desc",
            DescribeShareGroupOffsetsRequestTopic {
                topic_name: "orders".into(),
                partitions: vec![0],
                ..Default::default()
            },
        )
        .await;

        let expected = DescribeShareGroupOffsetsResponseTopic {
            topic_name: "orders".into(),
            topic_id: Uuid(*topic_id.as_bytes()),
            partitions: vec![DescribeShareGroupOffsetsResponsePartition {
                partition_index: 0,
                start_offset: 33,
                leader_epoch: -1,
                lag: -1,
                error_code: codes::NONE,
                error_message: None,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(topic == expected);
        broker_handle.shutdown().await;
    }

    #[test]
    fn null_topics_resolves_all_initialized_topic_partitions() {
        let alpha_id = uuid::Uuid::from_u128(1);
        let beta_id = uuid::Uuid::from_u128(2);
        let missing_id = uuid::Uuid::from_u128(3);
        let mut image = image_with_topic("beta", beta_id);
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "alpha".into(),
            topic_id: alpha_id,
            partitions: 2,
            replication_factor: 1,
        }));
        let metadata = crate::coordinator::unified::share::persistence::ShareGroupStatePartitionMetadataValue {
            initialized: vec![(beta_id, vec![0]), (missing_id, vec![7]), (alpha_id, vec![0, 1])],
            deleting: Vec::new(),
        };

        let topics = requested_topics(None, Some(&metadata), &image);

        assert!(
            topics
                .iter()
                .map(|topic| (topic.topic_name.as_str(), topic.partitions.as_slice()))
                .collect::<Vec<_>>()
                == vec![("alpha", &[0, 1][..]), ("beta", &[0][..])]
        );
    }
}
