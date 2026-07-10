//! `ListConfigResources` (`api_key=74`). KIP-1142 enumeration of every
//! resource an admin client can target with `DescribeConfigs` /
//! `AlterConfigs`. The same RPC at v0 was historically called
//! `ListClientMetricsResources` (KIP-714) and returned only client-metrics
//! subscriptions; v1 generalises it with a `resource_types` filter.
//!
//! Crabka surfaces three resource types — `TOPIC` (2), `BROKER` (4), and
//! `CLIENT_METRICS` (16). `BROKER_LOGGER` (8) and `GROUP` (32) are
//! intentionally omitted: dynamic per-broker `log4j`-style loggers aren't a
//! concept here (the broker reads `RUST_LOG` from its `ConfigMap`),
//! and Crabka has no group-config knobs today. Clients filtering for those
//! types get an empty list — the same surface the JVM client expects when
//! the broker doesn't recognise the type.
//!
//! `CLIENT_METRICS` enumerates configured subscription names from the
//! metadata image (see `MetadataImage::client_metrics_subscriptions`).

use bytes::Bytes;
use crabka_metadata::AclOperation;
use crabka_protocol::{
    Decode,
    owned::{
        list_config_resources_request::ListConfigResourcesRequest,
        list_config_resources_response::{ConfigResource, ListConfigResourcesResponse},
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
};

/// Kafka wire-level resource-type ids. Match the JVM
/// `org.apache.kafka.common.config.ConfigResource.Type` enum.
const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;

/// Default set returned when v1 callers omit `resource_types` (KIP-1142).
/// Mirrors the JVM admin client's expectation: every supported type the
/// broker can describe configs for.
const DEFAULT_RESOURCE_TYPES: [i8; 3] = [
    RESOURCE_TYPE_TOPIC,
    RESOURCE_TYPE_BROKER,
    RESOURCE_TYPE_CLIENT_METRICS,
];

#[allow(clippy::unused_async)]
#[tracing::instrument(
    name = "handle_list_config_resources",
    level = "info",
    skip_all,
    fields(api = "ListConfigResources", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let image = broker.controller.current_image();

    let mut cur: &[u8] = req_bytes;
    let req = ListConfigResourcesRequest::decode(&mut cur, version)?;

    // Whole-request Cluster Describe gate. Mirrors DescribeCluster /
    // DescribeConfigs: ListConfigResources is a cluster-wide enumeration,
    // so the same ACL gates it.
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Describe,
        },
    );
    if allow == AuthorizationResult::Deny {
        let resp = ListConfigResourcesResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };
        return crate::handlers::encode_response(&resp, version);
    }

    let resources = collect_resources(&image, version, &req.resource_types);

    let resp = ListConfigResourcesResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        config_resources: resources,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

/// Resolve the effective filter (v0 → client-metrics-only; v1 with empty
/// list → default set; v1 with explicit types → caller's filter) and
/// enumerate each requested type from the image. Pure — testable without
/// a broker. The output is sorted by `(resource_type, resource_name)` so
/// the wire payload is deterministic regardless of underlying iteration
/// order in `MetadataImage`.
fn collect_resources(
    image: &crabka_metadata::MetadataImage,
    version: i16,
    requested: &[i8],
) -> Vec<ConfigResource> {
    let types: &[i8] = if version < 1 {
        // v0 (legacy ListClientMetricsResources): always client metrics.
        &[RESOURCE_TYPE_CLIENT_METRICS]
    } else if requested.is_empty() {
        &DEFAULT_RESOURCE_TYPES
    } else {
        requested
    };

    let mut out: Vec<ConfigResource> = Vec::new();
    for &rt in types {
        match rt {
            RESOURCE_TYPE_TOPIC => {
                for t in image.topics() {
                    out.push(ConfigResource {
                        resource_name: t.name.clone(),
                        resource_type: RESOURCE_TYPE_TOPIC,
                        ..Default::default()
                    });
                }
            }
            RESOURCE_TYPE_BROKER => {
                for b in image.brokers() {
                    out.push(ConfigResource {
                        resource_name: b.node_id.to_string(),
                        resource_type: RESOURCE_TYPE_BROKER,
                        ..Default::default()
                    });
                }
            }
            RESOURCE_TYPE_CLIENT_METRICS => {
                for (name, _cfgs) in image.client_metrics_subscriptions() {
                    out.push(ConfigResource {
                        resource_name: name.clone(),
                        resource_type: RESOURCE_TYPE_CLIENT_METRICS,
                        ..Default::default()
                    });
                }
            }
            // Unknown types (BROKER_LOGGER, GROUP, anything new) silently drop.
            _ => {}
        }
    }

    out.sort_by(|a, b| {
        a.resource_type
            .cmp(&b.resource_type)
            .then_with(|| a.resource_name.cmp(&b.resource_name))
    });
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord};
    use crabka_protocol::UnknownTaggedFields;
    use uuid::Uuid;

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 1;

    fn image_with_topics_and_brokers(topics: &[&str], broker_ids: &[u64]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        for t in topics {
            img.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: (*t).to_string(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            }));
        }
        for &id in broker_ids {
            img.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: crabka_audit::NodeId(id),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                    endpoints: vec![],
                },
            ));
        }
        img
    }

    crate::test_support::wire_helpers!(
        ListConfigResourcesRequest,
        ListConfigResourcesResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    async fn seed_topic(handle: &BrokerHandle, name: &str) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            })])
            .await
            .expect("seed topic");
    }

    fn image_with_subs(names: &[&str]) -> MetadataImage {
        use crabka_metadata::ClientMetricsConfigRecord;
        let mut img = MetadataImage::new(Uuid::nil());
        for n in names {
            let mut cfgs = std::collections::BTreeMap::new();
            cfgs.insert("interval.ms".to_string(), "60000".to_string());
            img.apply(&MetadataRecord::V1ClientMetricsConfig(
                ClientMetricsConfigRecord {
                    name: (*n).into(),
                    configs: cfgs,
                },
            ));
        }
        img
    }

    #[test]
    fn v0_returns_client_metrics_subscriptions() {
        let img = image_with_subs(&["sub-b", "sub-a"]);
        let out = collect_resources(&img, 0, &[]);
        assert_eq!(out.len(), 2);
        assert!(
            out.iter()
                .all(|r| r.resource_type == RESOURCE_TYPE_CLIENT_METRICS)
        );
        assert_eq!(out[0].resource_name, "sub-a"); // sorted
        assert_eq!(out[1].resource_name, "sub-b");
    }

    #[test]
    fn v1_client_metrics_filter_returns_subscriptions() {
        let img = image_with_subs(&["sub-a"]);
        let out = collect_resources(&img, 1, &[RESOURCE_TYPE_CLIENT_METRICS]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].resource_type, RESOURCE_TYPE_CLIENT_METRICS);
        assert_eq!(out[0].resource_name, "sub-a");
    }

    #[test]
    fn v1_empty_filter_returns_default_set() {
        let img = image_with_topics_and_brokers(&["t-a", "t-b"], &[1, 2]);
        let out = collect_resources(&img, 1, &[]);
        // 2 topics + 2 brokers + 0 client_metrics = 4 entries, sorted by
        // (type, name): topics (type 2) before brokers (type 4).
        let expected = vec![
            ConfigResource {
                resource_name: "t-a".to_string(),
                resource_type: RESOURCE_TYPE_TOPIC,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            ConfigResource {
                resource_name: "t-b".to_string(),
                resource_type: RESOURCE_TYPE_TOPIC,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            ConfigResource {
                resource_name: "1".to_string(),
                resource_type: RESOURCE_TYPE_BROKER,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            ConfigResource {
                resource_name: "2".to_string(),
                resource_type: RESOURCE_TYPE_BROKER,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(out == expected);
    }

    #[test]
    fn v1_explicit_topic_only_filter_skips_brokers() {
        let img = image_with_topics_and_brokers(&["t-a"], &[1, 2]);
        let out = collect_resources(&img, 1, &[RESOURCE_TYPE_TOPIC]);
        let expected = vec![ConfigResource {
            resource_name: "t-a".to_string(),
            resource_type: RESOURCE_TYPE_TOPIC,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }];
        assert!(out == expected);
    }

    #[test]
    fn v1_explicit_broker_only_filter_skips_topics() {
        let img = image_with_topics_and_brokers(&["t-a", "t-b"], &[5, 7]);
        let out = collect_resources(&img, 1, &[RESOURCE_TYPE_BROKER]);
        let expected = vec![
            ConfigResource {
                resource_name: "5".to_string(),
                resource_type: RESOURCE_TYPE_BROKER,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            ConfigResource {
                resource_name: "7".to_string(),
                resource_type: RESOURCE_TYPE_BROKER,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(out == expected);
    }

    #[test]
    fn v1_unknown_resource_type_returns_empty() {
        let img = image_with_topics_and_brokers(&["t-a"], &[1]);
        // type 8 = BROKER_LOGGER, type 32 = GROUP — neither supported.
        let out = collect_resources(&img, 1, &[8, 32]);
        assert!(
            out.is_empty(),
            "unsupported resource types silently drop; got {out:?}"
        );
    }

    #[test]
    fn v1_mixed_supported_and_unsupported_returns_only_supported() {
        let img = image_with_topics_and_brokers(&["t-a"], &[1]);
        let out = collect_resources(
            &img,
            1,
            &[
                RESOURCE_TYPE_TOPIC,
                8, // BROKER_LOGGER — unsupported, dropped
                RESOURCE_TYPE_BROKER,
            ],
        );
        let expected = vec![
            ConfigResource {
                resource_name: "t-a".to_string(),
                resource_type: RESOURCE_TYPE_TOPIC,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            ConfigResource {
                resource_name: "1".to_string(),
                resource_type: RESOURCE_TYPE_BROKER,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(out == expected);
    }

    #[test]
    fn output_is_deterministic_regardless_of_input_order() {
        let img = image_with_topics_and_brokers(&["z-late", "a-early"], &[10, 1]);
        let out = collect_resources(&img, 1, &[]);
        // Topics sorted lexicographically, brokers sorted lexicographically
        // by id-as-string.
        let names: Vec<&str> = out.iter().map(|r| r.resource_name.as_str()).collect();
        assert!(names == vec!["a-early", "z-late", "1", "10"]);
    }

    #[tokio::test]
    async fn denied_handler_response_preserves_error_fields() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let req = encode_request(&ListConfigResourcesRequest {
            resource_types: vec![RESOURCE_TYPE_TOPIC],
            ..Default::default()
        });

        let bytes = handle(&broker, VERSION, 123, &req, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&bytes);

        let expected = ListConfigResourcesResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            config_resources: vec![],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn successful_handler_response_preserves_resource_fields() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic(&broker_handle, "orders").await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let req = encode_request(&ListConfigResourcesRequest {
            resource_types: vec![RESOURCE_TYPE_TOPIC],
            ..Default::default()
        });

        let bytes = handle(&broker, VERSION, 123, &req, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&bytes);

        let expected = ListConfigResourcesResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            config_resources: vec![
                ConfigResource {
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: "__consumer_offsets".into(),
                    ..Default::default()
                },
                ConfigResource {
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: "orders".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
