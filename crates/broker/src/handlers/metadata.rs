//! `Metadata` (`api_key=3`). It returns available brokers and the partitions
//! of the requested topics, or of all topics when `topics` is `None`.
//!
//! The metadata comes from `controller.current_image()`, the
//! quorum-replicated snapshot, and not from a local in-memory struct.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        metadata_request::MetadataRequest,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::authorized_operations::authorized_operations_bits,
};

// ACL preamble + asymmetric loop.
#[tracing::instrument(
    name = "handle_metadata",
    level = "info",
    skip_all,
    fields(api = "Metadata", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();
    let inter_broker_name = broker.config.inter_broker_listener_name.clone();

    let mut cur: &[u8] = req_bytes;
    let req = MetadataRequest::decode(&mut cur, version)?;

    let image = controller.current_image();

    // ── ACL preamble ────────────────────────────────────────
    // Metadata has asymmetric authorization semantics for `Describe`:
    //   • Named-topic request (`req.topics = Some([...])`): every
    //     requested topic appears in the response — Allow rows carry
    //     `error_code = 0`, Deny rows carry
    //     `error_code = TOPIC_AUTHORIZATION_FAILED (29)`.
    //   • Fetch-all (`req.topics = None`): only `Allow` topics appear
    //     in the response. Deny topics are silently omitted so the
    //     broker doesn't leak their existence to unauthorized clients.
    //
    // For a named request we resolve each requested `(name, topic_id)`
    // pair up front via the KIP-516 strict resolver, carrying the outcome
    // (`Ok(record)` or an error wire code) per request entry so the
    // response loop below can echo errors without collapsing an unknown
    // id to an empty name. The set of names we authorize is sourced from
    // the *resolved* records (plus the requested name for the
    // name-only-miss case), so a topic requested by id is still
    // ACL-checked under its real name.
    let resolved: Vec<(
        &crabka_protocol::owned::metadata_request::MetadataRequestTopic,
        Result<&crabka_metadata::TopicRecord, i16>,
    )> = match &req.topics {
        Some(list) => list
            .iter()
            .map(|t| {
                let name_str = t.name.as_deref().unwrap_or("");
                (
                    t,
                    crate::topic_resolve::resolve(&image, name_str, t.topic_id),
                )
            })
            .collect(),
        None => Vec::new(),
    };
    // Names to batch-authorize: resolved records' real names, plus the
    // requested name for a name-only miss (so the UNKNOWN_TOPIC_OR_PARTITION
    // row still respects Deny → omit / auth-failed semantics). Topic-id
    // errors carry no trustworthy name and are surfaced unconditionally.
    let candidate_topics: Vec<String> = match &req.topics {
        Some(_) => resolved
            .iter()
            .filter_map(|(t, r)| match r {
                Ok(rec) => Some(rec.name.clone()),
                Err(code) if *code == codes::UNKNOWN_TOPIC_OR_PARTITION => {
                    t.name.clone().filter(|n| !n.is_empty())
                }
                Err(_) => None,
            })
            .collect(),
        None => image.topics().map(|t| t.name.clone()).collect(),
    };
    let acl_by_name = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Describe,
        candidate_topics.iter().map(String::as_str),
    );

    // The controller's heartbeat registry is the authoritative source for
    // broker availability. Unknown entries remain eligible so a fresh
    // controller does not briefly advertise an empty cluster before seeding
    // the registry. Non-controller brokers do not own authoritative liveness
    // state and therefore retain the image-only projection.
    let is_controller = *controller.watch_leader().borrow() == Some(broker.config.node_id);
    let unavailable = if is_controller {
        broker.liveness.unavailable_snapshot().await
    } else {
        std::collections::HashSet::new()
    };

    // Brokers: enumerate available registered nodes from the metadata image.
    // Each broker's `host:port` is projected from the endpoint matching the
    // listener this request arrived on (Kafka returns the connection
    // listener's advertised address), falling back to the inter-broker
    // endpoint when the connection listener isn't recorded on that broker.
    let brokers = project_available_brokers(
        &image,
        &unavailable,
        ctx.connection_listener_name,
        &inter_broker_name,
    );

    let topics_out = build_topic_rows(
        broker,
        &image,
        ctx,
        &req,
        &resolved,
        &candidate_topics,
        &acl_by_name,
    );

    // controller_id: the current Raft leader, or -1 when unknown.
    let controller_id: i32 = controller
        .watch_leader()
        .borrow()
        .and_then(|id| i32::try_from(id.0).ok())
        .unwrap_or(-1);

    // KIP-430: the cluster-level field only exists on the wire for v8-10;
    // the codegen drops it on other versions. Compute when the opt-in
    // flag is set so the response carries the value on the in-range
    // versions, leaving the default `i32::MIN` otherwise.
    let cluster_authorized_operations = if req.include_cluster_authorized_operations {
        authorized_operations_bits(
            broker.config.authorizer.as_ref(),
            &image,
            ctx.principal,
            ctx.peer,
            ResourceType::Cluster,
            "kafka-cluster",
        )
    } else {
        i32::MIN
    };

    let resp = MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        cluster_id: Some(image.cluster_id().to_string()),
        controller_id,
        topics: topics_out,
        cluster_authorized_operations,
        ..Default::default()
    };
    tracing::debug!(
        version,
        req_topics = ?req.topics.as_ref().map(|ts| ts.iter().filter_map(|t| t.name.clone()).collect::<Vec<_>>()),
        resp_brokers = ?resp.brokers.iter().map(|b| format!("{}@{}:{}", b.node_id, b.host, b.port)).collect::<Vec<_>>(),
        resp_controller_id = resp.controller_id,
        resp_cluster_id = ?resp.cluster_id,
        resp_topics = ?resp.topics.iter().map(|t| format!("{}={:?}/p{}", t.name.as_deref().unwrap_or("?"), t.error_code, t.partitions.len())).collect::<Vec<_>>(),
        "metadata response"
    );
    crate::handlers::encode_response(&resp, version)
}

type ResolvedTopic<'a> = (
    &'a crabka_protocol::owned::metadata_request::MetadataRequestTopic,
    Result<&'a crabka_metadata::TopicRecord, i16>,
);

fn build_topic_rows(
    broker: &Broker,
    image: &crabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    request: &MetadataRequest,
    resolved: &[ResolvedTopic<'_>],
    candidates: &[String],
    authorization: &std::collections::HashMap<&str, AuthorizationResult>,
) -> Vec<MetadataResponseTopic> {
    let allowed = |name: &str| {
        authorization
            .get(name)
            .copied()
            .unwrap_or(AuthorizationResult::Deny)
            == AuthorizationResult::Allow
    };
    if request.topics.is_none() {
        return candidates
            .iter()
            .filter(|name| allowed(name))
            .filter_map(|name| {
                image
                    .topic(name)
                    .map(|record| success_topic_row(broker, image, context, request, name, record))
            })
            .collect();
    }
    resolved
        .iter()
        .map(|(topic, outcome)| match outcome {
            Ok(record) if allowed(&record.name) => {
                success_topic_row(broker, image, context, request, &record.name, record)
            }
            Ok(record) => MetadataResponseTopic {
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                name: Some(record.name.clone()),
                topic_id: WireUuid::ZERO,
                ..Default::default()
            },
            Err(code) if *code == codes::UNKNOWN_TOPIC_OR_PARTITION => {
                let name = topic.name.as_deref().unwrap_or("");
                MetadataResponseTopic {
                    error_code: if !name.is_empty() && !allowed(name) {
                        codes::TOPIC_AUTHORIZATION_FAILED
                    } else {
                        codes::UNKNOWN_TOPIC_OR_PARTITION
                    },
                    name: topic.name.clone(),
                    topic_id: topic.topic_id,
                    ..Default::default()
                }
            }
            Err(code) => MetadataResponseTopic {
                error_code: *code,
                name: topic.name.clone(),
                topic_id: topic.topic_id,
                ..Default::default()
            },
        })
        .collect()
}

fn success_topic_row(
    broker: &Broker,
    image: &crabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    request: &MetadataRequest,
    name: &str,
    record: &crabka_metadata::TopicRecord,
) -> MetadataResponseTopic {
    let partitions = image
        .partitions_of(name)
        .map(|partition| MetadataResponsePartition {
            error_code: codes::NONE,
            partition_index: partition.partition,
            leader_id: i32::try_from(partition.leader.0).unwrap_or(i32::MAX),
            leader_epoch: partition.leader_epoch.0,
            replica_nodes: partition
                .replicas
                .iter()
                .map(|replica| i32::try_from(replica.0).unwrap_or(i32::MAX))
                .collect(),
            isr_nodes: partition
                .isr
                .iter()
                .map(|replica| i32::try_from(replica.0).unwrap_or(i32::MAX))
                .collect(),
            ..Default::default()
        })
        .collect();
    let topic_authorized_operations = if request.include_topic_authorized_operations {
        authorized_operations_bits(
            broker.config.authorizer.as_ref(),
            image,
            context.principal,
            context.peer,
            ResourceType::Topic,
            name,
        )
    } else {
        i32::MIN
    };
    MetadataResponseTopic {
        error_code: codes::NONE,
        name: Some(record.name.clone()),
        topic_id: WireUuid(record.topic_id.into_bytes()),
        partitions,
        is_internal: false,
        topic_authorized_operations,
        ..Default::default()
    }
}

/// Projects a stored [`crabka_metadata::BrokerRegistrationRecord`] into one
/// wire-format [`MetadataResponseBroker`].
///
/// The Kafka `MetadataResponse` wire format, v0 to v12 at the time of writing,
/// carries exactly one `host:port` and `rack` tuple per broker.
/// `MetadataResponseBroker` has no `endpoints[]` array. Apache Kafka returns
/// the advertised address **of the listener that the request arrived on**, so
/// a TLS client gets the TLS endpoint and a plaintext client gets the
/// plaintext endpoint. This function follows that rule and selects, in order:
///   1. the endpoint whose name matches the connection's listener
///      (`connection_listener_name`), which is the correct, Kafka-faithful
///      choice;
///   2. the inter-broker endpoint, matched by name, as a defensive fallback
///      when this broker has no record of the connection listener, for example
///      in a cluster with heterogeneous listeners;
///   3. the first recorded endpoint;
///   4. the legacy top-level `host` and `port` when `endpoints` is empty.
///
/// This function clamps `node_id` to `i32::MAX` when the openraft `u64`
/// overflows. Broker ids are small in practice, so that clamp is purely
/// defensive.
fn project_broker(
    b: &crabka_metadata::BrokerRegistrationRecord,
    connection_listener_name: &str,
    inter_broker_name: &str,
) -> MetadataResponseBroker {
    let (host, port) = pick_endpoint_host_port(b, connection_listener_name, inter_broker_name);
    MetadataResponseBroker {
        node_id: i32::try_from(b.node_id.0).unwrap_or(i32::MAX),
        host,
        port,
        rack: b.rack.clone(),
        ..Default::default()
    }
}

/// Selects the `(host, port)` to advertise for a registered broker, from the
/// listener that the request arrived on.
///
/// Every handler that projects a broker address into a wire response, such as
/// `Metadata` and `DescribeCluster`, shares this function, so they all treat
/// the connection listener the same way. The selection order is:
///   1. the endpoint whose name matches `connection_listener_name`, because
///      Kafka returns the connection listener's advertised address;
///   2. the inter-broker endpoint, matched by name;
///   3. the first recorded endpoint;
///   4. the legacy top-level `host` and `port` when `endpoints` is empty.
pub(crate) fn pick_endpoint_host_port(
    b: &crabka_metadata::BrokerRegistrationRecord,
    connection_listener_name: &str,
    inter_broker_name: &str,
) -> (String, i32) {
    let primary = b
        .endpoints
        .iter()
        .find(|e| e.name == connection_listener_name)
        .or_else(|| b.endpoints.iter().find(|e| e.name == inter_broker_name))
        .or_else(|| b.endpoints.first());
    match primary {
        Some(e) => (e.host.clone(), i32::from(e.port)),
        None => (b.host.clone(), i32::from(b.port)),
    }
}

fn project_available_brokers(
    image: &crabka_metadata::MetadataImage,
    unavailable: &std::collections::HashSet<u64>,
    connection_listener_name: &str,
    inter_broker_name: &str,
) -> Vec<MetadataResponseBroker> {
    image
        .brokers()
        .filter(|broker| !unavailable.contains(&broker.node_id.0))
        .map(|broker| project_broker(broker, connection_listener_name, inter_broker_name))
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn endpoint(name: &str, host: &str, port: u16) -> crabka_metadata::BrokerEndpoint {
        crabka_metadata::BrokerEndpoint {
            name: name.to_string(),
            host: host.to_string(),
            port,
            protocol: crabka_security::ListenerProtocol::Plaintext,
        }
    }

    fn record(
        endpoints: Vec<crabka_metadata::BrokerEndpoint>,
    ) -> crabka_metadata::BrokerRegistrationRecord {
        crabka_metadata::BrokerRegistrationRecord {
            node_id: crabka_metadata::NodeId(7),
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::nil(),
            host: "legacy-host".to_string(),
            port: 1000,
            rack: Some("rack-a".to_string()),
            log_dirs: vec![],
            endpoints,
            features: std::collections::BTreeMap::new(),
        }
    }

    /// The connection-listener endpoint wins when it is present. A request
    /// that arrived on the `"tls"` listener gets the tls endpoint's host and
    /// port, even though `"plain"` is the inter-broker listener.
    #[test]
    fn project_broker_picks_connection_listener_endpoint() {
        let rec = record(vec![
            endpoint("plain", "plain-host", 9092),
            endpoint("tls", "tls-host", 9094),
        ]);
        let out = project_broker(&rec, "tls", "plain");
        let expected = MetadataResponseBroker {
            node_id: 7,
            host: "tls-host".to_string(),
            port: 9094,
            rack: Some("rack-a".to_string()),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(out == expected);
    }

    /// A plaintext client on the `"plain"` listener gets the plain endpoint.
    /// This is a regression guard against the behaviour before the fix.
    #[test]
    fn project_broker_picks_plain_for_plain_connection() {
        let rec = record(vec![
            endpoint("plain", "plain-host", 9092),
            endpoint("tls", "tls-host", 9094),
        ]);
        let out = project_broker(&rec, "plain", "plain");
        assert!(out.host == "plain-host");
        assert!(out.port == 9092);
    }

    /// When the broker has no record of the connection listener, it falls back
    /// to the inter-broker endpoint. That keeps the previous behaviour.
    #[test]
    fn project_broker_falls_back_to_inter_broker() {
        let rec = record(vec![
            endpoint("plain", "plain-host", 9092),
            endpoint("tls", "tls-host", 9094),
        ]);
        let out = project_broker(&rec, "external", "plain");
        assert!(out.host == "plain-host");
        assert!(out.port == 9092);
    }

    /// When neither the connection listener nor the inter-broker listener is
    /// present, the broker falls back to the first recorded endpoint.
    #[test]
    fn project_broker_falls_back_to_first_endpoint() {
        let rec = record(vec![
            endpoint("other-a", "host-a", 5000),
            endpoint("other-b", "host-b", 5001),
        ]);
        let out = project_broker(&rec, "tls", "plain");
        assert!(out.host == "host-a");
        assert!(out.port == 5000);
    }

    /// With no endpoint at all, the broker falls back to the legacy top-level
    /// host and port.
    #[test]
    fn project_broker_falls_back_to_legacy_host_port() {
        let rec = record(vec![]);
        let out = project_broker(&rec, "tls", "plain");
        assert!(out.host == "legacy-host");
        assert!(out.port == 1000);
    }

    #[test]
    fn unavailable_brokers_are_not_advertised() {
        let mut image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for node_id in [7, 8] {
            let mut broker = record(vec![]);
            broker.node_id = crabka_metadata::NodeId(node_id);
            image.apply(&crabka_metadata::MetadataRecord::V1BrokerRegistration(
                broker,
            ));
        }

        let projected = project_available_brokers(
            &image,
            &std::collections::HashSet::from([8]),
            "plain",
            "plain",
        );

        assert!(
            projected
                .iter()
                .map(|broker| broker.node_id)
                .collect::<Vec<_>>()
                == vec![7]
        );
    }
}
