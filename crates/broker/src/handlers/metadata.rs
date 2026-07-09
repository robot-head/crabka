//! `Metadata` (`api_key=3`). Returns all registered brokers and the
//! requested topics' (or all topics, if `topics: None`) partitions.
//! Metadata is sourced from `controller.current_image()` — the
//! quorum-replicated snapshot — rather than a local in-memory struct.

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

#[allow(clippy::too_many_lines)] // ACL preamble + asymmetric loop
#[allow(clippy::unused_async)]
// Handler is wholly sync but we keep the
// `async fn` shape so it mirrors the other inline-intercept handlers
// (produce/fetch/etc) and lets future Metadata work (e.g. waiting on
// topic creation) add `.await`s without changing the signature.
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
    let named = req.topics.is_some();
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

    // Brokers: enumerate all registered nodes from the metadata image.
    // Each broker's `host:port` is projected from the endpoint matching the
    // listener this request arrived on (Kafka returns the connection
    // listener's advertised address), falling back to the inter-broker
    // endpoint when the connection listener isn't recorded on that broker.
    let brokers: Vec<MetadataResponseBroker> = image
        .brokers()
        .map(|b| project_broker(b, ctx.connection_listener_name, &inter_broker_name))
        .collect();

    let allowed = |name: &str| {
        acl_by_name
            .get(name)
            .copied()
            .unwrap_or(AuthorizationResult::Deny)
            == AuthorizationResult::Allow
    };
    // Build a fully-populated success row for a known topic by name.
    let success_row = |name: &str, rec: &crabka_metadata::TopicRecord| {
        // `partitions_of` yields ascending partition-index order, so clients
        // (and tests) see a deterministic ordering.
        let partitions: Vec<MetadataResponsePartition> = image
            .partitions_of(name)
            .map(|p| MetadataResponsePartition {
                error_code: codes::NONE,
                partition_index: p.partition,
                leader_id: i32::try_from(p.leader.0).unwrap_or(i32::MAX),
                leader_epoch: p.leader_epoch.0,
                replica_nodes: p
                    .replicas
                    .iter()
                    .map(|&r| i32::try_from(r.0).unwrap_or(i32::MAX))
                    .collect(),
                isr_nodes: p
                    .isr
                    .iter()
                    .map(|&r| i32::try_from(r.0).unwrap_or(i32::MAX))
                    .collect(),
                ..Default::default()
            })
            .collect();
        // KIP-430: per-topic bitfield, only when the client opted in.
        // Schema gates the field on version (v8+) so the value is
        // harmlessly dropped on the wire below v8.
        let topic_authorized_operations = if req.include_topic_authorized_operations {
            authorized_operations_bits(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
                ResourceType::Topic,
                name,
            )
        } else {
            i32::MIN
        };
        MetadataResponseTopic {
            error_code: codes::NONE,
            name: Some(rec.name.clone()),
            topic_id: WireUuid(rec.topic_id.into_bytes()),
            partitions,
            is_internal: false,
            topic_authorized_operations,
            ..Default::default()
        }
    };

    let mut topics_out: Vec<MetadataResponseTopic> = Vec::with_capacity(candidate_topics.len());
    if named {
        // Named request: drive off the per-entry resolution outcome so
        // KIP-516 topic-id errors echo the requested id rather than
        // collapsing to an empty name.
        for (t, outcome) in &resolved {
            match outcome {
                Ok(rec) => {
                    if !allowed(&rec.name) {
                        // Named-topic Deny: surface explicit auth-failed row.
                        topics_out.push(MetadataResponseTopic {
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                            name: Some(rec.name.clone()),
                            topic_id: WireUuid::ZERO,
                            ..Default::default()
                        });
                        continue;
                    }
                    topics_out.push(success_row(&rec.name, rec));
                }
                Err(code) if *code == codes::UNKNOWN_TOPIC_OR_PARTITION => {
                    // Name-only miss. Preserve the existing behavior: a
                    // Deny on the requested name yields an auth-failed row
                    // (don't reveal whether the topic exists); otherwise
                    // surface UNKNOWN_TOPIC_OR_PARTITION.
                    let name_str = t.name.as_deref().unwrap_or("");
                    if !name_str.is_empty() && !allowed(name_str) {
                        topics_out.push(MetadataResponseTopic {
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                            name: t.name.clone(),
                            topic_id: WireUuid::ZERO,
                            ..Default::default()
                        });
                        continue;
                    }
                    topics_out.push(MetadataResponseTopic {
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        name: t.name.clone(),
                        topic_id: t.topic_id,
                        ..Default::default()
                    });
                }
                Err(code) => {
                    // KIP-516: UNKNOWN_TOPIC_ID / INCONSISTENT_TOPIC_ID.
                    // Echo the requested name (may be `None`) and id.
                    topics_out.push(MetadataResponseTopic {
                        error_code: *code,
                        name: t.name.clone(),
                        topic_id: t.topic_id,
                        ..Default::default()
                    });
                }
            }
        }
    } else {
        // Fetch-all: only `Allow` topics appear; Deny topics are silently
        // omitted so the broker doesn't leak their existence.
        for name in &candidate_topics {
            if !allowed(name) {
                continue;
            }
            if let Some(rec) = image.topic(name) {
                topics_out.push(success_row(name, rec));
            }
        }
    }

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

/// Project a stored [`crabka_metadata::BrokerRegistrationRecord`] into a
/// single wire-format [`MetadataResponseBroker`].
///
/// The Kafka `MetadataResponse` wire format (v0..v12 at time of writing)
/// carries exactly one `host:port`/`rack` tuple per broker — there is no
/// `endpoints[]` array on `MetadataResponseBroker`. Apache Kafka returns the
/// advertised address **of the listener the request arrived on**, so a TLS
/// client gets the TLS endpoint and a plaintext client gets the plaintext
/// endpoint. We honor that by selecting, in order:
///   1. the endpoint whose name matches the connection's listener
///      (`connection_listener_name`) — the correct, Kafka-faithful choice;
///   2. the inter-broker endpoint (matched by name) — defensive fallback
///      when the connection listener isn't recorded on this broker (e.g. a
///      heterogeneous-listener cluster);
///   3. the first recorded endpoint;
///   4. the legacy top-level `host`/`port` when `endpoints` is empty.
///
/// Clamps `node_id` to `i32::MAX` if the openraft `u64` overflows — broker
/// ids are tiny in practice so this is purely defensive.
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

/// Select the `(host, port)` to advertise for a registered broker given the
/// listener the request arrived on. Shared by every handler that projects a
/// broker address into a wire response (`Metadata`, `DescribeCluster`) so
/// they all honor the connection listener identically. Selection order:
///   1. the endpoint whose name matches `connection_listener_name`
///      (Kafka returns the connection listener's advertised address);
///   2. the inter-broker endpoint (matched by name);
///   3. the first recorded endpoint;
///   4. the legacy top-level `host`/`port` when `endpoints` is empty.
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

fn parse_host_port(addr: &str) -> (String, i32) {
    let (host, port) = crate::handlers::parse_advertised_host_port(addr);
    (host, i32::from(port))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn parse_host_port_ok() {
        assert!(parse_host_port("foo:1234") == ("foo".into(), 1234));
    }

    #[test]
    fn parse_host_port_falls_back() {
        assert!(parse_host_port("not-an-addr") == ("localhost".into(), 9092));
    }

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
            endpoints,
        }
    }

    /// The connection-listener endpoint wins when present: a request that
    /// arrived on the `"tls"` listener gets the tls endpoint's host:port,
    /// even though `"plain"` is the inter-broker listener.
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

    /// A plaintext client on the `"plain"` listener gets the plain endpoint
    /// (regression guard for the pre-fix behaviour).
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

    /// When the connection listener isn't registered on the broker, fall back
    /// to the inter-broker endpoint (preserves the previous behaviour).
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

    /// When neither the connection listener nor the inter-broker listener are
    /// present, fall back to the first recorded endpoint.
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

    /// With no endpoints at all, fall back to the legacy top-level host/port.
    #[test]
    fn project_broker_falls_back_to_legacy_host_port() {
        let rec = record(vec![]);
        let out = project_broker(&rec, "tls", "plain");
        assert!(out.host == "legacy-host");
        assert!(out.port == 1000);
    }
}
