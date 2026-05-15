//! `Metadata` (`api_key=3`). Returns all registered brokers and the
//! requested topics' (or all topics, if `topics: None`) partitions.
//! Metadata is sourced from `controller.current_image()` — the
//! quorum-replicated snapshot — rather than a local in-memory struct.

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};

use crabka_metadata::AclOperation;
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::{Decode, Encode};
use crabka_security::Principal;

use crate::authorizer::{AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

#[allow(clippy::too_many_lines)] // T12 ACL preamble + asymmetric loop
#[allow(clippy::unused_async)] // Handler is wholly sync but we keep the
// `async fn` shape so it mirrors the other inline-intercept handlers
// (produce/fetch/etc) and lets future Metadata work (e.g. waiting on
// topic creation) add `.await`s without changing the signature.
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    principal: &Principal,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();
    let inter_broker_name = broker.config.inter_broker_listener_name.clone();

    let mut cur: &[u8] = req_bytes;
    let req = MetadataRequest::decode(&mut cur, version)?;

    let image = controller.current_image();

    // ── slice-13 ACL preamble ────────────────────────────────────────
    // Metadata has asymmetric authorization semantics for `Describe`:
    //   • Named-topic request (`req.topics = Some([...])`): every
    //     requested topic appears in the response — Allow rows carry
    //     `error_code = 0`, Deny rows carry
    //     `error_code = TOPIC_AUTHORIZATION_FAILED (29)`.
    //   • Fetch-all (`req.topics = None`): only `Allow` topics appear
    //     in the response. Deny topics are silently omitted so the
    //     broker doesn't leak their existence to unauthorized clients.
    //
    // We collect candidate topic *names* up front (resolving topic_id
    // only when needed for v ≥ 10 named requests) and batch-authorize
    // them once.
    let named = req.topics.is_some();
    let candidate_topics: Vec<String> = match &req.topics {
        Some(list) => list
            .iter()
            .map(|t| {
                if let Some(n) = t.name.as_ref()
                    && !n.is_empty()
                {
                    n.clone()
                } else if t.topic_id != WireUuid::ZERO {
                    image
                        .topics()
                        .find(|tt| tt.topic_id.into_bytes() == t.topic_id.0)
                        .map(|tt| tt.name.clone())
                        .unwrap_or_default()
                } else {
                    String::new()
                }
            })
            .collect(),
        None => image.topics().map(|t| t.name.clone()).collect(),
    };
    let acl_by_name = authorize_topics(
        &image,
        &broker.config.super_users,
        principal,
        peer,
        AclOperation::Describe,
        candidate_topics.iter().map(String::as_str),
    );

    // Brokers: enumerate all registered nodes from the metadata image.
    let brokers: Vec<MetadataResponseBroker> = image
        .brokers()
        .map(|b| project_broker(b, &inter_broker_name))
        .collect();

    let mut topics_out: Vec<MetadataResponseTopic> = Vec::with_capacity(candidate_topics.len());
    for name in &candidate_topics {
        let allowed = acl_by_name
            .get(name.as_str())
            .copied()
            .unwrap_or(AuthorizationResult::Deny)
            == AuthorizationResult::Allow;
        if !allowed {
            if named {
                // Named-topic Deny: surface explicit auth-failed row so
                // the client knows the request was rejected. Don't
                // populate partitions / topic_id — we treat this as
                // "you may not look at this topic".
                topics_out.push(MetadataResponseTopic {
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    name: Some(name.clone()),
                    topic_id: WireUuid::ZERO,
                    ..Default::default()
                });
            }
            // Fetch-all Deny: silently omit (don't leak existence).
            continue;
        }

        match image.topic(name) {
            None => {
                // Allowed but unknown — surface UNKNOWN_TOPIC_OR_PARTITION.
                // This path is only reachable for `named` requests
                // (the fetch-all `candidate_topics` is sourced from
                // `image.topics()`, so every entry resolves).
                topics_out.push(MetadataResponseTopic {
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    name: Some(name.clone()),
                    topic_id: WireUuid::ZERO,
                    ..Default::default()
                });
            }
            Some(t) => {
                // Partitions are stored in a `HashMap`; sort by index so
                // clients (and tests) see a deterministic ordering.
                let mut sorted: Vec<_> = image.partitions_of(name).collect();
                sorted.sort_by_key(|p| p.partition);
                let partitions: Vec<MetadataResponsePartition> = sorted
                    .into_iter()
                    .map(|p| MetadataResponsePartition {
                        error_code: codes::NONE,
                        partition_index: p.partition,
                        leader_id: i32::try_from(p.leader).unwrap_or(i32::MAX),
                        leader_epoch: p.leader_epoch,
                        replica_nodes: p
                            .replicas
                            .iter()
                            .map(|&r| i32::try_from(r).unwrap_or(i32::MAX))
                            .collect(),
                        isr_nodes: p
                            .isr
                            .iter()
                            .map(|&r| i32::try_from(r).unwrap_or(i32::MAX))
                            .collect(),
                        ..Default::default()
                    })
                    .collect();
                topics_out.push(MetadataResponseTopic {
                    error_code: codes::NONE,
                    name: Some(name.clone()),
                    topic_id: WireUuid(t.topic_id.into_bytes()),
                    partitions,
                    is_internal: false,
                    ..Default::default()
                });
            }
        }
    }

    // controller_id: the current Raft leader, or -1 when unknown.
    let controller_id: i32 = controller
        .watch_leader()
        .borrow()
        .and_then(|id| i32::try_from(id).ok())
        .unwrap_or(-1);

    let resp = MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        cluster_id: Some(image.cluster_id().to_string()),
        controller_id,
        topics: topics_out,
        ..Default::default()
    };
    tracing::info!(
        version,
        req_topics = ?req.topics.as_ref().map(|ts| ts.iter().filter_map(|t| t.name.clone()).collect::<Vec<_>>()),
        resp_brokers = ?resp.brokers.iter().map(|b| format!("{}@{}:{}", b.node_id, b.host, b.port)).collect::<Vec<_>>(),
        resp_controller_id = resp.controller_id,
        resp_cluster_id = ?resp.cluster_id,
        resp_topics = ?resp.topics.iter().map(|t| format!("{}={:?}/p{}", t.name.as_deref().unwrap_or("?"), t.error_code, t.partitions.len())).collect::<Vec<_>>(),
        "metadata response"
    );
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Project a stored [`crabka_metadata::BrokerRegistrationRecord`] into a
/// single wire-format [`MetadataResponseBroker`].
///
/// The Kafka `MetadataResponse` wire format (v0..v12 at time of writing)
/// carries exactly one `host:port`/`rack` tuple per broker — there is no
/// `endpoints[]` array on `MetadataResponseBroker`. To honor the Task-11
/// per-listener registration we pick the broker's inter-broker endpoint
/// (matched by name) and fall back to the first recorded endpoint, then
/// to the legacy top-level `host`/`port` if `endpoints` is empty.
/// Clamps `node_id` to `i32::MAX` if the openraft `u64` overflows — broker
/// ids are tiny in practice so this is purely defensive.
fn project_broker(
    b: &crabka_metadata::BrokerRegistrationRecord,
    inter_broker_name: &str,
) -> MetadataResponseBroker {
    let primary = b
        .endpoints
        .iter()
        .find(|e| e.name == inter_broker_name)
        .or_else(|| b.endpoints.first());
    let (host, port) = match primary {
        Some(e) => (e.host.clone(), i32::from(e.port)),
        None => (b.host.clone(), i32::from(b.port)),
    };
    MetadataResponseBroker {
        node_id: i32::try_from(b.node_id).unwrap_or(i32::MAX),
        host,
        port,
        rack: b.rack.clone(),
        ..Default::default()
    }
}

fn parse_host_port(addr: &str) -> (String, i32) {
    if let Some((h, p)) = addr.rsplit_once(':')
        && let Ok(port) = p.parse::<u16>()
    {
        return (h.to_string(), i32::from(port));
    }
    tracing::warn!(
        addr,
        "advertised_listener not host:port; falling back to localhost:9092"
    );
    ("localhost".into(), 9092)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_host_port_ok() {
        assert_eq!(parse_host_port("foo:1234"), ("foo".into(), 1234));
    }

    #[test]
    fn parse_host_port_falls_back() {
        assert_eq!(parse_host_port("not-an-addr"), ("localhost".into(), 9092));
    }
}
