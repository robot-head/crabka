//! `Metadata` (`api_key=3`). Returns all registered brokers and the
//! requested topics' (or all topics, if `topics: None`) partitions.
//! Metadata is sourced from `controller.current_image()` — the
//! quorum-replicated snapshot — rather than a local in-memory struct.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    let inter_broker_name = broker.config.inter_broker_listener_name.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = MetadataRequest::decode(&mut cur, version)?;

        let image = controller.current_image();

        // Brokers: enumerate all registered nodes from the metadata image.
        let brokers: Vec<MetadataResponseBroker> = image
            .brokers()
            .map(|b| project_broker(b, &inter_broker_name))
            .collect();

        // Topic names to include: all (None) or the explicitly requested set.
        let topic_names: Vec<String> = match &req.topics {
            None => image.topics().map(|t| t.name.clone()).collect(),
            Some(topics) => topics.iter().filter_map(|t| t.name.clone()).collect(),
        };

        let mut topics_out: Vec<MetadataResponseTopic> = Vec::with_capacity(topic_names.len());
        for name in topic_names {
            match image.topic(&name) {
                None => {
                    topics_out.push(MetadataResponseTopic {
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        name: Some(name),
                        topic_id: WireUuid::ZERO,
                        ..Default::default()
                    });
                }
                Some(t) => {
                    // Partitions are stored in a `HashMap`; sort by index so
                    // clients (and tests) see a deterministic ordering.
                    let mut sorted: Vec<_> = image.partitions_of(&name).collect();
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
                        name: Some(name),
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
    })
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
