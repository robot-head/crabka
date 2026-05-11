//! `Metadata` (`api_key=3`). Returns this broker (always one entry) and
//! the requested topics' (or all topics, if `topics: None`) partitions.

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
    let broker_id = broker.config.broker_id;
    let advertised = broker.config.advertised_listener.clone();
    let metadata = broker.metadata.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = MetadataRequest::decode(&mut cur, version)?;

        // Parse "host:port" → (host, port). If parse fails, fall back to
        // ("localhost", 9092) and log.
        let (host, port) = parse_host_port(&advertised);

        let brokers = vec![MetadataResponseBroker {
            node_id: broker_id,
            host,
            port,
            rack: None,
            ..Default::default()
        }];

        let meta = metadata.read().expect("metadata poisoned");
        let topic_names: Vec<String> = match &req.topics {
            None => meta.topic_names(),
            Some(topics) => topics.iter().filter_map(|t| t.name.clone()).collect(),
        };

        let mut topics_out: Vec<MetadataResponseTopic> = Vec::with_capacity(topic_names.len());
        for name in topic_names {
            match meta.get(&name) {
                None => {
                    topics_out.push(MetadataResponseTopic {
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        name: Some(name),
                        topic_id: WireUuid::ZERO,
                        ..Default::default()
                    });
                }
                Some(t) => {
                    let partitions = t
                        .partitions
                        .iter()
                        .map(|p| MetadataResponsePartition {
                            error_code: codes::NONE,
                            partition_index: p.partition_id,
                            leader_id: p.leader_broker_id,
                            replica_nodes: p.replicas.clone(),
                            isr_nodes: p.isr.clone(),
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

        let resp = MetadataResponse {
            throttle_time_ms: 0,
            brokers,
            cluster_id: Some(format!("crabka-{broker_id}")),
            controller_id: broker_id,
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
