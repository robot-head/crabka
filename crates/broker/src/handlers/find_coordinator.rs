//! `FindCoordinator` (`api_key=10`). Supports:
//!   - `key_type=0` (GROUP): returns this broker as coordinator for every
//!     group key (single-broker MVP).
//!   - `key_type=1` (TRANSACTION): ensures `__transaction_state` exists,
//!     hashes the transaction-id to a partition, resolves the leader, and
//!     returns that broker's address.
//!
//! Response fields are populated in both the legacy single-coordinator form
//! (v0-v3) and the per-key `coordinators` array (v4+).

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;
use crabka_protocol::owned::find_coordinator_response::{Coordinator, FindCoordinatorResponse};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

const KEY_TYPE_GROUP: i8 = 0;
const KEY_TYPE_TRANSACTION: i8 = 1;

#[allow(clippy::too_many_lines)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let broker_id = broker.config.broker_id;
    let node_id = broker.config.node_id;
    let advertised = broker.config.advertised_listener.clone();
    let controller = Arc::clone(&broker.controller);
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = FindCoordinatorRequest::decode(&mut cur, version)?;

        // For v4+, requests carry `coordinator_keys`. For v0-v3 the single
        // `key` field is what the client cares about — populate the legacy
        // top-level fields and also emit a single `Coordinator` entry for
        // that key so the encode path is uniform.
        let keys: Vec<String> = if req.coordinator_keys.is_empty() {
            vec![req.key.clone()]
        } else {
            req.coordinator_keys.clone()
        };

        let coordinators: Vec<Coordinator> = match req.key_type {
            KEY_TYPE_GROUP => {
                let (host, port) = parse_host_port(&advertised);
                let port_i32 = i32::from(port);
                keys.into_iter()
                    .map(|k| Coordinator {
                        key: k,
                        node_id: broker_id,
                        host: host.clone(),
                        port: port_i32,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    })
                    .collect()
            }
            KEY_TYPE_TRANSACTION => {
                // Ensure __transaction_state topic exists before we try to
                // look up partitions in it.
                if let Err(e) =
                    crate::txn::bootstrap::ensure_topic(&controller).await
                {
                    tracing::warn!(
                        error = %e,
                        "txn bootstrap failed; replying COORDINATOR_NOT_AVAILABLE"
                    );
                    return encode_error_response(
                        broker_id,
                        &advertised,
                        version,
                        codes::COORDINATOR_NOT_AVAILABLE,
                        Some("txn topic bootstrap failed"),
                    );
                }

                let mut result = Vec::with_capacity(keys.len());
                for k in keys {
                    let p = crate::txn::partitioner::partition_for_tid(
                        &k,
                        crate::txn::bootstrap::NUM_PARTITIONS,
                    );
                    let image = controller.current_image();
                    let Some(pr) = image.partition(crate::txn::bootstrap::TOPIC, p)
                    else {
                        result.push(Coordinator {
                            key: k,
                            node_id: -1,
                            host: String::new(),
                            port: -1,
                            error_code: codes::COORDINATOR_NOT_AVAILABLE,
                            error_message: Some("partition not found".into()),
                            ..Default::default()
                        });
                        continue;
                    };
                    let leader = pr.leader;
                    let Some(broker_info) = image.broker(leader) else {
                        result.push(Coordinator {
                            key: k,
                            node_id: -1,
                            host: String::new(),
                            port: -1,
                            error_code: codes::COORDINATOR_NOT_AVAILABLE,
                            error_message: Some("leader broker not registered".into()),
                            ..Default::default()
                        });
                        continue;
                    };
                    let node_id_i32 = i32::try_from(leader).unwrap_or(-1);
                    // Prefer our own `advertised_listener` when the leader is
                    // this broker: the metadata record may carry the pre-bind
                    // port (0) in test setups where the OS assigns the port.
                    let (host, port_i32) = if leader == node_id {
                        let (h, p) = parse_host_port(&advertised);
                        (h, i32::from(p))
                    } else {
                        (broker_info.host.clone(), i32::from(broker_info.port))
                    };
                    result.push(Coordinator {
                        key: k,
                        node_id: node_id_i32,
                        host,
                        port: port_i32,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    });
                }
                result
            }
            unknown => {
                tracing::warn!(key_type = unknown, "unknown FindCoordinator key_type");
                let (host, port) = parse_host_port(&advertised);
                let port_i32 = i32::from(port);
                keys.into_iter()
                    .map(|k| Coordinator {
                        key: k,
                        node_id: broker_id,
                        host: host.clone(),
                        port: port_i32,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    })
                    .collect()
            }
        };

        // Derive the legacy top-level fields from the first coordinator in
        // the list (matches Apache Kafka v0-v3 behaviour).
        let (top_node_id, top_host, top_port, top_error) =
            if let Some(first) = coordinators.first() {
                (
                    first.node_id,
                    first.host.clone(),
                    first.port,
                    first.error_code,
                )
            } else {
                let (host, port) = parse_host_port(&advertised);
                (broker_id, host, i32::from(port), codes::NONE)
            };

        let resp = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: top_error,
            error_message: None,
            node_id: top_node_id,
            host: top_host,
            port: top_port,
            coordinators,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

/// Build an error response (all coordinators carry the given error code).
/// Used when a top-level failure (e.g. bootstrap) prevents per-key lookup.
fn encode_error_response(
    broker_id: i32,
    advertised: &str,
    version: i16,
    error_code: i16,
    error_message: Option<&str>,
) -> Result<Bytes, BrokerError> {
    let (host, port) = parse_host_port(advertised);
    let resp = FindCoordinatorResponse {
        throttle_time_ms: 0,
        error_code,
        error_message: error_message.map(str::to_owned),
        node_id: broker_id,
        host,
        port: i32::from(port),
        coordinators: vec![],
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn parse_host_port(addr: &str) -> (String, u16) {
    if let Some((h, p)) = addr.rsplit_once(':')
        && let Ok(port) = p.parse::<u16>()
    {
        return (h.to_string(), port);
    }
    tracing::warn!(
        addr,
        "advertised_listener not host:port; falling back to localhost:9092"
    );
    ("localhost".into(), 9092)
}
