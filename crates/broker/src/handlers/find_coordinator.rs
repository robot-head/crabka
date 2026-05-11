//! `FindCoordinator` (`api_key=10`). Single-broker MVP: we are the
//! coordinator for every key. Returns this broker's
//! `(node_id, host, port)` in both the legacy single-coordinator fields
//! (v0-v3) and the per-key `coordinators` array (v4+).

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;
use crabka_protocol::owned::find_coordinator_response::{Coordinator, FindCoordinatorResponse};
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
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = FindCoordinatorRequest::decode(&mut cur, version)?;

        let (host, port) = parse_host_port(&advertised);
        let port_i32 = i32::from(port);

        // For v4+, requests carry `coordinator_keys`. For v0-v3 the single
        // `key` field is what the client cares about — populate the legacy
        // top-level fields and also emit a single `Coordinator` entry for
        // that key so the encode path is uniform.
        let keys: Vec<String> = if req.coordinator_keys.is_empty() {
            vec![req.key.clone()]
        } else {
            req.coordinator_keys.clone()
        };

        let coordinators: Vec<Coordinator> = keys
            .into_iter()
            .map(|k| Coordinator {
                key: k,
                node_id: broker_id,
                host: host.clone(),
                port: port_i32,
                error_code: codes::NONE,
                error_message: None,
                ..Default::default()
            })
            .collect();

        let resp = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            node_id: broker_id,
            host,
            port: port_i32,
            coordinators,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
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
