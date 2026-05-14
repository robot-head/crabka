//! `DescribeCluster` (`api_key=60`). Pure projection over the metadata
//! image. The `cluster_authorized_operations` field is set to
//! `i32::MIN` (Apache Kafka's "not present" sentinel) because this
//! slice doesn't implement authorization.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::describe_cluster_response::{
    DescribeClusterBroker, DescribeClusterResponse,
};
use crabka_protocol::Encode;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    _req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let controller = broker.controller.clone();

    Box::pin(async move {
        let image = controller.current_image();
        let controller_id = controller
            .watch_leader()
            .borrow()
            .map_or(-1, |n| i32::try_from(n).unwrap_or(-1));

        let brokers: Vec<DescribeClusterBroker> = image
            .brokers()
            .map(|b| DescribeClusterBroker {
                broker_id: i32::try_from(b.node_id).unwrap_or(-1),
                host: b.host.clone(),
                port: i32::from(b.port),
                rack: b.rack.clone(),
                ..Default::default()
            })
            .collect();

        let resp = DescribeClusterResponse {
            error_code: codes::NONE,
            error_message: None,
            cluster_id: image.cluster_id().to_string(),
            controller_id,
            brokers,
            cluster_authorized_operations: i32::MIN,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
