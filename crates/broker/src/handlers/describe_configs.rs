//! `DescribeConfigs` (`api_key=32`). MVP returns empty configs for every
//! requested resource — JVM admin clients tolerate an empty configs list
//! per resource and the broker has no tunable surface yet.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::describe_configs_request::DescribeConfigsRequest;
use crabka_protocol::owned::describe_configs_response::{
    DescribeConfigsResponse, DescribeConfigsResult,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    _broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DescribeConfigsRequest::decode(&mut cur, version)?;

        let results: Vec<DescribeConfigsResult> = req
            .resources
            .into_iter()
            .map(|r| DescribeConfigsResult {
                error_code: codes::NONE,
                error_message: None,
                resource_type: r.resource_type,
                resource_name: r.resource_name,
                configs: Vec::new(),
                ..Default::default()
            })
            .collect();

        let resp = DescribeConfigsResponse {
            throttle_time_ms: 0,
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
